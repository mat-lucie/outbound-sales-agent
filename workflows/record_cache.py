"""RecordCache and bulk preload helper for Attio person record lookups.

# PR-14 (B-PD-006)

Pre-PR-14 `get()` returned the literal string `"Unknown"` for the name
and company fields when the Attio record wasn't found. The literal
string was a §0 #9 silent fallback — downstream consumers that
compared against `== "Unknown"` (cli.py, weekly_report.py,
email_campaign.py) had to defensive-string-compare, and a typo on
either side broke the contract silently.

PR-14 returns `None` instead. Consumers now use `is None` /
`is not None`, which the type system enforces (tuple type changed
from `tuple[str, str, str, str, str]` to
`tuple[str | None, str | None, str | None, str, str]` —
linkedin_url stays empty-string for backward-compat with the URL
truthy checks in pre_invite_check). All call sites updated in this
PR.
"""

from typing import TYPE_CHECKING, Any

import click

from clients.crm.attio_provider import AttioProvider
from clients.crm.base import CRMProvider
from workflows.metrics import phase_timer, record_phase_or_skip
from workflows.run_evidence import observed

if TYPE_CHECKING:
    from clients.attio import AttioClient

# Tuple shape: (name, company, linkedin_url, industry, title).
# - name / company / industry can be None (record missing OR field absent)
# - linkedin_url stays "" for backward-compat with `if linkedin_url:` checks
# - title stays "" because Attio always returns a string (possibly empty)
RecordInfo = tuple[
    str | None,  # name
    str | None,  # company
    str,         # linkedin_url
    str | None,  # industry
    str,         # title
]


class RecordCache:
    """Cache for Attio person record lookups to avoid N+1 API calls."""

    def __init__(self, attio: "CRMProvider | AttioClient"):
        """Construct a cache over a CRM provider.

        Union shim (mirrors :class:`clients.attio_writer.AttioWriter`):
        the param stays NAMED ``attio`` because callers pass it
        positionally. A ``CRMProvider`` is used directly; a raw
        ``AttioClient`` (or a test double mocking its methods) is wrapped
        in an :class:`AttioProvider` so reads route through the
        vendor-neutral contract while the inner-client methods still fire
        at the layer the tests target. This lets ``daily_check`` thread a
        ``CRMProvider`` while the unmigrated callers
        (``detect_responses``, ``scripts/``) keep passing a raw client.
        """
        if isinstance(attio, CRMProvider):
            self._crm: CRMProvider = attio
        else:
            self._crm = AttioProvider(attio)
        self._cache: dict[str, RecordInfo] = {}

    def get(self, record_id: str) -> RecordInfo:
        """Get (name, company, linkedin_url, industry, title) for a record, cached.

        PR-14 (B-PD-006): name/company/industry are None when the
        record isn't found OR when the underlying Attio field is
        unset. Pre-PR-14 returned the literal string "Unknown",
        which silently coerced through downstream string comparisons
        and let consumers ship messages with "Unknown" as a literal
        placeholder. Consumers MUST use `is None` / `is not None`.
        """
        if record_id not in self._cache:
            record = self._crm.get_person(record_id)
            if record is not None:
                info = self._crm.extract_person_info(record)
                self._cache[record_id] = (
                    info.name,
                    info.company,
                    info.linkedin_url,
                    info.industry,
                    info.title,
                )
            else:
                self._cache[record_id] = (None, None, "", None, "")
        return self._cache[record_id]

    def prime(self, record_id: str, info: RecordInfo) -> None:
        """Pre-populate the cache without an API call. Used by bulk preload."""
        self._cache[record_id] = info


@observed("crm_preload", "phase")
def preload_pipeline_persons(
    attio: "CRMProvider | AttioClient",
    cache: RecordCache,
    record_ids: set[str],
    *,
    metrics: Any = None,
) -> int:
    """Bulk-fetch person records for `record_ids` and prime `cache`.

    Backed by parallel `get_person` calls (8 concurrent). A per-record
    failure is logged + counted (when `metrics` is supplied) and
    skipped — phases fall back to per-record GETs via RecordCache.get
    for any uncached id.

    Accepts the same ``CRMProvider | AttioClient`` union as
    :class:`RecordCache` — a raw client is wrapped locally in an
    :class:`AttioProvider` so reads route through the contract.
    """
    if not record_ids:
        return 0
    crm: CRMProvider = attio if isinstance(attio, CRMProvider) else AttioProvider(attio)
    _t = phase_timer()
    try:
        records = crm.bulk_fetch_persons(record_ids, metrics=metrics)
    except Exception as e:
        click.echo(f"  Warning: bulk preload failed ({e}); falling back to per-record fetches.")
        return 0
    finally:
        record_phase_or_skip(metrics, "preload_person_fetch_parallel", _t)
    # Pre-warm the linked companies through the provider's bounded pool, so
    # the per-person resolve loop below reads them from cache instead of one
    # blocking company GET per first-seen company (the dominant cost of a
    # large preload). Optional optimization: providers without a company
    # lookup no-op, and any company left unwarmed resolves lazily as before.
    _t = phase_timer()
    try:
        crm.prefetch_companies_for_persons(records.values(), metrics=metrics)
    except Exception as e:
        # Fail-open (the prefetch is an optimization; the resolve loop
        # still resolves every company) — but LOUD in metrics: a total
        # prefetch collapse means the run silently regresses to the slow
        # serial path, so it must reach the end-of-run summary, not just
        # scrollback.
        msg = (
            f"bulk company prefetch failed "
            f"({type(e).__name__}: {e}); "
            f"falling back to serial company fetches"
        )
        if metrics is not None:
            metrics.warn(msg)
        click.echo(f"  Warning: {msg}.")
    finally:
        record_phase_or_skip(metrics, "preload_company_fetch_parallel", _t)
    primed = 0
    failures = 0
    # Residual serial bucket: cache hits after the prefetch above, plus a
    # lazy blocking GET for any company the prefetch failed to warm.
    _t = phase_timer()
    for rid, record in records.items():
        try:
            info = crm.extract_person_info(record)
            cache.prime(
                rid,
                (info.name, info.company, info.linkedin_url, info.industry, info.title),
            )
            primed += 1
        except Exception:
            failures += 1
    record_phase_or_skip(metrics, "preload_company_resolve_serial", _t)
    if failures:
        click.echo(
            f"  Warning: {failures} record(s) failed to prime "
            f"(transient errors during company resolve); these will lazy-fetch."
        )
    return primed
