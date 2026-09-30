"""Attio CRM API v2 client."""

import json
import logging
import os
import random
import re
import sys
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import unquote

import httpx

from models.pipeline import STAGE_RANK, PipelineStage
from models.resolution import LANGUAGE_OVERRIDE_READ_FAILED
from workflows.run_evidence import observed, timed_call

logger = logging.getLogger(__name__)


# request_with_retry backoff shape: min(BASE * 2^attempt, MAX) scaled by a
# uniform jitter factor in [0.5, 1.5) — so an individual sleep can reach
# 1.5 × _RETRY_MAX_WAIT. Jitter survives the cap on purpose (no thundering herd
# of synchronized retries against an already-struggling Attio).
_RETRY_BASE_WAIT = 1.0
_RETRY_MAX_WAIT = 30.0

# Full deal-sweep ceiling (PR-220). The 500 default on ``search_deals``
# silently drops the tail once the deal count passes it; the Follow-up Radar
# and weekly-report deal-recency join sweep the WHOLE deals object, so they
# pass ``limit=DEAL_FULL_SCAN_LIMIT, fail_if_truncated=True`` — hitting this
# ceiling raises ``AttioResultTruncated`` instead of returning a short set.
DEAL_FULL_SCAN_LIMIT = 10_000


def request_with_retry(
    attio: "AttioClient",
    method: str,
    path: str,
    *,
    attempts: int = 5,
    recheck=None,
    **kwargs,
) -> dict:
    """Retry safe operations; reconcile ambiguous creates once without replay.

    A recheck can return a confirmed landed record. A miss or failed probe
    does not prove absence (the original write may still be committing),
    so ambiguity propagates instead of authorizing another POST.
    """
    if attempts < 1:
        raise ValueError(f"attempts must be >= 1, got {attempts}")
    for attempt in range(attempts):
        if attempt and recheck is not None:
            found = recheck()
            if found is not None:
                return found
        try:
            return attio._request(method, path, **kwargs)
        except (AmbiguousAttioWrite, httpx.HTTPStatusError, httpx.TransportError) as exc:
            if isinstance(exc, AmbiguousAttioWrite) or _ambiguous_failure(method, path, exc):
                ambiguous = exc if isinstance(exc, AmbiguousAttioWrite) else AmbiguousAttioWrite(method, path)
                if recheck is not None:
                    try:
                        found = recheck()
                    except Exception as probe_error:
                        raise ambiguous from probe_error
                    if found is not None:
                        return found
                if isinstance(exc, AmbiguousAttioWrite):
                    raise
                raise ambiguous from exc
            if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code < 500:
                raise
            if attempt == attempts - 1:
                raise
            status = type(exc).__name__
        wait = min(_RETRY_BASE_WAIT * (2 ** attempt), _RETRY_MAX_WAIT) * (
            0.5 + random.random()
        )
        logger.warning(
            "request_with_retry: %s %s failed with %s "
            "(attempt %d/%d) — retrying in %.1fs",
            method, path, status, attempt + 1, attempts, wait,
        )
        time.sleep(wait)
    raise AssertionError("unreachable: loop always returns or raises")


class AttioResultTruncated(RuntimeError):
    """A paginated query hit its ``limit`` with records (possibly) remaining.

    Raised only when the caller passed ``fail_if_truncated=True`` — full-sweep
    callers prefer a loud failure over a silently incomplete result set
    (PR-234: suppression sweeps, per-stage campaign queries, and list-scan
    exports must not drop the tail past their fetch ceiling).
    """


class AmbiguousAttioWrite(RuntimeError):
    """A write may have committed; reconcile before issuing another create."""

    def __init__(self, method: str, path: str):
        super().__init__(
            f"ambiguous Attio write: {method} {path}; outcome unknown. "
            "Reconcile the existing record before retrying; no automatic replay."
        )


def _retry_safe(method: str, path: str) -> bool:
    # Attio query endpoints are POST reads. Do not infer safety from retry_500.
    return method.upper() in {"GET", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"} or (
        method.upper() == "POST"
        and re.fullmatch(r"/(?:objects/[^/]+/records|lists/[^/]+/entries)/query", path) is not None
    )


def _ambiguous_failure(method: str, path: str, exc: Exception) -> bool:
    if _retry_safe(method, path):
        return False
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, httpx.TransportError) and not isinstance(
        exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)
    )


def _is_stage_regression(prior_stage: str, new_stage: str) -> bool:
    """True iff ``new_stage`` ranks strictly below ``prior_stage``.

    Defense-in-depth backstop for the weekly re-stamp cadence-desync class: a
    caller PATCHing an existing list entry to a LOWER stage (e.g. Accepted over
    DM3 Sent) would wipe cadence depth. Unknown/garbage stage strings fall back
    to rank 0 (mirrors the ``_filter_and_rank_entries_for_record`` defensive
    fallback), so a malformed prior never reads as "advanced" and a malformed
    new stage is treated as the lowest rank.

    NOTE on the CRM-provider boundary: this client is the vendor layer, and it
    is mapping-unaware by design — the ``AttioProvider`` translates canonical
    ``PipelineStage`` values to vendor option labels BEFORE handing them here.
    Under the shipped IDENTITY ``stage_mapping`` (vendor label == canonical
    value) both the incoming ``new_stage`` and the stored ``prior_stage`` parse
    cleanly via ``PipelineStage(...)`` and the rank comparison is exact. Under a
    NON-identity mapping the stored/incoming strings are vendor labels that
    ``PipelineStage(...)`` cannot parse, so both fall to rank 0 and this backstop
    safely NO-OPs (it never strips a legitimate advance, only fails to catch a
    regression). The comparison is internally consistent either way — both sides
    come from the same label space — so the guard can never misfire and corrupt
    a forward write; the monotonicity gate at the writer boundary remains the
    primary defense for non-identity deployments.
    """
    def _rank(stage_str: str) -> int:
        try:
            return STAGE_RANK[PipelineStage(stage_str)]
        except (KeyError, ValueError):
            return 0

    return _rank(new_stage) < _rank(prior_stage)


# Substrings that mark a company record as carrying LinkedIn's own Clearbit
# enrichment instead of the prospect's real employer. Matches subdomains
# (br.linkedin.com, etc.) without admitting "linkedin.com.example.co".
_LINKEDIN_DOMAIN_TOKENS: tuple[str, ...] = (
    "linkedin.com",
)
# A company record whose `linkedin` field points at LinkedIn's own company
# page is a tell that Clearbit resolved `linkedin.com` itself.
_LINKEDIN_SELF_COMPANY_URL = "linkedin.com/company/linkedin"


def is_linkedin_clearbit_corrupted(
    company_record: dict,
    field_slug: Callable[[str], str] | None = None,
) -> bool:
    """Detect the LinkedIn-Clearbit corruption fingerprint on a company record.

    Pre-2026-04-11 ingestion treated PhantomBuster's
    `companyUrl=https://linkedin.com/company/<slug>` as a literal company
    domain (`linkedin.com`). Clearbit then enriched that domain with
    LinkedIn's own profile data, while the `name` field was overwritten
    from PB's `companyName`. The resulting record looks like a real
    employer by `name` but carries LinkedIn's clearbit payload everywhere
    else — and personalising a DM with `[Company]` ships the real-looking
    fake name.

    Two signals, ORed:
        - `domains` contains a value matching `linkedin.com` (or a subdomain)
        - `linkedin` field points at `linkedin.com/company/linkedin`

    The real LinkedIn record (name = "LinkedIn") is excluded so this guard
    never false-positives on it. We don't ship DMs to LinkedIn itself.

    ``field_slug`` resolves the two CONFIGURABLE engine fields this guard reads
    to the workspace's vendor slugs: the company web-domain (``company_domain``)
    and the LinkedIn-profile field (``linkedin_url``). It defaults to the
    identity resolver, so a direct call (the unit tests) reads the canonical
    Attio slugs ``domains`` / ``linkedin`` byte-identically to pre-seam.
    ``extract_record_info`` passes the client's ``_field_slug`` so an operator's
    renamed slugs are honored. The company-``name`` read stays the LITERAL
    ``"name"`` — there is no documented field_mapping key for company name
    (``full_name`` is the PERSON name), so routing it would invent a mapping.
    """
    if field_slug is None:
        # Default resolver = the canonical Attio slugs, so a direct call reads
        # "domains" / "linkedin" exactly as the pre-seam code did (byte-identical).
        # Use the SAME forgiving .get(field, field) idiom as AttioClient._field_slug
        # (not a strict __getitem__): an unmapped engine field passes through
        # unchanged, so a future read added here can't KeyError on this default
        # path while the injected (production) path stays green and masks it.
        _canonical = {"company_domain": "domains", "linkedin_url": "linkedin"}
        resolve = lambda f: _canonical.get(f, f)  # noqa: E731
    else:
        resolve = field_slug
    domains_slug = resolve("company_domain")
    linkedin_slug = resolve("linkedin_url")
    values = company_record.get("values", {})

    name_data = values.get("name") or []
    name_value = ""
    if name_data:
        first = name_data[0]
        name_value = (first.get("value", "") or "") if isinstance(first, dict) else str(first)
    if name_value.strip().lower() == "linkedin":
        return False

    for entry in values.get(domains_slug) or []:
        domain = entry.get("domain", "") if isinstance(entry, dict) else str(entry)
        domain_l = domain.lower()
        for token in _LINKEDIN_DOMAIN_TOKENS:
            if domain_l == token or domain_l.endswith("." + token):
                return True

    for entry in values.get(linkedin_slug) or []:
        url = entry.get("value", "") if isinstance(entry, dict) else str(entry)
        if _LINKEDIN_SELF_COMPANY_URL in url.lower():
            return True

    return False


def _canonical_linkedin_url(url: str) -> str:
    """Return the canonical form of a LinkedIn profile URL.

    Attio's text-field filter is exact-string match: `iñigo-marchal` and
    `i%C3%B1igo-marchal` are *not* the same, nor are `www.linkedin.com` and
    `linkedin.com`, nor are trailing-slash variants. Without normalization,
    every time PB emits a subtly different URL form for the same profile we
    fail the upsert's duplicate check and create a new record.

    Canonical form: URL-decoded, no `www.` prefix, no trailing slash,
    lowercase scheme. LinkedIn slugs themselves are case-insensitive, so we
    also lowercase the path.
    """
    if not url:
        return ""
    decoded = unquote(url).strip()
    # Lowercase scheme but preserve the rest until we reach the path
    if "://" in decoded:
        scheme, rest = decoded.split("://", 1)
        decoded = f"{scheme.lower()}://{rest}"
    decoded = decoded.replace("://www.", "://")
    decoded = decoded.rstrip("/")
    return decoded.lower()


def _vanity_url_slug(url: str) -> str:
    """Extract the vanity slug from a LinkedIn profile URL.

    Given 'https://linkedin.com/in/mateo-lt-12345' returns 'mateo-lt-12345'.
    Given 'https://linkedin.com/in/mateo-lt-12345/' returns 'mateo-lt-12345'.
    Non-/in/ URLs (company pages, etc.) return ''.

    Uses the canonical form so the slug is always lowercase and URL-decoded.
    Returns '' when the URL is empty, malformed, or not a profile URL.
    """
    if not url:
        return ""
    canonical = _canonical_linkedin_url(url)
    # Expect: scheme://linkedin.com/in/<slug>
    # or: linkedin.com/in/<slug> (no scheme)
    for prefix in (
        "https://linkedin.com/in/",
        "http://linkedin.com/in/",
        "linkedin.com/in/",
    ):
        if canonical.startswith(prefix):
            slug = canonical[len(prefix):].strip("/")
            slug = slug.split("?", 1)[0]
            return slug
    return ""


# LinkedIn's auto-generated /in/ slugs end with a numeric member-id suffix
# (e.g. `dana-q-70481235`), and slug VARIANTS for the same person keep that
# suffix while the name portion drifts (`dana-quiroga-ramos-mba-70481235` —
# the shape of the upstream 2026-08-18 cadence-leak incident). >=6 digits so a
# short user-chosen tail like `john-smith-2` is never mistaken for a member id.
_LINKEDIN_PROFILE_ID_RE = re.compile(r"-(\d{6,})$")


def linkedin_profile_id(url: str) -> str:
    """Extract the trailing numeric profile-id from a /in/ slug, or ''.

    Two slug variants of the same profile share this suffix, so it is a
    stronger identity key than the full slug. Returns '' for slugs without a
    >=6-digit numeric tail (custom vanity URLs), non-/in/ URLs, and empty
    input — callers must fall back to full-URL comparison then.
    """
    slug = _vanity_url_slug(url)
    if not slug:
        return ""
    # `_vanity_url_slug` strips a plain trailing `?query`, but a `/?query`,
    # `#fragment`, or `/sub-path` suffix survives into the slug and would
    # defeat the $-anchored regex — the id must come from the first path
    # segment only.
    slug = slug.split("?", 1)[0].split("#", 1)[0].split("/", 1)[0]
    match = _LINKEDIN_PROFILE_ID_RE.search(slug)
    return match.group(1) if match else ""


def linkedin_identity_key(url: str) -> str:
    """Identity key for LinkedIn-URL dedup: profile-id when present, else
    the canonical URL.

    Two URLs whose slugs share the numeric profile-id suffix map to the same
    key even when the name portion of the slug differs — closing the
    exact-string-dedup gap that let a weekly ingest re-prospect a
    DM3-complete person under a shortened slug variant. URLs without a
    numeric suffix compare by canonical form, identical to the old behavior.

    The `li-id:` prefix keeps id-keys disjoint from URL-shaped keys so a mixed
    set can hold both without collision.
    """
    profile_id = linkedin_profile_id(url)
    if profile_id:
        return f"li-id:{profile_id}"
    return _canonical_linkedin_url(url)


def linkedin_identity_map(urls) -> dict[str, str]:
    """Map `li-id:<profile-id>` identity key → the input URL, for READ-side
    match-back bridging.

    Scraper CSVs can echo a profile under its CURRENT slug (the
    `linkedinProfileUrl` column) rather than the slug we queried, so an
    exact-string match-back misses when the person renamed their vanity URL
    between our ingest and the scrape. Callers keep the exact match primary
    and consult this map only on a miss: the CSV URL's identity key resolves
    back to OUR url form, so downstream lookups keyed on our form still hit.

    Only URLs whose slug carries a numeric profile-id participate (vanity
    slugs without one can't be bridged safely). A profile-id shared by two
    DIFFERENT input URLs is dropped from the map — a bridge that could pick
    the wrong sibling is worse than no bridge; those fall back to exact
    matching only.
    """
    mapping: dict[str, str] = {}
    ambiguous: set[str] = set()
    for url in urls:
        key = linkedin_identity_key(url)
        if not key.startswith("li-id:"):
            continue
        if key in mapping and mapping[key] != url:
            ambiguous.add(key)
        else:
            mapping[key] = url
    for key in ambiguous:
        del mapping[key]
    return mapping


def resolve_identity_match(
    norm: str, exact_set: set[str], by_id: dict[str, str]
) -> str:
    """Match a scraped/echoed URL back to one of our requested URLs.

    Exact match stays primary: if `norm` is already one of ours, return it
    unchanged. Only on an exact miss do we bridge via the numeric profile-id
    (`by_id`, from `linkedin_identity_map`), which catches a slug-variant echo
    — the scraper reporting a profile under its CURRENT slug rather than the
    one we queried. Returns "" when neither matches, so callers can
    `if not matched: continue`.

    `norm` must already be normalized by the CALLER's normalizer, and
    `exact_set` / `by_id` must be built from that SAME normalizer, so the
    returned value is always in the caller's own key space.
    """
    if norm in exact_set:
        return norm
    return by_id.get(linkedin_identity_key(norm), "")


def _linkedin_url_variants(url: str) -> list[str]:
    """Return every variant of a LinkedIn URL that Attio may have stored.

    Attio's exact-string filter plus historical PB inconsistency means the
    same profile can live under several forms. Try the canonical form first,
    then fall back to with-www and with-trailing-slash variants. Also try
    the raw input in case it was stored URL-encoded.
    """
    if not url:
        return []
    canonical = _canonical_linkedin_url(url)
    variants: list[str] = [canonical]
    if "://" in canonical:
        scheme, rest = canonical.split("://", 1)
        with_www = f"{scheme}://www.{rest}"
        variants.append(with_www)
        variants.append(with_www + "/")
    variants.append(canonical + "/")
    raw = url.strip()
    if raw and raw not in variants:
        variants.append(raw)
    # De-duplicate, preserve order
    seen: set[str] = set()
    out: list[str] = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def person_record_linkedin_url(
    record: dict, field_slug: Callable[[str], str] | None = None
) -> str:
    """Extract the stored LinkedIn URL string from a raw person record.

    Attio returns the LinkedIn text attribute as a list of value dicts;
    returns '' when the attribute is empty or the record has no values.

    ``field_slug`` resolves the configurable ``linkedin_url`` engine field to
    the workspace's vendor slug (the same seam
    ``is_linkedin_clearbit_corrupted`` uses); it defaults to the canonical
    Attio slug ``linkedin``.
    """
    slug = field_slug("linkedin_url") if field_slug else "linkedin"
    items = (record.get("values") or {}).get(slug) or []
    if not items:
        return ""
    first = items[0]
    return str(first.get("value", "") if isinstance(first, dict) else first)


def first_option_title(raw) -> str:
    """Extract the first option title from a CRM select-attribute value list.

    Canonical parser for record select attrs shaped
    ``[{"option": {"title": "Food & Beverage"}}]`` (e.g. ``industry_vertical``,
    ``industry_vertical_status``). Returns ``""`` for None/empty/malformed
    values. (PR-225)
    """
    if raw and isinstance(raw, list) and isinstance(raw[0], dict):
        opt = raw[0].get("option") or {}
        if isinstance(opt, dict):
            return str(opt.get("title", "") or "")
    return ""


def first_text_value(raw) -> str:
    """Extract the first plain value from a CRM text-attribute value list.

    Companion to ``first_option_title`` for attrs shaped
    ``[{"value": "Acme makes widgets"}]`` (e.g. ``description``). Returns
    ``""`` for None/empty/malformed values, and tolerates the bare-scalar
    shape some fixtures use.
    """
    if raw and isinstance(raw, list):
        first = raw[0]
        if isinstance(first, dict):
            return str(first.get("value", "") or "")
        return str(first)
    return ""


class AttioClient:
    """Client for the Attio REST API v2."""

    BASE_URL = "https://api.attio.com/v2"

    def __init__(
        self,
        api_key: str | None = None,
        *,
        field_mapping: dict[str, str] | None = None,
    ):
        self.api_key = api_key or os.environ["ATTIO_API_KEY"]
        # engine-field-name -> vendor attribute slug, for the three documented
        # configurable fields. Defaults to the canonical Attio slugs, which makes
        # every existing `AttioClient(...)` construction byte-identical: each
        # `self._field_slug(...)` call below resolves to the same literal slug the
        # code hardcoded before this seam. An operator whose workspace renames an
        # attribute (e.g. linkedin -> linkedin_profile) overrides it via
        # config/crm.yaml's `field_mapping`, threaded in by the CRM factory.
        self._field_mapping: dict[str, str] = (
            field_mapping
            if field_mapping is not None
            else {
                "linkedin_url": "linkedin",
                "full_name": "name",
                "company_domain": "domains",
            }
        )
        self._client = httpx.Client(
            base_url=self.BASE_URL,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=60.0,
        )
        self._company_cache: dict[str, str] = {}  # company_record_id → name
        self._industry_cache: dict[str, str] = {}  # company_record_id → industry_vertical
        # company_record_id → True iff this company record carries the
        # LinkedIn-Clearbit corruption fingerprint. Populated lazily as
        # extract_record_info fetches each company; consult via
        # is_person_company_corrupted.
        self._company_corruption_cache: dict[str, bool] = {}
        # person_record_id → company_record_id, kept in lockstep with
        # _company_corruption_cache so the corruption lookup can hop from
        # a person id to the right cached flag.
        self._person_to_company: dict[str, str] = {}
        # company_record_id → ISO HQ country code (upper-case) or None when the
        # company record carries no `hq_country_code`. Populated lazily by
        # company_hq_country_code(); mirrors _company_cache's memoization so the
        # DM/connection language guard (PR-240) costs at most one company fetch
        # per distinct company per run.
        self._company_hq_country_cache: dict[str, str | None] = {}
        # person_record_id → raw `people.language` code, or None when the
        # person carries no override. Populated as a SIDE EFFECT of
        # extract_record_info (same idiom as _person_to_company) so the
        # language override rides the existing bulk person preload and
        # costs zero extra API calls on the send path — which matters,
        # since this lives inside the send-dms latency budget.
        self._person_language_cache: dict[str, str | None] = {}
        # Count of stage-regressing add_list_entry PATCHes the defense-in-depth
        # backstop neutralized over this client's lifetime. A batch orchestrator
        # can read this after a run and escalate if it fired — escalate() can't
        # be called from here (circular import with workflows.escalation, which
        # imports AttioClient).
        self.stage_regressions_blocked: int = 0

    def _field_slug(self, engine_field: str) -> str:
        """Resolve an engine field name to this workspace's vendor attribute slug.

        Unmapped fields pass through unchanged (an engine field with no explicit
        mapping shares its name with the vendor slug). Under the default mapping
        the three documented fields resolve to their canonical Attio slugs, so
        every call site below is byte-identical to the pre-seam literal.
        """
        return self._field_mapping.get(engine_field, engine_field)

    @observed("attio.request", "api")
    def _request(
        self,
        method: str,
        path: str,
        retries: int = 3,
        *,
        retry_500: bool = False,
        **kwargs,
    ) -> dict:
        if retries < 1:
            raise ValueError("retries must be >= 1")
        retryable = (429, 500, 502, 503) if retry_500 else (429, 502, 503)
        safe = _retry_safe(method, path)
        for attempt in range(retries):
            try:
                resp = timed_call("attio.http", "api_service", self._client.request, method, path, **kwargs)
                if not safe and resp.status_code >= 500:
                    try:
                        resp.raise_for_status()
                    except httpx.HTTPStatusError as exc:
                        raise AmbiguousAttioWrite(method, path) from exc
                if resp.status_code in retryable and attempt < retries - 1:
                    timed_call("attio.backoff", "retry_wait", time.sleep, 2 ** attempt * 5)
                    continue
                resp.raise_for_status()
                try:
                    return resp.json() if resp.content else {}
                except ValueError as exc:
                    if not safe:
                        raise AmbiguousAttioWrite(method, path) from exc
                    raise
            except httpx.TransportError as exc:
                if _ambiguous_failure(method, path, exc):
                    raise AmbiguousAttioWrite(method, path) from exc
                if attempt < retries - 1:
                    timed_call("attio.backoff", "retry_wait", time.sleep, 2 ** attempt * 5)
                    continue
                raise
        raise AssertionError("unreachable")

    # ── People records ────────────────────────────────────────

    def _query_paginated(
        self,
        path: str,
        filter_: dict | None,
        limit: int,
        *,
        fail_if_truncated: bool = False,
    ) -> list[dict]:
        """Shared pagination loop for the record/entry query endpoints.

        Pages through ``path`` until ``limit`` records are collected or the API
        returns a short page (exhausted). With ``fail_if_truncated=True``,
        raises :class:`AttioResultTruncated` when records remain past ``limit``
        (probing one extra page when the count lands exactly on the limit, so a
        complete result never raises). Full-sweep callers (suppression sweep,
        per-stage campaign queries, list-scan exports) use this so a silently
        truncated sweep becomes a loud operator-visible failure (PR-234).
        """
        all_records: list[dict] = []
        page_size = min(limit, 100)
        offset: int = 0
        exhausted = False

        while len(all_records) < limit:
            body: dict = {"limit": page_size, "offset": offset}
            if filter_:
                body["filter"] = filter_
            data = self._request("POST", path, json=body, retry_500=True)
            records = data.get("data", [])
            all_records.extend(records)
            if len(records) < page_size:
                exhausted = True
                break
            offset += len(records)

        if fail_if_truncated and len(all_records) >= limit:
            truncated = len(all_records) > limit
            if not truncated and not exhausted:
                # Exactly ``limit`` records with a full final page — a complete
                # result is indistinguishable from a truncated one without
                # probing the next page. One extra request here (boundary case
                # only) avoids a false-positive crash when the true count lands
                # exactly on the limit.
                body = {"limit": page_size, "offset": offset}
                if filter_:
                    body["filter"] = filter_
                probe = self._request("POST", path, json=body, retry_500=True)
                truncated = bool(probe.get("data", []))
            if truncated:
                raise AttioResultTruncated(
                    f"query {path} hit its scan limit ({limit}) with more "
                    f"records remaining — raise the limit so the sweep sees "
                    f"every record"
                )
        return all_records[:limit]

    def search_people(
        self,
        filter_: dict | None = None,
        limit: int = 50,
        *,
        fail_if_truncated: bool = False,
    ) -> list[dict]:
        """Search person records with optional filter. Auto-paginates."""
        return self._query_paginated(
            "/objects/people/records/query", filter_, limit,
            fail_if_truncated=fail_if_truncated,
        )

    def get_person(self, record_id: str, *, retry_500: bool = True) -> dict | None:
        """Get a person record by its record ID. Returns None if not found.

        ``retry_500=False`` opts back out of transient-500 retry — the
        fail-open bulk fetch uses it so a systemic Attio outage fails fast per
        record instead of blocking hours in backoff sleeps.
        """
        try:
            data = self._request("GET", f"/objects/people/records/{record_id}", retry_500=retry_500)
            return data.get("data", data)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def _bulk_fetch_fail_open(
        self, ids: list[str], fetch_one, on_error, max_workers: int = 8,
    ):
        """Yield ``(id, record)`` from a bounded-pool fan-out of `fetch_one`.

        Shared skeleton of the person/company bulk paths. Per-id failures
        of the fail-open classes — transport, missing-key, malformed
        response shape, undecodable JSON body — are routed to
        ``on_error(id, exc)`` and the id is skipped; other exception
        types propagate so refactor bugs surface immediately instead of
        silently dropping every id. ``record`` may be None (404) —
        callers decide what that means. Records are yielded on the
        calling thread; pool workers only fetch.

        retry_500=False lives in the callers' `fetch_one`: these paths
        are fail-open per record, so under a systemic 500 outage
        retrying would burn the full backoff budget per record (hours
        across a sweep) and still end in the same degraded result —
        fail fast and let the metrics surface it.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_id = {pool.submit(fetch_one, i): i for i in ids}
            for future in as_completed(future_to_id):
                i = future_to_id[future]
                try:
                    record = future.result()
                except (
                    httpx.HTTPError, KeyError, TypeError, json.JSONDecodeError,
                ) as exc:
                    on_error(i, exc)
                    continue
                yield i, record

    def _bulk_query_subset(self, object_slug: str, record_ids: set[str]) -> dict[str, dict]:
        """Read full pages instead of thousands of individual record GETs.

        Only requested records leave this helper. Missing IDs still get an
        individual read (including records created while pages were read).
        This cache is invocation-local; mutable send guards keep live reads.
        """
        records = self._query_paginated(
            f"/objects/{object_slug}/records/query", None, 50_000,
            fail_if_truncated=True,
        )
        found = {}
        for record in records:
            rid = record["id"]["record_id"]
            if rid in record_ids:
                if rid in found:
                    raise ValueError("duplicate record in Attio bulk query")
                found[rid] = record
        return found

    def bulk_fetch_persons_by_record_ids(
        self, record_ids: set[str], max_workers: int = 8,
        *, metrics: Any = None,
    ) -> dict[str, dict]:
        """Per-record isolation: a single failure is logged + counted
        but never propagated out of the batch. Returns the subset of
        `record_ids` that resolved to a non-None record.

        `metrics` (optional): when supplied, bumps
        `bulk_fetch_records_requested/returned/failed` so the
        end-of-run summary surfaces partial-outage volume rather than
        hiding it in stderr noise.

        Uses a bounded ThreadPoolExecutor scoped to the pipeline; calls
        flow through `_request` which handles 429 backoff.
        """
        if not record_ids:
            return {}
        ids = list(record_ids)
        if metrics is not None:
            metrics.bulk_fetch_records_requested += len(ids)

        def on_error(rid: str, exc: Exception) -> None:
            if metrics is not None:
                metrics.bulk_fetch_records_failed += 1
                metrics.warn(
                    f"bulk_fetch get_person({rid}) failed: "
                    f"{type(exc).__name__}"
                )
            print(
                f"WARNING: bulk_fetch_persons_by_record_ids: "
                f"get_person({rid}) failed: "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
            )

        result: dict[str, dict] = {}
        if len(ids) >= 100:
            try:
                result = self._bulk_query_subset("people", record_ids)
            except (httpx.HTTPError, AttioResultTruncated, KeyError, TypeError, ValueError) as exc:
                message = f"bulk people query failed ({type(exc).__name__}); falling back to individual reads"
                print(f"WARNING: {message}", file=sys.stderr)
                if metrics is not None:
                    metrics.warn(message)
            if metrics is not None:
                metrics.bulk_fetch_records_returned += len(result)
        for rid, record in self._bulk_fetch_fail_open(
            [rid for rid in ids if rid not in result], lambda r: self.get_person(r, retry_500=False),
            on_error, max_workers,
        ):
            if record is not None:
                result[rid] = record
                if metrics is not None:
                    metrics.bulk_fetch_records_returned += 1
        return result

    def search_person_by_linkedin(self, linkedin_url: str) -> dict | None:
        """Find a person record by LinkedIn URL. Returns None if not found.

        Two phases:
        1. Exact-variant probes — every URL form Attio may have stored:
           canonical (URL-decoded, no www, no trailing slash), with-www,
           with-trailing-slash, and the raw input. Attio's exact-string
           filter means otherwise-equivalent URLs miss their duplicates
           and the caller creates yet another record.
        2. Identity-key fallback — slug VARIANTS of the same profile share
           the numeric member-id suffix but differ as exact strings (the
           slug-variant cadence leak), so the probes above cannot see them.
           When the slug carries a profile-id, a `$contains` search on the
           id suffix finds them; because `$contains` over-matches (id
           `-70481235` is a substring of `-704812350`), every candidate is
           verified by `linkedin_identity_key` before being returned. This
           closes the variant-slug duplicate gap for ingest paths that do
           not run the weekly URL gate (intake, backfill scripts,
           reconciliation).
        """
        if not linkedin_url:
            return None
        slug = self._field_slug("linkedin_url")
        seen: set[str] = set()
        for variant in _linkedin_url_variants(linkedin_url):
            if variant in seen:
                continue
            seen.add(variant)
            results = self.search_people(filter_={slug: variant}, limit=1)
            if results:
                return results[0]
        profile_id = linkedin_profile_id(linkedin_url)
        if not profile_id:
            return None
        target_key = f"li-id:{profile_id}"
        candidates = self.search_people(
            filter_={slug: {"$contains": f"-{profile_id}"}}, limit=10,
        )
        for candidate in candidates:
            stored_url = person_record_linkedin_url(candidate, self._field_slug)
            if stored_url and linkedin_identity_key(stored_url) == target_key:
                return candidate
        return None

    def create_person(self, attributes: dict) -> dict:
        """Create a new person record."""
        data = self._request("POST", "/objects/people/records", json={"data": {"values": attributes}})
        return data.get("data", {})

    def update_person(self, record_id: str, attributes: dict) -> dict:
        """Update an existing person record."""
        # Same-values PATCH is idempotent — safe to retry a transient 500.
        data = self._request("PATCH", f"/objects/people/records/{record_id}", json={"data": {"values": attributes}}, retry_500=True)
        return data.get("data", {})

    def upsert_person(self, matching_attribute: str, attributes: dict) -> dict:
        """Upsert a person record: search by matching_attribute value, then create or update.

        Attio's PUT upsert requires the field to have a unique constraint; since 'linkedin'
        is not unique we implement it manually: search first, PATCH if found, POST otherwise.

        NOTE — field_mapping residual: the `linkedin_url` field_mapping covers the
        READ/search side (search_person_by_linkedin honors a renamed slug), but this
        WRITE/dedup path deliberately stays on the literal `linkedin` slug
        (matching_attribute + the canonicalized attrs below). A workspace that has
        BOTH a renamed slug and the canonical `linkedin` slug could split dedup
        silently. Routing this write path is a deferred follow-up — see
        docs/LIMITATIONS.md §1 (residual 2).
        """
        if matching_attribute == "linkedin":
            linkedin_url = attributes.get("linkedin", "")
            # Canonicalize at write time so duplicates on subsequent runs hit
            # the existing record regardless of encoding or trailing-slash
            # variance in PB's output.
            canonical = _canonical_linkedin_url(linkedin_url)
            if canonical:
                attributes = {**attributes, "linkedin": canonical}
            existing = self.search_person_by_linkedin(linkedin_url)
            if existing:
                record_id = existing.get("id", {}).get("record_id", "")
                if record_id:
                    # READ the stored URL through the workspace's resolved
                    # slug (the WRITE key below stays the literal `linkedin`
                    # per the residual documented above) — a renamed slug must
                    # not read as "no stored URL" and silently skip the guard.
                    stored_canonical = _canonical_linkedin_url(
                        person_record_linkedin_url(existing, self._field_slug)
                    )
                    if canonical and stored_canonical and stored_canonical != canonical:
                        # Found via the identity-key fallback under a DIFFERENT
                        # slug variant. Keep the stored URL: it is the
                        # exact-string join key for list entries
                        # (canonical_linkedin_url) and historical sends, and
                        # rewriting it to the incoming variant would desync
                        # those joins. Encoding/www variants share a canonical
                        # form and never reach this branch.
                        attributes = {
                            k: v for k, v in attributes.items() if k != "linkedin"
                        }
                    return self.update_person(record_id, attributes)
            return self.create_person(attributes)
        # Fallback: use the native PUT upsert for truly unique attributes (email, record_id)
        # The native upsert is assert-by-key (idempotent) — safe to retry a 500.
        data = self._request(
            "PUT",
            "/objects/people/records",
            params={"matching_attribute": matching_attribute},
            json={"data": {"values": attributes}},
            retry_500=True,
        )
        return data.get("data", {})

    def delete_person(self, record_id: str) -> bool:
        """Delete a person record. Returns True if deleted, False if not found."""
        try:
            self._request("DELETE", f"/objects/people/records/{record_id}")
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            raise

    # ── Company records ──────────────────────────────────────

    def search_companies(
        self,
        filter_: dict | None = None,
        limit: int = 50,
        *,
        fail_if_truncated: bool = False,
    ) -> list[dict]:
        """Search company records with optional filter. Auto-paginates.

        Routes through the shared truncation-guarded paginator so a full sweep
        can opt into ``fail_if_truncated=True`` (PR-220 — a silently truncated
        company sweep drops the tail past the limit).
        """
        return self._query_paginated(
            "/objects/companies/records/query", filter_, limit,
            fail_if_truncated=fail_if_truncated,
        )

    def create_company(self, attributes: dict) -> dict:
        """Create a new company record."""
        data = self._request("POST", "/objects/companies/records", json={"data": {"values": attributes}})
        return data.get("data", {})

    def get_company(self, record_id: str, *, retry_500: bool = True) -> dict | None:
        """Get a company record by its record ID. Returns None if not found.

        `retry_500=False` opts back out of transient-500 retry — the
        fail-open bulk company prefetch uses it so a systemic Attio
        outage fails fast per record instead of blocking in backoff
        sleeps (mirrors get_person).
        """
        try:
            data = self._request("GET", f"/objects/companies/records/{record_id}", retry_500=retry_500)
            return data.get("data", data)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def company_hq_country_code(self, company_id: str) -> str | None:
        """Return the linked company's ISO HQ country code (upper-cased),
        or None when it can't be determined.

        This is the SAME signal that seeds the entry `language` attribute
        (scripts/backfill_language.py) — the company's `hq_country_code`
        array — surfaced here so the DM/connection language guard (PR-240)
        can recompute the expected language from source and compare it to
        the stored value.

        Fail-open contract: returns None (never raises) when `company_id`
        is empty, the company record is missing, the record lacks a
        `hq_country_code`, or the fetch errors. Callers MUST treat None as
        "unknown — never flag a mismatch" so a sparsely-populated attribute
        can never manufacture a false positive.
        """
        if not company_id:
            return None
        if company_id in self._company_hq_country_cache:
            return self._company_hq_country_cache[company_id]

        code: str | None = None
        try:
            record = self.get_company(company_id)
            if record:
                values = record.get("values", {})
                country_list = values.get("hq_country_code") or []
                if country_list:
                    first = country_list[0]
                    raw = (
                        first.get("country_code", "")
                        if isinstance(first, dict)
                        else str(first)
                    )
                    raw = (raw or "").strip().upper()
                    code = raw or None
        except Exception as exc:  # noqa: BLE001 — fail-open contract above
            # Fail open: a transient error — or an off-shape `hq_country_code`
            # value (data drift) — must NOT be read as a language mismatch, and
            # per the docstring this getter never raises. Surface on stderr (no
            # silent swallow) and cache None so we don't hammer the API on
            # retries this run.
            print(
                f"  WARN: company_hq_country_code({company_id!r}) hit "
                f"{type(exc).__name__}: {exc}. Treating HQ country as unknown.",
                file=sys.stderr,
            )
            code = None

        self._company_hq_country_cache[company_id] = code
        return code

    def get_list_attributes(self, list_id: str) -> list[dict]:
        """Live attribute definitions for a list
        (GET /lists/{list_id}/attributes). Used by the DM-writer
        schema preflight; auto-pagination unnecessary (attribute
        counts are far below the page limit)."""
        data = self._request("GET", f"/lists/{list_id}/attributes")
        return data.get("data", [])

    def get_object_attributes(self, object_slug: str) -> list[dict]:
        """Live attribute definitions for an object
        (GET /objects/{object_slug}/attributes)."""
        data = self._request("GET", f"/objects/{object_slug}/attributes")
        return data.get("data", [])

    def update_company(self, record_id: str, attributes: dict) -> dict:
        """Update an existing company record."""
        data = self._request("PATCH", f"/objects/companies/records/{record_id}", json={"data": {"values": attributes}})
        return data.get("data", {})

    def invalidate_company_hq_country(self, company_id: str) -> None:
        """Drop one company from the HQ-country read cache.

        Public seam for writers that mutate `hq_country_code` and then
        verify through `company_hq_country_code` (e.g.
        scripts/backfill_company_hq_country.py) — without reaching into
        the private cache dict.
        """
        self._company_hq_country_cache.pop(company_id, None)

    def invalidate_person_language(self, person_record_id: str) -> None:
        """Drop one person from the language-override read cache.

        Public seam for writers that set `people.language` and then verify
        through `person_language_override` (scripts/set_person_language.py),
        without reaching into the private cache dict.
        """
        self._person_language_cache.pop(person_record_id, None)

    def search_company_by_domain(self, domain: str) -> dict | None:
        """Find a company record by domain. Returns None if not found."""
        results = self.search_companies(
            filter_={self._field_slug("company_domain"): domain},
            limit=1,
        )
        return results[0] if results else None

    def delete_company(self, record_id: str) -> bool:
        """Delete a company record. Returns True if deleted, False if not found."""
        try:
            self._request("DELETE", f"/objects/companies/records/{record_id}")
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            raise

    # ── Deal records ──────────────────────────────────────────

    def get_deal(self, record_id: str) -> dict | None:
        """Get a deal record by its record ID. Returns None if not found."""
        try:
            data = self._request("GET", f"/objects/deals/records/{record_id}")
            return data.get("data", data)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_list_entry(self, entry_id: str, list_id: str | None = None) -> dict | None:
        """Get a single list entry by its entry ID. Returns None if not found.

        Used by the Follow-up Radar WAITING-lane CLI (``followup-await``) to
        read the current awaiting_reply_* state off one entry before a
        clear/nudge write (PR-247).
        """
        lid = list_id or os.environ.get("ATTIO_LIST_ID", "")
        try:
            data = self._request("GET", f"/lists/{lid}/entries/{entry_id}")
            return data.get("data", data)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def create_deal(self, attributes: dict) -> dict:
        """Create a new deal record. Mirrors ``create_company``.

        ``attributes`` is the flat dict of Attio attribute slugs → values
        (e.g. ``{"name": "...", "stage": "Lead", "creation_idempotency_key":
        "...", "associated_people": [{"target_object": "people",
        "target_record_id": "..."}]}``). The deal's record_id is in the
        returned ``id.record_id`` field.

        Callers handle idempotency via ``creation_idempotency_key`` —
        ``workflows.deal_creation.create_deal_from_response`` is the
        write-owner-registry-authorized writer for that slug per
        docs/attio_schema_deltas.yaml.
        """
        data = self._request(
            "POST",
            "/objects/deals/records",
            json={"data": {"values": attributes}},
        )
        return data.get("data", {})

    def update_deal(self, record_id: str, attributes: dict) -> dict:
        """Update an existing deal record."""
        data = self._request("PATCH", f"/objects/deals/records/{record_id}", json={"data": {"values": attributes}})
        return data.get("data", {})

    def delete_deal(self, record_id: str) -> bool:
        """Delete a deal record. Returns True if deleted, False if not found."""
        try:
            self._request("DELETE", f"/objects/deals/records/{record_id}")
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            raise

    def search_deals(
        self,
        filter_: dict | None = None,
        limit: int = 500,
        *,
        fail_if_truncated: bool = False,
    ) -> list[dict]:
        """Search deal records with optional filter. Auto-paginates.

        Full-sweep callers (the Follow-up Radar deal cohort, the weekly-report
        deal-recency join) pass ``limit=DEAL_FULL_SCAN_LIMIT,
        fail_if_truncated=True`` — the 500 default silently drops the tail once
        the deal count passes it, so a whole-object sweep routes through the
        shared truncation-guarded paginator instead (PR-220).
        """
        return self._query_paginated(
            "/objects/deals/records/query", filter_, limit,
            fail_if_truncated=fail_if_truncated,
        )

    # ── List entries (pipeline) ───────────────────────────────

    def query_list_entries(
        self,
        list_id: str | None = None,
        filter_: dict | None = None,
        limit: int = 50000,
        *,
        fail_if_truncated: bool = False,
    ) -> list[dict]:
        """Query entries in a list (pipeline) with optional filters. Auto-paginates.

        With ``fail_if_truncated=True`` a full-list sweep that hits ``limit``
        with entries remaining raises :class:`AttioResultTruncated` instead of
        silently dropping the tail (PR-234).
        """
        lid = list_id or os.environ.get("ATTIO_LIST_ID", "")
        return self._query_paginated(
            f"/lists/{lid}/entries/query", filter_, limit,
            fail_if_truncated=fail_if_truncated,
        )

    def add_list_entry(
        self,
        record_id: str,
        stage_name: str,
        entry_attributes: dict | None = None,
        list_id: str | None = None,
        existing_entries: list[dict] | None = None,
    ) -> dict:
        """Add a person record to the pipeline list, or update the existing
        entry if one already exists for this (record_id, list) pair.

        Attio does not enforce uniqueness of (parent_record_id, list) — you
        can POST the same record repeatedly and get a fresh entry each time.
        The sales workflow calls this every time it (re-)processes a prospect,
        so without an upsert check, old prospects accumulate divergent list
        entries over time (Prospect + Connection Sent + DM1 Sent for the
        same person, stages going out of sync, DMs firing multiple times).

        Behavior:
          - If no entry exists for record_id in this list: POST a fresh entry.
          - If one entry exists: PATCH it with the new stage/attributes.
          - If multiple entries exist: PATCH the most-advanced one and leave
            the duplicates alone (callers can run the list-entry dedup script
            to collapse them).

        `existing_entries`: optional pre-fetched list of all entries in the
        target list. When supplied, the upsert filter runs client-side against
        this list instead of triggering a fresh `query_list_entries(limit=50000)`
        scan per call. Use this in batch flows that already hold the full list.
        """
        lid = list_id or os.environ.get("ATTIO_LIST_ID", "")
        attrs: dict = dict(entry_attributes or {})
        if stage_name:
            attrs["stage"] = stage_name

        if existing_entries is None:
            existing = self._find_list_entries_for_record(record_id, lid)
        else:
            existing = self._filter_and_rank_entries_for_record(existing_entries, record_id)
        if existing:
            target_entry_id = existing[0].get("id", {}).get("entry_id", "")
            if target_entry_id:
                # Defense-in-depth backstop for the weekly re-stamp cadence-desync
                # class: a PATCH that would move the existing entry to a LOWER
                # stage (e.g. Accepted/Prospect over DM3 Sent) wipes cadence
                # depth. When that's detected we strip the cadence-depth PAIR
                # — both `stage` AND `dm_step` — so the write cannot leave the
                # row in the self-contradictory state this guard exists to
                # prevent (stage=DM3 Sent + dm_step=0 is the exact "regressed"
                # fingerprint). Stripping `stage` alone would manufacture that
                # corruption. The other attributes still PATCH. Scope is
                # deliberately the universal cadence pair; the weekly finalize
                # path's own _commit_prospect already-listed skip is the primary
                # fix — this generic client chokepoint must not couple to any
                # caller's attr schema. Log loudly (this must NEVER be silent)
                # and bump a counter so a fired backstop is observable in
                # aggregate without grepping logs. We do NOT escalate() here:
                # workflows.escalation imports AttioClient, so calling it risks a
                # circular import; the batch orchestrator drains
                # `stage_regressions_blocked` and escalates. The fresh-POST branch
                # below needs no guard (a brand-new entry has no prior stage).
                # See _is_stage_regression for the non-identity-mapping caveat
                # (under a non-identity stage_mapping this guard safely no-ops).
                if "stage" in attrs:
                    prior_stage = AttioClient.parse_entry(existing[0]).get("stage", "") or ""
                    if _is_stage_regression(prior_stage, attrs["stage"]):
                        self.stage_regressions_blocked += 1
                        logger.warning(
                            "add_list_entry: dropping regressing stage+dm_step on "
                            "entry_id=%s — prior_stage=%r, attempted new "
                            "stage=%r would lower cadence depth; stage and dm_step "
                            "keys stripped, other attrs still patched "
                            "(stage_regressions_blocked=%d)",
                            target_entry_id,
                            prior_stage,
                            attrs["stage"],
                            self.stage_regressions_blocked,
                        )
                        attrs = {
                            k: v for k, v in attrs.items() if k not in ("stage", "dm_step")
                        }
                return self.update_list_entry(
                    entry_id=target_entry_id,
                    entry_attributes=attrs,
                    list_id=lid,
                )

        body: dict = {
            "data": {
                "parent_record_id": record_id,
                "parent_object": "people",
                "entry_values": attrs,
            }
        }
        data = self._request("POST", f"/lists/{lid}/entries", json=body)
        return data.get("data", {})

    # F-PR-1: stage rank lookup delegates to the canonical STAGE_RANK in
    # models/pipeline.py. Used to pick the "most-advanced" entry when a
    # record has legacy duplicates; unknown stage strings fall back to 0
    # (sort to the end, oldest-created-at tiebreak wins).

    def _find_list_entries_for_record(self, record_id: str, list_id: str) -> list[dict]:
        """Return list entries in `list_id` whose parent is `record_id`,
        sorted highest-stage first. Empty list if none.

        Used by add_list_entry to upsert instead of blindly POSTing a fresh
        duplicate. Falls back to a client-side filter over a bulk query
        because Attio's list-entries API doesn't accept filters on
        parent_record_id directly at v2.
        """
        try:
            entries = self.query_list_entries(list_id=list_id, limit=50000)
        except httpx.HTTPStatusError:
            return []
        return self._filter_and_rank_entries_for_record(entries, record_id)

    def _filter_and_rank_entries_for_record(
        self, entries: list[dict], record_id: str
    ) -> list[dict]:
        """Filter `entries` to those whose parent is `record_id`, sorted
        highest-stage first. Pure client-side; no API call.
        """
        own: list[dict] = []
        for e in entries:
            parsed = AttioClient.parse_entry(e)
            if parsed.get("record_id") == record_id:
                own.append(e)
        if not own:
            return []

        def rank(entry: dict) -> tuple[int, str]:
            parsed = AttioClient.parse_entry(entry)
            stage_str = parsed.get("stage", "") or ""
            try:
                r = STAGE_RANK[PipelineStage(stage_str)]
            except (KeyError, ValueError):
                r = 0
            created = entry.get("created_at", "") or ""
            # Higher rank first, tiebreak by OLDER created_at (preserves history).
            return (-r, created)

        own.sort(key=rank)
        return own

    def update_list_entry(
        self,
        entry_id: str,
        entry_attributes: dict,
        list_id: str | None = None,
    ) -> dict:
        """Update a list entry (change stage, attributes, etc.)."""
        lid = list_id or os.environ.get("ATTIO_LIST_ID", "")
        data = self._request(
            "PATCH",
            f"/lists/{lid}/entries/{entry_id}",
            json={"data": {"entry_values": entry_attributes}},
        )
        return data.get("data", {})

    def delete_list_entry(self, entry_id: str, list_id: str | None = None) -> bool:
        """Delete a list entry. Returns True if deleted, False if not found."""
        lid = list_id or os.environ.get("ATTIO_LIST_ID", "")
        try:
            self._request("DELETE", f"/lists/{lid}/entries/{entry_id}")
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            raise

    # ── Notes ─────────────────────────────────────────────────

    def create_note(self, record_id: str, title: str, content: str, parent_object: str = "people") -> dict:
        """Create a note on a person or company record.

        Attio's /v2/notes endpoint requires `format` (either "plaintext" or
        "markdown") paired with a `content` field. Historical versions of
        this client used `content_plaintext`, which the API now rejects with
        HTTP 400 validation_type.
        """
        data = self._request(
            "POST",
            "/notes",
            json={
                "data": {
                    "parent_object": parent_object,
                    "parent_record_id": record_id,
                    "title": title,
                    "format": "plaintext",
                    "content": content,
                },
            },
        )
        return data.get("data", {})

    def list_notes_for_record(
        self, record_id: str, parent_object: str = "people", limit: int = 50
    ) -> list[dict]:
        """List notes attached to a person or company record."""
        data = self._request(
            "GET",
            "/notes",
            params={
                "parent_object": parent_object,
                "parent_record_id": record_id,
                "limit": limit,
            },
        )
        return data.get("data", [])

    def delete_note(self, note_id: str) -> bool:
        """Delete a note. Returns True if deleted, False if not found."""
        try:
            self._request("DELETE", f"/notes/{note_id}")
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            raise

    # ── Entry parsing helpers ────────────────────────────────

    @staticmethod
    def object_record_first_value(values: dict, slug: str):
        """First value of a slug on an object-record's `values` shape.

        Object-record reads (Operator Review Queue, Migration Run, Data
        Quality Report, daily_run, etc.) carry plain text/number/dict.
        Both `status.title` (status-typed attrs) and `option.title`
        (SELECT-typed attrs, e.g. `daily_run.status` /
        `reply_detection_status`) are unwrapped to their title string so
        the helper returns a scalar for those without a per-caller branch
        — mirroring the entry-side `_extract_value` select handling. This
        is the read that lets the daily_run reattach path see a select's
        real value through the normalized `Record.attributes` instead of
        a stale `None`.
        """
        items = (values or {}).get(slug) or []
        if not items:
            return None
        item = items[0]
        if isinstance(item, dict):
            return (
                item.get("value")
                or item.get("status", {}).get("title")
                or item.get("option", {}).get("title")
            )
        return item

    @staticmethod
    def parse_entry(entry: dict) -> dict:
        """Extract flat attributes from an Attio list entry."""
        values = entry.get("entry_values", {})
        return {
            "entry_id": entry.get("entry_id", entry.get("id", {}).get("entry_id", "")),
            "record_id": entry.get("parent_record_id", entry.get("id", {}).get("record_id", "")),
            "entry_created_at": entry.get("created_at"),
            "stage": AttioClient._extract_stage(values),
            "persona": AttioClient._extract_value(values, "persona"),
            "language": AttioClient._extract_value(values, "language"),
            "dm_step": AttioClient._extract_value(values, "dm_step"),
            "quality_score": AttioClient._extract_value(values, "quality_score"),
            "last_contact_date": AttioClient._extract_value(values, "last_contact_date"),
            "experiment_id": AttioClient._extract_value(values, "experiment_id"),
            "response_classification": AttioClient._extract_value(values, "response_classification"),
            "last_response_text": AttioClient._extract_value(values, "last_response_text"),
            "score_breakdown": AttioClient._extract_value(values, "score_breakdown"),
            "scoring_lane": AttioClient._extract_value(values, "scoring_lane"),
            "verdict_path": AttioClient._extract_value(values, "verdict_path"),
            "llm_rationale": AttioClient._extract_value(values, "llm_rationale"),
            # Phase 1 auto-research signals — read as None on legacy entries.
            "icp_lane_persisted": AttioClient._extract_value(values, "icp_lane_persisted"),
            "quality_score_band": AttioClient._extract_value(values, "quality_score_band"),
            "defensive_score": AttioClient._extract_value(values, "defensive_score"),
            "engagement_score": AttioClient._extract_value(values, "engagement_score"),
            # Fresh-prospect quarantine attrs. invite_eligible_after gates the
            # daily invite slice via is_invite_eligible; prospect_committed_at
            # is the forensic origin timestamp consumed by the starvation
            # evaluator. Without these extractions both signals would read None
            # for every entry and the §3.1 defense layer becomes a no-op.
            "prospect_committed_at": AttioClient._extract_value(values, "prospect_committed_at"),
            "invite_eligible_after": AttioClient._extract_value(values, "invite_eligible_after"),
            # PR-9a attrs — read by PR-9.5's §3.11 union-merge (per-step
            # timestamps + cohort identity freeze marker + canonical URL).
            # Without extractions every entry would read None and the
            # MAX-non-null per-attribute rule collapses to a no-op.
            "dm1_sent_at": AttioClient._extract_value(values, "dm1_sent_at"),
            "dm2_sent_at": AttioClient._extract_value(values, "dm2_sent_at"),
            "dm3_sent_at": AttioClient._extract_value(values, "dm3_sent_at"),
            "response_received_at": AttioClient._extract_value(values, "response_received_at"),
            "canonical_linkedin_url": AttioClient._extract_value(values, "canonical_linkedin_url"),
            "vanity_url_slug": AttioClient._extract_value(values, "vanity_url_slug"),
            # ---- Pain-signal discovery lane (PR-280 / PR-284) ----
            # prospect_source routes the invite-note selection in
            # _build_invite_send_data (pain note vs persona note);
            # pain_source_type picks the reference frame (poster = authorship
            # note; commenter/liker = engagement note). The remaining three
            # are operator-review + measurement context. None on every
            # non-pain-lane entry, and on every workspace that never
            # provisioned the optional attributes.
            "prospect_source": AttioClient._extract_value(values, "prospect_source"),
            "pain_source_type": AttioClient._extract_value(values, "pain_source_type"),
            "pain_snippet": AttioClient._extract_value(values, "pain_snippet"),
            "source_post_url": AttioClient._extract_value(values, "source_post_url"),
            "source_post_at": AttioClient._extract_value(values, "source_post_at"),
            # Cohort archaeology sentinel (§3.10). The DQR counts entries
            # stamped legacy_* as the observability metric for how much
            # of the cohort is locked out of sends; without this
            # extraction the DQR's legacy_archaeology_pool_count would
            # be a silent zero.
            "experiment_id_frozen_at": AttioClient._extract_value(values, "experiment_id_frozen_at"),
            # PR-9.5 dedup soft-delete pointer + cross-channel suppression
            # OR-merge participants. merged_into is a record-reference; the
            # extractor returns the target_record_id string for non-null
            # references (or None for losers that haven't been merged).
            "merged_into": AttioClient._extract_value(values, "merged_into"),
            "suppress_re_engagement": AttioClient._extract_value(values, "suppress_re_engagement"),
            "had_connection_note": AttioClient._extract_value(values, "had_connection_note"),
            # PR-39 cadence policy attributes. nurture_re_eligible_at is
            # the target date (post-DM3 cooldown end) after which the
            # NURTURE → DM1_SENT re-engagement is gated by the §3.18
            # four-gate check. cadence_lane is the typed lane stamp set
            # at PROSPECT-commit (promotes the legacy scoring_lane string).
            "nurture_re_eligible_at": AttioClient._extract_value(values, "nurture_re_eligible_at"),
            "cadence_lane": AttioClient._extract_value(values, "cadence_lane"),
            # Delivery-transport routing stamp (optional attribute; reads
            # None on any workspace that never provisioned it, which
            # `_resolve_send_channel` resolves to "pb"). EVERY consumer of
            # the stamp goes through this dict — the invite exclusion, the
            # DM queue hold-out, the Phase 0 / 0.5 scope skips and the
            # event-ingest scope guard. Without the extraction they would
            # all read "pb" for every row, the hold-out would never fire,
            # and a prospect another transport still holds could take a
            # second first-touch from PhantomBuster.
            "send_channel": AttioClient._extract_value(values, "send_channel"),
        }

    @staticmethod
    def parse_deal(record: dict) -> dict:
        """Extract flat attributes from an Attio deal record."""
        values = record.get("values", {})

        def first(key: str) -> dict | None:
            arr = values.get(key, [])
            return arr[0] if arr and isinstance(arr, list) else None

        name_obj = first("name")
        stage_obj = first("stage")
        value_obj = first("value")
        company_obj = first("associated_company")
        country_obj = first("country")
        owner_obj = first("owner")

        stage = ""
        if isinstance(stage_obj, dict):
            status = stage_obj.get("status")
            if isinstance(status, dict):
                stage = status.get("title", "")

        deal_value = None
        currency = None
        if isinstance(value_obj, dict):
            deal_value = value_obj.get("currency_value")
            currency = value_obj.get("currency_code")

        return {
            "record_id": record.get("id", {}).get("record_id", ""),
            "name": name_obj.get("value") if isinstance(name_obj, dict) else None,
            "stage": stage,
            "value": deal_value,
            "currency": currency,
            "company_id": company_obj.get("target_record_id") if isinstance(company_obj, dict) else None,
            "country": country_obj.get("country_code") if isinstance(country_obj, dict) else None,
            "owner_id": owner_obj.get("referenced_actor_id") if isinstance(owner_obj, dict) else None,
            # ---- Follow-up Radar state (PR-211) — deal-side twins ----
            "followup_draft_at": AttioClient._extract_value(values, "followup_draft_at"),
            "followup_draft_id": AttioClient._extract_value(values, "followup_draft_id"),
            "followup_snooze_until": AttioClient._extract_value(values, "followup_snooze_until"),
            "followup_muted": AttioClient._extract_value(values, "followup_muted"),
            "followup_callback_date": AttioClient._extract_value(values, "followup_callback_date"),
            # ---- WAITING lane: awaiting-reply state (PR-247) — deal-side twins ----
            "awaiting_reply_since": AttioClient._extract_value(values, "awaiting_reply_since"),
            "awaiting_reply_thread_id": AttioClient._extract_value(values, "awaiting_reply_thread_id"),
            "awaiting_reply_note_id": AttioClient._extract_value(values, "awaiting_reply_note_id"),
            "awaiting_reply_nudge_count": AttioClient._extract_value(values, "awaiting_reply_nudge_count"),
            # Partner attribution: the canonical lowercase email of the partner
            # who introduced this deal (deal-side twin of the Partner Intro
            # entry stage). Non-empty → the radar routes the deal to the PARTNER
            # lane and treats the intro as email evidence for channel_hint.
            "referred_by": AttioClient._extract_value(values, "referred_by"),
            # ---- Follow-up Radar v2: real deal recency (PR-214) ----
            # Person record_ids the radar joins against for native interaction
            # timestamps (people.last_interaction). Empty list when the deal has
            # no associated people — recency then falls back per _deal_recency.
            "associated_people": [
                p.get("target_record_id")
                for p in (values.get("associated_people") or [])
                if isinstance(p, dict) and p.get("target_record_id")
            ],
            # Skill-layer-verified true last-touch (date), stamped via the
            # `followup-touch` CLI after a successful verification. Highest-
            # precedence recency source; None until first verified (or until the
            # schema is provisioned — both read the same).
            "last_verified_touch": AttioClient._extract_value(values, "last_verified_touch"),
            # Deal creation timestamp — the tier-3 recency fallback. Attio's
            # system field lives at the record top level; the `values` read
            # tolerates the timestamp moving under values (same shape-drift
            # guard as followup_radar._deal_last_touch).
            "created_at": record.get("created_at")
            or AttioClient._extract_value(values, "created_at"),
        }

    @staticmethod
    def _extract_stage(values: dict) -> str:
        """Extract stage name from Attio entry values."""
        stage_data = values.get("stage", [])
        if stage_data and isinstance(stage_data, list):
            return stage_data[0].get("status", {}).get("title", "")
        return ""

    @staticmethod
    def _extract_value(values: dict, key: str):
        """Extract a simple attribute value from Attio entry values."""
        data = values.get(key, [])
        if data and isinstance(data, list):
            item = data[0]
            if not isinstance(item, dict):
                return item
            # Select-type attributes store value under option.title
            if item.get("attribute_type") == "select":
                return item.get("option", {}).get("title")
            # Record references expose their ID directly, without a value key.
            if item.get("target_record_id"):
                return item["target_record_id"]
            return item.get("value", item)
        return None

    @staticmethod
    def person_company_ref_id(record: dict) -> str | None:
        """Return the linked company record id from a person record, or None.

        Pure parse — no API call, total over malformed shapes. None when
        the person has no company field, the field holds a plain value
        rather than a record reference (extract_record_info renders
        those verbatim), or the record is off-shape. Totality matters:
        the preload harvests ids from every fetched person in one
        comprehension, and one raising record would abort the whole
        prefetch instead of costing one person.
        """
        if not isinstance(record, dict):
            return None
        values = record.get("values", {})
        if not isinstance(values, dict):
            return None
        company_data = values.get("company", values.get("primary_company", []))
        if company_data and isinstance(company_data, list):
            ref = company_data[0]
            if isinstance(ref, dict) and ref.get("target_record_id"):
                return ref["target_record_id"]
        return None

    def _prime_company(self, cid: str, cr_values: dict) -> tuple[str, str]:
        """Parse a fetched company's values and populate the per-run caches.

        Shared by extract_record_info's lazy fetch and the bulk prefetch
        so both paths cache identically. `cr_values` is `{}` when the
        record was missing or the fetch failed — that primes the same
        empty-string sentinel the lazy path always cached. Off-shape
        `name` entries (non-dict — the same shape
        is_linkedin_clearbit_corrupted already guards against) degrade
        to the empty sentinel instead of raising.
        """
        if not isinstance(cr_values, dict):
            cr_values = {}
        # COMPANY name stays the LITERAL "name" — no documented
        # field_mapping key for company name (full_name is the PERSON
        # name), so routing it would invent a mapping.
        company = ""
        cr_name = cr_values.get("name", [])
        if isinstance(cr_name, list) and cr_name and isinstance(cr_name[0], dict):
            company = cr_name[0].get("value", "")
        industry = first_option_title(cr_values.get("industry_vertical"))
        self._company_cache[cid] = company
        self._industry_cache[cid] = industry
        self._company_corruption_cache[cid] = (
            is_linkedin_clearbit_corrupted({"values": cr_values}, self._field_slug)
        )
        return company, industry

    def bulk_prime_company_caches(
        self, company_ids: set[str], max_workers: int = 8,
        *, metrics: Any = None,
    ) -> int:
        """Parallel-fetch companies and prime the per-run company caches.

        ``extract_record_info`` otherwise does one blocking company GET
        per first-seen company, serially, inside the preload prime loop.
        Pre-fetching the distinct company ids through a bounded pool
        turns that loop into cache hits.

        Fail-open per company: a fetch or parse failure is logged +
        counted and the id is left unprimed, so extract_record_info's
        existing lazy serial fetch covers it. A 404 primes the same
        empty-string sentinel the lazy path caches, but does NOT count
        as `bulk_fetch_companies_returned` — mirroring the persons
        contract, so dangling company refs stay visible as
        requested != returned + failed. No send-time guard reads these
        caches (name/industry/corruption only — hq_country_code is
        deliberately NOT primed here; the language guard resolves it
        live at queue-build time).

        Returns the number of companies primed (404 sentinels included).
        """
        todo = [
            cid for cid in company_ids
            if cid and cid not in self._company_cache
        ]
        if not todo:
            return 0
        if metrics is not None:
            metrics.bulk_fetch_companies_requested += len(todo)

        def on_error(cid: str, exc: Exception) -> None:
            if metrics is not None:
                metrics.bulk_fetch_companies_failed += 1
                metrics.warn(
                    f"bulk company prefetch get_company({cid}) "
                    f"failed: {type(exc).__name__}"
                )
            print(
                f"WARNING: bulk_prime_company_caches: "
                f"get_company({cid}) failed: "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
            )

        primed = 0
        queried = {}
        if len(todo) >= 100:
            try:
                queried = self._bulk_query_subset("companies", set(todo))
            except (httpx.HTTPError, AttioResultTruncated, KeyError, TypeError, ValueError) as exc:
                message = f"bulk company query failed ({type(exc).__name__}); falling back to individual reads"
                print(f"WARNING: {message}", file=sys.stderr)
                if metrics is not None:
                    metrics.warn(message)
        for cid, record in queried.items():
            try:
                self._prime_company(cid, record.get("values", {}))
            except (KeyError, TypeError, AttributeError, IndexError) as exc:
                # Parsing can populate display fields before corruption
                # detection raises. Do not let partial caches hide the ID
                # from the individual repair loop below.
                for cache in (self._company_cache, self._industry_cache,
                              self._company_corruption_cache):
                    cache.pop(cid, None)
                message = (
                    f"bulk company query parse failed for {cid}: "
                    f"{type(exc).__name__}; retrying individually"
                )
                print(f"WARNING: {message}", file=sys.stderr)
                if metrics is not None:
                    metrics.warn(message)
                continue
            primed += 1
            if metrics is not None:
                metrics.bulk_fetch_companies_returned += 1
        for cid, record in self._bulk_fetch_fail_open(
            [cid for cid in todo if cid not in self._company_cache], lambda c: self.get_company(c, retry_500=False),
            on_error, max_workers,
        ):
            # Primes on the calling thread — pool workers only fetch.
            # Per-company isolation: a malformed record shape skips that
            # one company (lazy path covers it) instead of aborting the
            # rest of the batch.
            try:
                self._prime_company(cid, (record or {}).get("values", {}))
            except (KeyError, TypeError, AttributeError, IndexError) as exc:
                on_error(cid, exc)
                continue
            primed += 1
            if record is not None and metrics is not None:
                metrics.bulk_fetch_companies_returned += 1
        return primed

    @staticmethod
    def extract_person_select_value(values: dict, field: str) -> str:
        """Extract a select attribute value from a PERSON RECORD's ``values``.

        Handles the shapes ``GET /v2/objects/people/records/{id}`` returns:
        - ``{field: [{"value": "pt"}]}``                 (plain value)
        - ``{field: [{"option": {"title": "pt"}}]}``     (select option)
        - ``{field: "pt"}``                              (scalar, test fakes)

        Returns "" when absent or off-shape — never raises.

        Canonical home for this shape handling: person-record values carry
        no ``attribute_type`` discriminator, so ``_extract_value`` (which
        targets LIST-ENTRY values and branches on it) cannot read them.
        """
        raw = values.get(field)
        if raw is None:
            return ""
        if isinstance(raw, str):
            return raw.strip()
        if isinstance(raw, list) and raw:
            item = raw[0]
            if isinstance(item, dict):
                opt = item.get("option") or {}
                if isinstance(opt, dict) and opt.get("title"):
                    return str(opt["title"]).strip()
                v = item.get("value")
                if v is not None:
                    return str(v).strip()
        return ""

    @staticmethod
    def _extract_person_language(values: dict) -> str | None:
        """Raw `people.language` override code, or None when unset.

        Returns the code verbatim (lower-cased) WITHOUT validating it
        against the `Language` enum — the CRM select may also offer codes
        the copy library has no templates for. Validation is the
        resolver's job (`models.resolution.coerce_language`), so an
        unsupported value degrades to "no override" at exactly one place
        instead of being silently dropped here.
        """
        code = AttioClient.extract_person_select_value(values, "language")
        return code.lower() or None

    def person_language_override(self, person_record_id: str) -> str | None:
        """Return this person's explicit `people.language` override, or None.

        The person-level override outranks every company-derived language
        inference (see `models.resolution` module docstring). It is a
        NARROW exception list — empty for almost everyone — so None is the
        normal answer and means "no human has contradicted the inference".

        Cost: free for any person the run already resolved through
        `RecordCache.get` / `preload_pipeline_persons`, which is every
        prospect on the send path. Falls back to a single lazy fetch for
        an unseen person.

        Never raises. Returns None when the id is empty or the record has
        no override, and the `LANGUAGE_OVERRIDE_READ_FAILED` sentinel when
        the fetch itself errored.

        The sentinel matters: None is the NORMAL answer here, and a row
        with no override can still classify as a corroborated source that
        prints nothing. Collapsing a failed read into None would therefore
        make a prospect whose override was just lost look HEALTHIER than an
        ordinary row — and ship the very message the override existed to
        prevent. The sentinel keeps the two apart all the way to the
        operator's screen, and the failure is deliberately NOT cached, so a
        one-off blip is retried rather than frozen in for the whole run.
        """
        if not person_record_id:
            return None
        if person_record_id in self._person_language_cache:
            return self._person_language_cache[person_record_id]

        try:
            record = self.get_person(person_record_id)
        except Exception as exc:  # noqa: BLE001 — never raise into the send path
            print(
                f"  WARN: person_language_override({person_record_id!r}) hit "
                f"{type(exc).__name__}: {exc}. Language override UNKNOWN for "
                f"this prospect (not cached; will retry).",
                file=sys.stderr,
            )
            return LANGUAGE_OVERRIDE_READ_FAILED

        code = self._extract_person_language(record.get("values", {})) if record else None
        self._person_language_cache[person_record_id] = code
        return code

    def extract_record_info(
        self, record: dict,
    ) -> tuple[str | None, str | None, str, str | None, str]:
        """Extract (name, company, linkedin_url, industry, title) from an Attio person record.

        Resolves record-reference company fields by fetching the linked company.
        Industry comes from the company's industry_vertical field. Title is the
        person's own job_title, surfaced so dry-run output can show it for ICP
        review before sending.

        PRECONDITION — `record` must be a COMPLETE person record, not a
        projection. This method populates the `_person_language_cache`
        side-effect cache from `record["values"]`, and a partial record
        would cache "no override" for a person who has one, silently
        disabling the override with no signal
        (`person_language_override` trusts the cache and won't re-fetch).
        Every caller today passes a full `get_person` / bulk-fetch record.
        """
        values = record.get("values", {})

        name_data = values.get(self._field_slug("full_name"), [])
        name = ""
        if name_data:
            first = name_data[0].get("first_name", "")
            last = name_data[0].get("last_name", "")
            name = f"{first} {last}".strip()

        title_data = values.get("job_title", [])
        title = ""
        if title_data:
            tv = title_data[0]
            title = str(tv.get("value", tv) if isinstance(tv, dict) else tv)

        person_record_id = record.get("id", {}).get("record_id", "")
        if person_record_id:
            # Side-effect cache, same idiom as _person_to_company below:
            # the person record is already in hand, so harvesting the
            # language override here makes person_language_override() free
            # for every person the run has resolved. The returned tuple is
            # deliberately NOT widened — many call sites unpack it
            # positionally, and none of them want this field.
            self._person_language_cache[person_record_id] = (
                self._extract_person_language(values)
            )
        company = ""
        industry = ""
        company_data = values.get("company", values.get("primary_company", []))
        if company_data and isinstance(company_data, list) and company_data:
            ref = company_data[0]
            # Same parse as the bulk prefetch's id harvest — keeping the
            # two definitionally identical so the prefetch always covers
            # exactly the companies this lazy path would fetch.
            cid = self.person_company_ref_id(record)
            if cid:
                if person_record_id:
                    self._person_to_company[person_record_id] = cid
                if cid in self._company_cache:
                    company = self._company_cache[cid]
                    industry = self._industry_cache.get(cid, "")
                else:
                    cr_values: dict = {}
                    try:
                        cr = self._request("GET", f"/objects/companies/records/{cid}")
                        cr_data = cr.get("data", cr)
                        cr_values = cr_data.get("values", {})
                    except httpx.HTTPStatusError:
                        pass
                    company, industry = self._prime_company(cid, cr_values)
            else:
                company = str(ref.get("value", ref))

        linkedin = ""
        linkedin_data = values.get(self._field_slug("linkedin_url"), [])
        if linkedin_data:
            val = linkedin_data[0]
            linkedin = str(val.get("value", val) if isinstance(val, dict) else val)

        # PR-14 (B-PD-006): return None on missing fields rather than
        # the literal string "Unknown". Downstream consumers MUST use
        # `is None` / `is not None` instead of `== "Unknown"`. Pre-PR-14
        # callers that compared against the literal were all updated
        # in this PR; see workflows/record_cache.py docstring for
        # the rationale.
        return (
            name or None,
            company or None,
            linkedin,
            industry or None,
            title,
        )

    def is_person_company_corrupted(self, person_record_id: str) -> bool:
        """Return True iff this person's linked company carries the
        LinkedIn-Clearbit fingerprint.

        Reads from caches populated by extract_record_info; safe to call
        for any person that the surrounding workflow has already resolved
        via RecordCache.get (the normal Phase A/B path). For unseen
        persons the answer is conservatively False — callers that need a
        guaranteed-fresh check should resolve the person first.
        """
        cid = self._person_to_company.get(person_record_id)
        if not cid:
            return False
        return self._company_corruption_cache.get(cid, False)

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
