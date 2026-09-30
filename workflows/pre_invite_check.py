"""Pre-invite LinkedIn-degree check.

Scrapes LinkedIn degree for each PROSPECT before inviting; partitions
already-connected (1st-degree) prospects out of the invite queue and
flips them to ACCEPTED in Attio. Catches Pattern A (re-prospected after
dedup) and Pattern B (the operator connected externally) before LinkedIn no-ops
the invite.

A second-layer `is_invite_eligible` quarantine gate (defense-in-depth)
runs before any invite candidate is scraped — the daily_check caller
already filtered, but this re-verifies so a future refactor of either
side can't silently regress the §3.1 contract.

# PR-15 (B-SD-010)

Three pieces shipped together:

  1. **STRICT mode env gate** — `STRICT_PRE_INVITE_DEGREE_CHECK`
     (default true) raises `ConfigError` when the send-path lacks a
     `profile_scraper_id`. The pre-PR-15 silent-bypass (a click.echo
     warning) was a §0 #9 violation: invites would ship without
     degree verification, risking re-invites of already-1st-degree
     connections.

  2. **Pattern-A flip codepath carve-out** — STRICT applies ONLY to
     the send_invite path. The cache_hit_flip path (cache reports a
     1st-degree connection; we record it as ACCEPTED without
     scraping) does NOT need a scraper-id by definition — it's a
     read-then-record path that never issues a new invite. The
     codepath is set explicitly before the STRICT gate so the
     carve-out is readable in the diff.

  3. **Atomic Pattern-A flip via AttioWriter.apply(WriteIntent)** —
     B-SD-010's load-bearing §3.1 protection. The flip writes
     `stage=ACCEPTED + last_contact_date` as a single multi-attribute
     PATCH through F-PR-4's contract. A torn write (stage flips but
     last_contact_date doesn't, or vice versa) would let a retry
     re-invite an already-1st-degree connection. F-PR-4 guarantees
     single-PATCH atomicity for `WriteIntent(updates=[...])`.

# Partial-CSV signal

When the pre-invite scrape CSV returns fewer rows than were requested
(scrape attempted N profiles, CSV reported M < N), PR-15 emits ONE
`degree_unknown` Operator Review Queue row PER MISSING PROSPECT with
payload `{record_id, last_known_degree, scrape_attempt_id, requested_at,
csv_row_count_observed, csv_row_count_expected}`. Aggregated/summary
writes are forbidden — operator triages per-prospect. The aggregated
`degree_unknown_count` attribute is PR-17's responsibility (run-end
summary).

# PR-21 experiment_id inheritance (Pattern-A + Pattern-B flip)

When a 1st-degree match flips a row from PROSPECT (or CONNECTION_SENT) →
ACCEPTED, the flip ALSO stamps `experiment_id_frozen_at="accepted"` while
preserving the existing `experiment_id`. This ensures cohort-attribution is
correct regardless of which phase catches the acceptance first.

Immutability guard (`_IMMUTABLE_FROZEN_AT_VALUES`): once a row reaches a
terminal frozen_at value, it cannot be overwritten. The guard raises
`ExperimentIdImmutableError(RuntimeError)` on violation — subclass of
RuntimeError to avoid silent swallow by `except ValueError:` blocks.

Carve-out: `prospect → accepted` is EXPLICITLY ALLOWED — this is the
Pattern-B case (the operator connected externally on a never-invited prospect). The
`connection_sent` → `accepted` path is the normal Pattern-A path. Both are
valid. The immutability guard only blocks overwriting already-terminal values
(accepted, connection_sent, legacy_*).

to_send_data row CONTRACT (PR-21): every row MUST carry `experiment_id` and
`experiment_id_frozen_at` keys (populated by `run_connection_requests` from
Attio-read attrs). Missing keys → `KeyError` propagates as a caller bug.
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import click
import gspread
import httpx

from clients.attio import (
    AttioClient,
    linkedin_identity_key,
    linkedin_identity_map,
    resolve_identity_match,
)
from clients.attio_writer import (
    AttioError,
    AttioWriter,
    WriteIntent,
)
from clients.attio_writer_registry import UnauthorizedAttioWriteError
from clients.pb_envelope import PBRunFailed, PBRunTimeout, has_scraper_dedup_marker
from models.business_calendar import operator_today
from models.experiment import ExperimentIdImmutableError, FrozenAtState
from models.pipeline import PipelineStage, is_invite_eligible
from workflows import recheck_cache
from workflows.daily_check_helpers import (
    _normalize_linkedin_url,
    _resolve_degree_check_backend,
    preflight_legacy_profile_scraper,
)
from workflows.escalation import escalate

if TYPE_CHECKING:
    from clients.phantombuster import PhantomBusterClient
    from workflows.audit import AuditLogger


# ---------------------------------------------------------------------------
# PR-21 experiment_id immutability guard
# ---------------------------------------------------------------------------

# Frozen-at values that are TERMINAL — once a row carries one of these,
# experiment_id + experiment_id_frozen_at must not be overwritten.
# "prospect" and None are NOT in this set — both are valid source states
# that can transition to "accepted" or "connection_sent".
# "no_active_experiment" is NOT here and MUST NOT be added — it is not
# a valid Attio select option (Lesson 1).
_IMMUTABLE_FROZEN_AT_VALUES: frozenset[FrozenAtState] = frozenset({
    "connection_sent",
    "accepted",
    "legacy_pre_tsv_era",
    "legacy_inferred_by_archaeology",
    "legacy_pure_unknown",
})


def _check_experiment_id_immutability(
    *,
    record_id: str,
    entry_id: str | None,
    prior_frozen_at: str | None,
    new_frozen_at: str,
    prior_experiment_id: str | None,
    new_experiment_id: str | None,
) -> None:
    """Guard against overwriting a terminal experiment_id_frozen_at value.

    Raises `ExperimentIdImmutableError` when `prior_frozen_at` is in
    `_IMMUTABLE_FROZEN_AT_VALUES` AND we're not keeping the same value.

    Carve-outs (both are valid, explicitly documented):
    - `prospect → accepted`: Pattern-B case — the operator connected externally on
      a never-invited prospect. The row went PROSPECT → ACCEPTED without
      passing through CONNECTION_SENT. This is NOT a violation; the guard
      explicitly allows it.
    - `None → accepted` or `None → connection_sent`: Row has no prior
      frozen_at (pre-PR-21 row). Stamping from NULL is always allowed.

    The guard is NOT raised when:
    - prior_frozen_at is None (NULL → any value is fine)
    - prior_frozen_at == "prospect" (prospect → accepted is Pattern-B)
    - prior_frozen_at == new_frozen_at (idempotent write is fine)
    """
    if prior_frozen_at is None:
        # Null → anything: allowed (pre-PR-21 row).
        return
    if prior_frozen_at == new_frozen_at:
        # Idempotent write: allowed.
        return
    if prior_frozen_at not in _IMMUTABLE_FROZEN_AT_VALUES:
        # Source state is "prospect" or another non-terminal value:
        # the transition is allowed (prospect → accepted is Pattern-B).
        return
    # prior_frozen_at is a terminal value AND we're trying to overwrite it.
    raise ExperimentIdImmutableError(
        record_id=record_id,
        entry_id=entry_id,
        field="experiment_id_frozen_at",
        old_value=prior_frozen_at,
        new_value=new_frozen_at,
    )


# ---------------------------------------------------------------------------
# STRICT mode
# ---------------------------------------------------------------------------

STRICT_PRE_INVITE_DEGREE_CHECK_ENV = "STRICT_PRE_INVITE_DEGREE_CHECK"

# ---------------------------------------------------------------------------
# Pattern-A recency quarantine (PR-241 René RCA)
# ---------------------------------------------------------------------------

# A 1st-degree row that only became a prospect within this window is a
# suspected URL-variant duplicate (the weekly ingest re-created an existing
# person under a new LinkedIn vanity slug). Flipping it Pattern-A → ACCEPTED
# would re-start a cadence on someone who already completed one. Skip the flip
# and escalate instead. Old records still flip (legitimate silent-acceptance
# Pattern-A) — a missing/unparseable timestamp is NEVER quarantined.
PATTERN_A_QUARANTINE_DAYS = 14


def _prospect_committed_within_days(
    committed_at: str | None, *, today: date, days: int,
) -> bool:
    """True iff `committed_at` parses to a date within the last `days`.

    Missing or unparseable → False (NOT quarantined): old records must keep
    the legitimate Pattern-A flip. Accepts ISO date or datetime strings.
    """
    if not committed_at:
        return False
    raw = str(committed_at).strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        committed_date = parsed.date()
    except ValueError:
        try:
            committed_date = date.fromisoformat(raw[:10])
        except ValueError:
            # Present-but-unparseable is NOT silently equivalent to absent.
            # Still return False (don't quarantine on a value we can't trust),
            # but name the raw value so a malformed timestamp is observable
            # rather than indistinguishable from a missing one.
            click.echo(
                f"  ⚠ prospect_committed_at present but unparseable "
                f"({committed_at!r}) — treating as NOT within the Pattern-A "
                f"quarantine window (no quarantine); value may need repair.",
                err=True,
            )
            return False
    return (today - committed_date).days < days


class ConfigError(RuntimeError):
    """Pre-invite degree check is missing a required configuration value
    AND the STRICT env gate is enabled.

    Raised exclusively from the send_invite codepath in
    `_pre_invite_degree_check` — the cache_hit_flip path is exempt
    because it never issues a new invite.

    Caller contract: this is a fatal config error that must NOT be
    caught and silently bypassed. The pre-PR-15 behavior of warning
    and proceeding was the §0 #9 violation B-SD-010 closes.
    """


class DryRunSandboxSheetUnreachable(ConfigError):
    """GSHEET_DRYRUN_ID is set but Google refuses the sandbox sheet (404 = deleted,
    trashed, or not shared with the OAuth Google account the agent runs as —
    clients/google_sheets.py authorizes as a user, not a service account;
    403 = permission).

    Raised only from `_launch_sales_nav_scrape` under `dry_run=True`, BEFORE
    any PB launch — the caller turns it into the same skip-and-preview as the
    missing-sandbox gate, so a stale sheet id degrades to one typed line
    instead of a traceback (2026-09-07 incident). Wet runs never raise it: a
    production-sheet failure stays a loud raw error.
    """


def _describe_sheet_error(exc: Exception) -> str:
    """Render a gspread error as ``SpreadsheetNotFound HTTP 404``.

    gspread keeps the HTTP response on ``APIError.response``, as ``args[0]``
    on ``SpreadsheetNotFound``, and only on ``__cause__`` for the bare
    ``PermissionError`` it raises on 403 — read any, tolerate none.
    """
    resp = getattr(exc, "response", None)
    if resp is None and exc.args:
        resp = exc.args[0]
    if resp is None and exc.__cause__ is not None:
        # open_by_key raises a bare PermissionError `from` the APIError 403.
        resp = getattr(exc.__cause__, "response", None)
    status = getattr(resp, "status_code", None)
    return f"{type(exc).__name__}{f' HTTP {status}' if status else ''}"


def _strict_mode_enabled() -> bool:
    """Read `STRICT_PRE_INVITE_DEGREE_CHECK` from env.

    Defaults to True — the SAFE default per §3.1 (never silently skip
    degree verification on the send path). Operators can opt out with
    `STRICT_PRE_INVITE_DEGREE_CHECK=false` for debugging only.
    """
    return os.environ.get(STRICT_PRE_INVITE_DEGREE_CHECK_ENV, "true").lower() != "false"


# ---------------------------------------------------------------------------
# Sales Nav CSV column constants (PR-B.4)
# ---------------------------------------------------------------------------
#
# Verified against PB phantom 602790655114603 on 2026-05-25 — see migration
# memory at ~/.claude/projects/.../project_sales_nav_migration.md. These
# column names match the legacy regular Profile Scraper output (by design;
# both PB phantoms emit the same schema), so the parser can reuse a single
# resolver helper across both backends — see _resolve_csv_columns below.

SALES_NAV_DEGREE_COL = "connectionDegree"
SALES_NAV_LINKEDIN_URL_COL = "linkedinProfileUrl"
SALES_NAV_QUERY_COL = "query"  # echo of the input URL exactly as passed
SALES_NAV_FULL_NAME_COL = "fullName"
SALES_NAV_HAS_PENDING_INVITATION_COL = "hasPendingInvitation"  # "true" | "false"

# Legacy regular-scraper columns we still match for the regular backend path.
LEGACY_DEGREE_COLS = ("connectionDegree",)
LEGACY_URL_COLS = ("linkedin_url", "linkedinProfileUrl", "profileUrl", "query")


# ---------------------------------------------------------------------------
# Pre-invite §3.1 failure_reason constants (PR-B.8)
# ---------------------------------------------------------------------------
#
# Distinct strings per failure mode so the operator-review queue UI can
# triage `degree_unknown` escalations by category. Closes adversarial
# finding F10 (operator confusion from a single opaque "scrape returned no
# row" string masking three distinct conditions).

REASON_LEGACY_CSV_MISS = "regular_scrape_missing_row"
REASON_LEGACY_DEGREE_BLANK = "regular_scrape_empty_degree"

REASON_SALES_NAV_CSV_MISS = "sales_nav_scrape_missing_row"
REASON_SALES_NAV_DEGREE_BLANK = "sales_nav_scrape_empty_degree"
REASON_SALES_NAV_DEGREE_UNKNOWN = "sales_nav_scrape_unknown_degree_value"
# §3.1 stronger gate (NEW from CSV finding 2026-05-25): the Sales Nav CSV
# includes hasPendingInvitation="true|false". If true, we already invited
# this prospect — NEVER re-invite regardless of degree.
REASON_SALES_NAV_PENDING_INVITATION = "sales_nav_has_pending_invitation"


# ---------------------------------------------------------------------------
# Sales Nav launch + escalation helpers (PR-B.5)
# ---------------------------------------------------------------------------


def _launch_sales_nav_scrape(
    pb: PhantomBusterClient,
    scraper_id: str,
    urls: list[str],
    *,
    max_wait: int = 300,
    retry_on_timeout: bool = True,
    dry_run: bool = False,
    identity_universe: set[str] | None = None,
    conflicting_urls: set[str] | None = None,
) -> tuple[str, dict[str, str], dict[str, dict]]:
    """Run the Sales Nav Profile Scraper against the given LinkedIn URLs.

    Returns (container_id, degree_by_normalized_url, extras_by_normalized_url).
    ``extras`` carries the per-prospect signals beyond degree —
    currently just ``hasPendingInvitation`` ("true"/"false") so the
    PR-B.5 partition can apply the stronger §3.1 gate.

    The Sales Nav Profile Scraper rejects partial argument objects with
    "Your Phantom Argument Isn't Valid" (verified 2026-05-25 against
    phantom 602790655114603). Always pass the FULL saved argument shape,
    overriding only the fields we need:

    - ``identities[0].sessionCookie`` ← fresh value from
      :envvar:`PB_LI_SALES_NAV_SESSION_COOKIE`
    - ``identities[0].userAgent`` ← :envvar:`PB_LI_USER_AGENT`
    - ``spreadsheetUrl`` ← single ``linkedin.com/in/<slug>`` URL OR a real
      Google Sheets URL. The phantom auto-converts ``/in/`` URLs to
      ``/sales/lead/<id>,<auth>`` internally — no separate URL Converter
      phantom needed (see project_sales_nav_migration memory).
    - ``numberOfProfilesPerLaunch`` ← tight cap matching the input batch;
      scraper sheets are written without a header.

    For multi-URL launches we still use a Google Sheets URL via
    :func:`workflows.daily_check.write_prospects_to_sheet`; for single
    URLs we can pass the URL string directly in ``spreadsheetUrl`` (the
    phantom accepts both forms).

    On ``PBRunTimeout``, retries ONCE with the same args before giving up
    (closes L3 review must-fix #2 — Sales Nav is faster than the legacy
    scraper but PB queue depth can still spike; one retry catches the
    queue-saturation case without doubling cost on a real failure).

    Returns empty dicts (not None) on ``PBRunFailed`` or empty CSV — the
    caller's batch-level fail-safe (drop the whole batch rather than
    risk re-inviting connected people) lives one level up.
    """
    import workflows.daily_check as _dc

    # Build sheet input. For a single URL, pass it directly (no sheet write);
    # for >1, build a Google Sheets URL via the existing helper. In dry-run the
    # multi-URL write MUST target the dedicated sandbox sheet (GSHEET_DRYRUN_ID)
    # so a preview never mutates the production autoconnect sheet (2026-06-03
    # design). The caller's dry-run gate already guarantees GSHEET_DRYRUN_ID is
    # set before we reach a multi-URL dry-run scrape; assert it here too so a
    # future caller can't silently route a preview onto the production sheet.
    if len(urls) == 1:
        # Bare profile URL as input — no sheet, no header line, so the raw
        # batch size is the correct per-launch count.
        sheet_url = urls[0]
        launch_count = len(urls)
    else:
        sheet_rows = [{"profileUrl": u} for u in urls]
        dry_sheet_id = os.environ.get("GSHEET_DRYRUN_ID") if dry_run else None
        if dry_run and not dry_sheet_id:
            raise ConfigError(
                "dry-run multi-profile Sales Nav scrape requires GSHEET_DRYRUN_ID "
                "(a sandbox sheet) so the preview never writes the production "
                "autoconnect sheet."
            )
        try:
            sheet_url = _dc.write_prospects_to_sheet(
                sheet_rows, columns=["profileUrl"], spreadsheet_id=dry_sheet_id,
                include_header=False,
            )
        except (
            gspread.exceptions.SpreadsheetNotFound,  # open_by_key on 404
            PermissionError,  # open_by_key on 403 (gspread raises the builtin)
            gspread.exceptions.APIError,  # anything else the Sheets API refused
        ) as exc:
            # 2026-09-07: GSHEET_DRYRUN_ID pointed at a sheet Google answered
            # 404 for and the whole preview died on a raw traceback. The sheet
            # write is the FIRST side effect of a dry-run scrape (no launch
            # yet), so fail closed with a typed reason the caller can render
            # as skip-and-preview. Wet runs keep the loud raw failure.
            if not dry_run:
                raise
            if isinstance(exc, (gspread.exceptions.SpreadsheetNotFound, PermissionError)):
                hint = "deleted, trashed, or not shared with the agent's Google account"
            else:
                hint = "Google Sheets API refused the write; sheet may still be fine"
            raise DryRunSandboxSheetUnreachable(
                f"sandbox sheet GSHEET_DRYRUN_ID={(dry_sheet_id or '')[:6]}… is unreachable "
                f"({_describe_sheet_error(exc)}: {hint})"
            ) from exc
        # Scraper sheets contain URLs only; the exact row count covers all.
        launch_count = len(urls)

    # Saved-args + identities-inject contract (raises SalesNavConfigError if
    # the cookie env var is missing) lives in the shared helper.
    import workflows.daily_check_helpers as _dch
    launch_args = _dch.build_sales_nav_launch_args(
        pb, scraper_id, spreadsheet_url=sheet_url, launch_count=launch_count
    )
    # csvName is deliberately NOT set here — it is generated PER ATTEMPT
    # inside _do_launch_and_fetch (see below): a timed-out first container
    # can still finish asynchronously (PB is async — see pb_send_recovery)
    # and register the batch in its file's dedup DB; reusing the same name
    # on retry would make the retry refuse to re-scrape and defeat the
    # fresh-name dedup-bust.

    def _do_launch_and_fetch() -> tuple[str, str]:
        # Fresh csvName PER ATTEMPT: a timed-out first container can still
        # finish asynchronously (PB is async — see pb_send_recovery) and
        # register the batch in its file's dedup DB; reusing the name would
        # make the retry refuse to re-scrape and defeat itself.
        csv_name = _dch._fresh_csv_name("deg")
        attempt_args = {**launch_args, "csvName": csv_name}
        launch = pb.launch_agent(scraper_id, attempt_args)
        completion = pb.wait_for_completion(launch, poll_interval=10, max_wait=max_wait)
        if has_scraper_dedup_marker(getattr(completion, "log_output", "")):
            click.echo(
                f"  ⚠ SN scraper container {launch.container_id} reported a "
                "dedup refusal ('already scraped') DESPITE per-launch csvName "
                f"{csv_name!r} — PB may have ignored the argument. Degrees from "
                "this container are NOT fresh.",
                err=True,
            )
        # Defensive str() cast: real PB always returns a string container_id,
        # but escalate() downstream validates against TypedDict-typed payloads
        # and MagicMock-shaped test fixtures or hypothetical PB API changes
        # could surface as `EscalationSchemaError: expected str`. Cast at the
        # boundary so the helper's caller never has to.
        return str(launch.container_id or ""), (pb.download_result_csv(launch, csv_name=csv_name) or "")

    try:
        container_id, csv_text = _do_launch_and_fetch()
    except httpx.RequestError as exc:
        # A timed-out POST may have launched remotely. Never retry it blindly:
        # an empty lookup holds every unverified profile out of the invite batch.
        click.echo(
            f"  ⚠ Sales Nav request failed ({type(exc).__name__}); launch outcome "
            "unknown — holding unverified profiles, no automatic relaunch.",
            err=True,
        )
        return "", {}, {}
    except PBRunTimeout:
        if not retry_on_timeout:
            raise
        click.echo(
            f"  ⚠ Sales Nav scrape timed out at {max_wait}s — retrying once before "
            "dropping the batch.",
            err=True,
        )
        try:
            container_id, csv_text = _do_launch_and_fetch()
        except httpx.RequestError as exc:
            click.echo(
                f"  ⚠ Sales Nav retry request failed ({type(exc).__name__}); "
                "launch outcome unknown — holding unverified profiles.",
                err=True,
            )
            return "", {}, {}
        except PBRunFailed as exc:
            click.echo(
                f"  ⚠ Sales Nav retry scrape failed: {exc} — holding "
                "unverified profiles.",
                err=True,
            )
            return "", {}, {}
        except PBRunTimeout:
            click.echo(
                "  ⚠ Sales Nav scrape timed out a second time — DROPPING invite batch.",
                err=True,
            )
            return "", {}, {}
    except PBRunFailed as exc:
        click.echo(
            f"  ⚠ Sales Nav scrape failed: {exc} — DROPPING invite batch.",
            err=True,
        )
        return "", {}, {}

    if not csv_text:
        return container_id, {}, {}

    our_urls = {_normalize_linkedin_url(u) for u in urls}
    # Slug-variant bridge: `linkedinProfileUrl` carries the profile's CURRENT
    # slug, which can differ from the slug we queried (the person renamed
    # their vanity URL). On an exact miss, re-key the row to OUR url form via
    # the numeric profile-id so callers' lookups (keyed on our form) still
    # hit — otherwise the row reads as scrape-missing and re-queues/escalates
    # forever (2026-08-18 leak family).
    # Retry subsets must still recognize every original candidate, and a
    # later clean-looking row cannot clear an earlier identity conflict.
    universe = our_urls if identity_universe is None else identity_universe
    our_urls_by_id = linkedin_identity_map(universe)
    degree_lookup: dict[str, str] = {}
    extras: dict[str, dict] = {}
    if conflicting_urls is None:
        conflicting_urls = set()
    for row in csv.DictReader(io.StringIO(csv_text)):
        # The phantom echoes the input URL exactly as passed under `query`,
        # plus the canonical form under `linkedinProfileUrl`. Try both for
        # match-back robustness.
        matches = {
            matched
            for column in (SALES_NAV_LINKEDIN_URL_COL, SALES_NAV_QUERY_COL)
            if (url := row.get(column, ""))
            if (matched := resolve_identity_match(
                _normalize_linkedin_url(url), universe, our_urls_by_id
            ))
        }
        observed_keys = {
            linkedin_identity_key(row.get(column, ""))
            for column in (SALES_NAV_LINKEDIN_URL_COL, SALES_NAV_QUERY_COL)
            if row.get(column, "")
        }
        numeric_keys = {key for key in observed_keys if key.startswith("li-id:")}
        if len(numeric_keys) > 1:
            conflicting_urls.update(matches)
            for matched in matches:
                degree_lookup.pop(matched, None)
                extras.pop(matched, None)
            click.echo(f"  ⚠ Sales Nav container {container_id}: distinct profile/query IDs; withholding degree.", err=True)
            continue
        if len(matches) > 1:
            conflicting_urls.update(matches)
            for matched in matches:
                degree_lookup.pop(matched, None)
                extras.pop(matched, None)
            click.echo(
                f"  ⚠ Sales Nav container {container_id}: conflicting profile/query "
                "identities; withholding this row's degree.", err=True,
            )
            continue
        if not matches:
            continue
        norm = matches.pop()
        if norm in conflicting_urls or norm not in our_urls:
            continue
        degree = (row.get(SALES_NAV_DEGREE_COL) or "").strip()
        unavailable = (row.get("error") or "").strip().lower() == "profile unavailable"
        if unavailable:
            degree = ""  # Terminal failure never constitutes degree evidence.
            degree_lookup.pop(norm, None)
        if degree:
            degree_lookup[norm] = degree
        extras[norm] = {
            "profile_unavailable": unavailable,
            "source_container_id": container_id,
            "hasPendingInvitation": (
                row.get(SALES_NAV_HAS_PENDING_INVITATION_COL) or ""
            ).strip().lower(),
            "fullName": (row.get(SALES_NAV_FULL_NAME_COL) or "").strip(),
        }
    return container_id, degree_lookup, extras


def _emit_degree_unknown(
    row: dict,
    *,
    attio: AttioClient,
    today_iso: str,
    failure_reason: str,
    last_known_degree: str = "",
    scrape_attempt_id: str = "",
    csv_row_count_observed: int = 0,
    csv_row_count_expected: int = 0,
) -> None:
    """Open a ``degree_unknown`` operator-review row for one prospect.

    `failure_reason` (NEW field in PR-B.8) classifies the miss for
    operator triage: ``regular_scrape_missing_row``, ``sales_nav_scrape_*``,
    ``sales_nav_has_pending_invitation``, etc.

    Asserts ``row['record_id']`` is non-empty so the idempotency key stays
    stable per prospect (URL-only keys collide on the rare same-URL-two-
    records case). Caller MUST populate ``record_id`` upstream.
    """
    if not row.get("record_id"):
        raise RuntimeError(
            f"degree_unknown emit requires record_id; got {row!r}. "
            "Caller must populate `record_id` on every row."
        )
    escalate(
        type="degree_unknown",
        idempotency_key=(
            f"degree-unknown|{row['record_id']}"
            f"|{failure_reason}"
            f"|{scrape_attempt_id or today_iso[:10]}"
        ),
        payload={
            "record_id": str(row["record_id"]),
            "linkedin_url": row["linkedInUrl"],
            "failure_reason": failure_reason,
            "last_known_degree": last_known_degree,
            "scrape_attempt_id": scrape_attempt_id,
            "requested_at": datetime.now(UTC).isoformat(),
            "csv_row_count_observed": csv_row_count_observed,
            "csv_row_count_expected": csv_row_count_expected,
        },
        attio=attio,
    )


# ---------------------------------------------------------------------------
# Pre-invite degree check
# ---------------------------------------------------------------------------

def _prior_cadence_entries_for_url(
    attio: AttioClient,
    list_id: str,
    linkedin_url: str,
    own_entry_id: str,
    cache: dict,
) -> tuple[list[dict], str]:
    """Existing pipeline entries whose URL shares `linkedin_url`'s identity key.

    Slug-variant cadence-leak fix: when the Pattern-A quarantine flags a
    suspected URL-variant duplicate, the operator previously had NO tooling to
    check whether a slug variant of the same person was already in the
    pipeline — upstream's quarantine row was correctly flagged but misjudged
    as a real accept because the prior DM3-complete entry (under the long
    slug) was invisible. This surfaces those siblings in the escalation
    payload so the prior cadence is in front of the operator at judgment time.

    Matching uses `linkedin_identity_key`: the numeric profile-id suffix when
    the slug carries one (bridges slug variants), else the canonical URL
    (still surfaces exact-URL duplicate entries).

    The pipeline-list fetch happens at most ONCE per run — `cache` (caller-
    owned dict) memoizes the built index or the fetch error. Quarantine hits
    are rare, so the fetch is lazy: no cost on runs without a quarantine. A
    fetch/parse failure degrades to an empty match list plus an error string
    the caller records in the payload — never raises.

    Merged-away entries (`merged_into` set — §3.11 soft-deleted losers) are
    excluded: their winner inherited the cadence, so listing the loser would
    show the operator a stale duplicated view. Entries whose entry-level
    `canonical_linkedin_url` is unset (the backfill skews to later stages, so
    PROSPECT rows often lack it) fall back to the parent person record's
    LinkedIn URL in one bulk read — without this, exactly the sibling the
    operator needs could be invisible and an empty list would read as "no
    prior cadence".
    """
    if "index" not in cache and "error" not in cache:
        try:
            index: dict[str, list[dict]] = {}
            pending: dict[str, list[dict]] = {}  # record_id -> summaries sans URL
            for raw in attio.query_list_entries(list_id=list_id):
                parsed = AttioClient.parse_entry(raw)
                if parsed.get("merged_into"):
                    continue
                summary = {
                    "entry_id": parsed.get("entry_id") or "",
                    "record_id": parsed.get("record_id") or "",
                    "linkedin_url": parsed.get("canonical_linkedin_url") or "",
                    "stage": parsed.get("stage") or "",
                    "dm_step": str(parsed.get("dm_step") or ""),
                    "dm1_sent_at": str(parsed.get("dm1_sent_at") or ""),
                    "dm2_sent_at": str(parsed.get("dm2_sent_at") or ""),
                    "dm3_sent_at": str(parsed.get("dm3_sent_at") or ""),
                    "last_contact_date": str(parsed.get("last_contact_date") or ""),
                }
                if summary["linkedin_url"]:
                    index.setdefault(
                        linkedin_identity_key(summary["linkedin_url"]), []
                    ).append(summary)
                elif summary["record_id"]:
                    pending.setdefault(summary["record_id"], []).append(summary)
            if pending:
                persons = attio.bulk_fetch_persons_by_record_ids(set(pending))
                for rid, person in persons.items():
                    items = ((person or {}).get("values") or {}).get("linkedin") or []
                    first = items[0] if items else ""
                    url = str(
                        first.get("value", "") if isinstance(first, dict) else first
                    )
                    if not url:
                        continue
                    for summary in pending.get(rid, []):
                        summary["linkedin_url"] = url
                        index.setdefault(
                            linkedin_identity_key(url), []
                        ).append(summary)
            cache["index"] = index
        except Exception as exc:  # noqa: BLE001 — degrade to empty, but LOUD
            cache["error"] = f"{type(exc).__name__}: {exc}"
            click.echo(
                f"  ⚠ prior-cadence lookup for the Pattern-A quarantine payload "
                f"failed ({cache['error']}) — escalation rows this run carry no "
                f"sibling-entry context.",
                err=True,
            )
    if "error" in cache:
        return [], cache["error"]
    matches = cache["index"].get(linkedin_identity_key(linkedin_url), [])
    return [m for m in matches if m["entry_id"] != own_entry_id], ""


def _pre_invite_degree_check(
    to_send_data: list[dict],
    pb: PhantomBusterClient,
    profile_scraper_id: str | None,
    attio: AttioClient,
    list_id: str,
    *,
    sales_nav_profile_scraper_id: str | None = None,
    today: date | None = None,
    dry_run: bool = False,
    audit_logger: AuditLogger | None = None,
) -> tuple[list[dict], list[dict]]:
    """Scrape LinkedIn degree for each PROSPECT before inviting.

    Partitions `to_send_data` into:
    - already_connected: degree=1st profiles. These are flipped to ACCEPTED in
      Attio immediately via a single-PATCH AttioWriter call (PR-15 atomicity);
      they shouldn't get a phantom invite (LinkedIn no-ops it) nor a "thanks
      for accepting" DM later.
    - still_to_invite: degree=2nd/3rd or unknown — proceed to invite.

    On PB failure or empty CSV, returns (still_to_invite=[], already_connected=[])
    — fail-safe: drop the whole batch rather than risk re-inviting already-
    connected people. Better to lose a day's invites than re-burn relationships.

    Public dry-run contract: return the proposed inventory without scrapes,
    Sheets writes, cache updates or CRM mutations. Live degree and send
    readiness remain unchecked until an authorized wet run.

    # PR-15 codepath split

    The function detects two codepaths up front:

      - `send_invite` — there are stale URLs that need PB scraping to
        determine degree. STRICT mode applies: a missing
        `profile_scraper_id` raises `ConfigError`.
      - `cache_hit_flip` — all URLs already have a cached degree
        result, so no scraping is needed. STRICT mode is EXEMPT
        because the flip path never issues a new invite.

    Catches both Pattern A leftovers (re-prospected after dedup) and Pattern B
    (Mat connected externally without the system knowing).

    NOTE: write_prospects_to_sheet and _pb_session_args are imported via
    `workflows.daily_check` as a module reference inside the function so that:
    (1) test patches targeting workflows.daily_check.<attr> resolve at call
        time and take effect, and
    (2) this module stays import-order-agnostic — a top-level module-as-namespace
        import works in production (daily_check is loaded first by cli.py) but
        fails fresh imports like `python3 -c 'import workflows.pre_invite_check'`
        because daily_check then tries to from-import _pre_invite_degree_check
        from a partially-loaded pre_invite_check. The in-function import sees a
        fully-loaded daily_check whenever this code path runs.
    """
    # In-function module-as-namespace import — see docstring for rationale.
    import workflows.daily_check as _dc

    if not to_send_data:
        return [], []

    # Public dry-run boundary: no scrapes, Sheets, cache or CRM mutations.
    if dry_run:
        click.echo(f"  [DRY RUN] Pre-invite check would inspect {len(to_send_data)} profiles; live readiness unchecked.")
        return to_send_data, []

    # §3.1 defense-in-depth: re-verify quarantine. The daily_check caller
    # already filtered on `is_invite_eligible`, but rows reach us via a
    # separate code path (the `to_send_data` shape) and a future caller
    # could forget the upstream filter. The CI grep guard
    # `tests/test_invite_eligible_guard.py` asserts this call stays here.
    today_op = today if today is not None else operator_today()
    quarantined = [r for r in to_send_data if not is_invite_eligible(r, today_op)]
    if quarantined:
        click.echo(
            f"  ⚠ Pre-invite quarantine guard dropped {len(quarantined)} row(s) "
            f"whose invite_eligible_after has not yet elapsed (defense-in-depth).",
            err=True,
        )
        to_send_data = [r for r in to_send_data if is_invite_eligible(r, today_op)]
        if not to_send_data:
            return [], []

    # Consult recheck cache to skip URLs scraped within the TTL. Cached "1st"
    # short-circuits to ACCEPTED; cached "2nd"/"3rd" short-circuits to invite
    # queue. Only stale/unknown URLs get scraped via PB.
    cached_results: dict[str, str] = {}
    urls_in = [p["linkedInUrl"] for p in to_send_data]
    fresh_cache, stale_urls = recheck_cache.partition(urls_in)
    stale_set = set(stale_urls)
    for url, entry in fresh_cache.items():
        deg = (entry.get("degree") or "").strip()
        if deg:
            cached_results[_normalize_linkedin_url(url)] = deg
        else:
            # Cached without degree (e.g., a prior NB-only visit). Re-scrape.
            stale_set.add(url)

    to_send_stale = [p for p in to_send_data if p["linkedInUrl"] in stale_set]
    cache_hit_count = len(to_send_data) - len(to_send_stale)
    if cache_hit_count:
        click.echo(
            f"  Cache hit: skipping degree scrape for {cache_hit_count} profiles "
            f"(within {recheck_cache.RECHECK_TTL_DAYS} days)."
        )

    # PR-B.5: resolve backend (regular | sales_nav) per-invocation so the
    # flag is hot-reloadable on a fresh process. Centralizes the cross-wire
    # + missing-cookie guards (closes adversarial F5). Raises
    # SalesNavConfigError on invalid config — fail-loud at the top of the
    # function so callers don't waste a PB launch on a bad env.
    backend = _resolve_degree_check_backend()
    scraper_id_for_strict = (
        sales_nav_profile_scraper_id if backend == "sales_nav"
        else profile_scraper_id
    )
    scraper_arg_name = (
        "sales_nav_profile_scraper_id" if backend == "sales_nav"
        else "profile_scraper_id"
    )

    # 2026-06-03 dry-run scrape-safety gate. A live degree scrape needs a PB
    # phantom input. Two write hazards make a naive dry-run scrape mutate the
    # *production* autoconnect GSheet (the 2026-05-25 leak vector):
    #   1. the legacy `regular` scraper reads its URLs FROM that sheet — its
    #      scrape IS a sheet write; and
    #   2. even the sales_nav scraper writes that sheet for any batch >1 URL
    #      (only a single-URL scrape passes the URL directly).
    # Per the approved design, dry-run scrapes that need a sheet go to a
    # dedicated sandbox (GSHEET_DRYRUN_ID), never the production sheet. When
    # the scrape would write a sheet but no sandbox is configured — or the
    # backend is `regular` — degrade to the safe skip-and-preview: show the
    # stale rows as would-invite and bail before the STRICT check (which would
    # otherwise raise on a missing id). The cache_hit_flip path (no stale URLs)
    # needs no scrape and falls through safely.
    # Shared by the three dry-run skip-and-preview exits below (legacy backend,
    # missing sandbox, unreachable sandbox).
    _unresolved_note = (
        "shown as would-invite, but their degree is UNRESOLVED in this "
        "preview — some may be 1st-degree/OON/pending and would NOT be "
        "invited in a wet run"
    )
    if dry_run and to_send_stale:
        _scrape_writes_sheet = backend != "sales_nav" or len(to_send_stale) > 1
        if backend != "sales_nav":
            click.echo(
                f"  [DRY RUN] live degree check requires the sales_nav backend "
                f"(legacy scrape writes the production GSheet); skipping "
                f"{len(to_send_stale)} stale profile(s) — {_unresolved_note}."
            )
            return to_send_data, []
        if _scrape_writes_sheet and not os.environ.get("GSHEET_DRYRUN_ID"):
            click.echo(
                f"  [DRY RUN] live multi-profile scrape needs a sandbox sheet "
                f"(set GSHEET_DRYRUN_ID) so the preview never writes the "
                f"production autoconnect sheet; skipping {len(to_send_stale)} "
                f"stale profile(s) — {_unresolved_note}."
            )
            return to_send_data, []

    # PR-15 codepath split — Pattern-A flip carve-out from STRICT.
    # If there are no stale URLs to scrape, ALL operations from here on
    # are cache_hit_flip work — no scraper-id is needed. If there ARE
    # stale URLs, we're on the send_invite path and STRICT applies.
    codepath = "send_invite" if to_send_stale else "cache_hit_flip"
    if codepath == "send_invite" and not scraper_id_for_strict:
        if _strict_mode_enabled():
            raise ConfigError(
                f"{scraper_arg_name} is required when "
                f"{STRICT_PRE_INVITE_DEGREE_CHECK_ENV}=true (default) and the "
                f"send_invite codepath needs to scrape {len(to_send_stale)} "
                f"profile(s) via backend={backend!r}. Set the env var to "
                f"'false' to bypass for debugging only — silent bypass risks "
                f"re-inviting already-1st-degree connections (§3.1)."
            )
        # PR-15 fold-in (salesman-daily-QA-build15 BLOCKING B1):
        # STRICT=false MUST still be loud. The pre-PR-15 code echoed a
        # ⚠ warning at the daily_check.py call site; PR-15 deleted that
        # warning when it moved the gate into pre_invite_check. Restore
        # operator-visible signal here so the silent-bypass §0 #9
        # violation doesn't re-emerge behind the STRICT=false escape
        # hatch. Loud-by-design even in debug mode.
        click.echo(
            f"  ⚠ STRICT_PRE_INVITE_DEGREE_CHECK=false + no {scraper_arg_name} "
            f"+ {len(to_send_stale)} stale URL(s) → DEGREE CHECK SKIPPED. "
            f"Risk of re-inviting already-1st-degree connections (§3.1).",
            err=True,
        )

    # PR-B.10: scraper-mode audit log line. Self-describing daily-run logs
    # during the regular → sales_nav rollout; sha-prefix the scraper-id so
    # operators can sanity-check without leaking the full ID into logs.
    if scraper_id_for_strict:
        sid_prefix = hashlib.sha256(
            scraper_id_for_strict.encode()
        ).hexdigest()[:8]
        click.echo(
            f"  pre-invite backend: {backend} "
            f"(scraper_id_prefix={sid_prefix})"
        )

    degree_lookup: dict[str, str] = dict(cached_results)
    extras: dict[str, dict] = {}
    scrape_container_id: str = ""
    # PR-B.9 fold-in: see comment block at the scrape-time emit site below
    # for why this lives at function scope (must be visible to the
    # partition loop even when to_send_stale is empty).
    csv_miss_emitted_for_record_ids: set[str] = set()

    if to_send_stale and scraper_id_for_strict:
        our_urls = {_normalize_linkedin_url(p["linkedInUrl"]) for p in to_send_stale}

        if backend == "sales_nav":
            # PR-B.5: Sales Nav launch via helper that handles the
            # saved-args + identities-inject contract (verified
            # 2026-05-25). Returns extras dict with hasPendingInvitation
            # per prospect so the §3.1-hardened partition can apply the
            # stronger gate.
            conflicting_urls: set[str] = set()
            try:
                container_id, scraped_lookup, extras = _launch_sales_nav_scrape(
                    pb,
                    scraper_id_for_strict,
                    [p["linkedInUrl"] for p in to_send_stale],
                    dry_run=dry_run,
                    identity_universe=our_urls,
                    conflicting_urls=conflicting_urls,
                )
            except DryRunSandboxSheetUnreachable as exc:
                # Dry-run only (the helper re-raises raw when wet). Nothing was
                # launched, so degrade to the same skip-and-preview as the
                # missing-sandbox gate above: the operator still gets the
                # would-invite rows instead of a traceback.
                click.echo(
                    f"  ⚠ [DRY RUN] {exc}; skipping the degree scrape for "
                    f"{len(to_send_stale)} stale profile(s) — {_unresolved_note}. "
                    f"If deleted/unshared: recreate the sandbox sheet (or share "
                    f"it with the agent's Google account), then update "
                    f"GSHEET_DRYRUN_ID in .env.",
                    err=True,
                )
                return to_send_data, []
            scrape_container_id = container_id
            if not scraped_lookup and not container_id:
                # Both empty => PB failure path (helper already logged).
                # Drop the whole batch — fail-safe.
                return [], []
            if not scraped_lookup and container_id and to_send_stale:
                # Completion alone does not establish usable batch output.
                # A self-profile probe cannot distinguish missing CSV, failed
                # URL matching, and authentication trouble for this batch.
                click.echo(
                    f"  ⚠ Sales Nav scrape returned 0 degrees for "
                    f"{len(to_send_stale)} profile(s) from container {container_id}. "
                    "Inspect this container's output, CSV retrieval and URL matches; "
                    "this does not establish an expired cookie. Candidates remain held.",
                    err=True,
                )
            elif scraped_lookup:
                # Bug-2 (2026-06-15): the Sales Nav phantom intermittently
                # omits 1-3 of every ~25 requested rows from its result CSV
                # (observed csv_row_count 22-24 / 25). A requested URL with no
                # scraped degree falls to the blank/missing arm of the
                # partition below, opens a `degree_unknown` queue row, stays
                # at PROSPECT and silently re-queues on the NEXT run — the same
                # row can churn for days without ever being invited. Re-scrape
                # ONLY the still-missing rows once and merge the recovered
                # degrees before the partition (and its escalations) run.
                #
                # Bounded to a single retry — mirrors the timeout-retry-once
                # convention in _launch_sales_nav_scrape — so a phantom that is
                # systematically dropping rows can never spin the daily run.
                # Gated on `scraped_lookup` being non-empty: a wholesale-0
                # result has no usable degree evidence; its cause is uncertain.
                # Hold that batch for inspection rather than blindly relaunching.
                terminal_unavailable = {
                    url for url, evidence in extras.items()
                    if evidence.get("profile_unavailable") is True
                    and evidence.get("source_container_id") == container_id
                }
                missing_norm = our_urls - set(scraped_lookup) - terminal_unavailable
                if terminal_unavailable:
                    click.echo(
                        f"  Holding {len(terminal_unavailable)} explicitly unavailable profile(s) "
                        f"from container {container_id}; no immediate retry.", err=True,
                    )
                if missing_norm:
                    missing_urls = [
                        p["linkedInUrl"] for p in to_send_stale
                        if _normalize_linkedin_url(p["linkedInUrl"]) in missing_norm
                    ]
                    # Loud in BOTH modes: a recurring shortfall is a
                    # phantom-health signal the operator must see even in a
                    # preview run.
                    click.echo(
                        f"  ⚠ Sales Nav scrape returned {len(scraped_lookup)} of "
                        f"{len(to_send_stale)} requested degree(s) — "
                        f"{len(missing_urls)} row(s) missing from the result CSV.",
                        err=True,
                    )
                    if dry_run:
                        # Preview only — don't burn a second live scrape. The
                        # missing rows show as unknown in the partition below;
                        # a wet run re-scrapes and recovers most of them. (The
                        # silent-requeue bug is wet-only: dry-run commits no
                        # stage change and the CSV-miss escalation below is
                        # itself gated `not dry_run`.)
                        click.echo(
                            "  (dry-run: skipping the bounded re-scrape; wet runs "
                            "retry the missing row(s) once before escalating.)",
                            err=True,
                        )
                    else:
                        click.echo(
                            "  Re-scraping the missing row(s) once before escalating.",
                            err=True,
                        )
                        retry_cid, retry_lookup, retry_extras = _launch_sales_nav_scrape(
                            pb,
                            scraper_id_for_strict,
                            missing_urls,
                            identity_universe=our_urls,
                            conflicting_urls=conflicting_urls,
                        )
                        scraped_lookup.update(retry_lookup)
                        extras.update(retry_extras)
                        # Retry conflicts may implicate a candidate whose
                        # first launch had looked valid. Withdraw both signals.
                        for conflicted in conflicting_urls:
                            scraped_lookup.pop(conflicted, None)
                            extras.pop(conflicted, None)
                        still_missing = our_urls - set(scraped_lookup)
                        recovered = len(missing_norm & set(scraped_lookup))
                        # Rows still missing after the retry fall through to the
                        # per-row `degree_unknown` escalation below — NOT
                        # swallowed either way.
                        if not retry_cid and not retry_lookup:
                            # The retry LAUNCH itself failed/timed out:
                            # _launch_sales_nav_scrape returns ("", {}, {}) and
                            # has already echoed the cause above. Call this out
                            # distinctly — otherwise the "recovered 0" line below
                            # reads like the phantom persistently dropping these
                            # specific rows, when the whole second launch died.
                            click.echo(
                                f"  ⚠ Sales Nav re-scrape launch did not complete "
                                f"(see the error above) — {len(still_missing)} "
                                f"row(s) escalating as degree_unknown below.",
                                err=True,
                            )
                        else:
                            # Completed retry (container present). Surface the
                            # retry container id so the operator can pull THIS
                            # launch's PB log for the rows still missing — the
                            # CSV-miss escalation below records only the first
                            # scrape's container.
                            click.echo(
                                f"  Sales Nav re-scrape (container {retry_cid or '?'}) "
                                f"recovered {recovered} of {len(missing_norm)} missing "
                                f"degree(s); {len(still_missing)} still missing"
                                f"{' (degree_unknown rows opened below)' if still_missing else ''}.",
                                err=True,
                            )
        else:
            # Legacy regular Profile Scraper path — DOCUMENTED-DEAD since
            # 2026-06-12: the legacy agent was deleted from the PB workspace,
            # so backend=regular (now explicit-only; default is sales_nav)
            # requires deploying a NEW phantom first. Preflight before the
            # sheet write so a dead agent id fails as a config error instead
            # of a raw httpx 404 mid-launch. Launch logic itself is kept
            # unchanged (plan PR-B subtask 5 rollback parity) for a future
            # re-deployed phantom.
            preflight_legacy_profile_scraper(pb, scraper_id_for_strict)
            sheet_rows = [{"profileUrl": p["linkedInUrl"]} for p in to_send_stale]
            sheet_url = _dc.write_prospects_to_sheet(
                sheet_rows, columns=["profileUrl"], include_header=False
            )
            launch = pb.launch_agent(scraper_id_for_strict, {
                "spreadsheetUrl": sheet_url,
                **_dc._pb_session_args(),
                # 2026-06-10 cap-trickle fix (legacy mirror of the sales_nav
                # helper's numberOfProfilesPerLaunch): PB API launches that
                # pass `arguments` REPLACE the phantom's saved console
                # argument wholesale, so any console-saved per-launch cap
                # never applied — the phantom fell back to its built-in
                # default (10) and silently truncated bigger batches, which
                # here surfaces as degree_unknown misses on rows 11+.
                # Explicit count matches the headerless scraper sheet.
                "numberOfProfilesPerLaunch": len(to_send_stale),
            })
            # Drop-the-batch on failure/timeout, mirroring the no-CSV branch
            # below: pre-fix the per-launch cap truncated every batch to 10
            # rows and the 600s ceiling was effectively unreachable; a full
            # batch now scrapes end-to-end, so an uncaught PBRunTimeout here
            # would crash the whole daily run before any invite is sent.
            # No invites without a degree check (§3.1) — retry tomorrow.
            try:
                pb.wait_for_completion(launch, poll_interval=10, max_wait=600)
            except (PBRunFailed, PBRunTimeout) as exc:
                click.echo(
                    f"  ⚠ Pre-invite degree check scrape "
                    f"{'failed' if isinstance(exc, PBRunFailed) else 'timed out'} "
                    f"({exc}) — DROPPING invite batch to avoid re-inviting "
                    f"already-connected people. Will retry tomorrow.",
                    err=True,
                )
                return [], []
            scrape_container_id = str(getattr(launch, "container_id", "") or "")

            # F-PR-5: CSV keyed to launch.container_id, not "latest".
            result_csv = pb.download_result_csv(launch)
            if not result_csv:
                click.echo(
                    "  ⚠ Pre-invite degree check returned no CSV — DROPPING invite batch "
                    "to avoid re-inviting already-connected people. Will retry tomorrow.",
                    err=True,
                )
                return [], []

            scraped_lookup = {}
            our_urls_by_id = linkedin_identity_map(our_urls)
            for row in csv.DictReader(io.StringIO(result_csv)):
                url = (
                    row.get("linkedin_url", "")
                    or row.get("linkedinProfileUrl", "")
                    or row.get("profileUrl", "")
                    or row.get("query", "")
                )
                degree = (row.get("connectionDegree") or "").strip()
                if not url or not degree:
                    continue
                # Exact match primary; slug-variant echo bridges to OUR url
                # form via the profile-id (see _launch_sales_nav_scrape).
                norm = resolve_identity_match(
                    _normalize_linkedin_url(url), our_urls, our_urls_by_id
                )
                if norm:
                    scraped_lookup[norm] = degree
        degree_lookup.update(scraped_lookup)

        # PR-15: partial-CSV signal. When the scrape returned fewer
        # rows than requested, open ONE `degree_unknown` queue row
        # PER MISSING PROSPECT so the operator can triage per-row.
        # Aggregated/summary writes are forbidden — PR-17's run-end
        # summary writes the `degree_unknown_count` attribute (the
        # producer-consumer pair per Round-4 D12).
        #
        # PR-B.5: backend-aware failure_reason — closes adversarial F10
        # (operator confusion when distinct miss types share an opaque
        # reason string). `_emit_degree_unknown` helper (above) bundles
        # the contract including the new `failure_reason` field.
        # PR-B.9 fold-in: track record_ids that emitted a scrape-time
        # CSV_MISS so the partition-time BLANK-degree arm below can
        # suppress its redundant emit. Function-local set declared at
        # function scope above (must be visible when to_send_stale is
        # empty). DO NOT promote to module/class state — cross-run
        # pollution would silently suppress real escalations and
        # re-introduce §3.1 risk (adversarial review concern on the (a)
        # fix design). Suppression is consulted ONLY in the sales_nav
        # BLANK-degree path of the partition loop — Pattern-A flip-fail
        # (degree=="1st" arm) is unaffected because it never reads this
        # set.
        # `not dry_run`: the scrape-time CSV-miss opens an operator-review row
        # per missing prospect — a write. In dry-run these missing rows fall to
        # the partition loop's blank-degree branch (→ dry_unknown preview),
        # which `continue`s before the suppression set is consulted, so skipping
        # this block entirely is correct.
        if not dry_run and len(scraped_lookup) < len(to_send_stale):
            today_iso_for_miss = (today_op or date.today()).isoformat()
            miss_reason = (
                REASON_SALES_NAV_CSV_MISS if backend == "sales_nav"
                else REASON_LEGACY_CSV_MISS
            )
            for p in to_send_stale:
                norm = _normalize_linkedin_url(p["linkedInUrl"])
                if norm in scraped_lookup:
                    continue
                _emit_degree_unknown(
                    p,
                    attio=attio,
                    today_iso=today_iso_for_miss,
                    failure_reason=miss_reason,
                    scrape_attempt_id=scrape_container_id,
                    csv_row_count_observed=len(scraped_lookup),
                    csv_row_count_expected=len(to_send_stale),
                )
                # Only used to suppress the sales_nav BLANK-degree partition
                # arm. Legacy backend does NOT consult this set (legacy keeps
                # its bit-for-bit pre-PR-B behaviour for rollback parity).
                if backend == "sales_nav" and p.get("record_id"):
                    csv_miss_emitted_for_record_ids.add(str(p["record_id"]))

        # Record fresh scrape results in the cache, keyed by original URL.
        recheck_cache.record_many({
            p["linkedInUrl"]: scraped_lookup.get(_normalize_linkedin_url(p["linkedInUrl"]))
            for p in to_send_stale
            if scraped_lookup.get(_normalize_linkedin_url(p["linkedInUrl"]))
        })

    still_to_invite: list[dict] = []
    already_connected: list[dict] = []
    failed_to_flip: list[dict] = []
    # 2026-07-02 Álex RCA: 1st-degree rows quarantined out of the Pattern-A
    # flip because they only became prospects within PATTERN_A_QUARANTINE_DAYS
    # (suspected URL-variant duplicates). Surfaced in the summary.
    pattern_a_quarantined: list[dict] = []
    # Memo for `_prior_cadence_entries_for_url` — the pipeline-entry index is
    # built lazily on the first quarantine hit and reused for the rest of the
    # run (2026-08-18 cadence-leak fix).
    prior_cadence_cache: dict = {}
    # Wave-1.6.2 FIX-B (adversarial EXT-SB-3 IMPORTANT): tally failed
    # `experiment_id_immutability_violation` escalate() calls so the
    # end-of-function summary surfaces them. Mirrors FIX-A at
    # daily_check.py.
    escalate_failed_count = 0
    # Wave-1.6.3 (adversarial follow-up to FIX-B): tally failed
    # `degree_unknown` escalate() calls in the Pattern-A flip-fail
    # branch. Separate from the immutability tally above so the
    # end-of-function summary names the right escalate type for the
    # operator. See the try/except at the escalate site for the
    # orphan-prevention rationale.
    flip_fail_escalate_failed_count = 0
    # A2 telemetry: count successful Pattern-A pending→CONNECTION_SENT flips
    # (the heal-path volume). `already_connected` already tallies the
    # 1st-degree→ACCEPTED flips; `failed_to_flip` tallies the failures.
    pending_flipped_count = 0
    today_iso = (today_op or date.today()).isoformat()
    # Dry-run preview-only buckets (never returned; surfaced in the dry-run
    # summary so the operator sees why these rows are NOT in the invite queue).
    dry_pending: list[dict] = []
    dry_oon: list[dict] = []
    dry_unknown: list[dict] = []
    dry_quarantined: list[dict] = []
    # AttioWriter is only used on the wet path — every write site below is
    # gated on `not dry_run` (dry-run rows `continue` before reaching .apply),
    # so we don't even construct it in dry-run. Keeps "dry-run writes nothing"
    # literally true at the AttioWriter boundary.
    writer = AttioWriter(attio=attio) if not dry_run else None
    for row in to_send_data:
        norm = _normalize_linkedin_url(row["linkedInUrl"])
        degree = degree_lookup.get(norm, "")
        if degree == "1st":
            if dry_run:
                # Preview must match the wet run (finding 6): evaluate the same
                # Pattern-A recency quarantine here so a row the wet run would
                # QUARANTINE is not mislabeled as would-flip-ACCEPTED. No write,
                # no escalate — preview only.
                if _prospect_committed_within_days(
                    row.get("prospect_committed_at"),
                    today=today_op,
                    days=PATTERN_A_QUARANTINE_DAYS,
                ):
                    committed_raw = row.get("prospect_committed_at")
                    try:
                        committed_date = date.fromisoformat(
                            str(committed_raw)[:10]
                        )
                        age_days = (today_op - committed_date).days
                        age_str = f"{age_days}d ago"
                    except ValueError:
                        age_str = "recently"
                    dry_quarantined.append(row)
                    click.echo(
                        f"  [DRY RUN] would QUARANTINE {row['linkedInUrl']} "
                        f"(committed {age_str}) — suspected URL-variant "
                        f"duplicate; would NOT flip to ACCEPTED, would escalate."
                    )
                    continue
                # Classify as already-connected (would flip to ACCEPTED, not
                # invited) without the AttioWriter flip or any escalate.
                # Decoupled from write success so the preview partition is
                # accurate.
                already_connected.append(row)
                continue
            # Pattern-A recency quarantine (2026-07-02 Álex RCA). A 1st-degree
            # row that only became a prospect within PATTERN_A_QUARANTINE_DAYS
            # is a suspected URL-variant duplicate — flipping it to ACCEPTED
            # would re-start a cadence on someone who already completed one.
            # Skip today (no invite, stage unchanged) and escalate for triage.
            # A missing/unparseable timestamp is NOT quarantined: old records
            # must keep the legitimate silent-acceptance Pattern-A flip.
            if _prospect_committed_within_days(
                row.get("prospect_committed_at"),
                today=today_op,
                days=PATTERN_A_QUARANTINE_DAYS,
            ):
                pattern_a_quarantined.append(row)
                click.echo(
                    f"  ⚠ Pattern-A quarantine: {row['linkedInUrl']} "
                    f"(record_id={row.get('record_id')!r}) became a prospect "
                    f"within {PATTERN_A_QUARANTINE_DAYS}d and is already "
                    f"1st-degree — suspected URL-variant duplicate. NOT "
                    f"flipping to ACCEPTED; escalating for operator review.",
                    err=True,
                )
                if row.get("record_id"):
                    # 2026-08-18 cadence-leak fix: surface pipeline entries
                    # whose URL shares the profile-id suffix so the operator
                    # sees any prior cadence (e.g. "DM3 Sent since 06-01")
                    # BEFORE judging the quarantine a real accept.
                    prior_entries, prior_err = _prior_cadence_entries_for_url(
                        attio, list_id, row["linkedInUrl"],
                        str(row.get("entry_id") or ""), prior_cadence_cache,
                    )
                    quarantine_payload: dict = {
                        "record_id": str(row["record_id"]),
                        "entry_id": str(row.get("entry_id") or ""),
                        "linkedin_url": row["linkedInUrl"],
                        "name": str(row.get("name") or ""),
                        "company": str(row.get("company") or ""),
                        "prospect_committed_at": str(
                            row.get("prospect_committed_at") or ""
                        ),
                        "degree": "1st",
                        "prior_cadence_entries": prior_entries,
                    }
                    if prior_err:
                        quarantine_payload["prior_cadence_lookup_error"] = prior_err
                    if prior_entries:
                        click.echo(
                            f"      ↳ {len(prior_entries)} existing pipeline "
                            f"entr{'y' if len(prior_entries) == 1 else 'ies'} "
                            f"share this profile-id: "
                            + "; ".join(
                                f"{m['linkedin_url']} at {m['stage'] or '?'}"
                                for m in prior_entries[:3]
                            ),
                            err=True,
                        )
                    try:
                        escalate(
                            type="pattern_a_suspected_duplicate",
                            idempotency_key=str(row["record_id"]),
                            payload=quarantine_payload,
                            attio=attio,
                        )
                    except Exception as esc_exc:  # noqa: BLE001 — never block the batch
                        click.echo(
                            f"  ⚠ escalate(pattern_a_suspected_duplicate) failed "
                            f"for record_id={row.get('record_id')!r} "
                            f"[{type(esc_exc).__name__}]: {esc_exc}. Row still "
                            f"held out of invite + flip; continuing batch.",
                            err=True,
                        )
                continue
            # PR-21 (Lesson 6): use direct key access — KeyError propagates
            # as a caller bug. `run_connection_requests` MUST populate both
            # keys on every `to_send_data` row. See module docstring contract.
            prior_experiment_id = row["experiment_id"]
            prior_frozen_at = row["experiment_id_frozen_at"]

            # PR-21 immutability guard. "prospect" → "accepted" is the
            # Pattern-B case (Mat connected externally on a never-invited
            # row) — explicitly allowed. See `_check_experiment_id_immutability`
            # docstring for the full carve-out rationale.
            #
            # Wave-1.6.2 FIX-B (adversarial EXT-SB-3 IMPORTANT): the
            # pre-Wave-1.6.2 raise here was the same orphan-risk pattern
            # FIX-A closed in daily_check.py. If the guard raises
            # `ExperimentIdImmutableError` for one 1st-degree row, the
            # surrounding `for row in to_send_data:` loop terminates and
            # OTHER 1st-degree rows after this offender never get their
            # Pattern-A flip to ACCEPTED — they stay at PROSPECT and
            # become eligible to re-invite tomorrow (§3.1 violation).
            #
            # Behavior now: preserve prior cohort tag (it's already what
            # we pass as `new_experiment_id`, so the data layer is safe),
            # open an `experiment_id_immutability_violation` queue row,
            # AND still perform the Pattern-A flip below with the prior
            # experiment_id preserved. Continue processing the batch.
            try:
                _check_experiment_id_immutability(
                    record_id=row["record_id"],
                    entry_id=row.get("entry_id"),
                    prior_frozen_at=prior_frozen_at,
                    new_frozen_at="accepted",
                    prior_experiment_id=prior_experiment_id,
                    new_experiment_id=prior_experiment_id,
                )
            except ExperimentIdImmutableError as imm_exc:
                # The guard only raises when prior_frozen_at is in the
                # terminal set AND new_frozen_at differs. Here new is
                # always "accepted"; the only way to hit this is a row
                # whose prior_frozen_at is "connection_sent" or another
                # terminal state we'd be overwriting. Escalate-and-continue
                # so other rows process; the Pattern-A flip below still
                # fires with the prior experiment_id preserved.
                try:
                    escalate(
                        type="experiment_id_immutability_violation",
                        idempotency_key=(
                            f"experiment-id-immutability-pattern-a"
                            f"|{row.get('record_id') or ''}"
                            f"|{prior_frozen_at}"
                            f"|accepted"
                        ),
                        payload={
                            "record_id": str(row.get("record_id") or ""),
                            "entry_id": row.get("entry_id") or "",
                            "prior_experiment_id": prior_experiment_id,
                            "current_experiment_id": prior_experiment_id,
                            "effective_experiment_id": prior_experiment_id,
                            "context": (
                                "pre_invite_check.Pattern-A flip: 1st-degree "
                                f"row with prior_frozen_at={prior_frozen_at!r} "
                                "tried to overwrite to 'accepted'. Preserving "
                                "prior experiment_id; row still flips to "
                                "ACCEPTED so it is not re-invited tomorrow."
                            ),
                        },
                        attio=attio,
                    )
                except Exception as esc_exc:  # noqa: BLE001 — see FIX-A rationale
                    escalate_failed_count += 1
                    click.echo(
                        f"  ⚠ escalate(experiment_id_immutability_violation) "
                        f"failed for record_id={row.get('record_id')!r} "
                        f"during Pattern-A flip "
                        f"[{type(esc_exc).__name__}]: {esc_exc}. "
                        f"Continuing batch — Pattern-A flip still fires.",
                        err=True,
                    )
                click.echo(
                    f"  ⚠ experiment_id immutability violation on Pattern-A "
                    f"flip for record_id={row['record_id']!r} "
                    f"(prior_frozen_at={prior_frozen_at!r} → 'accepted'): "
                    f"{imm_exc}. Preserving prior experiment_id; flip still "
                    f"fires.",
                    err=True,
                )

            # Build updates dict. Only stamp experiment_id_frozen_at when
            # we have a prior experiment_id to associate it with.
            updates: dict = {
                "stage": PipelineStage.ACCEPTED.value,
                "last_contact_date": today_iso,
            }
            if prior_experiment_id is not None:
                # Preserve existing experiment_id; stamp frozen_at="accepted".
                updates["experiment_id_frozen_at"] = "accepted"
            # If prior_experiment_id is None (row had no active experiment at
            # PROSPECT-commit), leave frozen_at absent — don't stamp NULL or
            # a spurious "accepted" with no associated experiment.

            # PR-15 (B-SD-010) atomic Pattern-A flip. Single-PATCH
            # multi-attribute write via F-PR-4's AttioWriter — stage,
            # last_contact_date, and (when present) experiment_id_frozen_at
            # all succeed or all fail. PR-21 widens the updates dict above.
            # A1 (duplicate-entry fix): a dedup-merged prospect carries every
            # associated list entry in `entry_ids` (injected by
            # _dedupe_by_linkedin_url before this function runs). Flip ALL of
            # them — a duplicate left at PROSPECT re-leaks into tomorrow's
            # invite pool. Fall back to the singular `entry_id` when entry_ids
            # is absent. The PR-15 atomicity contract (stage + last_contact_date
            # + frozen_at succeed/fail together) holds PER ENTRY; a partial
            # failure routes the whole row to failed_to_flip so no invite leaks.
            flip_exc: Exception | None = None
            for entry_id in row.get("entry_ids") or [row.get("entry_id")]:
                if not entry_id:
                    continue
                try:
                    assert writer is not None
                    writer.apply(WriteIntent(
                        object="linkedin_outreach",
                        record_id=entry_id,
                        updates=updates,
                        prior_values={
                            # Pre-flip stage is the row's current stage from
                            # the upstream filter (PROSPECT or CONNECTION_SENT
                            # depending on caller). Pass the value the row
                            # actually has so monotonicity check passes.
                            "stage": str(row.get("current_stage", PipelineStage.PROSPECT.value)),
                        },
                        writer_module="workflows.pre_invite_check._pre_invite_degree_check",
                        is_list_entry=True,
                        list_id=list_id,
                    ))
                except (AttioError, UnauthorizedAttioWriteError,
                        httpx.HTTPStatusError, httpx.RequestError,
                        ConnectionError, TimeoutError) as exc:
                    # PR-21 (Lesson 5): narrow to Attio/transient exceptions.
                    # KeyError, AttributeError, TypeError propagate as code bugs.
                    # PR-15 fold-in (silent-failure-hunter BLOCKING B1):
                    # UnauthorizedAttioWriteError extends PermissionError not
                    # AttioError; widened catch keeps batch processing alive.
                    flip_exc = exc
                    click.echo(
                        f"  ⚠ Pattern-A flip failed for {row['linkedInUrl']} "
                        f"(record_id={row['record_id']!r}, entry_id={entry_id!r}) "
                        f"({type(exc).__name__}): {exc}; skipping invite to be safe",
                        err=True,
                    )
            if flip_exc is None:
                click.echo(
                    f"  [PR-21] Pattern-A flip: record_id={row['record_id']!r} "
                    f"entry_ids={row.get('entry_ids') or [row.get('entry_id')]!r} "
                    f"url={row['linkedInUrl']} "
                    f"experiment_id={prior_experiment_id!r} frozen_at: "
                    f"{prior_frozen_at!r} → 'accepted'",
                    err=True,
                )
                already_connected.append(row)
            else:
                # At least one entry failed to flip. Keep the prospect OUT of
                # the invite pool (never fall through to still_to_invite —
                # the §3.1 brand-event failure) and surface for triage.
                # `flip_exc` carries the last failure for the escalate payload.
                failed_to_flip.append(row)
                # PR-15 fold-in (silent-failure-hunter IMPORTANT I1):
                # Emit an Operator Review Queue row so operator can triage.
                if row.get("record_id"):
                    # Wave-1.6.3 (adversarial follow-up to FIX-B): if this
                    # escalate raises (e.g. Attio degraded enough that the
                    # queue write also fails after retries), the propagation
                    # kills the outer `for row in to_send_data:` loop and
                    # subsequent rows — including degree='2nd'/'3rd' rows in
                    # the else branch — never get appended to still_to_invite.
                    # NOT a §3.1 re-invite (those rows just go un-invited
                    # this run), but a §3 #9 silent batch-truncation. Catch
                    # is intentionally broad because the alternative is
                    # silent truncation; tally for end-of-function summary.
                    try:
                        escalate(
                            type="degree_unknown",
                            idempotency_key=(
                                f"pattern-a-flip-fail|{row['entry_id']}"
                                f"|{(today_op or date.today()).isoformat()}"
                            ),
                            payload={
                                "record_id": str(row["record_id"]),
                                "linkedin_url": row["linkedInUrl"],
                                "last_known_degree": "1st",
                                "scrape_attempt_id": f"flip-fail-{type(flip_exc).__name__}",
                                "requested_at": datetime.now(UTC).isoformat(),
                                "csv_row_count_observed": 0,
                                "csv_row_count_expected": 1,
                            },
                            attio=attio,
                        )
                    except Exception as esc_exc:  # noqa: BLE001 — see comment above
                        flip_fail_escalate_failed_count += 1
                        click.echo(
                            f"  ⚠ escalate(degree_unknown) for Pattern-A "
                            f"flip-fail failed for "
                            f"record_id={row['record_id']!r} "
                            f"entry_id={row.get('entry_id')!r} "
                            f"[{type(esc_exc).__name__}]: {esc_exc}. "
                            f"Continuing batch — subsequent rows still "
                            f"evaluated. End-of-function summary will "
                            f"surface this failure.",
                            err=True,
                        )
                # Don't fall back to inviting — that's the bug we're fixing.
        else:
            # PR-B.5 / PR-B.13 / PR-B.16 — §3.1 SAFETY
            # ============================================================
            # Do not change the default arm of this partition without
            # reviewing the migration plan
            # the internal migration plan (not shipped).
            #
            # The §3.1 contract: "zero re-sends, zero re-invites of
            # already-1st-degree connections." Three adversarial review
            # findings (F1, F2, F4) all collapsed to the same root cause:
            # the pre-PR-B default arm sent any non-"1st" prospect to
            # `still_to_invite` — including blank degrees, missing CSV
            # rows, unknown values, and rows where LinkedIn ALREADY shows
            # a pending invitation from us. PR-B's Sales Nav branch
            # narrows the default arm to escalate-and-drop; ONLY explicit
            # "2nd" or "3rd" routes to invite.
            #
            # PR-B.16 (NEW from CSV wet-probe 2026-05-25): Sales Nav CSV
            # exposes `hasPendingInvitation="true|false"`. When true,
            # LinkedIn already received an invite from us — re-inviting
            # is the brand-event failure mode regardless of degree.
            # ============================================================
            if backend == "sales_nav":
                row_extras = extras.get(norm, {})
                has_pending = row_extras.get("hasPendingInvitation") == "true"
                if has_pending:
                    if dry_run:
                        dry_pending.append(row)
                        continue
                    # Pattern-A (pending): LinkedIn already holds our invite.
                    # The pre-PR-B.16 behavior only escalate-and-dropped, which
                    # left the row at PROSPECT to re-queue forever (the invite
                    # is re-prepared every run, PB dedups it, the advance gate
                    # never frees it). FLIP it to CONNECTION_SENT so it leaves
                    # the invite pool and Phase 0 watches for acceptance —
                    # mirroring the 1st-degree→ACCEPTED Pattern-A flip above.
                    pending_experiment_id = row.get("experiment_id")
                    pending_prior_frozen_at = row.get("experiment_id_frozen_at")
                    pending_updates: dict = {
                        "stage": PipelineStage.CONNECTION_SENT.value,
                        "last_contact_date": today_iso,
                    }
                    # Re-stamp connection_sent only when the row already carried
                    # an experiment AND its prior frozen_at is not terminal —
                    # re-stamping over accepted/legacy_* is an immutability
                    # violation (AttioWriter enforces stage monotonicity but NOT
                    # frozen_at immutability, so this is the guard).
                    if (
                        pending_experiment_id is not None
                        and pending_prior_frozen_at not in _IMMUTABLE_FROZEN_AT_VALUES
                    ):
                        pending_updates["experiment_id_frozen_at"] = "connection_sent"
                    # A1 (duplicate-entry fix): flip ALL of the prospect's list
                    # entries (entry_ids), not just the first. A duplicate left
                    # at PROSPECT is re-selected next run — the exact daily
                    # re-selection leak. Each entry's write is independently
                    # transient-guarded; ANY failure routes the whole row to
                    # failed_to_flip (leave at PROSPECT, retry next run, never
                    # fall through to still_to_invite — the §3.1 brand-event).
                    pending_flip_exc: Exception | None = None
                    for entry_id in row.get("entry_ids") or [row.get("entry_id")]:
                        if not entry_id:
                            continue
                        try:
                            assert writer is not None
                            writer.apply(WriteIntent(
                                object="linkedin_outreach",
                                record_id=entry_id,
                                updates=pending_updates,
                                prior_values={
                                    "stage": str(
                                        row.get("current_stage", PipelineStage.PROSPECT.value)
                                    ),
                                },
                                writer_module="workflows.pre_invite_check._pre_invite_degree_check",
                                is_list_entry=True,
                                list_id=list_id,
                            ))
                        except (AttioError, UnauthorizedAttioWriteError,
                                httpx.HTTPStatusError, httpx.RequestError,
                                ConnectionError, TimeoutError) as exc:
                            pending_flip_exc = exc
                            click.echo(
                                f"  ⚠ pending-flip failed for {row['linkedInUrl']} "
                                f"(record_id={row['record_id']!r}, "
                                f"entry_id={entry_id!r}) "
                                f"({type(exc).__name__}): {exc}; leaving at PROSPECT "
                                f"(will retry next run)",
                                err=True,
                            )
                    if pending_flip_exc is None:
                        pending_flipped_count += 1
                        click.echo(
                            f"  [Pattern-A pending] flip PROSPECT→CONNECTION_SENT: "
                            f"record_id={row['record_id']!r} "
                            f"entry_ids={row.get('entry_ids') or [row.get('entry_id')]!r} "
                            f"url={row['linkedInUrl']} (hasPendingInvitation)",
                            err=True,
                        )
                    else:
                        # Transient/registry failure on at least one entry: leave
                        # at PROSPECT and record for retry + operator triage.
                        # `pending_flip_exc` carries the last failure for escalate.
                        failed_to_flip.append(row)
                        # Durable operator-review row (mirrors the 1st-degree
                        # flip-fail path) so the failure survives beyond the
                        # ephemeral stderr line. Best-effort: a failed escalate
                        # must not crash the batch (tallied for the summary).
                        if row.get("record_id"):
                            try:
                                escalate(
                                    type="degree_unknown",
                                    idempotency_key=(
                                        f"pending-flip-fail|{row['entry_id']}"
                                        f"|{(today_op or date.today()).isoformat()}"
                                    ),
                                    payload={
                                        "record_id": str(row["record_id"]),
                                        "linkedin_url": row["linkedInUrl"],
                                        "last_known_degree": degree,
                                        "scrape_attempt_id":
                                            f"pending-flip-fail-{type(pending_flip_exc).__name__}",
                                        "requested_at": datetime.now(UTC).isoformat(),
                                        "csv_row_count_observed": 0,
                                        "csv_row_count_expected": 1,
                                    },
                                    attio=attio,
                                )
                            except Exception as esc_exc:  # noqa: BLE001
                                flip_fail_escalate_failed_count += 1
                                click.echo(
                                    f"  ⚠ escalate(degree_unknown) for "
                                    f"pending-flip-fail failed for "
                                    f"record_id={row['record_id']!r} "
                                    f"[{type(esc_exc).__name__}]: {esc_exc}.",
                                    err=True,
                                )
                elif degree in ("2nd", "3rd"):
                    still_to_invite.append(row)
                elif not degree:
                    if dry_run:
                        dry_unknown.append(row)
                        continue
                    # Missing CSV row OR cached as blank — no signal at all.
                    # PR-B.9 fold-in: suppress this BLANK-degree emit if the
                    # scrape-time CSV_MISS already fired for this record_id
                    # in this run. The CSV_MISS carries the richer payload
                    # (csv_row_count_observed/expected for fleet-vs-per-row
                    # diagnostics); emitting BOTH gives the operator two
                    # rows per missing prospect. Unanimous reviewer pick
                    # (adversarial + L3 + GTM, 2026-05-25). Pattern-A
                    # flip-fail path is unaffected — it lives in the
                    # degree=="1st" arm, never reads this set.
                    if (
                        row.get("record_id")
                        and str(row["record_id"]) in csv_miss_emitted_for_record_ids
                    ):
                        continue
                    _emit_degree_unknown(
                        row,
                        attio=attio,
                        today_iso=today_iso,
                        failure_reason=REASON_SALES_NAV_DEGREE_BLANK,
                        scrape_attempt_id=scrape_container_id,
                    )
                elif (
                    "out of network" in degree.strip().lower()
                    or degree.strip().lower() in ("oon", "out_of_network")
                ):
                    if dry_run:
                        dry_oon.append(row)
                        continue
                    # Wave-2-A: a genuinely Out-of-Network target — LinkedIn
                    # will not allow an invite from this account, ever. PR #150
                    # stops NEW OON at intake; this arm parks any pre-#150
                    # committed row (re-scraped here) or new slip-through at
                    # UNREACHABLE so it LEAVES the invite pool instead of being
                    # re-scraped + re-escalated every run (the wasted-scrape
                    # loop). The OON test mirrors the canonical one in
                    # weekly_prospect._connection_degree. Carved out of the
                    # catch-all `else` below ON PURPOSE: only a confirmed-OON
                    # degree is parked terminally; "You"/blank/garbage stay
                    # escalate-and-drop (transient scrape noise, retried).
                    # Still opens the degree_unknown queue row first for
                    # operator visibility (last_known_degree carries the OON
                    # value), then moves the stage.
                    _emit_degree_unknown(
                        row,
                        attio=attio,
                        today_iso=today_iso,
                        failure_reason=REASON_SALES_NAV_DEGREE_UNKNOWN,
                        last_known_degree=degree,
                        scrape_attempt_id=scrape_container_id,
                    )
                    if row.get("entry_id"):
                        try:
                            assert writer is not None
                            writer.apply(WriteIntent(
                                object="linkedin_outreach",
                                record_id=row["entry_id"],
                                updates={"stage": PipelineStage.UNREACHABLE.value},
                                prior_values={
                                    "stage": str(
                                        row.get("current_stage", PipelineStage.PROSPECT.value)
                                    ),
                                },
                                writer_module="workflows.pre_invite_check._pre_invite_degree_check",
                                is_list_entry=True,
                                list_id=list_id,
                            ))
                            click.echo(
                                f"  [Wave-2-A] OON → UNREACHABLE: "
                                f"record_id={row['record_id']!r} "
                                f"entry_id={row.get('entry_id')!r} "
                                f"url={row['linkedInUrl']} (degree={degree!r})",
                                err=True,
                            )
                        except (AttioError, UnauthorizedAttioWriteError,
                                httpx.HTTPStatusError, httpx.RequestError,
                                ConnectionError, TimeoutError) as exc:
                            # Park-fail: leave at PROSPECT (it simply won't be
                            # invited — the OON degree never routes to
                            # still_to_invite). The queue row above survives;
                            # the park retries on the next scrape. Never fall
                            # through to inviting an OON target.
                            click.echo(
                                f"  ⚠ OON→UNREACHABLE park failed for "
                                f"{row['linkedInUrl']} "
                                f"(record_id={row['record_id']!r}, "
                                f"entry_id={row.get('entry_id')!r}) "
                                f"({type(exc).__name__}): {exc}; leaving at "
                                f"PROSPECT (will retry next scrape).",
                                err=True,
                            )
                else:
                    if dry_run:
                        dry_unknown.append(row)
                        continue
                    # "You", any value the partition doesn't explicitly accept
                    # (and is not confirmed-OON above). Escalate, don't invite —
                    # transient scrape noise, retried next run.
                    _emit_degree_unknown(
                        row,
                        attio=attio,
                        today_iso=today_iso,
                        failure_reason=REASON_SALES_NAV_DEGREE_UNKNOWN,
                        last_known_degree=degree,
                        scrape_attempt_id=scrape_container_id,
                    )
            else:
                # Legacy backend: UNCHANGED for rollback parity. The §3.1
                # bug is real on the regular Profile Scraper path too, but
                # per plan PR-B.5 we preserve legacy bit-for-bit so a flag
                # flip back is a clean restoration of prior behavior. Once
                # Sales Nav has soaked for 28 days, cleanup PR-C can apply
                # the same §3.1-hardening to legacy or delete it entirely.
                #
                # Dry-run only reaches this arm via the cache_hit_flip path
                # (the scrape-safety gate returns early for legacy + stale),
                # where a non-1st cached degree would also be invited in wet —
                # so appending is correct. The explicit `if dry_run` makes the
                # dry-run contract locally enforced rather than dependent on a
                # gate ~600 lines up; it also keeps legacy from ever issuing a
                # write here (there is none today, but the guard is cheap).
                if dry_run:
                    still_to_invite.append(row)
                    continue
                still_to_invite.append(row)

    if failed_to_flip:
        click.echo(
            f"  ⚠ Pattern-A flip transient failures: {len(failed_to_flip)} row(s) "
            f"could not be flipped to ACCEPTED — "
            f"record_ids={[r['record_id'] for r in failed_to_flip]}",
            err=True,
        )

    if escalate_failed_count > 0:
        # Wave-1.6.2 FIX-B: surface swallowed escalate() failures so the
        # operator knows the queue is missing rows. ERROR level for
        # paging. See FIX-A in daily_check.py for the broader rationale.
        click.echo(
            f"  ❌ ERROR: {escalate_failed_count} "
            f"experiment_id_immutability_violation escalate() call(s) "
            f"failed during this Pattern-A batch. Flips still fired so "
            f"rows will not re-invite tomorrow, but the operator review "
            f"queue is missing those rows. See WARN log lines above.",
            err=True,
        )

    if flip_fail_escalate_failed_count > 0:
        # Wave-1.6.3: same calibration for the degree_unknown escalate in
        # the Pattern-A flip-fail branch. Per-row WARN already named each
        # failure; this is the paging-level rollup.
        click.echo(
            f"  ❌ ERROR: {flip_fail_escalate_failed_count} "
            f"degree_unknown escalate() call(s) failed during Pattern-A "
            f"flip-fail handling. The rows went into failed_to_flip (so "
            f"no invite leaks out — §3.1 still holds), but the operator "
            f"review queue is missing the per-row triage rows. See WARN "
            f"log lines above.",
            err=True,
        )

    # A2 telemetry (wet path only): surface the heal-path volume so the daily
    # re-selection leak is measurable day over day instead of invisible in
    # ephemeral stderr. `pending_flipped` is the count of PROSPECT rows that
    # LinkedIn already held a pending invite for (the leak being healed);
    # `already_first_degree` is the 1st-degree→ACCEPTED reconciliation.
    if not dry_run:
        click.echo(
            f"  Pattern-A: flipped {pending_flipped_count} pending"
            f"→CONNECTION_SENT, {len(already_connected)} already-1st-degree"
            f"→ACCEPTED, {len(failed_to_flip)} failed, "
            f"{len(pattern_a_quarantined)} quarantined (suspected "
            f"URL-variant duplicate, escalated)."
        )
        if audit_logger is not None:
            # Best-effort: this event fires AFTER every Pattern-A flip is
            # durable in Attio but BEFORE the caller proceeds to send the
            # invite queue. A telemetry sidecar failure must never abort the
            # wet send (mirrors the escalate() best-effort pattern above).
            try:
                audit_logger.event(
                    "pattern_a_pending_flips",
                    pending_flipped=pending_flipped_count,
                    already_first_degree=len(already_connected),
                    failed=len(failed_to_flip),
                )
            except Exception as audit_exc:  # noqa: BLE001 — telemetry never blocks send
                click.echo(
                    f"  ⚠ pattern_a_pending_flips audit event failed "
                    f"[{type(audit_exc).__name__}]: {audit_exc}. "
                    f"Flips are durable; continuing.",
                    err=True,
                )

    if dry_run:
        # Honest partition preview. The caller prints `already_connected`
        # ("would flip to ACCEPTED") and the invite queue (`still_to_invite`);
        # surface the buckets it does NOT — pending / OON / unknown — so rows
        # that drop out of the invite queue are never silently lost.
        click.echo(
            f"  [DRY RUN] live degree check ({backend}) partition: "
            f"{len(already_connected)} already-1st (→would flip ACCEPTED, not invited), "
            f"{len(still_to_invite)} confirmed 2nd/3rd (→invite), "
            f"{len(dry_pending)} pending-invite (→would flip CONNECTION_SENT, not invited), "
            f"{len(dry_oon)} out-of-network (→would mark UNREACHABLE, not invited), "
            f"{len(dry_unknown)} unknown/blank degree (→would escalate, not invited), "
            f"{len(dry_quarantined)} Pattern-A quarantined (→would escalate, not flipped)."
        )
        for label, bucket in (
            ("pending-invite", dry_pending),
            ("out-of-network", dry_oon),
            ("unknown-degree", dry_unknown),
            ("pattern-a-quarantined", dry_quarantined),
        ):
            for row in bucket:
                click.echo(
                    f"      [{label}] {row.get('name', '?')} "
                    f"@ {row.get('company', '?')} — {row['linkedInUrl']}"
                )

    return still_to_invite, already_connected
