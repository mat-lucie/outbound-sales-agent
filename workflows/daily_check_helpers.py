"""Utility helpers for the daily_check workflow.

Covers: URL normalization, dedup, PB CSV/log parsing, placeholder guard,
and entry fetch helpers. These are pure functions with no side effects on
Attio state — safe to test in isolation.
"""

import json
import os
import re
import sys
from typing import TYPE_CHECKING
from urllib.parse import unquote

import httpx

from clients.attio import AttioClient, linkedin_identity_key
from clients.pb_config import (
    _BACKEND_DEFAULT,
    li_user_agent_stripped,
    load_pb_config,
    sales_nav_session_cookie_stripped,
)
from clients.phantombuster import get_phantombuster_credentials

if TYPE_CHECKING:
    from clients.botdog import BotdogClient
    from clients.phantombuster import PhantomBusterClient


def _normalize_linkedin_url(url: str) -> str:
    """Normalize a LinkedIn URL for comparison.

    Handles: URL encoding, www. prefix, trailing slashes, case.
    """
    return unquote(url).replace("://www.", "://").rstrip("/").lower()


# ── delivery-transport send-channel routing ──────────────────────────
# Channel values for the `send_channel` list-entry attribute (manifest:
# docs/attio_schema_deltas.yaml).
#
# HOME: these live here, in the dependency-free helper module, rather than
# in `workflows/daily_check.py`, because THREE modules must agree on the
# answer — daily_check (send routing), detect_responses (Phase 0.5 skip)
# and botdog_ingest (ingest scope guard) — and neither of the latter two
# can import daily_check without a module-load cycle. One definition means
# the routing question is a SINGLE switch; duplicated, a change would move
# the send path and leave the detection paths behind, splitting a
# prospect's cadence across two transports. `workflows.daily_check`
# re-exports both constants and the resolver.
SEND_CHANNEL_PB = "pb"
SEND_CHANNEL_BOTDOG = "botdog"

# Channel resolved for entries whose `send_channel` attribute is unset —
# which is every row this engine writes. PhantomBuster owns sending: no
# code path stamps `botdog`, and no send path routes to Botdog. The
# constant exists so a row that WAS stamped (by an operator's own
# migration, or a prior deployment) is held OUT of PB sends rather than
# double-messaged by two transports.
SEND_CHANNEL_DEFAULT = SEND_CHANNEL_PB


def _resolve_send_channel(attrs: dict) -> str:
    """Resolve a parsed entry's delivery transport channel.

    Missing/empty `send_channel` → SEND_CHANNEL_DEFAULT (`pb`). Any
    unexpected stored value is passed through verbatim — the caller's
    channel comparison then treats it as non-botdog (== PB path), which is
    the safe direction: an unknown channel must never trigger a send on an
    unwired transport.
    """
    return attrs.get("send_channel") or SEND_CHANNEL_DEFAULT


# ── Botdog blacklist presence gate ───────────────────────────────────
# Botdog inherits NONE of PhantomBuster's internal never-contact memory,
# so `scripts/seed_botdog_blacklist.py --apply` is a hard pre-send step
# for any operator who wires the Botdog transport. The very first Botdog
# run could otherwise re-invite someone already burned, or cold-contact a
# company on the operator's never-contact denylist.
#
# THIS GATE IS A HELPER, NOT AN AUTOMATIC ONE. This engine sends through
# PhantomBuster; the Botdog surface is off by default and no production
# path calls this function. An operator who builds a Botdog send path
# MUST call it themselves before the first send — nothing else will.
#
# Memoized per PROCESS (one `get_blacklists` call per run, not one per
# send step). `reset_blacklist_gate()` exists for tests.
BOTDOG_SKIP_BLACKLIST_CHECK_ENV = "BOTDOG_SKIP_BLACKLIST_CHECK"

_blacklist_gate_checked = False


def reset_blacklist_gate() -> None:
    """Clear the per-process memo (tests only)."""
    global _blacklist_gate_checked
    _blacklist_gate_checked = False


def assert_botdog_blacklist_seeded(client: "BotdogClient") -> None:
    """Refuse to build a Botdog sender until the never-contact set is seeded.

    Blocks (RuntimeError) when the named collection is absent, or is
    present and explicitly empty. Two deliberate NON-blocking paths:

      * ``BOTDOG_SKIP_BLACKLIST_CHECK=1`` — operator emergency override,
        downgraded to a loud warning (a wet run must never be hostage to
        this gate at 2am, but it must be impossible to skip silently);
      * a ``BotdogError`` raised BY THE CHECK ITSELF — an API blip must
        not kill a run. The sends downstream carry their own error
        handling; failing the whole run on a flaky read would be a bigger
        outage than the risk it guards.

    Both non-blocking paths still set the memo, so the warning prints once
    per run rather than once per step.

    An UNREADABLE lead count (``collection_lead_count`` → None, meaning
    "unknown", never "empty") also passes — but loudly: the gate then
    verified EXISTENCE only, not that the set is populated.

    NOT WIRED INTO ANY SEND PATH IN THIS ENGINE. PhantomBuster owns
    sending here and the Botdog surface is off by default, so nothing
    calls this today (the Phase 0.7 event drain deliberately does not —
    it is read-only and sends nothing). It is the ready-made gate for an
    operator who wires a Botdog send path: call it before building the
    sender, or the never-contact set is never checked.
    """
    global _blacklist_gate_checked
    if _blacklist_gate_checked:
        return

    from clients.botdog import (
        BotdogError,
        blacklist_name,
        collection_lead_count,
        select_blacklist,
    )

    name = blacklist_name()

    if os.environ.get(BOTDOG_SKIP_BLACKLIST_CHECK_ENV) == "1":
        print(
            f"  ⚠ {BOTDOG_SKIP_BLACKLIST_CHECK_ENV}=1 — Botdog blacklist "
            f"presence gate BYPASSED. Sends proceed with the never-contact "
            f"set UNVERIFIED. Unset it and run "
            f"scripts/seed_botdog_blacklist.py --apply as soon as you can.",
            file=sys.stderr,
        )
        _blacklist_gate_checked = True
        return

    try:
        blacklists = client.get_blacklists()
    except BotdogError as exc:
        print(
            f"  ⚠ Botdog blacklist presence gate could not run "
            f"[{type(exc).__name__}: {exc}] — NOT blocking the run (an API "
            f"blip must not kill it), but the never-contact set is "
            f"UNVERIFIED this run. Re-check before the next wet send.",
            file=sys.stderr,
        )
        _blacklist_gate_checked = True
        return

    # Duplicate same-named collections exist in the wild (an empty
    # duplicate beside the populated one). Pick the POPULATED one by max
    # leadCount, not the first by list order — otherwise the API returning
    # the empty duplicate first would read 0 leads and block every send.
    match = select_blacklist(blacklists, name)
    if match is None:
        raise RuntimeError(
            f"Botdog blacklist not seeded — no collection named {name!r} "
            f"exists. Run scripts/seed_botdog_blacklist.py --apply before "
            f"enabling sends; override with "
            f"{BOTDOG_SKIP_BLACKLIST_CHECK_ENV}=1 for emergencies. "
            f"Nothing was sent."
        )
    lead_count = collection_lead_count(match)
    if lead_count is None:
        # "Unknown", never "empty" (see `collection_lead_count`), so the
        # gate PASSES on existence alone. That pass is deliberate — a
        # populated collection whose payload omits the count must not
        # block a run — but it is a WEAKER verdict than the operator
        # thinks they are getting, so it is never silent.
        print(
            f"  ⚠ Botdog blacklist presence gate: the collection {name!r} "
            f"exists but its payload carries NO readable lead count — the "
            f"gate is passing on EXISTENCE ONLY and has NOT verified the "
            f"never-contact set is populated. Confirm it in Botdog, or "
            f"re-run scripts/seed_botdog_blacklist.py --apply.",
            file=sys.stderr,
        )
    if lead_count == 0:
        raise RuntimeError(
            f"Botdog blacklist not seeded — the collection {name!r} exists "
            f"but is EMPTY (0 leads). Run "
            f"scripts/seed_botdog_blacklist.py --apply before enabling "
            f"sends; override with {BOTDOG_SKIP_BLACKLIST_CHECK_ENV}=1 for "
            f"emergencies. Nothing was sent."
        )
    _blacklist_gate_checked = True


# Pattern for any remaining bracket placeholder (e.g. [industria similar], [Name]).
# Used as a pre-send guard — a batch with unresolved placeholders must not ship.
_PLACEHOLDER_RE = re.compile(r"\[[^\[\]\n]+\]")


class UnresolvedPlaceholderError(RuntimeError):
    """Raised when an outbound message still contains a [...] template token."""

    partial_results: dict | None = None


class BlankMessageError(RuntimeError):
    """Raised when an outbound message rendered to empty/whitespace text."""

    partial_results: dict | None = None


def _dedupe_by_linkedin_url(rows: list[dict]) -> tuple[list[dict], list[str]]:
    """Collapse rows with duplicate LinkedIn URLs, keeping the first.

    When two Attio entries resolve to the same LinkedIn URL (duplicate records),
    we send a single message but remember *all* entry_ids so the stage-advance
    loop can flip every duplicate to the new stage in lock-step. Without this,
    the un-advanced duplicate sits at the old stage and gets re-queued the
    next day — which is exactly how Leo Teixeira received two DM2s.

    Kept rows expose `entry_ids: list[str]` containing every associated
    entry_id (at least one, potentially more). The legacy `entry_id` key is
    preserved for callers that only need one.

    Keyed on `linkedin_identity_key` (slug-variant cadence-leak fix): two slug
    VARIANTS sharing the numeric profile-id suffix are the same person and
    must collapse to one send, with both entry_ids advanced in lock-step.
    URLs without the suffix compare by canonical form as before.

    Returns (deduped_rows, dropped_urls) where dropped_urls lists each extra
    occurrence — useful for logging only.
    """
    by_url: dict[str, dict] = {}
    ordered_keys: list[str] = []
    dropped: list[str] = []
    for row in rows:
        url = row.get("linkedInUrl", "")
        if not url:
            continue
        key = linkedin_identity_key(url)
        entry_id = row.get("entry_id")
        if key in by_url:
            dropped.append(url)
            if entry_id:
                by_url[key]["entry_ids"].append(entry_id)
            continue
        first = dict(row)
        first["entry_ids"] = [entry_id] if entry_id else []
        by_url[key] = first
        ordered_keys.append(key)
    deduped = [by_url[k] for k in ordered_keys]
    return deduped, dropped


def _assert_no_unresolved_placeholders(rows: list[dict], step_label: str) -> None:
    """Refuse to ship any row whose message still has a [...] placeholder.

    This is the last line of defence: if personalization fell through for any
    reason (missing Attio field, new placeholder added to a template, etc.),
    we abort the batch rather than send a literal [placeholder] to a prospect.
    """
    offenders: list[tuple[str, str]] = []
    for row in rows:
        msg = row.get("message", "")
        match = _PLACEHOLDER_RE.search(msg)
        if match:
            offenders.append((row.get("linkedInUrl", "?"), match.group(0)))
    if offenders:
        preview = "; ".join(f"{url} → {tok}" for url, tok in offenders[:5])
        raise UnresolvedPlaceholderError(
            f"Refusing to send {step_label}: {len(offenders)} message(s) contain "
            f"unresolved placeholders. Examples: {preview}"
        )


def _assert_no_blank_messages(rows: list[dict], step_label: str) -> None:
    """Refuse to ship any row whose message rendered to empty/whitespace.

    A blank rendered message means template rendering broke upstream (e.g. a
    template variable rename emptying every render) — a batch-level, systemic
    condition, not a per-prospect one. Without this guard the run would
    reserve a lease, iterate every row, and finish "successfully" with 0
    sends, which reads as N flaky prospects rather than one broken template.
    Aborting the batch before its lease is reserved keeps the failure
    attributed to the template, loudly. A transport that validates per row
    (Botdog rejects blank text) is only a backstop — invite notes and PB rows
    have no transport-level blank check, so this guard is their only line of
    defence.
    """
    blank = [
        row.get("linkedInUrl", "?")
        for row in rows
        if not (row.get("message") or "").strip()
    ]
    if blank:
        preview = "; ".join(blank[:5])
        raise BlankMessageError(
            f"Refusing to send {step_label}: {len(blank)}/{len(rows)} rendered "
            f"message(s) are blank — template rendering likely broke upstream "
            f"(check the {step_label} templates before re-running; nothing in "
            f"THIS {step_label} batch was sent and its lease was never "
            f"reserved — earlier batches in the same run may already have "
            f"sent). Affected: {preview}"
        )


def _assert_email_not_blank(
    subject: str, body: str, recipient: str, step_label: str
) -> None:
    """Refuse to send an email whose subject or body is blank.

    Email counterpart of _assert_no_blank_messages. The email paths render
    subject and body separately but run the placeholder guard over their
    "{subject}\\n{body}" join — a blank subject is masked by a non-blank body
    (and vice versa), so each part must be validated on its own. A blank
    render is the same systemic template break the DM guard catches (e.g. a
    template edit emptying one language's subject): without this guard,
    Resend ships the email with an empty subject, the already_sent ledger
    records it, and the stage advances — silent and prospect-visible.

    The body is judged on its rendered text (tags stripped, entities
    unescaped), not the raw markup: a bad template edit typically leaves the
    HTML skeleton behind ("<p></p>", "<p>&nbsp;</p>"), which a raw strip()
    would wave through while the prospect sees an empty email.

    Called two ways by the email send paths: pre-flight over each distinct
    template in the batch (so a systemic break halts at ZERO sends, matching
    the DM batch guard), and per-send on the rendered copy as a backstop.
    A backstop halt can land mid-run — the message stays honest about that.
    """
    from workflows.email_compliance import html_to_text

    blank_parts = [
        label
        for label, text in (("subject", subject), ("body", html_to_text(body or "")))
        if not (text or "").strip()
    ]
    if blank_parts:
        raise BlankMessageError(
            f"Refusing to send {step_label} for {recipient}: "
            f"{' and '.join(blank_parts)} blank — template rendering likely "
            f"broke upstream. The {step_label} batch was halted before this "
            f"send went out; sends completed earlier in the run (if any) "
            f"already shipped and were recorded. Check the {step_label} "
            f"email templates before re-running."
        )


def _assert_email_renderable(
    subject: str, body: str, recipient: str, step_label: str
) -> None:
    """Run both per-send email guards: no unresolved placeholders, not blank.

    One call site per send path instead of a hand-copied pair — a future
    email path that adopts this can't take the placeholder guard and forget
    the blank guard (the omission that let blank subjects ship before).
    """
    _assert_no_unresolved_placeholders(
        [{"message": f"{subject}\n{body}", "linkedInUrl": recipient}],
        step_label,
    )
    _assert_email_not_blank(subject, body, recipient, step_label)


def _parse_pb_sent_urls_from_csv(csv_text: str) -> tuple[set[str], set[str]]:
    """Parse a PB Message Sender result CSV into (sent_urls, skipped_urls).

    Authoritative source: PB Message Sender emits a structured CSV with one
    row per input URL and a `status` column (`"Message sent"` on success,
    e.g. `"Can't send message"` / `"InMail required"` on skip). Prefer this
    over log parsing — log phrasing varies and silently defaults to skipped.

    Returns URLs normalized via _normalize_linkedin_url.
    """
    import csv
    import io

    sent: set[str] = set()
    skipped: set[str] = set()
    if not csv_text:
        return sent, skipped
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        url = (
            row.get("query")
            or row.get("linkedinProfileUrl")
            or row.get("linkedInUrl")
            or row.get("profileUrl")
            or ""
        )
        if not url:
            continue
        status = (row.get("status") or "").strip().lower()
        key = _normalize_linkedin_url(url)
        if status == "message sent":
            sent.add(key)
        else:
            skipped.add(key)
    return sent, skipped


def _parse_pb_sent_urls(pb_log: str) -> tuple[set[str], set[str]]:
    """Parse a PB Message Sender log into (sent_urls, skipped_urls).

    Fallback path used when the result CSV is unavailable. Walks the log
    line-by-line. Each `Converting regular URL <url>` line opens a per-profile
    block; within that block, a `Sending message to` line counts as sent,
    while `Can't send message` / `InMail required` counts as skipped. A block
    with neither is considered skipped (silent failure).

    Returns URLs normalized via _normalize_linkedin_url so callers can match
    against their own rows regardless of www/trailing-slash variance.
    """
    sent: set[str] = set()
    skipped: set[str] = set()
    current_url: str | None = None
    current_sent = False
    url_re = re.compile(r"Converting regular URL (https?://\S+?)(?:\.\.\.|\s|$)")

    def flush() -> None:
        if current_url is None:
            return
        key = _normalize_linkedin_url(current_url)
        if current_sent:
            sent.add(key)
        else:
            skipped.add(key)

    for raw_line in pb_log.splitlines():
        line = raw_line.strip()
        m = url_re.search(line)
        if m:
            flush()
            current_url = m.group(1)
            current_sent = False
            continue
        if current_url is None:
            continue
        if "Sending message to" in line:
            current_sent = True
        elif "Can't send message" in line or "InMail required" in line:
            current_sent = False
    flush()
    return sent, skipped


def _get_all_entries_with_raw(
    attio: AttioClient,
) -> tuple[list[dict], list[dict]]:
    """Fetch all list entries and return (raw_entries, parsed_filtered).

    raw_entries  — the unmodified API response objects; may be passed to
                   run_company_tally_consistency_sweep on write-free exits
                   so it can skip its own identical fetch (Fix 1 reuse).
    parsed_filtered — same §3.11 soft-delete filter as _get_all_entries_parsed:
                   entries with `merged_into` set are excluded so merged-away
                   losers never re-enter the send-eligibility path.
    """
    list_id = os.environ.get("ATTIO_LIST_ID", "")
    raw = attio.query_list_entries(list_id=list_id)
    parsed = [AttioClient.parse_entry(entry) for entry in raw]
    filtered = [entry for entry in parsed if not entry.get("merged_into")]
    return raw, filtered


def _get_all_entries_parsed(attio: AttioClient) -> list[dict]:
    """Fetch and parse all live list entries from Attio.

    §3.11 soft-delete filter: any entry with `merged_into` set was
    soft-deleted by PR-9.5's union-merge — its winner inherited the
    cohort identity, cadence, and send-suppression flags. Returning the
    loser would re-queue it in downstream send-eligibility checks
    (§3.1 hard red line: no resends of merged-away prospects).

    Delegates to _get_all_entries_with_raw; callers that also need the raw
    snapshot for the consistency sweep should call that helper directly.
    """
    _, filtered = _get_all_entries_with_raw(attio)
    return filtered


def _pb_session_args() -> dict:
    """Build PB session args from env vars (LinkedIn session cookie + UA)."""
    cookie, ua = get_phantombuster_credentials()
    if not cookie:
        return {}
    return {"sessionCookie": cookie, "userAgent": ua}


class SalesNavConfigError(RuntimeError):
    """Raised when Sales Nav cookie/scraper-id env vars are missing or wrong."""


def _pb_sales_nav_session_args() -> dict:
    """Build the session dict for the Sales Nav Profile Scraper from env vars.

    Returns ``{"sessionCookie": <li_at>, "userAgent": <ua>}``. The caller is
    expected to inject these INTO the saved phantom argument's
    ``identities[0]`` field — NOT pass them as top-level args. The Sales Nav
    Profile Scraper (verified 2026-05-25 against phantom 602790655114603)
    rejects partial argument objects with "Your Phantom Argument Isn't Valid"
    when ``identities`` is missing, and ignores top-level ``sessionCookie``
    when ``identities`` is present.

    Why this exists as a separate helper from :func:`_pb_session_args`:
    Sales Nav cookies rotate on a different schedule than the regular
    ``li_at`` cookie used by Search Export / Network Booster / Message
    Sender / SN Inbox Scraper. Keeping them in separate env vars
    (``PB_LI_SESSION_COOKIE`` vs ``PB_LI_SALES_NAV_SESSION_COOKIE``)
    prevents a Sales Nav rotation from cross-contaminating the other 4
    phantoms (closes adversarial review finding F6 from plan
    reflective-singing-waterfall.md).

    NOTE: this helper does not validate the ``PB_LI_SALES_NAV_LI_A_COOKIE``
    env var. The phantom build verified 2026-05-25 does not require an
    ``li_a`` cookie — only ``li_at`` from a Sales Nav session. The
    ``PB_LI_SALES_NAV_LI_A_COOKIE`` env var is kept as a placeholder for
    future PB phantom variants that may need it.

    Raises:
        SalesNavConfigError: ``PB_LI_SALES_NAV_SESSION_COOKIE`` is unset or
            empty. Fail-loud is correct: a missing cookie means the next
            scrape would return 0-row CSV (no auth), which under PR-B's
            §3.1-hardened partition routes every prospect to ``degree_unknown``
            escalation — operationally noisy and operator-blocking. Better
            to fail at config-resolve time.
    """
    cookie = sales_nav_session_cookie_stripped()
    if not cookie:
        raise SalesNavConfigError(
            "PB_LI_SALES_NAV_SESSION_COOKIE is unset or empty. Capture li_at "
            "from a logged-in Sales Navigator browser session (DevTools → "
            "Application → Cookies → linkedin.com) and add to .env. See "
            "docs/runbooks/phantombuster-cookie-rotation.md."
        )
    # Reuse the regular UA env var — operationally the User-Agent should
    # match the browser the cookies came from, and the operator captures both
    # cookies from the same browser.
    ua = li_user_agent_stripped()
    args: dict = {"sessionCookie": cookie}
    if ua:
        args["userAgent"] = ua
    return args


def _fresh_csv_name(prefix: str) -> str:
    """Unique-per-launch PB result file name (e.g. ``deg-20260610-153045-123456``).

    PB keys the phantom's processed-inputs database on the result CSV
    filename — a fresh name per launch forces a full re-scrape AND yields a
    per-launch CSV containing only this run's rows (no stale-row joins).
    Cost: result files accumulate in the agent's PB storage; names are
    timestamped so the operator can bulk-delete old ones from the dashboard.
    """
    from datetime import UTC, datetime

    return f"{prefix}-{datetime.now(UTC):%Y%m%d-%H%M%S-%f}"


_VALID_BACKENDS = ("regular", "sales_nav")

# Public name for the code default of PRE_INVITE_DEGREE_CHECK_BACKEND.
# The VALUE is owned by clients.pb_config._BACKEND_DEFAULT (the single
# source of truth for the yaml→env→default resolution) — this is a
# re-export so cli.py / scripts.drain_prospect_backlog can read the default
# at their routing gates without reaching into a private name, and so the
# default lives in exactly one place. Flipped to "sales_nav" when the legacy
# LinkedIn Profile Scraper agent was deleted from the PB workspace: a
# missing/typo'd env var falling back to "regular" guaranteed a mid-run
# httpx 404 at launch time, after a production sheet write was already
# burned. Under a sales_nav default a bare environment fails LOUD at resolve
# time instead (missing SN scraper-id / cookie raises SalesNavConfigError).
DEGREE_CHECK_BACKEND_DEFAULT = _BACKEND_DEFAULT


def _resolve_degree_check_backend() -> str:
    """Return the pre-invite degree-check backend, validating env vars.

    Returns one of ``"regular"`` (legacy LinkedIn Profile Scraper path) or
    ``"sales_nav"`` (Sales Navigator Profile Scraper path, PR-B). Per plan
    reflective-singing-waterfall.md, called at the top of
    ``_pre_invite_degree_check`` on every invocation so the flag is hot-
    reloadable on a fresh ``python3 cli.py daily`` process (rollback is
    "edit .env, restart cli.py" — no deploy needed).

    Centralizing the flag read here (instead of inline ``os.environ.get``)
    closes adversarial review finding F5:

    - Strict value parsing: only ``regular`` / ``sales_nav`` accepted;
      everything else raises. (Prior plan v1 used a boolean flag with
      brittle truthy/falsy parsing — ``True``/``1``/``yes`` would silently
      parse to false.)
    - Cross-wire guard: when backend is ``sales_nav``,
      ``PB_SALES_NAV_PROFILE_SCRAPER_ID`` must be set AND must not equal
      ``PB_PROFILE_SCRAPER_ID``. (If the operator accidentally pasted the
      legacy phantom ID into the Sales Nav slot, the regular scraper would
      run with whatever args we pass — silently returning the wrong CSV
      schema and bypassing the safety changes in PR-B's partition logic.)
    - Cookie sanity: also confirms ``PB_LI_SALES_NAV_SESSION_COOKIE`` is
      set when backend is ``sales_nav``, so callers don't burn a PB launch
      just to discover a missing cookie.

    Raises:
        SalesNavConfigError: any of the validation checks above fail. The
            exception message names the specific env var and the fix.
    """
    pb_cfg = load_pb_config()
    raw = pb_cfg.degree_check_backend_raw.strip()
    if raw not in _VALID_BACKENDS:
        raise SalesNavConfigError(
            f"PRE_INVITE_DEGREE_CHECK_BACKEND must be one of "
            f"{list(_VALID_BACKENDS)!r}, got {raw!r}. "
            "See .env.example and docs/runbooks/phantombuster-cookie-rotation.md."
        )
    if raw == "regular":
        return raw

    # sales_nav: validate scraper-id + cookie
    sales_nav_id = pb_cfg.sales_nav_profile_scraper_id.strip()
    if not sales_nav_id:
        raise SalesNavConfigError(
            "PRE_INVITE_DEGREE_CHECK_BACKEND=sales_nav requires "
            "PB_SALES_NAV_PROFILE_SCRAPER_ID to be set. Look up the phantom "
            "ID in the PhantomBuster dashboard for the Sales Navigator "
            "Profile Scraper."
        )
    legacy_id = pb_cfg.profile_scraper_id.strip()
    if legacy_id and sales_nav_id == legacy_id:
        raise SalesNavConfigError(
            "PB_SALES_NAV_PROFILE_SCRAPER_ID is identical to "
            "PB_PROFILE_SCRAPER_ID — the operator likely pasted the legacy "
            "phantom ID into the new slot. The two scrapers have different "
            "CSV column contracts and different argument shapes; running "
            "the legacy scraper from the Sales Nav code path silently "
            "bypasses PR-B's §3.1-hardened partition logic and risks "
            "re-inviting existing connections. Re-paste the correct ID "
            "from the PB dashboard."
        )
    # Sanity-check cookie presence at config-resolve time so callers don't
    # waste a PB launch to discover a missing cookie. The cookie value
    # itself is not validated — only its presence.
    if not sales_nav_session_cookie_stripped():
        raise SalesNavConfigError(
            "PRE_INVITE_DEGREE_CHECK_BACKEND=sales_nav requires "
            "PB_LI_SALES_NAV_SESSION_COOKIE to be set. See "
            "docs/runbooks/phantombuster-cookie-rotation.md."
        )
    return raw


def build_sales_nav_launch_args(
    pb: "PhantomBusterClient",
    scraper_id: str,
    *,
    spreadsheet_url: str,
    launch_count: int,
) -> dict:
    """Build the launch argument dict for the Sales Nav Profile Scraper.

    The SN phantom (verified 2026-05-25) rejects partial argument objects
    with "Your Phantom Argument Isn't Valid" — and silently ignores
    top-level ``sessionCookie`` when ``identities`` is present. So every
    launch must: fetch the phantom's SAVED argument, inject the fresh SN
    session into ``identities[0]``, and POST the full shape with only
    ``spreadsheetUrl`` / ``numberOfProfilesPerLaunch`` overridden. This
    helper is the single home for that contract — callers (pre-invite
    degree check, Phase 0 acceptance detection, repair-companies) merge
    their own ``csvName`` on top, because csvName policy differs per caller
    (pre-invite regenerates it PER RETRY ATTEMPT to bust the
    processed-inputs dedup DB; the others set it once per launch).

    ``launch_count`` is the value for ``numberOfProfilesPerLaunch`` — the
    caller computes it (``clients.google_sheets.profiles_per_launch`` for
    sheet-fed batches, which adds the header line PB counts; the raw batch
    size for a single bare-URL launch).

    Raises:
        SalesNavConfigError: the SN session cookie env var is missing
            (via :func:`_pb_sales_nav_session_args`), or the scraper id
            does not resolve in the PB workspace (``GET /agents/fetch``
            404s — the same deleted-agent event class that killed the
            legacy scraper, wrapped here so all three SN launch sites fail
            with the env var named instead of a raw httpx traceback).
    """
    try:
        agent = pb.get_agent(scraper_id)
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            raise SalesNavConfigError(
                f"Sales Nav Profile Scraper agent {scraper_id!r} does not "
                "exist in the PhantomBuster workspace (GET /agents/fetch → "
                "404). Check PB_SALES_NAV_PROFILE_SCRAPER_ID against the PB "
                "dashboard — the phantom may have been deleted or the id "
                "mistyped."
            ) from exc
        raise
    raw_arg = agent.get("argument") or "{}"
    saved = json.loads(raw_arg) if isinstance(raw_arg, str) else raw_arg
    session = _pb_sales_nav_session_args()
    identities = saved.get("identities") or [{}]
    identities[0].update(session)
    return {
        **saved,
        "identities": identities,
        "spreadsheetUrl": spreadsheet_url,
        "numberOfProfilesPerLaunch": launch_count,
    }


class LegacyScraperGoneError(RuntimeError):
    """Raised when the legacy LinkedIn Profile Scraper agent doesn't resolve in PB."""


def preflight_legacy_profile_scraper(
    pb: "PhantomBusterClient", profile_scraper_id: str
) -> None:
    """Fail loud if the legacy Profile Scraper agent doesn't exist in PB.

    The legacy agent (PB_PROFILE_SCRAPER_ID) was DELETED from the
    PhantomBuster workspace — verified via ``GET /api/v2/agents/fetch``
    returning 404 "Agent not found". Any ``backend=regular`` launch against
    it crashes mid-run with a raw httpx 404 AFTER the prospect sheet has
    already been written. This preflight runs before the launch so an
    explicitly selected (it is no longer the default — see
    ``DEGREE_CHECK_BACKEND_DEFAULT``) but dead legacy backend fails at
    config-error level with the fix named, matching how
    ``_resolve_degree_check_backend`` treats missing SN env vars.

    Raises:
        LegacyScraperGoneError: the agent id 404s in the PB workspace.
    """
    try:
        pb.get_agent(profile_scraper_id)
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            raise LegacyScraperGoneError(
                f"Legacy LinkedIn Profile Scraper agent {profile_scraper_id!r} "
                "does not exist in the PhantomBuster workspace (deleted). "
                "backend=regular cannot work against it. Either unset "
                "PRE_INVITE_DEGREE_CHECK_BACKEND (the default is sales_nav) "
                "or deploy a new Profile Scraper phantom and update "
                "PB_PROFILE_SCRAPER_ID."
            ) from exc
        raise
