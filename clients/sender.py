"""Sender seam: transport-agnostic send interface + the PB implementation.

This module is the seam that makes swapping the delivery transport a
routing decision instead of a rewrite:

- `Sender` is the transport protocol every sender implements. It is
  deliberately small — batch assembly, lane split, the pre-invite
  degree check, dry-run preview, the wet confirm gate, cap leases, and
  all CRM advance logic live with the callers in
  `workflows/daily_check.py`; only the launch/collect transport hop
  goes through a sender.
- `PBSender` wraps the PhantomBuster flow exactly as it ran inline
  before this seam existed: sheet write → phantom launch →
  `wait_for_completion` → result CSV download → `parse_send_outcome`
  (plus the `compute_invite_outcome` override on the invite path).
  `fetch_events` returns nothing — PB accept/reply detection stays
  scrape-based (Phase 0 / 0.5).
- PhantomBuster is the DEFAULT and only wired send transport. An
  optional alternative transport may implement the same protocol and
  return submission-shaped outcomes (see `BotdogSubmitOutcome`); such a
  transport is off by default and never routes a send unless an
  operator wires it explicitly.

The PB code in `workflows/daily_check.py` needs more than the protocol's
`SendOutcome`: the advance gate keys on the `PBLaunch` container id,
`emit_pb_silent_no_op` / audit events carry it, and the invite
per-person confirmation parser reads the completion log. So `PBSender`
also exposes PB-shaped per-launch methods (`launch_invite_batch` /
`launch_dm_batch`) returning the full `PBSendResult` envelope; the
protocol methods are thin wrappers over them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import click

from clients.botdog import (
    MAX_LEADS_PER_BATCH,
    BotdogError,
    BotdogInvalidMessage,
    validate_message_text,
)
from clients.google_sheets import write_prospects_to_sheet
from clients.pb_envelope import (
    NETWORK_BOOSTER_BUILTIN_PER_LAUNCH_CAP,
    PBCompletion,
    PBLaunch,
    SendOutcome,
    _normalize_url_for_match,
    compute_invite_outcome,
    parse_send_outcome,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from datetime import datetime

    from clients.botdog import BotdogClient
    from clients.phantombuster import PhantomBusterClient


@dataclass(frozen=True)
class LeadEvent:
    """One delivery/engagement event reported by a sender's transport.

    Consumed by an event-ingestion workflow to apply event-confirmed CRM
    advances on a poll-based transport. `raw` retains the transport's
    original event payload for audit/debugging; `occurred_at` is None
    when the transport omits a timestamp.
    """

    event_type: str
    lead_linkedin_url: str
    occurred_at: datetime | None
    raw: dict


class LeadEventBatch(list):  # list[LeadEvent]
    """One poll's events PLUS how many leads the poll never READ.

    Subclasses `list` so every caller keeps treating a poll result as a
    plain list of events (``fetch_events(...) == []`` still holds), while
    an ingest that cares about COMPLETENESS can read `leads_unread`:
    leads dropped by a per-poll detail-fetch budget, rows with no id, and
    leads whose detail fetch raised. Those leads produced NO events this
    run, so they cannot show up in any event-timestamp watermark — an
    ingest that advanced its cursor to `now` anyway would push their real
    events permanently below the next poll's floor (silent, total loss,
    with `failures=0` in the report).

    CONTRACT: a non-zero `leads_unread` means THIS POLL IS INCOMPLETE. An
    event-ingest MUST hold its cursor (see
    `workflows.botdog_ingest.ingest_botdog_events`) instead of advancing
    past leads the transport never read.
    """

    def __init__(
        self,
        events: Iterable[LeadEvent] = (),
        *,
        leads_unread: int = 0,
    ) -> None:
        super().__init__(events)
        self.leads_unread = int(leads_unread)


@dataclass(frozen=True)
class BotdogSubmitOutcome:
    """SUBMISSION-only outcome for one send call on a queueing transport.

    Reports what the transport ACCEPTED (it will send on its own
    schedule), never what LinkedIn delivered. Deliberately shares NO
    field names with `SendOutcome` — there is no `csv_status`, no
    `sent_urls`, no `sent_count` — so this object can never satisfy
    `should_advance_batch` or the per-row advance loops in
    `workflows/daily_check.py`: any code path that tries to treat a
    submission as a delivery confirmation crashes with AttributeError
    instead of silently advancing a stage. Stage/dm_step advances on a
    submission transport are event-confirmed only.

    URLs are normalized via `_normalize_url_for_match`. `failed` pairs
    each failed URL with a short reason string (safe for run reports —
    reasons come from typed transport errors, which scrub profile URLs).
    `skipped_existing_urls` is the idempotency pre-check result: leads
    already present in the target campaign, logged + surfaced, never
    re-submitted and never charged (a skip must never be silent).
    `raw_results` retains the raw response payloads for audit/forensics.
    """

    submitted_urls: frozenset[str] = frozenset()
    skipped_existing_urls: frozenset[str] = frozenset()
    failed: tuple[tuple[str, str], ...] = ()  # (url, reason)
    campaign_ids: tuple[str, ...] = ()
    raw_results: tuple[dict, ...] = field(default=(), repr=False)

    @property
    def submitted_count(self) -> int:
        return len(self.submitted_urls)

    @property
    def skipped_existing_count(self) -> int:
        return len(self.skipped_existing_urls)

    @property
    def failed_count(self) -> int:
        return len(self.failed)


@runtime_checkable
class Sender(Protocol):
    """Transport protocol for outbound LinkedIn sends.

    Implementations deliver what the caller already assembled and
    approved — they never select, filter, or advance prospects.

    Return types are transport-shaped: `PBSender` returns the delivery
    `SendOutcome` (PB reports per-URL delivery in its result CSV); a
    queueing transport returns `BotdogSubmitOutcome` (submission
    acknowledged — delivery arrives later as events). Callers branch on
    the concrete type they injected; the union keeps the protocol honest
    about both.
    """

    def send_invites(
        self, batch: list[dict]
    ) -> SendOutcome | BotdogSubmitOutcome:
        """Deliver one approved invite batch; report per-URL outcome."""
        ...

    def send_dm(
        self, prospect: dict, message: str
    ) -> SendOutcome | BotdogSubmitOutcome:
        """Deliver one DM to one prospect; report the outcome."""
        ...

    def fetch_events(self, since: datetime | None) -> list[LeadEvent]:
        """Return delivery/engagement events observed since `since`.

        Transports without an event feed (PB) return an empty list —
        their detection stays scrape-based.

        A transport whose poll can be INCOMPLETE (a fetch budget, a
        failed per-lead read) returns a `LeadEventBatch` so the caller
        can see `leads_unread` and hold its cursor; a plain list means
        "nothing was left unread".
        """
        ...


@dataclass(frozen=True)
class PBSendResult:
    """Full PB transport envelope for one phantom launch.

    Carries the `PBLaunch` + `PBCompletion` alongside the parsed
    `SendOutcome` because the PB code in `daily_check` still needs them:
    the advance gate checks the launch container id, the pb_silent_no_op
    / inmail_dead_end queue rows embed the launch, and the invite
    per-person confirmation parser reads the completion log. A queueing
    transport has no equivalent — this envelope is PB-internal.
    """

    launch: PBLaunch
    completion: PBCompletion
    outcome: SendOutcome


class PBSender:
    """`Sender` backed by the PhantomBuster send phantoms — the default.

    Behavior-preserving wrap of the flow previously inlined in
    `workflows/daily_check.py` (`run_connection_requests` /
    `run_dm_sequencing`): same sheet write, same launch args, same poll
    cadence, same CSV parse. Dependencies arrive via constructor
    injection — callers pass `write_sheet` / `session_args` from their
    own module namespace so existing test monkeypatches keep binding.
    """

    def __init__(
        self,
        pb: PhantomBusterClient,
        *,
        network_booster_id: str | None = None,
        message_sender_id: str | None = None,
        write_sheet: Callable[[list[dict]], str] = write_prospects_to_sheet,
        session_args: Callable[[], dict] | None = None,
        poll_interval: int = 15,
        max_wait: int = 1800,
    ) -> None:
        self._pb = pb
        self._network_booster_id = network_booster_id
        self._message_sender_id = message_sender_id
        self._write_sheet = write_sheet
        self._session_args: Callable[[], dict] = session_args or (lambda: {})
        self._poll_interval = poll_interval
        self._max_wait = max_wait

    # ── PB-shaped per-launch transport (the send loops call these) ─────

    def launch_invite_batch(
        self, rows: list[dict], requested_urls: set[str]
    ) -> PBSendResult:
        """One Network Booster launch for an approved invite batch."""
        if not self._network_booster_id:
            raise ValueError("PBSender: network_booster_id not configured")
        # Network Booster script 2818 exposes numberOfAddsPerLaunch (max 10).
        # numberOfProfilesPerLaunch belongs to the scraper/message phantoms
        # and was ignored here, silently truncating larger invite batches.
        if not 1 <= len(rows) <= NETWORK_BOOSTER_BUILTIN_PER_LAUNCH_CAP:
            raise ValueError(
                "Network Booster invite batches must contain 1-"
                f"{NETWORK_BOOSTER_BUILTIN_PER_LAUNCH_CAP} rows; got {len(rows)}"
            )
        sheet_url = self._write_sheet(rows)
        click.echo(f"  Wrote {len(rows)} rows to Google Sheet.")
        launch_args = {
            "spreadsheetUrl": sheet_url,
            # This limit counts additions, not CSV lines; the header is not
            # an addition. A larger daily allowance requires multiple
            # container-keyed launches, not a larger value here.
            "numberOfAddsPerLaunch": len(rows),
            **self._session_args(),
        }
        launch = self._pb.launch_agent(self._network_booster_id, launch_args)
        # Block until the phantom finishes — prevents workspace
        # parallel-execution cap when the next phantom (Message Sender)
        # launches in Part B.
        completion = self._pb.wait_for_completion(
            launch, poll_interval=self._poll_interval, max_wait=self._max_wait
        )
        csv_text = self._pb.download_result_csv(launch)
        outcome = parse_send_outcome(
            launch=launch,
            completion=completion,
            csv_text=csv_text,
            requested_urls=requested_urls,
        )
        # Invite CSVs accumulate stale rows. Advance only requested people
        # whose invitation is confirmed in this container's completion log.
        outcome = compute_invite_outcome(
            outcome,
            completion,
            requested_urls,
            launch_batch_size=len(rows),
        )
        return PBSendResult(launch=launch, completion=completion, outcome=outcome)

    def launch_dm_batch(
        self, rows: list[dict], requested_urls: set[str], *, step_label: str
    ) -> PBSendResult:
        """One Message Sender launch for an approved DM batch (one step)."""
        if not self._message_sender_id:
            raise ValueError("PBSender: message_sender_id not configured")
        sheet_url = self._write_sheet(rows)
        click.echo(
            f"  Wrote {len(rows)} {step_label} messages to Google Sheet."
        )
        launch_args = {
            "spreadsheetUrl": sheet_url,
            "message": "#message#",  # per-row from sheet column
            "numberOfProfilesPerLaunch": len(rows) + 1,
            **self._session_args(),
        }
        # Typed launch + raises-on-error wait. PBRunFailed bubbles up so the
        # operator sees the failure instead of silently skipping the batch.
        launch = self._pb.launch_agent(self._message_sender_id, launch_args)
        # Block until this DM batch finishes before launching the next step
        # (or anything else) — avoids PB workspace parallel cap.
        completion = self._pb.wait_for_completion(
            launch, poll_interval=self._poll_interval, max_wait=self._max_wait
        )
        result_csv = self._pb.download_result_csv(launch)
        outcome = parse_send_outcome(
            launch=launch,
            completion=completion,
            csv_text=result_csv,
            requested_urls=requested_urls,
        )
        return PBSendResult(launch=launch, completion=completion, outcome=outcome)

    # ── Sender protocol conformance ────────────────────────────────────

    def send_invites(self, batch: list[dict]) -> SendOutcome:
        requested = {
            _normalize_url_for_match(row["linkedInUrl"])
            for row in batch
            if row.get("linkedInUrl")
        }
        return self.launch_invite_batch(batch, requested).outcome

    def send_dm(self, prospect: dict, message: str) -> SendOutcome:
        row = {**prospect, "message": message}
        requested = (
            {_normalize_url_for_match(row["linkedInUrl"])}
            if row.get("linkedInUrl")
            else set()
        )
        return self.launch_dm_batch(
            [row], requested, step_label=str(row.get("dm_step") or "dm")
        ).outcome

    def fetch_events(self, since: datetime | None) -> LeadEventBatch:
        # PB has no event feed — accept/reply detection stays scrape-based
        # (Phase 0 / 0.5). Nothing is polled, so nothing is left unread.
        return LeadEventBatch()


# ---------------------------------------------------------------------
# BotdogSender — the OPTIONAL alternative transport (off by default)
# ---------------------------------------------------------------------

# Campaign-role scheme: invite campaign IDs live in `config/botdog.yaml`
# under `campaigns`, keyed by role slug. Invite send-data rows carry the
# resolved copy language (`row["language"]` — the same language that
# selected the connection-note template), so the role is derived from
# language alone: `invite_{language}` (invite_es, invite_en, invite_pt),
# falling back to a single catch-all `invite` role when no
# language-specific campaign is configured. Rows without a language go
# straight to the `invite` fallback. This matches "one single-step
# campaign per lane/language" without inventing a lane dimension the rows
# don't carry.
_INVITE_FALLBACK_ROLE = "invite"

# ── OPERATOR CONTRACT — do not rename without editing every campaign ──
# The per-lead invitation note travels to Botdog inside `customAttributes`
# under THIS key. Botdog substitutes it only where the campaign's
# invitation-note template references the variable VERBATIM as
# `{{inviteMessage}}`.
#
# So this constant is half of a contract whose other half lives in the
# Botdog UI, outside this repo and outside CI: every invite campaign MUST
# use `{{inviteMessage}}` in its note template. A mismatch is SILENT — the
# API accepts the lead, and LinkedIn gets an invite with an empty or
# literal-template note. Renaming this string therefore breaks live
# campaigns, not tests; the pinning test in tests/test_botdog_sender.py
# exists to force that conversation.
BOTDOG_INVITE_NOTE_VARIABLE = "inviteMessage"

# Detail-fetch budget for ONE `fetch_events` poll. Events are derived from
# lead DETAILS (list rows carry none), so the poll costs one request per
# lead — at Botdog's 60 req/min that is ~3.3 minutes for a full 200. The
# cap bounds RUNTIME, not correctness: the client's 429 backoff handles
# pacing, and any lead beyond the cap is named loudly so a skipped lead is
# never a silent one.
BOTDOG_MAX_DETAIL_FETCHES = 200

# Flat lead-detail timestamps → the canonical event vocabulary
# (`workflows.botdog_ingest.KNOWN_EVENT_TYPES`; kept as literals here so
# the transport layer does not import a workflow). These fields are
# first-class on GET /v1/leads/{id}, and they are the ONLY event evidence
# the API reliably exposes.
BOTDOG_DETAIL_TIMESTAMP_EVENTS: tuple[tuple[str, str], ...] = (
    ("invitedAt", "LEAD_INVITATION_SENT"),
    ("connectedAt", "LEAD_INVITATION_ACCEPTED"),
    ("repliedAt", "LEAD_MESSAGE_REPLIED"),
    ("withdrawnAt", "LEAD_INVITATION_WITHDRAWN"),
)


def invite_campaign_roles(language: str | None) -> tuple[str, ...]:
    """Campaign role slugs to try for an invite row, most specific first."""
    if language:
        return (f"invite_{language}", _INVITE_FALLBACK_ROLE)
    return (_INVITE_FALLBACK_ROLE,)


def _lead_url(lead: dict) -> str:
    """Best-effort LinkedIn URL extraction from a Botdog lead payload.

    `linkedinProfile` is the REAL field on both lead list rows and lead
    details and is therefore probed first — when it was missing from this
    list every probe fell through to "", silently breaking the invite
    idempotency pre-check (no campaign member ever matched, so existing
    leads looked absent and would be re-submitted) and blanking the URL on
    every polled event. The remaining names stay as defensive fallbacks:
    `linkedinUrl` is the key we SEND on the LeadCreateDto, and the rest
    cover per-lead result bodies whose shape is still unverified.
    """
    for key in (
        "linkedinProfile",
        "linkedinUrl",
        "linkedInUrl",
        "linkedin_url",
        "profileUrl",
        "url",
    ):
        value = lead.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _lead_id(lead: dict) -> str | None:
    """Best-effort lead-id extraction (Botdog response DTOs unverified)."""
    for key in ("id", "leadId", "_id"):
        value = lead.get(key)
        if value is not None and not isinstance(value, (dict, list)):
            return str(value)
    return None


def _lead_events(lead: dict) -> list[dict]:
    """Best-effort event-history extraction from a lead detail payload."""
    for key in ("events", "history", "leadEvents", "activities"):
        value = lead.get(key)
        if isinstance(value, list):
            return [ev for ev in value if isinstance(ev, dict)]
    return []


def _event_type(event: dict) -> str:
    for key in ("type", "eventType", "event", "name"):
        value = event.get(key)
        if isinstance(value, str) and value:
            return value
    return "unknown"


def _parse_utc_timestamp(value: object) -> datetime | None:
    """Parse one ISO-8601 timestamp to a UTC-AWARE datetime, or None.

    ALWAYS returns a timezone-aware value. A Botdog DTO timestamp without
    an offset (e.g. "2026-06-15T12:00:00" or a bare "2026-06-15") is
    assumed UTC — the API reports UTC elsewhere, and assuming a zone is
    strictly better than emitting a naive value: naive and aware datetimes
    cannot be compared, so one offset-less timestamp would crash the
    ingest watermark math (`sorted()` / `min()` in
    `workflows/botdog_ingest.py`) with a TypeError and abort the whole
    poll.
    """
    from datetime import UTC as _utc
    from datetime import datetime as _dt

    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = _dt.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=_utc)


def _event_time(event: dict) -> datetime | None:
    """Best-effort event timestamp parse; None when absent/unparseable."""
    for key in ("occurredAt", "createdAt", "timestamp", "date", "created_at"):
        parsed = _parse_utc_timestamp(event.get(key))
        if parsed is not None:
            return parsed
    return None


class BotdogSender:
    """`Sender` backed by the Botdog REST API — submission-only, OPT-IN.

    Delivers what the caller already assembled and approved; returns
    `BotdogSubmitOutcome` (see its docstring: submission acknowledged,
    delivery NOT confirmed — event ingestion confirms and advances).
    Botdog transport errors are captured per lead/chunk into
    `outcome.failed` rather than raised, so a partial batch failure reports
    exactly which leads stay queued for the next run; only caller/config
    errors (unresolvable campaign role) raise, and they raise BEFORE
    anything is submitted so the caller's cap lease refunds cleanly.

    `campaign_id_for_role` is the operator-config accessor
    (`BotdogConfig.campaign_id`) — campaign IDs are per-seat identity, like
    phantom IDs.

    NOTE: no send path in this engine constructs a BotdogSender. PB owns
    sending; this class is the wired-but-unrouted alternative transport, and
    its `fetch_events` is what the optional event-ingest drain polls.
    """

    def __init__(
        self,
        client: BotdogClient,
        *,
        campaign_id_for_role: Callable[[str], str | None],
        campaign_ids: tuple[str, ...] = (),
    ) -> None:
        self._client = client
        self._campaign_id_for_role = campaign_id_for_role
        # Event-poll scope: an account can hold thousands of
        # pre-existing/externally-synced leads, so an unfiltered
        # get_leads() poll hits the pagination cap. When the operator's
        # campaign ids are provided, fetch_events polls ONLY those
        # campaigns' leads — our leads are all campaign-injected, so
        # nothing of ours is missed. Empty tuple = full scan.
        self._campaign_ids = tuple(dict.fromkeys(campaign_ids))

    # ── invites ────────────────────────────────────────────────────────

    def _invite_campaign_id(
        self, row: dict
    ) -> tuple[str | None, tuple[str, ...]]:
        """Resolve a row's campaign id; returns (id-or-None, roles tried)."""
        roles = invite_campaign_roles(row.get("language") or None)
        for role in roles:
            campaign_id = self._campaign_id_for_role(role)
            if campaign_id:
                return campaign_id, roles
        return None, roles

    def send_invites(self, batch: list[dict]) -> BotdogSubmitOutcome:
        """Submit one approved invite batch to per-language campaigns.

        Flow per campaign group: idempotency pre-check via
        `get_campaign_leads` (one fetch per campaign per batch — the
        per-URL `get_leads` filter vocabulary is unverified, so we match
        normalized URLs client-side), then `add_leads_to_campaign` in
        <=`MAX_LEADS_PER_BATCH` chunks with the invite note in
        `customAttributes[BOTDOG_INVITE_NOTE_VARIABLE]` (the campaign's
        note template must reference `{{inviteMessage}}` verbatim — see
        that constant). The Google Sheet transport is NOT touched on this
        path.

        Raises ValueError (before any submission) when a row's campaign
        role cannot be resolved from operator config — a config gap is an
        operator error to fix, not a per-lead failure to swallow.
        """
        groups: dict[str, list[dict]] = {}
        unresolved: set[tuple[str, ...]] = set()
        for row in batch:
            campaign_id, roles = self._invite_campaign_id(row)
            if campaign_id is None:
                unresolved.add(roles)
                continue
            groups.setdefault(campaign_id, []).append(row)
        if unresolved:
            missing = sorted({role for roles in unresolved for role in roles})
            raise ValueError(
                f"BotdogSender: no Botdog campaign configured for invite "
                f"role(s) {missing} — add `campaigns` entries to "
                f"config/botdog.yaml (nothing was submitted)."
            )

        submitted: set[str] = set()
        skipped_existing: set[str] = set()
        failed: list[tuple[str, str]] = []
        raw_results: list[dict] = []

        for campaign_id, rows in groups.items():
            # Idempotency pre-check — never double-inject a lead. On a
            # pre-check FAILURE we fail closed for the whole group:
            # submitting without the check could re-invite.
            try:
                existing = {
                    _normalize_url_for_match(_lead_url(lead))
                    for lead in self._client.get_campaign_leads(campaign_id)
                } - {""}
            except BotdogError as exc:
                reason = f"idempotency_precheck_failed: {exc}"
                for row in rows:
                    failed.append(
                        (_normalize_url_for_match(row["linkedInUrl"]), reason)
                    )
                continue

            fresh: list[dict] = []
            for row in rows:
                key = _normalize_url_for_match(row["linkedInUrl"])
                if key in existing:
                    skipped_existing.add(key)
                    click.echo(
                        f"  Botdog idempotency: {row['linkedInUrl']} already "
                        f"in campaign {campaign_id} — skipped (not "
                        f"re-submitted, not charged)."
                    )
                else:
                    fresh.append(row)

            for start in range(0, len(fresh), MAX_LEADS_PER_BATCH):
                chunk = fresh[start:start + MAX_LEADS_PER_BATCH]
                leads = [
                    {
                        "linkedinUrl": row["linkedInUrl"],
                        "name": row.get("name") or "",
                        "title": row.get("title") or "",
                        "company": row.get("company") or "",
                        "customAttributes": {
                            BOTDOG_INVITE_NOTE_VARIABLE: row["message"]
                        },
                    }
                    for row in chunk
                ]
                try:
                    result = self._client.add_leads_to_campaign(
                        campaign_id, leads
                    )
                except BotdogError as exc:
                    reason = f"add_to_campaign_failed: {exc}"
                    for row in chunk:
                        failed.append(
                            (
                                _normalize_url_for_match(row["linkedInUrl"]),
                                reason,
                            )
                        )
                    continue
                raw_results.append(result.raw)
                # ── ghost-submitted reconciliation ──────────────────────
                # `lead_results` is a best-effort extraction from an
                # UNVERIFIED response DTO. When its length does not match
                # the chunk we sent — and the common case is 0, because the
                # real envelope key isn't one of the probed names — the
                # per-lead classification below has no data and marks EVERY
                # lead submitted. Those "ghost submissions" then get charged
                # to the invite lease while Botdog may hold none of them.
                #
                # Campaign membership is the ground truth the idempotency
                # pre-check above already trusts, so re-fetch it and let it
                # decide: present → really submitted; absent →
                # `unconfirmed_by_membership` failure (no charge, re-queues
                # next run; the pre-check makes the retry safe). Applied
                # ONLY on divergence — a matching count means the per-lead
                # body is real data and keeps its verdicts.
                if len(result.lead_results) != len(chunk):
                    click.echo(
                        f"  ⚠ Botdog add_leads_to_campaign returned "
                        f"{len(result.lead_results)} per-lead result(s) for "
                        f"{len(chunk)} submitted lead(s) in campaign "
                        f"{campaign_id} — cannot trust the body, "
                        f"re-checking campaign membership. Body snippet: "
                        f"{str(result.raw)[:300]}",
                        err=True,
                    )
                    try:
                        members = {
                            _normalize_url_for_match(_lead_url(lead))
                            for lead in self._client.get_campaign_leads(
                                campaign_id
                            )
                        } - {""}
                    except BotdogError as exc:
                        # Fail closed: unverifiable == not submitted. The
                        # pre-check catches any lead Botdog really did take,
                        # so the worst case is a one-run delay, not a double
                        # invite.
                        reason = f"unconfirmed_by_membership: {exc}"
                        click.echo(
                            f"  ⚠ membership re-check FAILED for campaign "
                            f"{campaign_id} ({exc}) — treating all "
                            f"{len(chunk)} lead(s) as NOT submitted "
                            f"(not charged, re-queued).",
                            err=True,
                        )
                        for row in chunk:
                            failed.append(
                                (
                                    _normalize_url_for_match(
                                        row["linkedInUrl"]
                                    ),
                                    reason,
                                )
                            )
                        continue
                    unconfirmed = 0
                    for row in chunk:
                        key = _normalize_url_for_match(row["linkedInUrl"])
                        if key in members:
                            submitted.add(key)
                        else:
                            unconfirmed += 1
                            failed.append((key, "unconfirmed_by_membership"))
                    click.echo(
                        f"  Membership re-check: "
                        f"{len(chunk) - unconfirmed}/{len(chunk)} lead(s) "
                        f"confirmed in campaign {campaign_id}; "
                        f"{unconfirmed} unconfirmed (not charged, "
                        f"re-queued next run)."
                    )
                    continue
                # Per-lead results inside a 2xx body (shape unverified —
                # best-effort classification; unmatched/absent entries
                # default to submitted, since the request succeeded).
                flagged: dict[str, str] = {}
                for entry in result.lead_results:
                    entry_key = _normalize_url_for_match(_lead_url(entry))
                    if not entry_key:
                        continue
                    verdict = _classify_lead_result(entry)
                    if verdict:
                        flagged[entry_key] = verdict
                for row in chunk:
                    key = _normalize_url_for_match(row["linkedInUrl"])
                    verdict = flagged.get(key)
                    if verdict == "skipped_existing":
                        skipped_existing.add(key)
                    elif verdict is not None:
                        failed.append((key, verdict))
                    else:
                        submitted.add(key)

        return BotdogSubmitOutcome(
            submitted_urls=frozenset(submitted),
            skipped_existing_urls=frozenset(skipped_existing),
            failed=tuple(failed),
            campaign_ids=tuple(sorted(groups)),
            raw_results=tuple(raw_results),
        )

    # ── DMs ────────────────────────────────────────────────────────────

    def send_dm(self, prospect: dict, message: str) -> BotdogSubmitOutcome:
        """Submit one DM for one prospect via `POST /v1/messages`.

        Resolves the Botdog lead by LinkedIn URL (`get_leads` filtered,
        then matched client-side on the normalized URL — defensive in case
        the server-side filter vocabulary differs). Transport errors return
        a failed outcome, never raise. Message text is validated FIRST
        (`invalid_message_text` reason, distinct from API-side failures) so
        a bad render costs zero API calls — not even the lead lookup.

        CAMPAIGN-FALLBACK SEAM: if a deployment finds Botdog cannot
        direct-message a fresh 1st-degree connection without an existing
        conversation, the branch below marked `lead_not_found` / the
        successful `send_message` call is where the swap goes — resolve the
        operator's message campaign for the step and call
        `add_leads_to_campaign` with the rendered message as the custom
        attribute, keeping this method's signature and submission-only
        outcome unchanged. Until then, an unresolvable lead is a surfaced
        failure (the row stays queued), not dead fallback code.
        """
        url = prospect.get("linkedInUrl") or ""
        key = _normalize_url_for_match(url)
        if not key:
            return BotdogSubmitOutcome(
                failed=(("", "missing_linkedin_url"),)
            )
        try:
            validate_message_text(message)
        except BotdogInvalidMessage as exc:
            return BotdogSubmitOutcome(
                failed=((key, f"invalid_message_text: {exc}"),)
            )
        try:
            leads = self._client.get_leads(linkedinUrl=url)
            lead = next(
                (
                    candidate
                    for candidate in leads
                    if _normalize_url_for_match(_lead_url(candidate)) == key
                ),
                None,
            )
            lead_id = _lead_id(lead) if lead is not None else None
            if lead_id is None:
                # ← campaign-fallback seam (see docstring): today this is a
                # loud per-prospect failure.
                return BotdogSubmitOutcome(
                    failed=((key, "lead_not_found"),)
                )
            raw = self._client.send_message(text=message, lead_id=lead_id)
        except BotdogError as exc:
            return BotdogSubmitOutcome(
                failed=((key, f"send_message_failed: {exc}"),)
            )
        return BotdogSubmitOutcome(
            submitted_urls=frozenset({key}),
            raw_results=(raw,) if isinstance(raw, dict) else (),
        )

    # ── events ─────────────────────────────────────────────────────────

    def fetch_events(self, since: datetime | None) -> LeadEventBatch:
        """Derive `LeadEvent`s from lead DETAILS (list rows have none).

        A lead LIST row carries no event data and no timestamps beyond
        `createdAt`; the DETAIL (`GET /v1/leads/{id}`) carries flat
        first-class timestamps — `invitedAt`, `connectedAt`, `repliedAt`,
        `withdrawnAt` — plus an `events` array. Reading embedded events off
        list rows therefore yields ZERO events forever. So this method
        pages the lead list, fetches each lead's detail, and SYNTHESIZES
        one event per present timestamp
        (`BOTDOG_DETAIL_TIMESTAMP_EVENTS`). Real entries in the detail's
        `events` array are still translated as-is; a timestamp is NOT
        synthesized when the array already carries that same type, so a
        lead never reports one transition twice. Downstream ingestion is
        idempotent regardless.

        MESSAGE-SENT IS A KNOWN BLIND SPOT — READ BEFORE TRUSTING DM
        CADENCE. The detail DTO exposes no per-message sent timestamp, so
        `LEAD_MESSAGE_SENT` can ONLY arrive via the `events` array, which
        was empty on every probed lead. Until that is verified against a
        lead that has actually been DM'd, assume DM-sent events do NOT
        arrive: `dm{N}_sent_at` will not advance, and
        `botdog_ingest.scan_stale_botdog_rows` will (correctly) report
        those prospects as frozen mid-cadence. Re-probe a lead's detail
        right after the first Botdog DM goes out; if the array stays empty,
        DM-sent confirmation needs another source (conversations endpoint
        or a webhook) before a DM lane can run on Botdog.

        Events without a parseable timestamp are INCLUDED regardless of
        `since` — dropping them would silently lose events, and the
        ingestion layer is idempotent.

        UNREAD LEADS ARE PART OF THE RESULT. Leads dropped by
        `BOTDOG_MAX_DETAIL_FETCHES`, rows with no id, and leads whose
        detail GET raised contribute NO events, so nothing downstream
        could otherwise tell them apart from "this lead had nothing new".
        Their count rides back on the returned `LeadEventBatch` as
        `leads_unread` so the ingest holds its cursor instead of
        advancing past events it never saw (see `LeadEventBatch`).
        """
        if self._campaign_ids:
            # Campaign-scoped poll (see __init__): union of our campaigns'
            # leads, deduped by lead id.
            seen_ids: set[str] = set()
            leads = []
            for campaign_id in self._campaign_ids:
                for row in self._client.get_campaign_leads(campaign_id):
                    row_id = _lead_id(row)
                    if row_id is not None and row_id in seen_ids:
                        continue
                    if row_id is not None:
                        seen_ids.add(row_id)
                    leads.append(row)
        else:
            leads = self._client.get_leads()
        # Prioritization for the detail-fetch budget: leads whose LIST row
        # already flags `hasReplied` go FIRST, because a reply is the one
        # event with a human waiting on the other end (and the only
        # transition that stops the cadence). Everything else keeps the
        # server's natural order — `sorted` is stable, so this is a
        # partition, not a reshuffle.
        ordered = sorted(leads, key=lambda row: not bool(row.get("hasReplied")))
        leads_unread = 0
        if len(ordered) > BOTDOG_MAX_DETAIL_FETCHES:
            skipped = ordered[BOTDOG_MAX_DETAIL_FETCHES:]
            leads_unread += len(skipped)
            click.echo(
                f"  ⚠ Botdog event poll: {len(ordered)} lead(s) but the "
                f"per-poll detail-fetch cap is {BOTDOG_MAX_DETAIL_FETCHES} "
                f"— {len(skipped)} lead(s) NOT polled this run (their "
                f"events are invisible until a later poll, and the ingest "
                f"cursor is HELD until they are read). Replied leads "
                f"were polled first. Narrow the lead set (campaign filter) "
                f"or raise BOTDOG_MAX_DETAIL_FETCHES deliberately.",
                err=True,
            )
            ordered = ordered[:BOTDOG_MAX_DETAIL_FETCHES]

        events: list[LeadEvent] = []
        detail_failures = 0
        for row in ordered:
            lead_id = _lead_id(row)
            if lead_id is None:
                click.echo(
                    f"  ⚠ Botdog event poll: lead row with no id "
                    f"({sorted(row)[:8]}) — cannot fetch its detail, so "
                    f"its events are invisible this run.",
                    err=True,
                )
                detail_failures += 1
                continue
            try:
                detail = self._client.get_lead(lead_id)
            except BotdogError as exc:
                # One unreadable lead must not abort the whole poll, but it
                # MUST be loud: its events are missed this run, and a
                # permanently failing detail could age past the poll window.
                click.echo(
                    f"  ⚠ Botdog event poll: detail fetch failed for lead "
                    f"{lead_id} ({exc}) — its events are missed this run.",
                    err=True,
                )
                detail_failures += 1
                continue
            if not isinstance(detail, dict) or not detail:
                detail = row
            url = _normalize_url_for_match(_lead_url(detail) or _lead_url(row))
            events.extend(
                self._lead_detail_events(detail, url=url, lead_id=lead_id)
            )

        leads_unread += detail_failures
        if detail_failures:
            click.echo(
                f"  ⚠ Botdog event poll: {detail_failures} of "
                f"{len(ordered)} lead detail(s) could not be read — their "
                f"events are missing from this run's report, and the "
                f"ingest cursor is HELD until they are read.",
                err=True,
            )

        if since is None:
            return LeadEventBatch(events, leads_unread=leads_unread)
        # Coerce an offset-less `since` rather than risk a naive/aware
        # TypeError aborting the poll (every occurred_at is UTC-aware).
        from datetime import UTC as _utc

        floor = since if since.tzinfo is not None else since.replace(tzinfo=_utc)
        return LeadEventBatch(
            (
                ev
                for ev in events
                if ev.occurred_at is None or ev.occurred_at >= floor
            ),
            leads_unread=leads_unread,
        )

    def _lead_detail_events(
        self, detail: dict, *, url: str, lead_id: str
    ) -> list[LeadEvent]:
        """Translate one lead detail into events (array + timestamps)."""
        out: list[LeadEvent] = []
        raw_events = _lead_events(detail)
        for raw_event in raw_events:
            out.append(
                LeadEvent(
                    event_type=_event_type(raw_event),
                    lead_linkedin_url=url,
                    occurred_at=_event_time(raw_event),
                    raw=raw_event,
                )
            )
        # Only synthesize what the array did not already report.
        present = {_event_type(ev).upper() for ev in raw_events}
        for field_name, event_type in BOTDOG_DETAIL_TIMESTAMP_EVENTS:
            if event_type in present:
                continue
            value = detail.get(field_name)
            if value in (None, ""):
                continue
            occurred = _parse_utc_timestamp(value)
            if occurred is None:
                click.echo(
                    f"  ⚠ Botdog event poll: lead {lead_id} has an "
                    f"unparseable {field_name} ({value!r}) — no "
                    f"{event_type} event derived from it.",
                    err=True,
                )
                continue
            out.append(
                LeadEvent(
                    event_type=event_type,
                    lead_linkedin_url=url,
                    occurred_at=occurred,
                    raw={
                        "source": "lead_detail_timestamp",
                        "field": field_name,
                        "value": value,
                        "leadId": lead_id,
                    },
                )
            )
        return out


def _classify_lead_result(entry: dict) -> str | None:
    """Best-effort per-lead verdict from an add_to_campaign 2xx body.

    Returns "skipped_existing" for conflict/duplicate signals, a
    failure-reason string for error signals, or None (treated as
    submitted). Botdog's per-lead result DTO is unverified — keys are
    probed defensively and anything unrecognizable defaults to submitted
    (the request itself returned 2xx).
    """
    status = str(
        entry.get("status") or entry.get("result") or ""
    ).strip().lower()
    if any(word in status for word in ("conflict", "exist", "duplicate")):
        return "skipped_existing"
    error = entry.get("error")
    if error:
        return f"lead_error: {error}"[:200]
    if status in ("error", "failed", "rejected"):
        return f"lead_status_{status}"
    return None
