"""Follow-up Radar detection engine (Phase C of /sales-daily).

Read-only detection of *warm-but-stale* accounts — the highest-value gap in the
sales motion. Cold outbound (LinkedIn DMs, the email drip) is fully automated;
what nothing owned until now is the next touch on accounts that engaged and then
went quiet: replied, booked a call, got demo'd, or have an open deal.

Scope of THIS module (Python, Attio-REST only):
  * detect warm candidates from ``linkedin_outreach`` + ``deals``
  * exclude declines and active-cadence overlap (mid cold-email-drip) so a
    warm nudge never double-touches. LinkedIn-side decline suppression is via
    ``build_suppression_set`` on entry signals (entry stage +
    response_classification); EMAIL-side hard declines (not-interested /
    unsubscribed, keyed on people.email_campaign_stage) come from
    ``cross_channel_suppression.email_hard_decline_ids`` and suppress EVERY
    lane. ``email_responded`` is NOT a decline: it stays out of the WAITING
    lane but still surfaces, annotated. The skill-layer C.2 gate is the final
    cross-channel decline check.
  * compute a *coarse* staleness/urgency from Attio-resident timestamps
  * render a ranked, lane-split digest (Owed vs Nudge)

Deal recency (v2) is a three-tier precedence, all still Attio-resident:
  1. ``deals.last_verified_touch`` — the skill layer's C.2-verified true
     last-touch, stamped via the ``followup-touch`` CLI (source "verified")
  2. person-interaction join — max(``people.last_interaction``) over the
     deal's ``associated_people``, Attio's native email/calendar sync
     (source "interaction"); ONE bulk fetch per run, never per-candidate
  3. deal creation date — the v1 deal-age fallback, and the only tier that
     keeps ``last_touch_synthetic=True`` (source "created_at")
Tiers 1–2 are real recency (synthetic=False); every candidate carries
``last_touch_source`` so learned metrics can segment verified vs joined vs
synthetic and never blend them.

Cold-responder lane ("replied, then went quiet after your DM"): a RESPONDED
entry whose LAST message is the operator's own hand-written LinkedIn DM,
quiet for 7+ calendar days. The ONLY "ours last" evidence is the local
``exports/manual_touch_state.json`` written by Phase 0.5's manual-touch pass
(inbox-scrape truth, incl. the prospect reply the CRM can't see — see
``_thread_direction`` for why CRM stamps were rejected as evidence). Each row
carries the exchange (their reply, our DM) and a paste-ready manual DM in the
prospect's language, rendered from ``content/followup_dm.json``.
Paste-by-hand only: never auto-sent, never an email draft, no CRM write — the
next Phase 0.5 scrape records the pasted DM and the row clears itself.

Out of scope here, done by the skill layer via MCP (see SKILL.md Phase C):
  * true last-touch verification (email thread direction, call transcript)
  * the 3-fact draft-context extraction and Gmail draft creation
  * writing follow-up state back to Attio

Every candidate this module emits is ``verified=False`` — the coarse last-touch
is from Attio fields only. The skill layer MUST verify before drafting and MUST
fail closed (skip, never nag) when a source can't be resolved. A wrong "you went
quiet" to someone the operator just spoke to is the worst outcome here.

Observability contract: a partial Attio outage or a schema drift must NEVER
render as a clean "Radar limpio ✅". Detection tracks a ``degraded`` list (a
holed exclusion set, an unreadable schema, a zero-match cohort) and a
``dropped_no_touch`` count (candidates skipped because no datable touch could be
resolved). Both are surfaced in the summary and stamped on the digest so a
silent drop can't masquerade as an all-clear.

Provenance: ported from upstream Follow-up Radar — detection engine (PR-211),
conversation-ledger / person-interaction join + no-CRM catcher (PR-214), and
the WAITING lane (PR-247). CRM-agnostic adaptation: the command trunk takes a
``CRMProvider`` and derives the raw ``AttioClient`` via ``_attio_inner_client``
(the transport-semantics escape hatch — see ``run_followup_radar``).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

from clients.attio import DEAL_FULL_SCAN_LIMIT, AttioClient
from models.business_calendar import operator_today
from models.email_campaign import ACTIVE_STAGES as EMAIL_ACTIVE_STAGES
from models.email_campaign import EmailStage
from models.followup import (
    DEAL_STAGE_REASONS,
    DRAFT_COOLDOWN_DAYS,
    POLICY,
    WAITING_MAX_NUDGES,
    WARM_ENTRY_REASONS,
    FollowupReason,
    WarmLane,
    days_silent,
    is_positive_nurture,
    urgency_score,
)
from models.resolution import coerce_language
from workflows.cross_channel_suppression import (
    EmailLaneNotProvisioned,
    build_suppression_set,
    email_hard_decline_ids,
    email_person_ids_in_stages,
)
from workflows.metrics import DailyRunMetrics

if TYPE_CHECKING:
    from clients.crm.base import CRMProvider

# Deal value that earns the top value multiplier (a $200K stall must outrank a
# fresh chatty mid-market reply — see the sales-exec QA lens).
_VALUE_MULT_CAP = 3.0
_VALUE_REF = 100_000.0  # deal value at which value_mult ≈ 2.0
# Enterprise ICP entries (no deal value attached) get a modest bump so they
# don't sort below noisier mid-market rows.
_ENTERPRISE_ICP_MULT = 1.3
# Full deal sweep — 500 (search_deals default) silently truncates the tail and
# would leak any warm deal past #500. Shared with the weekly report and the
# repair path via the client-level constant.
_DEAL_SCAN_LIMIT = DEAL_FULL_SCAN_LIMIT

# ``last_touch_source`` values — which tier resolved a candidate's recency.
# Persisted into the candidate JSON so downstream metrics can segment; the
# three deal tiers are documented in the module docstring. Entries use
# CONTACT_STAMP (real last_contact_date/response_received_at) or CREATED_AT.
TOUCH_SOURCE_VERIFIED = "verified"          # deals.last_verified_touch (C.2-stamped)
TOUCH_SOURCE_INTERACTION = "interaction"    # people.last_interaction join
TOUCH_SOURCE_CONTACT_STAMP = "contact_stamp"  # entry contact/response stamps
TOUCH_SOURCE_CREATED_AT = "created_at"      # creation-date fallback (synthetic)
TOUCH_SOURCE_NONE = "none"                  # nothing datable (dropped/counted)
# WAITING lane: last_touch is the operator's own C.2-verified unanswered SEND
# (awaiting_reply_since), not an inbound contact. A distinct tier — NEVER
# blend with "verified" in learned metrics: the cohort bias differs (every
# stamped account is one the operator actively emailed AND that passed C.2
# verification — biased toward already-engaged prospects, and toward
# email-reachable ones; person-only counterparties can't even carry the attr).
TOUCH_SOURCE_AWAITING_SEND = "awaiting_send"

# WAITING lane read-side policy. TTL: a stamp older than this is a ghost —
# excluded from the lane and collapsed into a count (per-row wallpaper trains
# the operator's eye to skip the section). 60d keeps expiry INSIDE the skill
# layer's 90d Gmail sweep window, so a reply can still clear the stamp before
# the thread ages out of the sweep entirely.
_WAITING_TTL_DAYS = 60
# At most this many WAITING candidates survive the --limit trim — the draft
# budget is won on raw urgency, and days-waiting grows urgency monotonically,
# so an uncapped WAITING lane would eventually crowd Partner/Owed out of the
# skill layer's draft slots (and >2 nudge drafts per day is noise regardless).
_WAITING_DRAFT_SLOT_CAP = 2

# ── Cold-responder lane ─────────────────────────────────────────────────
# Slot cap inside the --limit trim, same rationale as WAITING: silence grows
# urgency monotonically, and these rows are paste-by-hand work that must not
# crowd Partner/Owed out of the skill layer's verify+draft budget. 3 not 2:
# a DM paste costs the operator seconds, a Gmail draft costs the skill layer
# a verified thread.
_COLD_RESPONDER_SLOT_CAP = 3
# Per-lane caps applied by the trim (a MAXIMUM, never a reservation).
_LANE_SLOT_CAPS: dict[WarmLane, int] = {
    WarmLane.WAITING: _WAITING_DRAFT_SLOT_CAP,
    WarmLane.COLD_RESPONDER: _COLD_RESPONDER_SLOT_CAP,
}
_COLD_RESPONDER_PREVIEW = 5
_COLD_SNIPPET_CHARS = 160
_COLD_DM_LANGS = ("es", "pt", "en")
# Silence buckets for the ``{when}`` placeholder (upper bounds in calendar
# days; the last is open-ended). Calibrated so the phrase never contradicts
# the real silence at a boundary: "a few days ago" at 13 days and "a few
# weeks back" at 59 days both read as not having checked. The BUCKETS are
# engine policy and the phrases are plain time vocabulary for the three
# languages the engine supports, so both stay in code; the DM BODY is
# operator copy and lives in ``content/followup_dm.json``.
_COLD_DM_WHEN_BUCKETS = (10, 35, 75)
_COLD_DM_WHEN: dict[str, tuple[str, str, str, str]] = {
    "es": ("hace unos días", "hace unas semanas", "hace un mes", "hace un tiempo"),
    "pt": ("uns dias atrás", "umas semanas atrás", "um mês atrás", "um tempo atrás"),
    "en": ("a few days ago", "a few weeks back", "about a month ago", "a while back"),
}
# Rendered when followup_dm.json is missing/corrupt or a language is absent
# from it. Deliberately unusable as outreach: a broken copy file must read as
# broken in the digest, never as a generic bot line someone might paste.
_COLD_DM_FALLBACK = (
    "{name} - [content/followup_dm.json is missing, unreadable, or has no "
    "copy for this language — write your follow-up DM there] ({when})"
)
# Prepended to the DM when the prospect's language is not on record, so a
# fast paste cannot ship Spanish to a Brazilian without a visible cue.
_COLD_DM_LANG_WARNING = (
    "[language not on record - defaulting to ES, check before pasting]"
)
# Shown when the person's name did not resolve — the operator fills it in.
_COLD_DM_NAME_PLACEHOLDER = "[Name]"


@dataclass
class FollowupCandidate:
    """One warm-stale account surfaced by the radar."""

    object: str  # "linkedin_outreach" | "deals"
    record_id: str  # parent person record_id (entries) or deal record_id
    reason: FollowupReason
    lane: WarmLane
    last_touch: date | None
    silent_days: int
    heat: int
    value_mult: float
    urgency: float
    entry_id: str = ""
    company_id: str | None = None
    # Channel PRIOR for the skill layer (F1), derived from cheap Attio-resident
    # evidence only — which is structurally absent at entry level today (see
    # _derive_channel_hint). NOT a routing verdict: the skill layer ALWAYS runs
    # the authoritative Gmail thread check for in-limit candidates regardless
    # of this hint; the hint only routes what happens when that Gmail search
    # comes back empty ("linkedin_only" → LinkedIn-warm/DM lane, "unknown"
    # deals → try Attio email search before declaring unverified, "email" →
    # evidence of an email relationship exists).
    email_campaign_stage: str | None = None
    email_address: str | None = None
    channel_hint: str = "unknown"
    # Deal-side partner attribution: the canonical lowercase email of the
    # partner who introduced this deal (from deals.referred_by). Non-empty →
    # the deal is routed to the PARTNER lane and the intro counts as email
    # evidence for channel_hint. Entries carry partner attribution via the
    # Partner Intro stage instead, so this stays None for entry candidates.
    referred_by: str | None = None
    # Enriched lazily for the top-N only (avoids a get_person per candidate).
    name: str | None = None
    company: str | None = None
    # Always False from this module — the skill layer verifies via MCP.
    verified: bool = False
    # True when last_touch is derived from the entry/deal creation date rather
    # than a real contact stamp — the skill layer must treat the silence figure
    # as approximate and re-derive from the email thread / call transcript.
    last_touch_synthetic: bool = False
    # Which tier resolved last_touch (TOUCH_SOURCE_*): "verified" /
    # "interaction" / "contact_stamp" / "created_at" / "awaiting_send" /
    # "none". Only "created_at" is synthetic; kept separate from the bool so
    # learned metrics can segment the tiers without blending.
    last_touch_source: str = TOUCH_SOURCE_NONE
    # WAITING-lane state (awaiting_reply_* attrs), carried on EVERY candidate
    # so the skill layer can enforce the nudge ceiling and reuse the canonical
    # note even when the account surfaced via a different lane (e.g. an
    # exhausted waiting account re-surfacing on plain stage staleness must
    # still never get another auto-nudge).
    awaiting_reply_since: date | None = None
    awaiting_reply_thread_id: str | None = None
    awaiting_reply_note_id: str | None = None
    awaiting_reply_nudge_count: int = 0
    # Gmail conversation-ledger signal (PR-214, opt-in sweep). True when the
    # optional Gmail sweep saw an inbound reply from this candidate's email
    # AFTER its CRM-derived last_touch — the ball already moved, so the
    # CRM-staleness read is stale. Advisory only: the sweep NEVER drops a
    # candidate (the skill layer/operator decides); it just annotates so a
    # "you went quiet" digest line can be reconciled against the real inbox.
    email_reply_seen: bool = False
    # True when the person (any associated person, for deals) replied to the
    # email drip (people.email_campaign_stage == "email_responded"). NOT a
    # decline and NOT suppression: the row surfaces so the account isn't
    # forgotten, but a human owns that thread — the skill layer renders it
    # and must never auto-draft on it (any channel).
    email_responded: bool = False
    # Cold-responder lane context — populated only on RESPONDED_COLD rows,
    # None everywhere else (stable JSON schema). ``their_last_reply`` is the
    # entry's last_response_text; ``our_last_dm`` / ``our_last_dm_at`` are the
    # operator's DM body + date from manual_touch_state; ``dm_language`` is
    # the resolved es/pt/en code (person override > entry attr) or None when
    # unset/unsupported (the DM then defaults to ES and the row is flagged).
    their_last_reply: str | None = None
    their_last_reply_at: date | None = None
    our_last_dm: str | None = None
    our_last_dm_at: date | None = None
    dm_language: str | None = None
    notes: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        # ENFORCE the synthetic ⟺ created_at coupling at construction — a
        # site that sets one but not the other would make the digest's approx
        # flag contradict the JSON's source label, and nothing downstream
        # could tell (the exact blending failure the source field prevents).
        if self.last_touch_synthetic != (self.last_touch_source == TOUCH_SOURCE_CREATED_AT):
            raise ValueError(
                f"FollowupCandidate {self.record_id}: last_touch_synthetic="
                f"{self.last_touch_synthetic} contradicts last_touch_source="
                f"{self.last_touch_source!r} (synthetic ⟺ source=='created_at')"
            )


@dataclass
class RadarResult:
    """Detection output plus observability signals (see module docstring)."""

    candidates: list[FollowupCandidate]
    degraded: list[str] = field(default_factory=list)
    dropped_no_touch: int = 0
    drafted_skipped: int = 0  # already-drafted within cooldown, awaiting your send
    # Parked pool by reason (muted / snoozed / callback) — surfaced so a
    # fully-parked radar can't read as a false "Radar limpio ✅".
    parked: dict = field(default_factory=lambda: {"muted": 0, "snoozed": 0, "callback": 0})
    # WAITING-lane observability: stamps past the 60d TTL (ghosts — reply is
    # not coming) and accounts at the nudge ceiling (2 sent, no reply — handed
    # to the operator). Counted, never rendered per-row (anti-wallpaper), and
    # never silently dropped.
    waiting_expired: int = 0
    waiting_exhausted: int = 0
    # Records hidden radar-wide by the §3.1 email hard-decline set (person
    # said no to / unsubscribed from the drip; deals count when ANY
    # associated person declined). Counted, never rendered per-row: a live
    # deal vanishing because one contact unsubscribed must be auditable,
    # not silent — the operator can re-associate people or close the deal.
    email_declined_suppressed: int = 0


def _parse_attio_date(val) -> date | None:
    """Parse an Attio date/datetime attribute value into a ``date``.

    Attio returns ISO strings ("2026-06-01" or "2026-06-01T09:30:00Z").
    Returns None for missing/unparseable values — a None last-touch means the
    candidate can't be ranked on silence and is dropped (counted, not silent).
    """
    if not val:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    if not s:
        return None
    # Normalize a trailing Z so datetime.fromisoformat accepts it.
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).date()
    except ValueError:
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            return None


def _entry_last_touch(attrs: dict) -> tuple[date | None, bool]:
    """Coarse last-touch for a list entry, and whether it is synthetic.

    Returns ``(date, synthetic)``. Real last-touch is
    max(last_contact_date, response_received_at). When neither exists we fall
    back to the entry creation date and mark it ``synthetic=True`` so the digest
    and skill layer know the silence figure is "since creation," not a real
    touch. Returns ``(None, False)`` when nothing is datable.

    DELIBERATE v2 SCOPE CUT: entries stay on these agent-written stamps.
    The person-interaction join built for deals (the entry ``record_id`` IS a
    person record_id, so it would apply directly) is deferred — deals were
    the proven mis-ranking, entries usually carry real stamps, and joining
    ~200-500 entry persons per run is a different cost class than a few dozen
    deal contacts. If entry stamps prove stale in practice, lift
    ``_deal_join_date`` to cover entries too.
    """
    real = [
        _parse_attio_date(attrs.get("last_contact_date")),
        _parse_attio_date(attrs.get("response_received_at")),
    ]
    dates = [d for d in real if d is not None]
    if dates:
        return max(dates), False
    created = _parse_attio_date(attrs.get("entry_created_at"))
    if created is not None:
        return created, True
    return None, False


def _deal_last_touch(record: dict) -> date | None:
    """LAST-RESORT recency for a deal: its creation date (deal age).

    v2 demoted this to tier 3 of ``_deal_recency`` — it only fires when the
    deal has no verified touch AND no person-interaction data, and it is the
    only tier that marks the candidate synthetic. Reads the top-level
    ``created_at`` (verified live), falling back to ``values.created_at`` in
    case Attio moves the system timestamp under ``values`` — so a shape change
    degrades to a still-correct read rather than dropping every deal.
    """
    top = _parse_attio_date(record.get("created_at"))
    if top is not None:
        return top
    return _parse_attio_date(AttioClient._extract_value(record.get("values", {}), "created_at"))


def _person_last_interaction_date(person_record: dict) -> date | None:
    """The date of a person's most recent Attio-synced interaction.

    Reads ``values.last_interaction`` — Attio's native email/calendar sync
    (interaction-type attribute; the timestamp lives in ``interacted_at``,
    verified live 2026-07-02). NOTE: ``last_interaction`` is the union
    attribute and is populated even when ``last_email_interaction`` is empty,
    so it is the one to read. Returns None for missing/never-interacted/
    malformed values — never raises on shape drift (a malformed person must
    degrade one deal's recency to the next tier, not kill the run).
    """
    items = (person_record.get("values") or {}).get("last_interaction") or []
    if not isinstance(items, list) or not items:
        return None
    first = items[0]
    if not isinstance(first, dict):
        return None
    return _parse_attio_date(first.get("interacted_at"))


def _sanitize_touch_date(d: date | None, today: date) -> date | None:
    """Guard a tier-1/tier-2 recency date against the future.

    A future-dated touch would read as silence ≤ 0 and hide the deal below
    every threshold forever, with no signal (``days_silent`` clamps to 0) —
    the read-side twin of ``stamp_verified_touch``'s write-side guard, needed
    because the stamp can also be hand-edited via the Attio UI/MCP and a
    calendar-sync quirk can future-date ``interacted_at``. One day of skew is
    legitimate (UTC timestamps read "tomorrow" during the operator's evening)
    and clamps to today; anything further future is invalid → None (the
    caller falls to the next tier).
    """
    if d is None or d <= today:
        return d
    if d == today + timedelta(days=1):
        return today
    return None


def _deal_join_date(deal: dict, interactions: dict[str, date], today: date) -> date | None:
    """max(``people.last_interaction``) over the deal's associated people.

    KNOWN LIMITATION (approved trade-off): ``last_interaction`` is
    person-scoped, not deal-scoped — activity with a shared contact about a
    DIFFERENT deal refreshes this deal's recency too, so a neglected deal
    whose champion stays otherwise active can under-surface. The C.0 Gmail
    sweep is the per-conversation backstop; Attio has no per-deal interaction
    timestamp to do better with.
    """
    joined = [
        _sanitize_touch_date(interactions[pid], today)
        for pid in (deal.get("associated_people") or [])
        if pid in interactions
    ]
    dates = [d for d in joined if d is not None]
    return max(dates) if dates else None


def _deal_recency(
    deal: dict, record: dict, interactions: dict[str, date], today: date,
) -> tuple[date | None, bool, str]:
    """Resolve a deal's recency through the three-tier precedence.

    Returns ``(last_touch, synthetic, source)``:
      1. ``last_verified_touch`` (skill-layer C.2 stamp) → (date, False,
         "verified")
      2. max(``interactions``) over the deal's associated_people → (date,
         False, "interaction") — see ``_deal_join_date`` for the
         person-scoped-join limitation
      3. deal creation date → (date, True, "created_at") — the only synthetic
         tier
      4. nothing datable → (None, False, "none") — dropped, counted

    Precedence is strict (approved design): a verified stamp wins even over a
    newer synced interaction, because C.2 re-verification refreshes the stamp
    every time the deal is worked — and the stamp can carry off-thread
    knowledge (WhatsApp/LinkedIn) the sync can never see. The DRAFT-ADVANCE
    check is the exception: the state gate receives max(recency, join) so a
    reply landing after a draft can re-surface even a verified deal (see
    detect_candidates pass 2). Future-dated stamps/interactions are
    sanitized out (``_sanitize_touch_date``) so they can't hide a deal.
    """
    verified = _sanitize_touch_date(
        _parse_attio_date(deal.get("last_verified_touch")), today
    )
    if verified is not None:
        return verified, False, TOUCH_SOURCE_VERIFIED
    join_date = _deal_join_date(deal, interactions, today)
    if join_date is not None:
        return join_date, False, TOUCH_SOURCE_INTERACTION
    created = _deal_last_touch(record)
    if created is not None:
        return created, True, TOUCH_SOURCE_CREATED_AT
    return None, False, TOUCH_SOURCE_NONE


def resolve_deal_recency(
    deal: dict, interactions: dict[str, date], today: date,
) -> tuple[date | None, bool, str]:
    """``_deal_recency`` for callers that only hold a PARSED deal.

    ``AttioClient.parse_deal`` carries the top-level ``created_at``, so the
    parsed deal satisfies both params of ``_deal_recency`` (tier 3 reads
    ``record.get("created_at")`` first). Shared with ``weekly_report``'s deal
    staleness so /sales-report and the radar cannot disagree about the same
    deal's recency.
    """
    return _deal_recency(deal, deal, interactions, today)


def _fetch_deal_interactions(
    attio: AttioClient, person_ids: set[str],
) -> tuple[dict[str, date], list[str]]:
    """ONE bulk person fetch for the whole run → {person_id: last_interaction}.

    Returns ``(interactions, degraded_reasons)``. Fail-open-soft, mirroring
    ``_active_email_person_ids``: a partial or total fetch failure returns
    whatever resolved plus a degraded reason — affected deals fall back to
    their creation date (tier 3), which makes them look STALER (over-surface,
    the safe direction for thresholds), and the skill layer's C.2 verify still
    gates any draft. The ONE under-surfacing exception is named in the
    degraded text: a fallback deal inside the draft cooldown can stay hidden
    even if the prospect has since replied. Never raises.

    The metrics object (``DailyRunMetrics`` — the same shape every other
    ``bulk_fetch_persons_by_record_ids`` caller passes) distinguishes real
    fetch FAILURES (transport/shape errors → degrade loudly) from 404s
    (deleted persons — legitimately absent → next tier, not a degradation).
    ``returned == 0`` with a non-empty request is treated as structural
    breakage: associated people ARE person records, so all-absent means the
    join is broken, not that the data is empty.
    """
    if not person_ids:
        return {}, []
    metrics = DailyRunMetrics()
    try:
        persons = attio.bulk_fetch_persons_by_record_ids(person_ids, metrics=metrics)
    except Exception as exc:  # noqa: BLE001 — degrade loudly, never kill the radar
        print(
            f"WARNING: followup_radar: person-interaction bulk fetch failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return {}, [
            "person-interaction join unavailable (bulk person fetch failed) — "
            "deal recency falls back to creation date this run: silence "
            "figures for deals are deal age, and a recently-drafted deal may "
            "stay hidden even if the prospect has since replied"
        ]
    degraded: list[str] = []
    if not persons:
        degraded.append(
            f"person-interaction join returned 0 of {len(person_ids)} people "
            "— the join may be broken (auth/scope/shape drift); deal recency "
            "is deal age this run, and a recently-drafted deal may stay "
            "hidden even if the prospect has since replied"
        )
    elif metrics.bulk_fetch_records_failed:
        degraded.append(
            f"person-interaction join incomplete — "
            f"{metrics.bulk_fetch_records_failed} of {len(person_ids)} person "
            "reads failed; affected deals fall back to creation-date recency "
            "(and, if recently drafted, may stay hidden despite a new reply)"
        )
    interactions: dict[str, date] = {}
    for rid, rec in persons.items():
        d = _person_last_interaction_date(rec)
        if d is not None:
            interactions[rid] = d
    return interactions, degraded


def _entry_value_mult(attrs: dict) -> float:
    """Value multiplier for an entry (no deal amount available): bump
    enterprise-ICP rows so they don't sort under mid-market noise."""
    if attrs.get("icp_lane_persisted") == 1:
        return _ENTERPRISE_ICP_MULT
    return 1.0


def _deal_value_mult(deal_value) -> float:
    """Value multiplier scaled by deal amount (capped)."""
    if not deal_value:
        return 1.0
    try:
        amount = float(deal_value)
    except (TypeError, ValueError):
        return 1.0
    if amount <= 0:
        return 1.0
    return min(1.0 + amount / _VALUE_REF, _VALUE_MULT_CAP)


def _is_email_evidence(val) -> bool:
    """True only for a non-empty string after strip — whitespace, empty
    strings, and non-str shapes (lists, dicts, numbers) are NOT evidence."""
    return isinstance(val, str) and bool(val.strip())


def _derive_channel_hint(
    *,
    object: str,
    email_address: str | None,
    email_campaign_stage: str | None,
    referred_by: str | None = None,
) -> str:
    """Derive the follow-up channel PRIOR from cheap Attio-resident signals (F1).

    Returns one of ``"email"`` / ``"linkedin_only"`` / ``"unknown"``:
      * ``linkedin_outreach`` entry with an email address OR any
        email_campaign_stage → ``"email"``; with neither → ``"linkedin_only"``
        (no email on file — the likely motion is a DM).
      * ``deals`` → ``"email"`` if any email evidence, else ``"unknown"``
        (deals are company-level; the skill layer still tries an Attio email
        search before declaring a deal unreachable by email). A partner
        referral (``referred_by`` non-empty) counts as email evidence: a
        partner intro email exists by construction. ``referred_by`` is a
        deal-only signal — it is ignored on the entry side.

    Evidence means a non-empty string (post-strip) — see ``_is_email_evidence``.
    Today's values are always None; when a person-attr mirror lands on the list
    entry, whitespace or list-shaped values must not silently upgrade a row to
    ``"email"``. NOTE for that future mirror: semantic validation of campaign
    stages is still needed here — e.g. an unsubscribed/bounced stage is email
    HISTORY, not an email relationship, and should not count as evidence.

    LIMITATION: ``parse_entry`` extracts neither the person email nor
    ``email_campaign_stage`` (both live on the person record, not the list
    entry), and F1 forbids adding a per-candidate person fetch here — detection
    must stay cheap. So today linkedin_outreach entries resolve to
    ``"linkedin_only"`` in practice (matching the live-run finding that most
    warm candidates are LinkedIn-only). If either signal is later mirrored onto
    the list entry, this derivation upgrades them to ``"email"`` automatically.
    """
    has_email = _is_email_evidence(email_address) or _is_email_evidence(email_campaign_stage)
    if object == "deals":
        # A partner intro (referred_by) is email evidence by construction — the
        # partner introduced the operator over email. Only weakens "unknown" upward, never
        # the entry-side derivation (referred_by is None for entries).
        if has_email or _is_email_evidence(referred_by):
            return "email"
        return "unknown"
    return "email" if has_email else "linkedin_only"


# Email-terminal stages split into two radar policies (the LinkedIn
# suppression set can't see either — it reads entry stages, not
# people.email_campaign_stage):
#   * HARD DECLINES (email_not_interested / unsubscribed) suppress EVERY lane
#     — the §3.1 red line, enforced via cross_channel_suppression.
#     email_hard_decline_ids (fail closed-hard: a fetch failure aborts the run
#     — except when the email lane was never provisioned, which degrades).
#   * RESPONDED is in a human's hands — NOT a decline. It still never enters
#     the WAITING lane (the wait is over), and every other candidate carrying
#     it is ANNOTATED (`email_responded`) so the skill layer surfaces the row
#     without ever auto-drafting over the human-owned thread.
_EMAIL_RESPONDED_STAGES = (EmailStage.RESPONDED,)


def _awaiting_reply_state(attrs: dict, today: date) -> tuple[str, date | None, int]:
    """Classify an account's awaiting-reply state from its parsed attrs.

    Returns ``(state, since, nudge_count)`` with state one of:
      * ``"none"``      — no (valid) stamp; the WAITING lane doesn't apply.
      * ``"eligible"``  — stamped, inside the TTL, under the nudge ceiling.
      * ``"expired"``   — stamped but past the 60d TTL (ghost — counted, not
                          rendered; the stage-derived path still applies).
      * ``"exhausted"`` — at the nudge ceiling (2 sent, no reply — handed to
                          the operator; counted, never auto-drafted again).

    A future-dated stamp is sanitized exactly like the other recency tiers
    (``_sanitize_touch_date``: 1-day UTC skew clamps to today, further future
    reads as no stamp — fail closed, never surface on invalid data). A
    MALFORMED nudge count reads as AT THE CEILING (exhausted), never as 0 —
    coercing corruption to 0 would silently reset the anti-nag ceiling and
    re-nudge an account that already got its 2; exhausted is the fail-closed
    direction (visible in the exhausted count, no extra touch, operator
    resets via ``followup-await --clear``).
    """
    since = _sanitize_touch_date(
        _parse_attio_date(attrs.get("awaiting_reply_since")), today
    )
    if since is None:
        return ("none", None, 0)
    try:
        nudges = int(attrs.get("awaiting_reply_nudge_count") or 0)
    except (TypeError, ValueError):
        nudges = WAITING_MAX_NUDGES
    if (today - since).days > _WAITING_TTL_DAYS:
        return ("expired", since, nudges)
    if nudges >= WAITING_MAX_NUDGES:
        return ("exhausted", since, nudges)
    return ("eligible", since, nudges)


def _waiting_candidate(
    *, object: str, record_id: str, entry_id: str, today: date,
    since: date, nudges: int, value_mult: float, company_id: str | None,
    name: str | None, email_campaign_stage: str | None,
    email_address: str | None, thread_id: str | None, note_id: str | None,
    gate_note: str | None,
) -> FollowupCandidate:
    """Build an AWAITING_REPLY candidate (WAITING lane).

    ``last_touch`` is the operator's own unanswered send (awaiting_reply_since)
    — tier ``awaiting_send``, real (non-synthetic) recency. Never called for
    partner-referred/partner-stage accounts (PARTNER outranks WAITING).
    """
    pol = POLICY[FollowupReason.AWAITING_REPLY]
    silent = days_silent(since, today, business=pol.business_days)
    notes: list[str] = []
    if gate_note:
        notes.append(gate_note)
    if nudges:
        notes.append(f"nudge {nudges}/{WAITING_MAX_NUDGES} sent, still no reply")
    return FollowupCandidate(
        object=object,
        record_id=record_id,
        entry_id=entry_id,
        reason=FollowupReason.AWAITING_REPLY,
        lane=pol.lane,
        last_touch=since,
        silent_days=silent,
        heat=pol.heat,
        value_mult=value_mult,
        urgency=urgency_score(pol.heat, silent, pol.threshold_days, value_mult),
        company_id=company_id,
        name=name,
        last_touch_synthetic=False,
        last_touch_source=TOUCH_SOURCE_AWAITING_SEND,
        email_campaign_stage=email_campaign_stage,
        email_address=email_address,
        # WAITING is email-semantic by construction: the stamp is only written
        # after C.2 verified a real Gmail thread, so the channel is email
        # regardless of what the cheap entry-level evidence says.
        channel_hint="email",
        awaiting_reply_since=since,
        awaiting_reply_thread_id=thread_id,
        awaiting_reply_note_id=note_id,
        awaiting_reply_nudge_count=nudges,
        notes=notes,
    )


def _waiting_pre_pass(
    *,
    attrs: dict,
    today: date,
    object: str,
    record_id: str,
    entry_id: str,
    partner_blocked: bool,
    responded_person_ids,
    responded_set: set[str] | None,
    value_mult_fn,
    counters: dict[str, int],
    parked: dict[str, int],
    company_id: str | None = None,
    name: str | None = None,
    referred_by: str | None = None,
) -> tuple[bool, FollowupCandidate | None]:
    """The WAITING lane's shared per-record pre-pass (entries AND deals).

    ONE implementation on purpose — the entry and deal loops feed it their
    loop-local differences (partner check, which person ids the responded
    set tests, value multiplier) and everything else (state classification,
    expired/exhausted counting, the inbound-advance-only state gate, the
    fresh-send suppression, callback precedence, emission) stays structurally
    identical; a rule change lands in both lanes or neither.

    Returns ``(consumed, candidate)``:
      * ``consumed=True``  — the record belongs to the WAITING path this run;
        the caller must ``continue`` (dedup: no stage-derived candidate).
        ``candidate`` may still be None (parked / in cooldown / fresh send).
      * ``consumed=False`` — WAITING doesn't apply (no/invalid stamp, partner
        precedence, prospect replied on email (``responded_set``), responded
        set unreadable (``responded_set is None`` — fail the lane closed), or
        the stamp is expired/exhausted — those two are counted here); the
        caller proceeds down the normal stage-derived path. Hard email
        declines never reach this pre-pass at all — they are suppressed from
        every lane before it (entry loop top / deal pass 1).

    The state gate deliberately receives NO activity date (inbound-advance-
    only): Attio's ``people.last_interaction`` counts the operator's own
    outbound, so a real date here would flip "activity advanced past the
    draft" on every send and churn re-drafts. Direction is only observable in
    the skill layer's get_thread re-check, which clears or re-stamps instead.
    """
    aw_state, aw_since, aw_nudges = _awaiting_reply_state(attrs, today)
    if aw_state == "none" or aw_since is None or partner_blocked:
        return (False, None)
    if isinstance(responded_person_ids, str):
        responded_person_ids = (responded_person_ids,)
    if responded_set is None or any(p in responded_set for p in responded_person_ids):
        # Unreadable set fails the lane closed; a reply on record means the
        # wait is over — never nudge (the skill layer clears the stamp).
        return (False, None)
    if aw_state == "expired":
        counters["waiting_expired"] += 1
        return (False, None)
    if aw_state == "exhausted":
        counters["waiting_exhausted"] += 1
        return (False, None)

    gate, gate_note = _state_gate(attrs, today, None)
    if gate == "exclude":
        if gate_note in parked:
            parked[gate_note] += 1
        return (True, None)
    if gate == "drafted_recent":
        counters["drafted_skipped"] += 1
        return (True, None)
    vmult = value_mult_fn()
    if gate == "callback":
        return (True, _callback_candidate(
            object=object,
            record_id=record_id,
            entry_id=entry_id,
            today=today,
            last_touch=aw_since,
            synthetic=False,
            value_mult=vmult,
            company_id=company_id,
            name=name,
            email_campaign_stage=attrs.get("email_campaign_stage"),
            email_address=attrs.get("email_address"),
            referred_by=referred_by,
            touch_source=TOUCH_SOURCE_AWAITING_SEND,
            awaiting_reply_since=aw_since,
            awaiting_reply_thread_id=attrs.get("awaiting_reply_thread_id"),
            awaiting_reply_note_id=attrs.get("awaiting_reply_note_id"),
            awaiting_reply_nudge_count=aw_nudges,
        ))
    pol = POLICY[FollowupReason.AWAITING_REPLY]
    if days_silent(aw_since, today, business=pol.business_days) < pol.threshold_days:
        return (True, None)  # fresh send — nothing is stale here yet
    return (True, _waiting_candidate(
        object=object,
        record_id=record_id,
        entry_id=entry_id,
        today=today,
        since=aw_since,
        nudges=aw_nudges,
        value_mult=vmult,
        company_id=company_id,
        name=name,
        email_campaign_stage=attrs.get("email_campaign_stage"),
        email_address=attrs.get("email_address"),
        thread_id=attrs.get("awaiting_reply_thread_id"),
        note_id=attrs.get("awaiting_reply_note_id"),
        gate_note=gate_note,
    ))


def _active_email_person_ids(attio: AttioClient) -> tuple[set[str], bool]:
    """Person record_ids currently in an active cold-email-drip stage.

    Excluded from the warm radar so a warm follow-up never collides with an
    in-flight cold email (the double-touch the systems QA lens flagged). Warm
    follow-up wins; the cold path is left to complete or be paused separately.

    A holed set silently fails *open* (someone mid-drip could slip through and
    get double-touched), so the caller marks the run degraded on ``ok=False``
    but still uses the partial set — exclusion still runs for the stages that
    did resolve.
    """
    return email_person_ids_in_stages(attio, EMAIL_ACTIVE_STAGES, "active")


def _responded_email_person_ids(attio: AttioClient) -> tuple[set[str], bool]:
    """Person record_ids whose email drip got a (non-decline) reply.

    Used two ways, with split failure philosophies on ``ok=False``:
      * WAITING-lane exclusion — fails CLOSED for the lane (a holed set could
        nudge someone whose reply is sitting in the inbox).
      * ``email_responded`` candidate annotation — fails OPEN-SOFT (the
        annotation may be missing on some rows; the run is marked degraded and
        the skill layer's per-candidate email-thread re-check remains the
        authoritative catch before any draft).
    """
    return email_person_ids_in_stages(attio, _EMAIL_RESPONDED_STAGES, "responded")


def _read_manual_touch_state() -> tuple[dict, bool]:
    """Phase 0.5's ``exports/manual_touch_state.json``, read-only → (state, ok).

    Missing file → ``({}, True)`` (nothing hand-worked yet). Unreadable /
    corrupt → ``({}, False)``: the cold-responder lane is OFF for the run
    (it has no other trusted evidence) and the run is flagged degraded, so a
    hand-worked row falls back to the plain nudge path instead of silently
    disappearing. Never raises — a side input must not kill detection.
    """
    try:
        from workflows.detect_responses import (
            MANUAL_TOUCH_STATE_PATH,
            _load_manual_touch_state,
        )
        state = _load_manual_touch_state(MANUAL_TOUCH_STATE_PATH)
    except Exception as exc:  # noqa: BLE001 — read-only side input, see docstring
        print(
            f"WARNING: follow-up radar: manual-touch state unreadable "
            f"({type(exc).__name__}: {exc}) — cold-responder lane OFF this run.",
            file=sys.stderr,
        )
        return {}, False
    if state is None:
        return {}, False
    return state, True


def _thread_direction(state_entry: object) -> tuple[str, date | None]:
    """Who spoke last on a RESPONDED entry, per the manual-touch state entry →
    ``("ours" | "theirs" | "unknown", date)``.

    * ``"ours"``   — our manual DM is the last message; date = its
      ``touch_date``. Entries written before ball tracking (no ``ball`` key)
      count as ours: they were only ever written when ours was last.
    * ``"theirs"`` — a later scrape saw the prospect's message last; date =
      ``ball_observed``. A REAL touch the CRM never records (RESPONDED
      entries skip the reply loop), so the caller uses it as the last touch.
    * ``"unknown"``— no (valid) entry.

    This is deliberately the ONLY "ours last" evidence. The CRM rule
    ``last_contact_date > response_received_at`` was tried and rejected:
    cadence drift repair (``terminal_dm_step_low``), the dedup MAX-merge and
    the consistency sweep all bump ``last_contact_date`` on Responded entries
    with no message from us, so that rule renders a paste-ready nudge to
    someone who wrote last — the worst outcome this lane guards against. And
    ``response_received_at`` is never rewritten after the first flip, so it
    cannot say who spoke last either.
    """
    se = state_entry if isinstance(state_entry, dict) else None
    if not se:
        return "unknown", None
    if str(se.get("ball") or "ours") == "theirs":
        return "theirs", _parse_attio_date(se.get("ball_observed"))
    return "ours", _parse_attio_date(se.get("touch_date"))


def _normalize_dm_language(raw: object) -> str | None:
    """A stored language value → "es"/"pt"/"en", or None when unset or not a
    language we have DM copy for (the CRM also offers ``fr``; never ship a
    mixed-language DM — the digest flags the row instead). Same strict code
    parsing as the send path (``coerce_language``)."""
    lang = coerce_language(raw)
    return lang.value if lang is not None and lang.value in _COLD_DM_LANGS else None


def _snippet(text: object, limit: int = _COLD_SNIPPET_CHARS) -> str:
    """One-line, whitespace-collapsed excerpt for a digest context line."""
    flat = " ".join(str(text or "").split())
    return flat if len(flat) <= limit else flat[: limit - 1].rstrip() + "…"


_COLD_DM_COPY: object = None
_COLD_DM_COPY_WARNED = False


def _reset_cold_dm_copy_cache() -> None:
    """Clear the memoized follow-up DM copy (test hook / long-lived procs)."""
    global _COLD_DM_COPY, _COLD_DM_COPY_WARNED
    _COLD_DM_COPY = None
    _COLD_DM_COPY_WARNED = False


def _cold_dm_template(lang: str) -> str:
    """The operator's paste-ready DM template for ``lang``.

    Copy lives in ``content/followup_dm.json`` (``cold_responder`` group) so
    it is operator-editable like every other outreach template — but NOT in
    messages.json, whose entries are the corpus the Phase 0.5 self-echo
    matcher compares scraped threads against (copy there would make a pasted
    DM look like one of our own template sends, and the row would never
    clear). A missing/corrupt file degrades to a visibly-broken placeholder:
    rendering happens inside the digest, which must never raise.
    """
    global _COLD_DM_COPY, _COLD_DM_COPY_WARNED
    if _COLD_DM_COPY is None:
        try:
            from models.campaign import load_followup_dm_templates

            group = load_followup_dm_templates().get("cold_responder") or {}
            _COLD_DM_COPY = {
                k: v for k, v in group.items() if isinstance(v, str) and v.strip()
            }
        except Exception as exc:  # noqa: BLE001 — digest render must not raise
            _COLD_DM_COPY = {}
            if not _COLD_DM_COPY_WARNED:
                _COLD_DM_COPY_WARNED = True
                print(
                    f"WARNING: follow-up radar: could not load "
                    f"content/followup_dm.json ({type(exc).__name__}: {exc}) — "
                    f"cold-responder rows render a placeholder DM.",
                    file=sys.stderr,
                )
    template = _COLD_DM_COPY.get(lang) if isinstance(_COLD_DM_COPY, dict) else None
    return template or _COLD_DM_FALLBACK


def render_manual_dm(c: FollowupCandidate) -> str:
    """The paste-ready manual DM for a cold-responder row.

    Operator copy in the prospect's language (ES when unknown — flagged in
    the digest AND prefixed with a warning line inside the text so it cannot
    be pasted unnoticed), first name only (a ``[Name]`` placeholder when the
    name didn't resolve), ``{when}`` scaled to the silence. This is a
    starting text: the skill layer may tailor it to the exchange in the same
    register, and the operator pastes it by hand. It is never sent by code.
    """
    lang = c.dm_language or "es"
    first = (c.name or "").split()[0] if (c.name or "").strip() else ""
    name = first or _COLD_DM_NAME_PLACEHOLDER
    bucket = sum(c.silent_days >= bound for bound in _COLD_DM_WHEN_BUCKETS)
    # .get, not [] — nothing in the engine sets dm_language outside
    # _COLD_DM_LANGS, but this renders inside the digest and a directly
    # constructed candidate must not be able to raise here.
    when = _COLD_DM_WHEN.get(lang, _COLD_DM_WHEN["es"])[bucket]
    try:
        text = _cold_dm_template(lang).format(name=name, when=when)
    except (KeyError, IndexError, ValueError):
        # An operator template with an unknown/malformed placeholder must not
        # blow up the whole digest — show the broken-copy placeholder instead.
        text = _COLD_DM_FALLBACK.format(name=name, when=when)
    if c.dm_language is None:
        text = f"{_COLD_DM_LANG_WARNING}\n{text}"
    return text


def _resolve_list_id(list_id: str | None) -> str:
    return (list_id or os.environ.get("ATTIO_LIST_ID", "")).strip()


def _entry_stage_schema_ok(attio: AttioClient, list_id: str) -> bool:
    """Verify the ``stage`` attribute is deployed on the outreach list.

    Mirrors ``_deal_stage_schema_ok`` for the entries side (the code review
    flagged that only deals were guarded). A renamed/undeployed ``stage`` would
    make every entry fall through ``reason is None`` and silently drop the whole
    entry category. Returns False (with a WARN) so the caller can degrade
    loudly. ``list_id`` is guaranteed non-empty here (build_suppression_set
    already raises on an unset ATTIO_LIST_ID before this runs).
    """
    try:
        slugs = {a.get("api_slug") for a in attio.get_list_attributes(list_id)}
    except Exception as exc:  # noqa: BLE001
        print(
            f"WARNING: followup_radar: could not read outreach list schema "
            f"({type(exc).__name__}); entry follow-ups may be incomplete.",
            file=sys.stderr,
        )
        return False
    if "stage" not in slugs:
        print(
            "WARNING: followup_radar: linkedin_outreach.stage attribute missing "
            "from live schema; skipping entry follow-ups this run.",
            file=sys.stderr,
        )
        return False
    return True


_FOLLOWUP_STATE_SLUGS = frozenset({
    "followup_draft_at",
    "followup_draft_id",
    "followup_snooze_until",
    "followup_muted",
    "followup_callback_date",
})


def _followup_state_schema_ok(attio: AttioClient, list_id: str) -> bool:
    """Verify all five followup_* state attrs are deployed on BOTH objects.

    Unlike stage, a missing state attr reads as None — indistinguishable from
    "not muted/snoozed/drafted" — so a half-run migration would silently ignore
    mutes (re-nag), skip dedup (re-draft), etc. Returns False (caller degrades
    loudly) so state being half-deployed can't masquerade as honored.
    """
    try:
        list_slugs = {a.get("api_slug") for a in attio.get_list_attributes(list_id)}
        deal_slugs = {a.get("api_slug") for a in attio.get_object_attributes("deals")}
    except Exception as exc:  # noqa: BLE001
        print(
            f"WARNING: followup_radar: could not read followup-state schema "
            f"({type(exc).__name__}); state may not be honored this run.",
            file=sys.stderr,
        )
        return False
    return list_slugs >= _FOLLOWUP_STATE_SLUGS and deal_slugs >= _FOLLOWUP_STATE_SLUGS


def _deal_stage_schema_ok(attio: AttioClient) -> bool:
    """Verify the ``stage`` attribute is deployed on the deals object.

    A renamed/undeployed stage attribute would silently match zero deals and
    drop the whole deal-follow-up category. Returns False (with a WARN) so the
    caller can skip deals loudly rather than fail the whole radar — Phase C is
    non-critical and must never abort the surrounding daily run.
    """
    try:
        slugs = {a.get("api_slug") for a in attio.get_object_attributes("deals")}
    except Exception as exc:  # noqa: BLE001
        print(
            f"WARNING: followup_radar: could not read deals schema "
            f"({type(exc).__name__}); skipping deal follow-ups this run.",
            file=sys.stderr,
        )
        return False
    if "stage" not in slugs:
        print(
            "WARNING: followup_radar: deals.stage attribute missing from live "
            "schema; skipping deal follow-ups this run.",
            file=sys.stderr,
        )
        return False
    return True


def _state_gate(attrs: dict, today: date, last_touch: date | None) -> tuple[str, str | None]:
    """Decide what the follow-up STATE (Phase 2 attrs) says about this account.

    Returns ``(verdict, detail)`` where verdict is one of:
      * ``"exclude"``       — parked. ``detail`` is the sub-reason
                              (``"muted"`` / ``"snoozed"`` / ``"callback"``) so
                              the caller can COUNT it — a parked pool must be
                              surfaced, never a silent structural exclusion.
      * ``"drafted_recent"``— a draft exists within the cooldown and no real
                              activity has advanced past it → don't re-draft.
      * ``"callback"``      — a promised callback has come due → hard-surface it,
                              bypassing the normal staleness threshold.
      * ``"surface"``       — proceed with the normal staleness path; ``detail``
                              is set when an un-actioned draft has gone stale
                              (re-surface as an escalation note).
    Rows created before the followup-state migration have all-None state → the
    function returns ``("surface", None)`` (v1 behavior preserved).
    """
    if attrs.get("followup_muted"):
        return ("exclude", "muted")
    snooze = _parse_attio_date(attrs.get("followup_snooze_until"))
    if snooze is not None and snooze >= today:
        return ("exclude", "snoozed")
    callback = _parse_attio_date(attrs.get("followup_callback_date"))
    if callback is not None and callback > today:
        return ("exclude", "callback")  # parked until the callback date arrives

    draft_at = _parse_attio_date(attrs.get("followup_draft_at"))
    if draft_at is not None:
        advanced = last_touch is not None and last_touch > draft_at
        if not advanced:
            days_since = (today - draft_at).days
            if days_since < DRAFT_COOLDOWN_DAYS:
                return ("drafted_recent", None)
            return ("surface", f"drafted {days_since}d ago, still unsent")

    if callback is not None and callback <= today:
        return ("callback", None)
    return ("surface", None)


def _callback_candidate(
    *, object: str, record_id: str, entry_id: str, today: date,
    last_touch: date | None, synthetic: bool, value_mult: float, company_id: str | None,
    name: str | None, email_campaign_stage: str | None = None,
    email_address: str | None = None, referred_by: str | None = None,
    touch_source: str = TOUCH_SOURCE_NONE,
    awaiting_reply_since: date | None = None,
    awaiting_reply_thread_id: str | None = None,
    awaiting_reply_note_id: str | None = None,
    awaiting_reply_nudge_count: int = 0,
) -> FollowupCandidate:
    """Build a CALLBACK_DUE candidate (bypasses the staleness threshold).

    A partner-referred deal (``referred_by`` non-empty) is routed to the PARTNER
    lane even when the callback is what surfaced it — the partner's credibility
    still rides on the intro. ``referred_by`` is None for entries (partner
    attribution there comes from the Partner Intro stage instead).

    The awaiting_reply_* passthroughs uphold the "carried on EVERY candidate"
    invariant (see FollowupCandidate) — a due callback on a mid-waiting-cycle
    account must still show the skill layer its real nudge count and note id,
    or the ceiling and canonical-note dedup break exactly there.
    """
    pol = POLICY[FollowupReason.CALLBACK_DUE]
    silent = days_silent(last_touch, today, business=pol.business_days) if last_touch else 0
    lane = WarmLane.PARTNER if _is_email_evidence(referred_by) else pol.lane
    return FollowupCandidate(
        object=object,
        record_id=record_id,
        entry_id=entry_id,
        reason=FollowupReason.CALLBACK_DUE,
        lane=lane,
        last_touch=last_touch,
        silent_days=silent,
        heat=pol.heat,
        value_mult=value_mult,
        urgency=urgency_score(pol.heat, silent, pol.threshold_days, value_mult),
        company_id=company_id,
        name=name,
        last_touch_synthetic=synthetic,
        last_touch_source=touch_source,
        email_campaign_stage=email_campaign_stage,
        email_address=email_address,
        referred_by=referred_by,
        channel_hint=_derive_channel_hint(
            object=object,
            email_address=email_address,
            email_campaign_stage=email_campaign_stage,
            referred_by=referred_by,
        ),
        awaiting_reply_since=awaiting_reply_since,
        awaiting_reply_thread_id=awaiting_reply_thread_id,
        awaiting_reply_note_id=awaiting_reply_note_id,
        awaiting_reply_nudge_count=awaiting_reply_nudge_count,
    )


def detect_candidates(
    attio: AttioClient,
    *,
    today: date | None = None,
    list_id: str | None = None,
) -> RadarResult:
    """Detect warm-stale candidates from Attio (read-only).

    Returns a ``RadarResult`` whose ``candidates`` are sorted by urgency
    (highest first), plus ``degraded`` reasons and a ``dropped_no_touch`` count
    for observability. Names/companies are NOT resolved here — call
    ``enrich_names`` on the trimmed top-N to avoid a per-candidate get_person.
    """
    today = today or operator_today()
    resolved_list_id = _resolve_list_id(list_id)
    # NOTE: the exclusion sets fail with OPPOSITE philosophies, deliberately.
    # build_suppression_set and email_hard_decline_ids fail CLOSED-HARD
    # (raise) — without either we could nag a hard-no, the §3.1 red line.
    # _active_email_person_ids and _responded_email_person_ids fail OPEN-SOFT
    # (partial set + degraded flag) — a holed set only over-includes /
    # under-annotates, and the skill layer's per-candidate email re-verify
    # (SKILL C.2) catches any real overlap. Do not "harmonize" these.
    #
    # The whole email block is skipped when the OPTIONAL email lane was never
    # installed: people.email_campaign_stage does not exist, every filter 400s,
    # and there is no email state to exclude on. Aborting there would produce
    # NO digest at all on an install that simply doesn't run email — so degrade
    # loudly and carry on with empty (not holed) sets. All three sets come from
    # the same attribute, so one absence answers for all three.
    suppressed = build_suppression_set(attio)
    degraded: list[str] = []
    try:
        email_declined = email_hard_decline_ids(attio)
        active_email, active_email_ok = _active_email_person_ids(attio)
        responded_email, responded_email_ok = _responded_email_person_ids(attio)
    except EmailLaneNotProvisioned as exc:
        print(
            f"WARNING: followup_radar: {exc} Skipping every email exclusion "
            "this run (nothing to exclude on).",
            file=sys.stderr,
        )
        email_declined, active_email, responded_email = set(), set(), set()
        active_email_ok = responded_email_ok = True
        degraded.append(
            "email lane not provisioned (people.email_campaign_stage absent) — "
            "email declines/drip/replies were NOT consulted this run; harmless "
            "if you never ran the email lane, otherwise provision the "
            "attribute and re-run"
        )

    # Cold-responder lane side input. Unreadable → the lane is OFF this run
    # (no other evidence is trusted), flagged degraded below; hand-worked rows
    # fall back to the plain RESPONDED nudge path.
    manual_state, manual_state_ok = _read_manual_touch_state()

    candidates: list[FollowupCandidate] = []
    dropped_no_touch = 0
    parked = {"muted": 0, "snoozed": 0, "callback": 0}
    # WAITING-lane responded exclusion fails CLOSED for the lane: pass None
    # so the pre-pass skips WAITING emission entirely (a holed set could
    # nudge someone whose reply is sitting in the inbox). The annotation
    # side below keeps using the partial set (fail open-soft).
    responded_for_waiting: set[str] | None = (
        responded_email if responded_email_ok else None
    )
    if not responded_email_ok:
        degraded.append(
            "email-responded set unreadable/incomplete — no WAITING nudges "
            "this run (stamped accounts fall back to their stage lane), and "
            "some rows may be missing the email_responded annotation; the "
            "C.2 thread re-check is the only remaining guard — verify before "
            "drafting"
        )
    # Mutable counters, shared with the WAITING pre-pass helper (which can't
    # rebind the caller's ints). drafted_skipped lives here too so the stage
    # paths and the pre-pass accumulate into ONE place; parked is passed to
    # the helper separately (keeps this dict int-valued).
    counters: dict[str, int] = {
        "waiting_expired": 0,
        "waiting_exhausted": 0,
        "drafted_skipped": 0,
        "email_declined_suppressed": 0,
    }
    # deal record_id → associated person ids, filled in the deal pass and
    # used by the email_responded annotation post-pass (deals are
    # company-scoped; membership is any-associated-person, matching the
    # suppression checks). Declared here so the post-pass runs even when the
    # deals schema check fails and only entry candidates exist.
    deal_people: dict[str, tuple[str, ...]] = {}

    if not active_email_ok:
        degraded.append(
            "active cold-email exclusion incomplete (Attio read failed) — "
            "some rows may overlap an in-flight cold drip; verify before drafting"
        )
    if not manual_state_ok:
        degraded.append(
            "manual-touch state unreadable — cold-responder lane OFF this run "
            "(hand-worked Responded rows fall back to Consider nudging / "
            "LinkedIn-warm); fix or delete exports/manual_touch_state.json"
        )

    # State-schema preflight: a half-deployed followup-state migration reads as
    # all-None (mutes/snoozes silently ignored), so degrade loudly if incomplete.
    if resolved_list_id and not _followup_state_schema_ok(attio, resolved_list_id):
        degraded.append(
            "follow-up state schema incomplete — mute/snooze/dedup may not be "
            "honored this run (re-run scripts/migrate_followup_state_schema.py)"
        )

    # ── Warm list entries ──────────────────────────────────────────────
    if _entry_stage_schema_ok(attio, resolved_list_id):
        entries_seen = 0
        warm_found = 0
        waiting_consumed_warm = False
        for entry in attio.query_list_entries(list_id=list_id):
            entries_seen += 1
            attrs = AttioClient.parse_entry(entry)
            record_id = attrs.get("record_id")
            if not record_id:
                continue
            if record_id in suppressed or record_id in active_email:
                continue
            # email_declined is the §3.1 hard-decline set (said no to /
            # unsubscribed from the email drip) — suppresses EVERY lane,
            # exactly like the LinkedIn suppression set, but COUNTED (new
            # behavior this run could hide from the operator, unlike the
            # long-standing silent LinkedIn-set skip above).
            if record_id in email_declined:
                counters["email_declined_suppressed"] += 1
                continue
            # merged_into losers are soft-deleted duplicates — skip.
            if attrs.get("merged_into"):
                continue
            stage = attrs.get("stage")
            reason = WARM_ENTRY_REASONS.get(stage or "")
            # NURTURE is only warm when the prior reply was positive; a plain
            # post-DM3 nurture row is cold.
            if reason is FollowupReason.NURTURE_POSITIVE_STALE and not is_positive_nurture(
                attrs.get("response_classification")
            ):
                reason = None

            # ── WAITING pre-pass (state-derived, evaluated BEFORE the stage
            # early-out: a C.2-stamped unanswered send surfaces even from a
            # non-warm stage — the stamp is stronger evidence than the stage;
            # red lines stay covered by the suppression/hard-decline sets).
            # Precedence: PARTNER (stage) > WAITING > stage lane. An eligible
            # stamp fully preempts the stage path for this entry (dedup — one
            # candidate per record), and a BELOW-THRESHOLD stamp suppresses it
            # too: the operator emailed this account days ago, so surfacing
            # "gone quiet" off older DM stamps would nag a just-touched
            # prospect. Expired/exhausted stamps only count — the stage path
            # still applies to them (the skill layer reads nudge_count off the
            # candidate JSON and never auto-nudges an exhausted account).
            waiting_consumed, waiting_cand = _waiting_pre_pass(
                attrs=attrs,
                today=today,
                object="linkedin_outreach",
                record_id=record_id,
                entry_id=attrs.get("entry_id", ""),
                partner_blocked=reason is FollowupReason.PARTNER_INTRO_UNWORKED,
                responded_person_ids=record_id,
                responded_set=responded_for_waiting,
                value_mult_fn=lambda attrs=attrs: _entry_value_mult(attrs),
                counters=counters,
                parked=parked,
            )
            if waiting_cand is not None:
                candidates.append(waiting_cand)
            if waiting_consumed:
                # NOTE: deliberately NOT counted in warm_found — that counter
                # feeds the stage-title-drift heuristic, and state-derived
                # candidates would mask a real drift. But remember whether the
                # pre-pass consumed any WARM-stage entry: those prove warm
                # stage titles still parse, so the drift alarm below must not
                # fire falsely when WAITING preempted every warm row.
                if reason is not None:
                    waiting_consumed_warm = True
                continue

            if reason is None:
                continue

            # Cold-responder promotion: a RESPONDED row where OUR manual DM is
            # the last message is not "replied, no next step" — the ball is
            # theirs. Route it to the paste-ready DM lane, unless the person
            # replied to the EMAIL drip (email_responded rows are render-only
            # on every channel — no DM text either).
            cold_note: str | None = None
            state_entry = manual_state.get(attrs.get("entry_id") or "")
            direction, direction_at = "unknown", None
            if reason is FollowupReason.RESPONDED_NO_NEXT_STEP:
                direction, direction_at = _thread_direction(state_entry)
                if direction == "ours" and record_id not in responded_email:
                    reason = FollowupReason.RESPONDED_COLD
                elif direction == "theirs":
                    cold_note = (
                        "they wrote last (inbox scrape) — you owe a reply, not a nudge"
                    )

            last_touch, synthetic = _entry_last_touch(attrs)
            if direction == "theirs" and direction_at is not None and (
                last_touch is None or direction_at > last_touch
            ):
                # Their reply is a real touch the CRM never recorded — silence
                # counts from it, not from our older DM (otherwise the row
                # keeps a stale urgency and the "you owe a reply" note nags
                # daily with no clearing path).
                last_touch, synthetic = direction_at, False
            cold_kwargs: dict = {}
            if reason is FollowupReason.RESPONDED_COLD:
                reply = attrs.get("last_response_text")
                body = state_entry.get("last_body") if isinstance(state_entry, dict) else None
                cold_kwargs = {
                    "their_last_reply": (str(reply).strip() or None) if reply else None,
                    "their_last_reply_at": _parse_attio_date(attrs.get("response_received_at")),
                    "our_last_dm": (str(body).strip() or None) if body else None,
                    "our_last_dm_at": direction_at,
                    "dm_language": _normalize_dm_language(attrs.get("language")),
                }
            # Entry source: real contact/response stamps vs created-at fallback.
            if last_touch is None:
                touch_source = TOUCH_SOURCE_NONE
            elif synthetic:
                touch_source = TOUCH_SOURCE_CREATED_AT
            else:
                touch_source = TOUCH_SOURCE_CONTACT_STAMP
            gate, gate_note = _state_gate(attrs, today, last_touch)
            if gate == "exclude":
                if gate_note in parked:
                    parked[gate_note] += 1
                continue
            if gate == "drafted_recent":
                counters["drafted_skipped"] += 1
                continue
            vmult = _entry_value_mult(attrs)
            # Awaiting state re-read for the passthrough fields below: the
            # pre-pass didn't consume this entry (expired/exhausted stamp, or
            # none), but the skill layer still needs the real nudge count on
            # EVERY candidate — the ceiling must hold even when an exhausted
            # account re-surfaces via plain stage staleness.
            _, aw_since, aw_nudges = _awaiting_reply_state(attrs, today)
            # Channel signals for F1. parse_entry does not extract these today
            # (they live on the person record, not the list entry), so they read
            # None → channel_hint == "linkedin_only". Kept as explicit reads so a
            # future list-entry mirror of either attr upgrades the hint for free.
            email_stage = attrs.get("email_campaign_stage")
            email_addr = attrs.get("email_address")
            if gate == "callback":
                warm_found += 1
                candidates.append(_callback_candidate(
                    object="linkedin_outreach",
                    record_id=record_id,
                    entry_id=attrs.get("entry_id", ""),
                    today=today,
                    last_touch=last_touch,
                    synthetic=synthetic,
                    value_mult=vmult,
                    company_id=None,
                    name=None,
                    email_campaign_stage=email_stage,
                    email_address=email_addr,
                    touch_source=touch_source,
                    awaiting_reply_since=aw_since,
                    awaiting_reply_thread_id=attrs.get("awaiting_reply_thread_id"),
                    awaiting_reply_note_id=attrs.get("awaiting_reply_note_id"),
                    awaiting_reply_nudge_count=aw_nudges,
                ))
                continue
            policy = POLICY[reason]
            if last_touch is None:
                dropped_no_touch += 1  # fail-closed, but counted (not silent)
                continue
            silent = days_silent(last_touch, today, business=policy.business_days)
            if silent < policy.threshold_days:
                continue
            warm_found += 1
            candidates.append(
                FollowupCandidate(
                    object="linkedin_outreach",
                    record_id=record_id,
                    entry_id=attrs.get("entry_id", ""),
                    reason=reason,
                    lane=policy.lane,
                    last_touch=last_touch,
                    silent_days=silent,
                    heat=policy.heat,
                    value_mult=vmult,
                    urgency=urgency_score(policy.heat, silent, policy.threshold_days, vmult),
                    last_touch_synthetic=synthetic,
                    last_touch_source=touch_source,
                    notes=[n for n in (gate_note, cold_note) if n],
                    **cold_kwargs,
                    email_campaign_stage=email_stage,
                    email_address=email_addr,
                    channel_hint=_derive_channel_hint(
                        object="linkedin_outreach",
                        email_address=email_addr,
                        email_campaign_stage=email_stage,
                    ),
                    # Carried even outside the WAITING lane so the skill layer
                    # can enforce the nudge ceiling on an exhausted account
                    # that re-surfaced via plain stage staleness.
                    awaiting_reply_since=aw_since,
                    awaiting_reply_thread_id=attrs.get("awaiting_reply_thread_id"),
                    awaiting_reply_note_id=attrs.get("awaiting_reply_note_id"),
                    awaiting_reply_nudge_count=aw_nudges,
                )
            )
        # Drift heuristic: a non-empty list that yields zero warm reasons is a
        # strong "stage titles changed under us" signal (cf. the "0 accepted is
        # a bug signal" playbook rule). A warm-stage entry the WAITING pre-pass
        # consumed still PROVES warm titles parse — suppress the false alarm
        # when WAITING preempted every warm row (but a fully-drifted list,
        # where no entry resolves a warm reason at all, still fires).
        if entries_seen > 0 and warm_found == 0 and not waiting_consumed_warm:
            degraded.append(
                f"0 warm entries matched across {entries_seen} rows — possible "
                "stage-title drift in linkedin_outreach; verify stage names"
            )
    else:
        degraded.append("entry follow-ups skipped — outreach list schema unreadable/drifted")

    # ── Open deals ─────────────────────────────────────────────────────
    if _deal_stage_schema_ok(attio):
        # Pass 1: collect warm-stage deals + the union of their associated
        # people, so the person-interaction join is ONE bulk fetch per run
        # (never per-candidate — detection must stay cheap). Drafted deals —
        # verified or not — are included on purpose: the state gate's
        # "activity advanced past the draft" check needs the join (a reply
        # landing during the draft cooldown must re-surface the deal, and the
        # verified stamp predates the draft by construction). Only deals the
        # gate excludes unconditionally BEFORE consulting recency (muted /
        # future-snoozed / future-callback — _state_gate with a None touch)
        # skip the fetch: their join data would be computed and discarded.
        warm_deals: list[tuple[dict, dict, FollowupReason]] = []
        join_person_ids: set[str] = set()
        for record in attio.search_deals(
            limit=_DEAL_SCAN_LIMIT, fail_if_truncated=True,
        ):
            deal = AttioClient.parse_deal(record)
            reason = DEAL_STAGE_REASONS.get(deal.get("stage") or "")
            if reason is None:
                continue
            if not deal.get("record_id"):
                continue
            associated = tuple(deal.get("associated_people") or ())
            # §3.1 hard-decline suppression, deal side: any associated person
            # who declined/unsubscribed on the email drip kills the whole
            # deal's follow-up (every lane, WAITING included). Any-person is
            # deliberate over-exclusion — the fail-closed direction for a
            # company-scoped record (same call as the WAITING-lane check).
            if any(pid in email_declined for pid in associated):
                counters["email_declined_suppressed"] += 1
                continue
            deal_people[deal["record_id"]] = associated
            warm_deals.append((record, deal, reason))
            if _state_gate(deal, today, None)[0] != "exclude":
                join_person_ids.update(associated)
        interactions, join_degraded = _fetch_deal_interactions(attio, join_person_ids)
        degraded.extend(join_degraded)

        # Pass 2: resolve recency through the three-tier precedence, then the
        # same state gate / threshold / ranking path as v1.
        for record, deal, reason in warm_deals:
            record_id = deal["record_id"]
            # Partner attribution: a non-empty referred_by (canonical lowercase
            # partner email) means a partner introduced this deal. Needed BEFORE
            # the WAITING pre-pass (PARTNER outranks WAITING) and reused by the
            # stage path below.
            deal_referred_by = deal.get("referred_by")
            has_partner_ref = _is_email_evidence(deal_referred_by)

            # ── WAITING pre-pass — deal-side twin of the entry pre-pass (ONE
            # shared implementation; see _waiting_pre_pass for the semantics).
            # LOST deals never reach here (warm_deals is DEAL_STAGE_REASONS-
            # filtered), which is exactly the "never nudge a dead deal" rule;
            # hard email declines never reach here either (dropped in pass 1).
            # Responded check is any-associated-person (over-exclude, the
            # fail-closed direction for a company-scoped record).
            waiting_consumed, waiting_cand = _waiting_pre_pass(
                attrs=deal,
                today=today,
                object="deals",
                record_id=record_id,
                entry_id="",
                partner_blocked=has_partner_ref,
                # The pass-1 tuple (deal_people) — same person set the
                # hard-decline check and the annotation post-pass use, so the
                # three membership checks can never diverge.
                responded_person_ids=deal_people[record_id],
                responded_set=responded_for_waiting,
                value_mult_fn=lambda deal=deal: _deal_value_mult(deal.get("value")),
                counters=counters,
                parked=parked,
                company_id=deal.get("company_id"),
                name=deal.get("name"),
                referred_by=deal_referred_by,
            )
            if waiting_cand is not None:
                candidates.append(waiting_cand)
            if waiting_consumed:
                continue

            last_touch, deal_synthetic, touch_source = _deal_recency(
                deal, record, interactions, today
            )
            # The gate sees the NEWEST real activity signal, not the ranking
            # precedence: a synced interaction newer than a (necessarily
            # older) verified stamp must still count as "activity advanced
            # past the draft", or a reply during the cooldown could never
            # re-surface a verified deal. Ranking keeps strict precedence.
            join_date = _deal_join_date(deal, interactions, today)
            gate_activity = max(
                (d for d in (last_touch, join_date) if d is not None),
                default=None,
            )
            gate, gate_note = _state_gate(deal, today, gate_activity)
            if gate == "exclude":
                if gate_note in parked:
                    parked[gate_note] += 1
                continue
            if gate == "drafted_recent":
                counters["drafted_skipped"] += 1
                continue
            vmult = _deal_value_mult(deal.get("value"))
            # Awaiting state re-read for the passthrough fields below (see the
            # entry-loop note — the ceiling must hold on every candidate).
            _, aw_since, aw_nudges = _awaiting_reply_state(deal, today)
            # Deals carry no email attr in parse_deal → no cheap email evidence,
            # so channel_hint resolves to "unknown"; the skill layer still tries
            # an Attio email search before declaring the deal email-unreachable.
            deal_email_stage = deal.get("email_campaign_stage")
            deal_email_addr = deal.get("email_address")
            if gate == "callback":
                candidates.append(_callback_candidate(
                    object="deals",
                    record_id=record_id,
                    entry_id="",
                    today=today,
                    last_touch=last_touch,
                    synthetic=deal_synthetic,
                    value_mult=vmult,
                    company_id=deal.get("company_id"),
                    name=deal.get("name"),
                    email_campaign_stage=deal_email_stage,
                    email_address=deal_email_addr,
                    referred_by=deal_referred_by,
                    touch_source=touch_source,
                    awaiting_reply_since=aw_since,
                    awaiting_reply_thread_id=deal.get("awaiting_reply_thread_id"),
                    awaiting_reply_note_id=deal.get("awaiting_reply_note_id"),
                    awaiting_reply_nudge_count=aw_nudges,
                ))
                continue
            policy = POLICY[reason]
            if last_touch is None:
                dropped_no_touch += 1
                continue
            silent = days_silent(last_touch, today, business=policy.business_days)
            if silent < policy.threshold_days:
                continue
            candidates.append(
                FollowupCandidate(
                    object="deals",
                    record_id=record_id,
                    reason=reason,
                    # A partner-referred deal is a partner intro regardless of
                    # its pipeline stage → override to the PARTNER lane so it
                    # surfaces in the partner section (a dropped partner intro
                    # burns the partner). Keep the stage-derived reason/label.
                    lane=WarmLane.PARTNER if has_partner_ref else policy.lane,
                    last_touch=last_touch,
                    silent_days=silent,
                    heat=policy.heat,
                    value_mult=vmult,
                    urgency=urgency_score(policy.heat, silent, policy.threshold_days, vmult),
                    company_id=deal.get("company_id"),
                    name=deal.get("name"),  # deals already carry a display name
                    email_campaign_stage=deal_email_stage,
                    email_address=deal_email_addr,
                    referred_by=deal_referred_by,
                    channel_hint=_derive_channel_hint(
                        object="deals",
                        email_address=deal_email_addr,
                        email_campaign_stage=deal_email_stage,
                        referred_by=deal_referred_by,
                    ),
                    # Synthetic ONLY on the created_at tier (no verified stamp,
                    # no person-interaction data) — verified/joined recency is
                    # real, and last_touch_source lets Phase 2's discard-rate
                    # segment the three tiers without blending.
                    last_touch_synthetic=deal_synthetic,
                    last_touch_source=touch_source,
                    # Carried even outside the WAITING lane so the skill layer
                    # can enforce the nudge ceiling on an exhausted account
                    # that re-surfaced via plain stage staleness.
                    awaiting_reply_since=aw_since,
                    awaiting_reply_thread_id=deal.get("awaiting_reply_thread_id"),
                    awaiting_reply_note_id=deal.get("awaiting_reply_note_id"),
                    awaiting_reply_nudge_count=aw_nudges,
                    notes=[gate_note] if gate_note else [],
                )
            )
    else:
        degraded.append("deal follow-ups skipped — deals schema unreadable/drifted")

    # ── email_responded annotation post-pass ───────────────────────────
    # ONE place on purpose (candidates are constructed at five sites — a
    # per-site flag would eventually miss one). RESPONDED is not a decline:
    # the row still surfaces, but the skill layer must never auto-draft over
    # a human-owned email thread. WAITING candidates can't carry the flag
    # (the pre-pass excludes responded accounts from the lane entirely).
    for c in candidates:
        pids = (
            (c.record_id,)
            if c.object == "linkedin_outreach"
            else deal_people.get(c.record_id, ())
        )
        if any(pid in responded_email for pid in pids):
            c.email_responded = True

    candidates.sort(key=lambda c: c.urgency, reverse=True)
    return RadarResult(
        candidates=candidates,
        degraded=degraded,
        dropped_no_touch=dropped_no_touch,
        drafted_skipped=counters["drafted_skipped"],
        parked=parked,
        waiting_expired=counters["waiting_expired"],
        waiting_exhausted=counters["waiting_exhausted"],
        email_declined_suppressed=counters["email_declined_suppressed"],
    )


def enrich_names(attio: AttioClient, candidates: list[FollowupCandidate]) -> None:
    """Resolve person/company display names on the given candidates in place.

    Batches the person lookups (top-N only) via ``bulk_fetch_persons_by_record_ids``
    so the digest is human-scannable without a per-candidate round-trip.
    """
    person_ids = {
        c.record_id for c in candidates if c.object == "linkedin_outreach"
    }
    records = attio.bulk_fetch_persons_by_record_ids(person_ids) if person_ids else {}
    for c in candidates:
        if c.object == "linkedin_outreach":
            rec = records.get(c.record_id)
            if rec:
                name, company, *_ = attio.extract_record_info(rec)
                c.name = name
                c.company = company
            if c.lane is WarmLane.COLD_RESPONDER:
                # The person-level `people.language` override outranks the
                # entry attr on the send path; honor it for the paste text
                # too (free when the record above was fetched — the client
                # caches it from extract_record_info).
                override = _normalize_dm_language(
                    attio.person_language_override(c.record_id)
                )
                if override:
                    c.dm_language = override
        elif c.object == "deals" and c.company_id and not c.company:
            comp = attio.get_company(c.company_id)
            if comp:
                # Company name lives in the standard "name" attribute.
                vals = (comp.get("values") or {})
                name_arr = vals.get("name") or []
                if name_arr and isinstance(name_arr, list):
                    first = name_arr[0]
                    if isinstance(first, dict):
                        c.company = first.get("value")


def to_json(candidates: list[FollowupCandidate]) -> list[dict]:
    """Serialize candidates for the skill layer to consume."""
    return [
        {
            "object": c.object,
            "record_id": c.record_id,
            "entry_id": c.entry_id,
            "reason": c.reason.value,
            "lane": c.lane.value,
            "last_touch": c.last_touch.isoformat() if c.last_touch else None,
            "last_touch_synthetic": c.last_touch_synthetic,
            "last_touch_source": c.last_touch_source,
            "silent_days": c.silent_days,
            "urgency": c.urgency,
            "name": c.name,
            "company": c.company,
            "company_id": c.company_id,
            "channel_hint": c.channel_hint,
            "email_campaign_stage": c.email_campaign_stage,
            "email_address": c.email_address,
            "referred_by": c.referred_by,
            "verified": c.verified,
            # WAITING-lane state — carried on EVERY candidate (see the
            # dataclass note): the skill layer enforces the nudge ceiling and
            # reuses the canonical note from these regardless of lane.
            "awaiting_reply_since": (
                c.awaiting_reply_since.isoformat() if c.awaiting_reply_since else None
            ),
            "awaiting_reply_thread_id": c.awaiting_reply_thread_id,
            "awaiting_reply_note_id": c.awaiting_reply_note_id,
            "awaiting_reply_nudge_count": c.awaiting_reply_nudge_count,
            "email_reply_seen": c.email_reply_seen,
            # True → surface only, NEVER auto-draft (a human owns the email
            # thread — see SKILL.md C.3).
            "email_responded": c.email_responded,
            # Operator notes (stale draft, "they wrote last — you owe a
            # reply", …) — the skill layer must see them, not just the digest.
            "notes": list(c.notes),
            # Cold-responder lane — None outside that lane. dm_text is the
            # paste-ready manual DM; the skill layer shows it (and may tailor
            # it to the exchange in the same register) — never sends.
            "their_last_reply": c.their_last_reply,
            "their_last_reply_at": (
                c.their_last_reply_at.isoformat() if c.their_last_reply_at else None
            ),
            "our_last_dm": c.our_last_dm,
            "our_last_dm_at": c.our_last_dm_at.isoformat() if c.our_last_dm_at else None,
            "dm_language": c.dm_language,
            "dm_text": render_manual_dm(c) if c.lane is WarmLane.COLD_RESPONDER else None,
        }
        for c in candidates
    ]


def _who(c: FollowupCandidate) -> str:
    """Best available display string for a candidate."""
    parts = [p for p in (c.name, c.company) if p]
    return " · ".join(parts) if parts else (c.name or c.company or c.record_id[:8])


def _degraded_banner(degraded: list[str], dropped_no_touch: int) -> list[str]:
    """Warning lines stamped on the digest so a degraded run never reads clean."""
    if not degraded and not dropped_no_touch:
        return []
    lines = ["", "> ⚠ **Detection degraded — do not treat this as a full picture:**"]
    lines.extend(f">  - {d}" for d in degraded)
    if dropped_no_touch:
        lines.append(
            f">  - {dropped_no_touch} warm row(s) had no datable touch — skipped "
            "(not clean); check for a nulled/renamed timestamp attribute"
        )
    return lines


# Stable within-lane ordering: heat first, then longest real silence (last_touch
# ascending), then record_id. Unlike urgency this does NOT drift day-to-day (all
# three keys are fixed per account until it's acted on), so the operator sees the
# same "do these" list each morning rather than a daily reshuffle — the specific
# way a stateless digest erodes trust (operator-trust QA lens). urgency still
# drives the cross-lane top-N and the --limit trim.
_FAR_FUTURE = date(9999, 1, 1)


def _stable_key(c: FollowupCandidate) -> tuple:
    return (-c.heat, c.last_touch or _FAR_FUTURE, c.record_id)


# How many rows to show per lane before collapsing to a count. Owed is "do
# these" so it gets a larger window, but it is STILL capped: a real workspace
# can carry 150+ owed rows (e.g. a stale Partner-Intro import), and an
# unscannable wall trains the operator to skip the digest just as fast as a
# noisy nudge lane. `--full` expands both.
_OWED_PREVIEW = 15
_NUDGE_PREVIEW = 3
# Partner intros are the highest-stakes lane (a dropped intro burns the
# partner), so they get a wider window than Nudge but are still capped — a stale
# Partner-Intro import can carry 100+ rows and an unscannable wall trains the
# operator to skip the digest. `--full` expands.
_PARTNER_PREVIEW = 10
# LinkedIn-warm accounts (channel_hint == "linkedin_only") have no email on
# file, so the likely motion is a DM — collapsed like Nudge so the
# email-actionable lanes stay above the fold. `--full` expands.
_LINKEDIN_WARM_PREVIEW = 3
# WAITING rows ("you sent, no reply") sit between Owed and LinkedIn-warm in
# stakes: real conversations, but the ball is nominally theirs. Capped hard —
# a founder's sent mail can carry dozens of unanswered threads, and a wall of
# them buries the Partner/Owed sections. `--full` expands.
_WAITING_PREVIEW = 5


def _parked_str(parked: dict | None) -> str:
    """'N parked (M muted · S snoozed · C awaiting callback)' or '' if none."""
    if not parked:
        return ""
    total = sum(parked.values())
    if not total:
        return ""
    return (
        f"{total} parked ({parked.get('muted', 0)} muted · "
        f"{parked.get('snoozed', 0)} snoozed · "
        f"{parked.get('callback', 0)} awaiting callback)"
    )


def partition_lanes(
    candidates: list[FollowupCandidate],
) -> dict[str, list[FollowupCandidate]]:
    """Split candidates into the six DISJOINT digest sections (stable-sorted).

    Single source of truth for the lane/channel split — both the digest header
    counts (render_digest) and the summary dict (run_followup_radar) derive from
    this, so the header and footer can never show different numbers for the
    same run. Partition rules:
      * ``partner``       — PARTNER lane, regardless of channel (their verify
                            path is the partner's intro thread, never DM).
      * ``waiting``       — WAITING lane ("you sent, no reply"). Email-semantic
                            by construction (the stamp requires a C.2-verified
                            Gmail thread), so it is NEVER pulled into
                            linkedin_warm regardless of channel_hint.
      * ``cold_responder``— COLD_RESPONDER lane: paste-by-hand LinkedIn DMs.
                            Own section regardless of channel_hint — never
                            pulled into linkedin_warm (which is "DM likely,
                            you decide"; this lane is "DM text ready").
      * ``linkedin_warm`` — remaining non-partner candidates with
                            ``channel_hint == "linkedin_only"`` — surfaced in
                            their own DM section, never silently dropped,
                            never double-listed under Owed/Nudge.
      * ``owed``/``nudge``— the remaining OWED/NUDGE candidates (email/unknown
                            channel).
    Invariant (ENFORCED, not just documented): the six buckets are disjoint
    and their sizes sum to ``len(candidates)`` — raises ``ValueError`` naming
    the unpartitioned record_ids otherwise.
    """
    partner = sorted((c for c in candidates if c.lane is WarmLane.PARTNER), key=_stable_key)
    non_partner = [c for c in candidates if c.lane is not WarmLane.PARTNER]
    waiting = sorted(
        (c for c in non_partner if c.lane is WarmLane.WAITING), key=_stable_key
    )
    cold = sorted(
        (c for c in non_partner if c.lane is WarmLane.COLD_RESPONDER), key=_stable_key
    )
    _own_section = (WarmLane.WAITING, WarmLane.COLD_RESPONDER)
    linkedin_warm = sorted(
        (c for c in non_partner
         if c.lane not in _own_section and c.channel_hint == "linkedin_only"),
        key=_stable_key,
    )
    owed = sorted(
        (c for c in non_partner
         if c.lane is WarmLane.OWED and c.channel_hint != "linkedin_only"),
        key=_stable_key,
    )
    nudge = sorted(
        (c for c in non_partner
         if c.lane is WarmLane.NUDGE and c.channel_hint != "linkedin_only"),
        key=_stable_key,
    )
    # ENFORCE the invariant, don't just document it. Unreachable with today's
    # WarmLane members and channel values, but a future lane (or an unexpected
    # channel_hint) would otherwise make candidates vanish SILENTLY from both
    # the digest and the summary counts — the exact failure mode this feature
    # exists to prevent. Loud > silent.
    assigned = (
        len(partner) + len(owed) + len(waiting) + len(cold) + len(linkedin_warm)
        + len(nudge)
    )
    if assigned != len(candidates):
        placed = {
            id(c)
            for bucket in (partner, owed, waiting, cold, linkedin_warm, nudge)
            for c in bucket
        }
        leftover = [c.record_id for c in candidates if id(c) not in placed]
        raise ValueError(
            f"partition_lanes invariant violated: {len(candidates)} candidates "
            f"but {assigned} partitioned; unpartitioned record_ids: {leftover} "
            "— a new WarmLane member or channel_hint value needs a bucket here."
        )
    return {
        "partner": partner,
        "owed": owed,
        "waiting": waiting,
        "cold_responder": cold,
        "linkedin_warm": linkedin_warm,
        "nudge": nudge,
    }


def render_digest(
    candidates: list[FollowupCandidate],
    *,
    top_n: int = 3,
    degraded: list[str] | None = None,
    dropped_no_touch: int = 0,
    drafted_skipped: int = 0,
    parked: dict | None = None,
    waiting_expired: int = 0,
    waiting_exhausted: int = 0,
    waiting_capped: int = 0,
    email_declined_suppressed: int = 0,
    full: bool = False,
    cold_capped: int = 0,
) -> str:
    """Render the scannable operator digest (markdown).

    Sections in stakes order: Partner intros (highest stakes — a dropped intro
    burns a partner), then Owed (near-certain actions), then Waiting, then
    Cold responders (paste-ready manual DMs), then LinkedIn-warm
    (channel_hint == "linkedin_only" — no email on file, DM likely; collapsed),
    then Nudge (judgment calls, collapsed). Partner rows stay in the Partner section
    regardless of channel; every other linkedin_only row is pulled out of
    Owed/Nudge into the LinkedIn-warm section so it is surfaced, never dropped or
    double-listed. Each section collapses to a preview + count unless ``full``.
    Empty state says so explicitly — but only as an all-clear when the run was
    NOT degraded; a degraded empty run is a warning, not a ✅.
    """
    degraded = degraded or []
    banner = _degraded_banner(degraded, dropped_no_touch)

    parked_str = _parked_str(parked)
    if not candidates:
        # Out-of-view pool (parked / at the nudge ceiling / TTL-expired) —
        # rendered in EVERY empty-state variant, including the degraded one:
        # an exhausted account is operator work regardless of whether this
        # run's detection also degraded, and dropping the count here was the
        # exact silent-drop failure this feature exists to prevent.
        hidden_bits = [b for b in (
            parked_str,
            f"{waiting_exhausted} waiting at the nudge ceiling (handed to you)"
            if waiting_exhausted else "",
            f"{waiting_expired} waiting stamps expired (>{_WAITING_TTL_DAYS}d)"
            if waiting_expired else "",
            f"{email_declined_suppressed} hidden by email hard declines (§3.1)"
            if email_declined_suppressed else "",
        ) if b]
        if banner:
            empty_lines = ["**Follow-up Radar** — 0 surfaced, but detection was degraded:", *banner]
            if hidden_bits:
                empty_lines.append("")
                empty_lines.append(
                    "_" + " · ".join(hidden_bits) + " — `sales followup --full` to audit._"
                )
            return "\n".join(empty_lines)
        if hidden_bits:
            # Nothing to ACT on, but the pool isn't empty — say so, don't imply
            # a clean slate when accounts are parked/exhausted out of view.
            return (
                "**Follow-up Radar** — nada urgente hoy. _"
                + " · ".join(hidden_bits)
                + " — `sales followup --full` to audit._"
            )
        return "**Follow-up Radar** — Radar limpio · nada urgente hoy. ✅"

    # Shared disjoint partition (see partition_lanes) — the summary dict in
    # run_followup_radar counts from the same helper, so the header split line
    # here and the CLI footer can never disagree.
    lanes = partition_lanes(candidates)
    partner = lanes["partner"]
    owed = lanes["owed"]
    waiting = lanes["waiting"]
    cold = lanes["cold_responder"]
    linkedin_warm = lanes["linkedin_warm"]
    nudge = lanes["nudge"]

    def _row(c: FollowupCandidate) -> str:
        label = POLICY[c.reason].label
        unit = "business days" if POLICY[c.reason].business_days else "days"
        # v2: deals usually carry REAL recency (verified stamp or person-
        # interaction join), so the per-row approx flag is informative again —
        # it marks the residual created_at-tier rows on both object types.
        if c.last_touch_synthetic:
            approx = (
                " (approx — deal age)"
                if c.object == "deals"
                else " (approx — no contact stamp)"
            )
        else:
            approx = ""
        note = "".join(f" · ⚠ {n}" for n in c.notes)
        # email_reply_seen (PR-214) renders as its OWN marker, NOT as a note:
        # notes are the row's warnings tail (a WAITING "nudge N/2 sent", a
        # state-gate "drafted Nd ago", a "they wrote last"), so folding the
        # reply-seen signal in there would bury the reconciliation line exactly
        # when it matters most — a drafted-stale/waiting account that actually
        # REPLIED must not read "you went quiet, N days silent" with no
        # counter-signal.
        reply_seen = " · ↩ reply seen" if c.email_reply_seen else ""
        # A human owns this account's email thread (they replied to the
        # drip) — the row is informational; the skill layer never drafts it.
        resp = " · ✉ replied by email — no auto-draft" if c.email_responded else ""
        # Partner-referred deals name the introducing partner so the operator
        # knows whose credibility is on the line. Best-effort: only rendered
        # when referred_by is actually a non-empty string.
        via = f" · via {c.referred_by}" if _is_email_evidence(c.referred_by) else ""
        # A candidate with NO datable touch (source "none" — only reachable
        # via a due callback) must not fabricate "0 days silent": that reads
        # as "touched today". Say the truth instead.
        silence = (
            "silence unknown (no datable touch)"
            if c.last_touch is None
            else f"{c.silent_days} {unit} silent{approx}"
        )
        return (
            f"- **{_who(c)}** — {label} · {silence} "
            f"· urgency {c.urgency}{reply_seen}{via}{resp}{note}"
        )

    def _cold_rows(c: FollowupCandidate) -> list[str]:
        """Cold-responder row: the standard line, the exchange (their reply,
        our DM — dates + one-line excerpts), then the paste-ready DM as a
        blockquote so the operator can copy it whole."""
        out = [_row(c)]
        for who, at, body in (
            ("them", c.their_last_reply_at, c.their_last_reply),
            ("you", c.our_last_dm_at, c.our_last_dm),
        ):
            if at or body:
                when = f" ({at.isoformat()})" if at else ""
                text = f': "{_snippet(body)}"' if body else ""
                out.append(f"  - {who}{when}{text}")
        lang = (c.dm_language or "es").upper()
        flag = (
            ""
            if c.dm_language
            else " · ⚠ language not on record — ES by default, check before pasting"
        )
        out.append(f"  - paste-ready DM ({lang}){flag}:")
        out.extend(
            f"    > {line}" if line else "    >"
            for line in render_manual_dm(c).split("\n")
        )
        return out

    lines: list[str] = ["**Follow-up Radar**"]
    lines.extend(banner)
    # Transparent lane split so a silently-empty email lane can't hide that most
    # warm accounts are LinkedIn-only (F3). Only non-zero lanes are named.
    count_parts = []
    if partner:
        count_parts.append(f"{len(partner)} partner intro")
    if owed:
        # No "(email)" qualifier — the owed bucket includes unknown-channel
        # deals, and the wording must match the cli.py footer exactly.
        count_parts.append(f"{len(owed)} owed")
    if waiting:
        count_parts.append(f"{len(waiting)} waiting")
    if cold:
        # Wording must match the cli.py footer exactly.
        count_parts.append(f"{len(cold)} cold responder")
    if linkedin_warm:
        count_parts.append(f"{len(linkedin_warm)} LinkedIn-warm")
    if nudge:
        count_parts.append(f"{len(nudge)} nudge")
    if count_parts:
        lines.append("")
        lines.append("_" + " · ".join(count_parts) + "_")
    top = candidates[:top_n]  # urgency-ordered (value-weighted) — the hottest few
    if top:
        lines.append("")
        lines.append(f"_Top {len(top)} to act on:_")
        lines.extend(_row(c) for c in top)

    def _section(
        title: str, rows: list[FollowupCandidate], preview: int, tail: str,
        row_fn=None,
    ) -> None:
        if not rows:
            return
        lines.append("")
        lines.append(f"### {title} ({len(rows)})")
        shown = rows if full else rows[:preview]
        for c in shown:
            lines.extend(row_fn(c) if row_fn else [_row(c)])
        if not full and len(rows) > preview:
            lines.append(
                f"- …+{len(rows) - preview} {tail} — run `sales followup --full` to see all."
            )

    # Partner intros above Owed — a dropped intro burns a partner's credibility.
    _section(
        "🤝 Partner intros — a partner's credibility is on the line",
        partner, _PARTNER_PREVIEW, "more partner intros",
    )
    _section("Owed — do these", owed, _OWED_PREVIEW, "more owed")
    # WAITING between Owed and LinkedIn-warm: real email conversations where
    # the ball is nominally theirs — review-gated nudge material, below owed
    # replies but above speculative lanes.
    _section(
        "Waiting on them — you sent, no reply",
        waiting, _WAITING_PREVIEW, "more waiting",
    )
    # Cold responders below Waiting (same "ball is theirs" semantics, but a
    # LinkedIn DM the operator pastes by hand) and above the speculative DM lane.
    _section(
        "Cold responders — replied, then went quiet (paste by hand)",
        cold, _COLD_RESPONDER_PREVIEW, "more cold responders", row_fn=_cold_rows,
    )
    # "no email on file" states the evidence, not a verdict — rows below the
    # skill layer's verify limit are never Gmail-checked, and a person mid
    # email-thread can still carry linkedin_only (entry attrs can't see email).
    _section(
        "LinkedIn warm — no email on file (DM likely)",
        linkedin_warm, _LINKEDIN_WARM_PREVIEW, "more LinkedIn-warm",
    )
    _section("Consider nudging", nudge, _NUDGE_PREVIEW, "more cooling")

    footer = (
        "_Coarse last-touch from Attio only — the skill layer verifies via "
        "the email thread / call transcript before drafting (fail-closed)._"
    )
    if any(c.object == "deals" and c.last_touch_synthetic for c in candidates):
        footer += (
            " _Deals marked approx have no verified touch or synced "
            "interaction data — their silence is deal age, not last contact._"
        )
    if drafted_skipped:
        footer += (
            f" _{drafted_skipped} already drafted and awaiting your send "
            "(hidden until acted on or the draft goes stale)._"
        )
    # Waiting-pool exits render as COUNTS, never rows (anti-wallpaper), but
    # are never silently dropped: exhausted = the nudge ceiling did its job —
    # these are handed to you; expired = ghosts past the 60d TTL.
    if waiting_exhausted:
        footer += (
            f" _{waiting_exhausted} waiting account(s) hit the "
            f"{WAITING_MAX_NUDGES}-nudge ceiling with no reply — handed to "
            "you (no more auto-drafts; `sales followup-await --clear` resets)._"
        )
    if waiting_expired:
        footer += (
            f" _{waiting_expired} waiting stamp(s) expired (>"
            f"{_WAITING_TTL_DAYS}d, reply isn't coming) — cleared from the "
            "lane; `sales followup --full` + `followup-await --clear` to tidy._"
        )
    if waiting_capped:
        footer += (
            f" _{waiting_capped} more waiting row(s) displaced by the "
            f"{_WAITING_DRAFT_SLOT_CAP}-slot draft cap — `sales followup "
            "--full` shows all._"
        )
    if cold:
        footer += (
            " _Cold-responder DMs are paste-by-hand on LinkedIn — never "
            "auto-sent, never an email draft; the next Phase 0.5 inbox scrape "
            "records your DM and the row clears itself._"
        )
    if cold_capped:
        footer += (
            f" _{cold_capped} more cold-responder row(s) displaced by the "
            f"{_COLD_RESPONDER_SLOT_CAP}-slot cap — `sales followup --full` "
            "shows all._"
        )
    if email_declined_suppressed:
        footer += (
            f" _{email_declined_suppressed} record(s) hidden by an email "
            "hard decline (said no / unsubscribed — §3.1, never re-contact; "
            "for a deal, one declined contact hides the whole deal)._"
        )
    if parked_str:
        footer += f" _{parked_str}._"
    lines.append("")
    lines.append(footer)
    return "\n".join(lines)


def _trim_with_lane_caps(
    candidates: list[FollowupCandidate], limit: int | None,
) -> tuple[list[FollowupCandidate], dict[WarmLane, int]]:
    """The ``--limit`` trim, with per-lane slot caps (``_LANE_SLOT_CAPS``:
    WAITING at ``_WAITING_DRAFT_SLOT_CAP``, COLD_RESPONDER at
    ``_COLD_RESPONDER_SLOT_CAP``).

    Allocate limited review slots in the same lane order as the digest:
    Partner, Owed, Waiting, Cold responder, LinkedIn warm, Nudge. Urgency
    orders candidates within a lane. Caps are maximums, not reservations;
    displaced slots backfill from later lanes. No limit keeps the full digest.

    Returns ``(trimmed, capped_by_lane)`` — the dict counts, per capped lane,
    the rows the CAP specifically displaced (they'd have made the limit
    otherwise). Surfaced in the digest footer so cap displacement is never a
    silent drop: unlike ordinary below-the-limit truncation, these rows lose
    their slot to a rule, and the operator must be able to see that.
    """
    capped = dict.fromkeys(_LANE_SLOT_CAPS, 0)
    if not limit:
        return candidates, capped
    out: list[FollowupCandidate] = []
    taken = dict.fromkeys(_LANE_SLOT_CAPS, 0)
    lanes = partition_lanes(candidates)
    ordered = [c for lane in (
        "partner", "owed", "waiting", "cold_responder", "linkedin_warm", "nudge"
    ) for c in sorted(lanes[lane], key=lambda item: item.urgency, reverse=True)]
    for c in ordered:
        if len(out) >= limit:
            break
        cap = _LANE_SLOT_CAPS.get(c.lane)
        if cap is not None:
            if taken[c.lane] >= cap:
                capped[c.lane] += 1
                continue
            taken[c.lane] += 1
        out.append(c)
    return out, capped


def _resolve_gmail_sweep_enabled(override: bool | None) -> bool:
    """Whether the Gmail sweep runs: explicit override, else the outreach knob.

    Defaults to OFF (False) if the config can't be read — a missing/unreadable
    outreach config must never silently turn the email side ON.
    """
    if override is not None:
        return override
    try:
        from clients.outreach_config import load_outreach_config

        return load_outreach_config().radar_gmail_sweep_enabled
    except Exception:  # noqa: BLE001 — config unreadable → stay OFF (fail-safe)
        return False


def _resolve_gmail_lookback_days(override: int | None) -> int:
    """Gmail sweep lookback window: explicit override, else the outreach knob
    (default 90)."""
    if override is not None:
        return override
    try:
        from clients.outreach_config import load_outreach_config

        return load_outreach_config().radar_gmail_lookback_days
    except Exception:  # noqa: BLE001 — config unreadable → default window
        return 90


def sweep_gmail_conversations(
    candidates: list[FollowupCandidate],
    *,
    today: date,
    lookback_days: int,
    client_factory: object = None,
) -> list[str]:
    """Reconcile warm candidates against the Gmail conversation ledger (PR-214).

    OFF by default (gated by ``outreach.radar.gmail_sweep_enabled``); the caller
    only reaches here when the operator opted in. For each candidate carrying an
    email address, search the inbox for an inbound reply on/after
    ``last_touch`` (bounded by ``lookback_days``). A hit sets
    ``email_reply_seen=True`` — the ball already moved, so the CRM-derived
    "you went quiet" is stale. This NEVER drops a candidate: it annotates so
    the digest/skill layer can reconcile against the real inbox.

    Degrades to a clean SKIP: if the Gmail token is absent
    (``GmailCredentialsMissing``) the sweep returns a single degraded reason
    and touches no candidate — the radar still runs on CRM signals alone.
    Per-candidate Gmail errors are collected as degraded reasons, never raised,
    so one bad lookup can't strand the whole run.

    Returns the list of degradation reasons (empty when the sweep ran clean).
    """
    from clients.gmail import GmailClient, GmailCredentialsMissing

    factory = client_factory or GmailClient.from_credentials
    try:
        client = factory()  # type: ignore[operator]
    except GmailCredentialsMissing as exc:
        return [
            "Gmail conversation-ledger sweep skipped — no Gmail credentials "
            f"({exc}); radar ran on CRM signals only"
        ]
    except Exception as exc:  # noqa: BLE001 — ANY sweep-construction failure must degrade, never blackout the CRM digest
        # from_credentials can also raise ValueError (a malformed token that
        # slips the shallow validity check), google.auth RefreshError, or a
        # transport/build error. None of those are the caller's problem: the
        # sweep is opt-in and advisory, so a construction failure degrades to a
        # surfaced reason and the already-computed CRM digest still renders —
        # the same "clean skip, radar runs on CRM signals alone" contract as
        # the credentials-missing case above.
        return [
            "Gmail conversation-ledger sweep skipped — client init failed "
            f"({type(exc).__name__}: {exc}); radar ran on CRM signals only"
        ]

    lookback_floor = today - timedelta(days=max(1, lookback_days))
    degraded: list[str] = []
    for c in candidates:
        email = (c.email_address or "").strip()
        if not email:
            continue
        # Search from the later of last_touch and the lookback floor — a reply
        # OLDER than the CRM last_touch isn't news (the staleness clock already
        # started after it).
        after = c.last_touch or lookback_floor
        if after < lookback_floor:
            after = lookback_floor
        try:
            inbound = client.search_inbound(email, after)
        except Exception as exc:  # noqa: BLE001 — one bad lookup must not strand the sweep
            degraded.append(
                f"Gmail lookup failed for {c.record_id} "
                f"({type(exc).__name__}: {exc})"
            )
            continue
        if inbound:
            c.email_reply_seen = True
            c.notes.append("email reply seen after last touch (Gmail ledger)")
    return degraded


def run_followup_radar(
    crm: CRMProvider,
    *,
    today: date | None = None,
    list_id: str | None = None,
    limit: int | None = None,
    full: bool = False,
    gmail_sweep: bool | None = None,
    gmail_lookback_days: int | None = None,
    gmail_client_factory: object = None,
) -> dict:
    """Detect → rank → enrich top-N → render. Read-only.

    Returns a summary dict: ``{"total", "surfaced", "partner", "owed",
    "waiting", "cold_responder", "nudge", "linkedin_warm", "degraded",
    "dropped_no_touch", "candidates", "digest", ...}``.
    The six lane counts come from ``partition_lanes`` (the same partition the
    digest header renders from) so they are DISJOINT and
    ``partner + owed + waiting + cold_responder + linkedin_warm + nudge ==
    surfaced``.
    Never writes to the CRM — the write-back (followup_draft_at etc.) is the
    skill layer's stamp step. ``full`` renders every lane in full instead of a
    preview.

    Radar is a legitimate Attio-transport-semantics module (schema probes,
    ``parse_deal``, the person-interaction join) with no contract equivalent
    yet, so the command trunk takes a ``CRMProvider`` and derives the raw
    ``AttioClient`` via the ``_attio_inner_client`` escape hatch — the same §7
    "convert at the call boundary" move the daily slice already uses (see
    clients/crm/CONTRACT.md tracked debt). A non-Attio provider raises a clear
    error there rather than silently mis-routing.
    """
    from workflows.weekly_prospect import _attio_inner_client

    attio = _attio_inner_client(crm)
    result = detect_candidates(attio, today=today, list_id=list_id)
    candidates = result.candidates
    total = len(candidates)
    surfaced, capped = _trim_with_lane_caps(candidates, limit)
    waiting_capped = capped.get(WarmLane.WAITING, 0)
    cold_capped = capped.get(WarmLane.COLD_RESPONDER, 0)
    # Name resolution is COSMETIC — detection already fully succeeded. A
    # transient Attio error mid-enrichment must not crash the run and lose the
    # whole digest: degrade instead, and _who falls back to record_id[:8] for
    # the unresolved rows so the digest still renders.
    try:
        enrich_names(attio, surfaced)
    except Exception as exc:  # noqa: BLE001 — name lookup is best-effort; never sink a completed detection
        result.degraded.append(
            "Name enrichment failed — display names fall back to record ids "
            f"({type(exc).__name__}: {exc})"
        )

    # Optional Gmail conversation-ledger sweep (PR-214). OFF by default; when
    # the operator enables it, reconcile the surfaced (top-N) candidates against
    # the real inbox. Resolves the enable flag + lookback from outreach config
    # unless the caller overrides them (tests pass a fake factory). Degrades to
    # a clean skip — a missing Gmail token adds a degraded reason and touches
    # no candidate, so the radar still runs without the email side.
    sweep_today = today if today is not None else operator_today()
    if _resolve_gmail_sweep_enabled(gmail_sweep):
        sweep_degraded = sweep_gmail_conversations(
            surfaced,
            today=sweep_today,
            lookback_days=_resolve_gmail_lookback_days(gmail_lookback_days),
            client_factory=gmail_client_factory,
        )
        result.degraded.extend(sweep_degraded)

    digest = render_digest(
        surfaced,
        degraded=result.degraded,
        dropped_no_touch=result.dropped_no_touch,
        drafted_skipped=result.drafted_skipped,
        parked=result.parked,
        waiting_expired=result.waiting_expired,
        waiting_exhausted=result.waiting_exhausted,
        waiting_capped=waiting_capped,
        email_declined_suppressed=result.email_declined_suppressed,
        full=full,
        cold_capped=cold_capped,
    )
    # Disjoint counts from the SAME partition the digest header uses —
    # partner + owed + waiting + cold_responder + linkedin_warm + nudge ==
    # surfaced, always.
    lanes = partition_lanes(surfaced)
    return {
        "total": total,
        "surfaced": len(surfaced),
        "partner": len(lanes["partner"]),
        "owed": len(lanes["owed"]),
        "waiting": len(lanes["waiting"]),
        "cold_responder": len(lanes["cold_responder"]),
        "nudge": len(lanes["nudge"]),
        "linkedin_warm": len(lanes["linkedin_warm"]),
        "degraded": result.degraded,
        "dropped_no_touch": result.dropped_no_touch,
        "drafted_skipped": result.drafted_skipped,
        "parked": result.parked,
        "waiting_expired": result.waiting_expired,
        "waiting_exhausted": result.waiting_exhausted,
        "waiting_capped": waiting_capped,
        "cold_capped": cold_capped,
        "email_declined_suppressed": result.email_declined_suppressed,
        "candidates": to_json(surfaced),
        "digest": digest,
    }
