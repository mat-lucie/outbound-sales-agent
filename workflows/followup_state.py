"""Follow-up Radar state writer (Phase 2) — the §3.15-authorized writer for the
five followup_* attributes on linkedin_outreach + deals.

Why this module exists: the stateless radar re-surfaces the whole warm backlog
every run. This module owns the small amount of memory that turns it into a
flush-once-then-steady-state loop:

  - followup_draft_at / followup_draft_id  — "a draft already exists" (don't
    re-draft daily; link the digest to the draft).
  - followup_snooze_until                  — parked until a date.
  - followup_muted                         — permanently excluded.
  - followup_callback_date                 — deferral tickler.
  - deals.last_verified_touch (v2)         — the C.2-verified true last-touch;
    the engine's highest-precedence deal recency source.
  - awaiting_reply_* (v3, both objects)    — WAITING-lane state: the operator's
    most recent unanswered send (since/thread), the canonical waiting-note id,
    and the nudge count (hard max 2, enforced here).

All writes go through AttioWriter so the registry (§3.15) authorizes them and
the DLQ/retry contract applies. These attributes are non-stage, so the
monotonicity / terminal-class gates are no-ops for them.

The skill layer (Phase C) calls the CLI commands that wrap these functions
after it creates a follow-up draft, or when the operator snoozes/mutes.

Provenance: ported from upstream Follow-up Radar state writer — PR-211 (the
five followup_* attrs), PR-214 (last_verified_touch + referred_by), PR-247
(WAITING-lane awaiting_reply_*).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, cast

from clients.attio_writer import AttioWriter, WriteIntent

WRITER_MODULE = "workflows.followup_state"

# The two warm object types the radar detects. linkedin_outreach is a LIST
# (writes target the entry_id); deals is a plain object.
_LIST_OBJECT = "linkedin_outreach"


def _apply(
    writer: AttioWriter,
    *,
    object: str,
    record_id: str,
    updates: dict,
    list_id: str | None,
) -> dict:
    """Build + apply a WriteIntent for a followup_* update.

    ``record_id`` is the entry_id for linkedin_outreach (a list) or the deal
    record_id for deals. No prior_values needed — these are non-stage attrs, so
    the monotonicity/terminal gates short-circuit.
    """
    is_list = object == _LIST_OBJECT
    if is_list and not list_id:
        raise ValueError("linkedin_outreach writes require list_id (the entry lives on a list)")
    intent = WriteIntent(
        object=object,
        record_id=record_id,
        updates=updates,
        prior_values={},
        writer_module=WRITER_MODULE,
        is_list_entry=is_list,
        list_id=list_id if is_list else None,
    )
    return cast("dict[str, Any]", writer.apply(intent))


def stamp_draft(
    writer: AttioWriter,
    *,
    object: str,
    record_id: str,
    draft_id: str,
    list_id: str | None = None,
    when: datetime | None = None,
) -> dict:
    """Record that a follow-up draft was generated (draft_at + draft_id)."""
    ts = (when or datetime.now(UTC)).isoformat()
    return _apply(
        writer,
        object=object,
        record_id=record_id,
        updates={"followup_draft_at": ts, "followup_draft_id": draft_id},
        list_id=list_id,
    )


def set_snooze(
    writer: AttioWriter,
    *,
    object: str,
    record_id: str,
    until: date,
    list_id: str | None = None,
) -> dict:
    """Park this account until ``until`` (inclusive)."""
    return _apply(
        writer,
        object=object,
        record_id=record_id,
        updates={"followup_snooze_until": until.isoformat()},
        list_id=list_id,
    )


def set_muted(
    writer: AttioWriter,
    *,
    object: str,
    record_id: str,
    muted: bool = True,
    list_id: str | None = None,
) -> dict:
    """Permanently exclude (or re-include) this account from the radar."""
    return _apply(
        writer,
        object=object,
        record_id=record_id,
        updates={"followup_muted": muted},
        list_id=list_id,
    )


def mute_batch(
    writer: AttioWriter,
    *,
    object: str,
    record_ids: list[str],
    list_id: str | None = None,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Mute many accounts in one pass — the first-run backlog flush.

    Returns ``(succeeded_ids, failures)`` where ``failures`` is a list of
    ``(record_id, error_str)``. The loop is intentionally sequential (Attio
    writes are sequential by project rule); a per-id failure is recorded and the
    loop continues so one bad record can't strand the rest.

    ``set_muted`` is idempotent, so re-running with the same ids is safe.

    The list_id requirement for linkedin_outreach is validated ONCE up front so a
    bad invocation raises before any write happens.
    """
    if object == _LIST_OBJECT and not list_id:
        raise ValueError("linkedin_outreach writes require list_id (the entry lives on a list)")

    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []
    for record_id in record_ids:
        try:
            set_muted(writer, object=object, record_id=record_id, muted=True, list_id=list_id)
        except Exception as exc:  # noqa: BLE001 — one bad id must not strand the rest
            failed.append((record_id, f"{type(exc).__name__}: {exc}"))
        else:
            succeeded.append(record_id)
    return succeeded, failed


def set_callback(
    writer: AttioWriter,
    *,
    object: str,
    record_id: str,
    callback: date,
    list_id: str | None = None,
) -> dict:
    """Set a deferral tickler — suppress until ``callback``, then hard-surface."""
    return _apply(
        writer,
        object=object,
        record_id=record_id,
        updates={"followup_callback_date": callback.isoformat()},
        list_id=list_id,
    )


def clear_callback(
    writer: AttioWriter,
    *,
    object: str,
    record_id: str,
    list_id: str | None = None,
) -> dict:
    """Null a discharged callback — the tickler fired and Mat acted on it.

    Without this, a fired callback could only be cleared by a manual Attio
    edit outside the §3.15 sole-writer contract, and the account could not
    enter the WAITING cycle (callback + waiting must never coexist — the
    radar hard-surfaces a due callback over WAITING every run).
    """
    return _apply(
        writer,
        object=object,
        record_id=record_id,
        updates={"followup_callback_date": None},
        list_id=list_id,
    )


# Loud-typo bounds for a verified touch. One day of FUTURE skew is legitimate
# (the skill layer extracts dates from UTC timestamps, which read "tomorrow"
# during the operator's evening); anything further future would silently hide
# the deal (silence reads 0). The PAST horizon catches year typos: a stamp
# 400+ days back would pin the deal as maximally stale at the top of every
# digest, and — being tier 1 — real interaction data could never contradict it.
_TOUCH_FUTURE_SKEW_DAYS = 1
_TOUCH_MAX_PAST_DAYS = 400


def stamp_verified_touch(writer: AttioWriter, *, deal_id: str, touch: date) -> dict:
    """Stamp the skill-layer-verified true last-touch date onto a deal.

    Deals-only (like ``stamp_referred_by`` — no ``object`` param, structurally
    impossible to target the list). The Phase C skill layer calls this (via
    ``followup-touch``) after each successful C.2 email/call verification, so
    the engine's deal recency (tier 1 of
    ``followup_radar._deal_recency``) gets truer every run.

    Both date directions are guarded loudly (ValueError, never a silent bad
    stamp): a future touch (beyond 1 day of UTC skew) would hide the deal
    from the radar; a touch older than ~400 days is almost certainly a year
    typo that would pin the deal to the top of every digest with no real
    data able to contradict it (tier-1 precedence). A genuinely ancient
    last touch loses nothing by not being stamped — the join/created_at
    fallback is equally stale. A bad stamp is corrected by re-stamping, or
    cleared entirely via ``clear_verified_touch``.
    """
    _guard_touch_date(touch, "verified touch")
    return _apply(
        writer,
        object="deals",
        record_id=deal_id,
        updates={"last_verified_touch": touch.isoformat()},
        list_id=None,
    )


def clear_verified_touch(writer: AttioWriter, *, deal_id: str) -> dict:
    """Null a deal's verified touch — the authorized correction path.

    Without this, a bad stamp could only be fixed by a manual Attio edit
    outside the §3.15 sole-writer contract. Clearing drops the deal back to
    the person-interaction join / created_at tiers.
    """
    return _apply(
        writer,
        object="deals",
        record_id=deal_id,
        updates={"last_verified_touch": None},
        list_id=None,
    )


def _guard_touch_date(d: date, label: str) -> None:
    """Shared loud-typo guard for operator-supplied touch dates.

    Same bounds as ``stamp_verified_touch`` (see the constants above): future
    beyond 1 day of UTC skew, or further back than ~400 days, raises
    ValueError rather than silently writing a stamp that would hide or pin
    the account.
    """
    from datetime import timedelta

    from models.business_calendar import operator_today

    today = operator_today()
    if d > today + timedelta(days=_TOUCH_FUTURE_SKEW_DAYS):
        raise ValueError(
            f"{label} {d.isoformat()} is in the future — it records a past "
            f"event; a future date would silently hide the account from the "
            f"radar. Refusing."
        )
    if d < today - timedelta(days=_TOUCH_MAX_PAST_DAYS):
        raise ValueError(
            f"{label} {d.isoformat()} is more than {_TOUCH_MAX_PAST_DAYS} "
            f"days ago — likely a year typo. Refusing."
        )


def set_awaiting_reply(
    writer: AttioWriter,
    *,
    object: str,
    record_id: str,
    since: date,
    thread_id: str,
    list_id: str | None = None,
) -> dict:
    """Stamp an account as awaiting a reply to the operator's ``since`` send.

    ALWAYS overwrites — the stamp tracks the MOST RECENT unanswered send, so a
    newer manual re-ping re-stamps rather than being skipped (a frozen date
    renders a wrong "waiting Nd" row, the operator-trust killer). The skill
    layer calls this (via ``followup-await --since``) only after a C.2
    fail-closed verification of the Gmail thread — never from the raw C.0
    sweep snapshot.

    Dual-clock coherence: for a DEAL, the operator's send IS a verified touch,
    so ``last_verified_touch`` is co-stamped in the SAME WriteIntent —
    otherwise ``resolve_deal_recency`` (shared with the weekly report) would
    call the deal stale-since-creation while the radar shows it waiting 3
    days. Entries have no ``last_verified_touch`` (deals-only attr) and their
    contact stamps belong to the DM cadence — they stamp the awaiting attrs
    only.
    """
    _guard_touch_date(since, "awaiting-reply since")
    updates: dict = {
        "awaiting_reply_since": since.isoformat(),
        "awaiting_reply_thread_id": thread_id,
    }
    if object == "deals":
        updates["last_verified_touch"] = since.isoformat()
    return _apply(
        writer,
        object=object,
        record_id=record_id,
        updates=updates,
        list_id=list_id,
    )


def clear_awaiting_reply(
    writer: AttioWriter,
    *,
    object: str,
    record_id: str,
    list_id: str | None = None,
) -> dict:
    """End a waiting cycle (reply arrived, or the operator resets a ghost).

    Nulls the since/thread/nudge-count state. ``awaiting_reply_note_id`` is
    DELIBERATELY kept: the canonical waiting-context note persists in Attio
    regardless, and a re-opened cycle should prepend to the same note instead
    of spawning a second one. Emitting the ``awaiting_reply_resolved``
    learning event is the CLI layer's job (it happens BEFORE this clear —
    clearing first would destroy the latency data the event carries).
    """
    return _apply(
        writer,
        object=object,
        record_id=record_id,
        updates={
            "awaiting_reply_since": None,
            "awaiting_reply_thread_id": None,
            "awaiting_reply_nudge_count": None,
        },
        list_id=list_id,
    )


def set_awaiting_note_id(
    writer: AttioWriter,
    *,
    object: str,
    record_id: str,
    note_id: str,
    list_id: str | None = None,
) -> dict:
    """Persist the canonical waiting-note's id after the skill creates it.

    The skill flow is attr-stamp FIRST, note second (a note without a stamp is
    memory the radar can't act on; a stamp without a note only costs prose) —
    so the note id arrives after ``set_awaiting_reply`` already ran and needs
    its own write. Without a persisted id, every later run would create a NEW
    note (Attio note updates require an id; there is no upsert-by-content)
    and the record accrues note sludge.
    """
    if not note_id.strip():
        raise ValueError("note_id must be a non-empty Attio note id")
    return _apply(
        writer,
        object=object,
        record_id=record_id,
        updates={"awaiting_reply_note_id": note_id.strip()},
        list_id=list_id,
    )


def increment_awaiting_nudge(
    writer: AttioWriter,
    *,
    object: str,
    record_id: str,
    current_count: int,
    note_id: str | None = None,
    list_id: str | None = None,
) -> dict:
    """Count a nudge draft against the waiting cycle's hard ceiling.

    Raises at ``WAITING_MAX_NUDGES`` so the ceiling cannot be bypassed by any
    caller — the 5-day cooldown only paces nagging; this is what stops it.
    ``note_id`` (optional) persists the canonical note's id on the same write
    when the skill layer just created it.
    """
    from models.followup import WAITING_MAX_NUDGES

    if current_count >= WAITING_MAX_NUDGES:
        raise ValueError(
            f"awaiting-reply nudge ceiling reached ({current_count}/"
            f"{WAITING_MAX_NUDGES}) — no more auto-nudges for this account; "
            f"it is handed to the operator (clear via followup-await --clear "
            f"to start a new cycle)."
        )
    updates: dict = {"awaiting_reply_nudge_count": current_count + 1}
    if note_id:
        updates["awaiting_reply_note_id"] = note_id
    return _apply(
        writer,
        object=object,
        record_id=record_id,
        updates=updates,
        list_id=list_id,
    )


def stamp_referred_by(writer: AttioWriter, *, deal_id: str, partner_email: str) -> dict:
    """Stamp the referring partner's email on a deal (deal-side attribution).

    Deals-only: partner intros arrive as email threads, so the attribution
    lives on the deals object (never on the linkedin_outreach list). The
    /sales-daily Phase C skill layer calls this (via ``followup-refer``) when it
    finds a known partner among a deal candidate's Gmail thread participants.

    ``partner_email`` is normalized to lowercase/stripped and validated to look
    like an email (contains "@", no internal whitespace). Junk is rejected loudly
    with ValueError rather than silently stamping a bad value onto a deal — a
    wrong ``referred_by`` misattributes a partner's introduction.
    """
    email = partner_email.strip().lower()
    if "@" not in email or any(ch.isspace() for ch in email):
        raise ValueError(
            f"partner_email does not look like an email: {partner_email!r} "
            f"(need exactly one '@' and no whitespace)"
        )
    # Reject the degenerate shapes a lone "@" check would let through
    # ("@x", "x@", "a@b@c") — a referred_by that can't route back to a real
    # partner inbox is worse than no attribution.
    local, _, domain = email.partition("@")
    if not local or not domain or "@" in domain or "." not in domain:
        raise ValueError(
            f"partner_email does not look like an email: {partner_email!r} "
            f"(need a non-empty local part and a dotted domain)"
        )
    intent = WriteIntent(
        object="deals",
        record_id=deal_id,
        updates={"referred_by": email},
        prior_values={},
        writer_module=WRITER_MODULE,
        is_list_entry=False,
        list_id=None,
    )
    return cast("dict[str, Any]", writer.apply(intent))
