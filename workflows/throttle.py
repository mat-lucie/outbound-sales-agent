"""Per-company outreach throttle (PR-13, B-PD-002).

The §3.8 hard rule: at most one prospect-per-company gets outbound
contact per 14-day window. The throttle is person-scope-aware in the
sense that it applies across ALL persons at the same company — if
person A at Acme received DM2 yesterday, person B at Acme is blocked
for the next 14 days unless an operator revises the
`throttle_ttl_policy` configuration_decision.

# Why a separate module

`company_throttle_permits` is consumed by two distinct caller classes:

  1. `workflows.daily_check.run_connection_requests` and
     `run_dm_sequencing` — the existing outbound paths. They consult
     the throttle BEFORE selecting a prospect from the daily queue;
     a throttled prospect skips with a `company_throttled` queue row.
  2. `workflows.daily_check.run_nurture_re_engagement` (PR-39, not
     yet shipped) — the NURTURE → DM1 re-engagement path. Per
     §3.18(c), `company_throttle_permits` is condition (c) of the
     four-gate allowlist that lets a NURTURE row re-enter
     send-eligibility.

Keeping the helper outside `daily_check.py` prevents a circular
import once PR-39's `run_nurture_re_engagement` lives in a sibling
module that needs to consult it.

# Side-effect contract

`company_throttle_permits` is PURE READ. It never writes Attio. The
write of `last_outreach_at` (and the three sibling attrs) belongs in
`run_dm_sequencing`'s post-confirmed-send tally per §3.15 — keeping
read and write separate makes the registry's sole-writer enforcement
unambiguous.

# Configuration-decision contract

The 14-day window is a default-on-expiry value of the
`throttle_ttl_policy` configuration_decision row (§3.8). The row must
exist in the operator queue before the throttle is enforced, so
`ensure_throttle_policy_decision_opened` is a separately-callable
idempotent helper that callers invoke once per daily run. Repeated
calls return immediately via escalate()'s `(type, decision_key,
idempotency_key)` dedup.

**v1 wiring caveat:** the operator-selectable 14d / 60d options are
*advisory only* in PR-13 — the runtime `company_throttle_permits`
always uses `DEFAULT_THROTTLE_WINDOW_DAYS = 14` regardless of what
the operator picks. A follow-up PR (target: PR-39 NURTURE
re-engagement) will read the resolved value back via a small
`resolve_throttle_window_days(attio)` reader and thread it through
the helper's `window_days` parameter. The decision row in v1 is
operator-facing config-capture; effective enforcement stays at 14.
The deadline on the decision row is 30 days from open — long enough
for the operator to take in the broader cadence policy.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

from clients.outreach_config import load_outreach_config
from workflows.escalation import escalate

if TYPE_CHECKING:
    from clients.attio import AttioClient
    from workflows.audit import AuditLogger

# Effective per-company throttle window, sourced from config/outreach.yaml
# (throttle.company_window_days). The helper accepts a per-call override for
# tests and special cases. 2026-06-01: lowered 30 → 14 for more throughput while
# still preventing same-week same-company collisions (sales-program.md:27).
# NOTE (known follow-up): the Attio `throttle_ttl_policy` configuration_decision
# row below is still ADVISORY ONLY — its recommended_option/default_on_expiry
# ("30d") are NOT reconciled with this runtime value. Wiring an Attio reader
# (`resolve_throttle_window_days`, precedence Attio > config > default) is a
# separate increment; until then the YAML value is authoritative at runtime.
DEFAULT_THROTTLE_WINDOW_DAYS = load_outreach_config().company_throttle_window_days


def company_throttle_permits(
    company_id: str | None,
    today: date,
    *,
    attio: AttioClient,
    window_days: int = DEFAULT_THROTTLE_WINDOW_DAYS,
    audit_logger: AuditLogger | None = None,
    person_record_id: str | None = None,
    person_dm_step: int | None = None,
) -> bool:
    """True iff outreach to this company is permitted today.

    The throttle is the second line of §3.1 defense (the first is the
    prospect's own `dm_step` + `last_contact_date`). It enforces that
    after ANY outbound contact to ANY person at a given company, no
    further outbound goes to that company for `window_days` days.

    Same-person exemption (2026-06-09): when `person_record_id` is
    provided, the company's `last_outreach_person_id` stamp matches it,
    AND the person's `person_dm_step` corroborates the stamped step
    (stamp DM_n requires dm_step >= n), the block is waived — the prior
    contact went to THIS prospect, whose own cadence guards own the
    pacing. Without the exemption, every confirmed send froze the same
    prospect's next DM step for the full window, structurally killing
    the 5-8 day DM2/DM3 cadence.

    Sibling-invite exemption (2026-09-02): when `person_record_id` is
    provided (an engaged candidate) and the blocking stamp is an
    invite-class step (`CONNECTION_SENT`) — to a colleague, or a
    legacy stamp with no readable person — the block is waived and
    `company_throttle_sibling_invite_exempt` is emitted. An invite to
    colleague B creates no same-message risk against a DM to A, whose
    connection already exists and whose own dm_step/last_contact_date
    guards pace the cadence. Without this, Part A re-arming the window
    every time it invited another person at the company froze accepted
    prospects at dm_step 0 indefinitely (2026-09-01/02: nine Accepted
    rows never received DM1). A sibling DM-class stamp (DM1/DM2/DM3)
    still blocks — that is the §3.8 one-thread-per-company rule
    ("never send the same message to two people at the same company").

    Fail-closed branches (all block): no person stamp with a DM-class
    or unreadable step (legacy rows), malformed stamp shapes,
    unreadable or unknown step (emits
    `malformed_company_last_outreach_person_stamp` with a `reason`),
    and the desync case — self-stamp step ahead of the person's
    dm_step, the signature of a person-side advance that failed after
    a confirmed send (emits `company_throttle_desync_blocked`;
    exempting it would re-send the same DM). A firing self-exemption
    emits `company_throttle_self_exempt` with step context (§0 #9).
    Callers must only thread `person_record_id` for engaged
    (post-PROSPECT) candidates — a PROSPECT with a self-stamp is
    corrupted state and must stay quarantined, and the invite path must
    keep throttling invites against invites.

    Permissive defaults:
      - `company_id is None` (prospect has no linked company) → True.
        The prospect's own per-person guards still apply; this helper
        won't block on incomplete CRM data.
      - `attio.get_company(company_id)` returns None (deleted record,
        stale `_person_to_company` cache, manual mid-run delete) →
        True, with a `stale_company_reference` audit event so operators
        can fix the cache. Distinguishing this case from
        `company_id is None` matters because the latter is intentional
        (no linked company) while the former is data corruption.
      - `last_outreach_at` null → True (company has no recorded prior
        outreach; not throttled).
      - `last_outreach_at` older than `window_days` → True.
      - `last_outreach_at` malformed (anything `date.fromisoformat`
        rejects) → True, with a `malformed_company_last_outreach_at`
        audit event mirroring §0 #9 ("audit event makes the silent
        fall-through observable"; the operator triages the upstream
        writer).

    Throttled:
      - `last_outreach_at` non-null AND (today - last_outreach_at) <
        `window_days` → False.

    Args:
        company_id: Attio Companies record_id, or None when the
            prospect has no linked company.
        today: the daily-check anchor date (typically `operator_today()`).
        attio: AttioClient for the Companies object read. Required —
            the helper does NOT fall back to a local cache because
            §3.1 demands fresh Attio state.
        window_days: per-call override of the 30-day default. Tests
            pass smaller windows; production calls accept the default.
        audit_logger: optional AuditLogger for the two §0 #9 audit
            events (stale_company_reference, malformed_company_last_outreach_at).
            Tests can pass a recording fake; production callers pass
            their `AuditLogger` instance.

    Returns:
        True if outreach is permitted; False if the company is
        currently throttled.

    Read side-effects: one Attio GET per call (the Companies record
    for `company_id`). No Attio write, no escalation. Throttled-row
    escalation belongs in the caller per §3.15 write-owner separation.
    Audit-log events (above) are best-effort and only fire when an
    `audit_logger` is provided.
    """
    if company_id is None:
        return True

    company = attio.get_company(company_id)
    if company is None:
        # Stale company reference — defensively permissive runtime
        # (the operator needs to fix the cache, not block the prospect)
        # but emit the audit event so the corruption is observable.
        if audit_logger is not None:
            audit_logger.event(
                "stale_company_reference",
                company_id=company_id,
            )
        return True

    values = company.get("values", {}) if isinstance(company, dict) else {}
    last_outreach_raw = _extract_datetime_value(
        values.get("last_outreach_at"),
        audit_logger=audit_logger,
        company_id=company_id,
    )
    if last_outreach_raw is None:
        return True

    elapsed = (today - last_outreach_raw).days
    if elapsed >= window_days:
        return True

    # Same-person exemption: the stamp that would block us belongs to
    # this very prospect AND their own dm_step corroborates the stamped
    # step. The corroboration requirement is the F1 guard: a self-stamp
    # whose step is AHEAD of the person's recorded dm_step is the
    # signature of a person-side advance that failed after a confirmed
    # send — exempting that row would re-queue the same DM to the same
    # human. Fail closed on every missing/malformed shape.
    if person_record_id:
        raw_stamp = values.get("last_outreach_person_id")
        items = raw_stamp if isinstance(raw_stamp, list) else [raw_stamp]
        first = items[0] if items else None
        stamped_person = (
            first.get("target_record_id") if isinstance(first, dict) else None
        )
        stamped_step = _extract_step_title(values.get("last_outreach_step"))
        if stamped_person and stamped_person == person_record_id:
            if stamped_step is None:
                # Present-but-unreadable stamp: cannot verify consistency.
                # Block, and make it observable (§0 #9) so a re-frozen
                # cadence is distinguishable from a sibling block.
                if audit_logger is not None:
                    # Same kwarg shape as the sibling branch below so one
                    # consumer can read every emission of this event.
                    audit_logger.event(
                        "malformed_company_last_outreach_person_stamp",
                        company_id=company_id,
                        person_record_id=person_record_id,
                        reason="unreadable_step",
                        stamped_person=stamped_person,
                        stamped_step="",
                    )
                return False
            required_dm_step = _SELF_EXEMPT_REQUIRED_DM_STEP.get(stamped_step)
            if required_dm_step is not None and (person_dm_step or 0) < required_dm_step:
                # Desync: company says DM_n confirmed-sent, person still
                # shows dm_step < n. The old throttle quarantined this
                # corrupted state by accident; keep quarantining it on
                # purpose, loudly.
                if audit_logger is not None:
                    audit_logger.event(
                        "company_throttle_desync_blocked",
                        company_id=company_id,
                        person_record_id=person_record_id,
                        stamped_step=stamped_step,
                        person_dm_step=person_dm_step or 0,
                    )
                return False
            if audit_logger is not None:
                audit_logger.event(
                    "company_throttle_self_exempt",
                    company_id=company_id,
                    person_record_id=person_record_id,
                    stamped_step=stamped_step,
                    person_dm_step=person_dm_step or 0,
                )
            return True

        # Sibling-invite exemption: the stamp that would block us is an
        # INVITE to someone else at the company (Part A keeps inviting
        # colleagues, re-arming the window). Invites throttle invites;
        # they must not freeze a DM to a prospect who already accepted —
        # the connection exists and their own cadence guards pace it. A
        # sibling DM-class stamp keeps blocking (one DM thread per
        # company per window). Unreadable / unknown step → fail closed,
        # loudly (§0 #9), so a frozen cadence is never a silent mystery.
        if stamped_step is None or stamped_step not in _KNOWN_STAMP_STEPS:
            if audit_logger is not None:
                audit_logger.event(
                    "malformed_company_last_outreach_person_stamp",
                    company_id=company_id,
                    person_record_id=person_record_id,
                    reason="unreadable_step" if stamped_step is None else "unknown_step",
                    stamped_person=stamped_person or "",
                    stamped_step=stamped_step or "",
                )
            return False
        if stamped_step in _INVITE_CLASS_STAMP_STEPS:
            if audit_logger is not None:
                audit_logger.event(
                    "company_throttle_sibling_invite_exempt",
                    company_id=company_id,
                    person_record_id=person_record_id,
                    stamped_person=stamped_person or "",
                    stamped_step=stamped_step,
                    person_dm_step=person_dm_step or 0,
                )
            return True

    return False


# Self-exemption consistency floor: a self-stamp of step DM_n is only
# corroborated when the person's own dm_step already records that send.
# Invite-class stamps (Connection Request / connection_note variants)
# have no dm_step floor — the caller only threads engaged (post-accept)
# candidates, for whom a recorded invite is consistent by construction.
_SELF_EXEMPT_REQUIRED_DM_STEP = {
    "DM1": 1,
    "DM2": 2,
    "DM3": 3,
}

# `companies.last_outreach_step` select options (docs/attio_schema_deltas.yaml,
# written only by `_write_company_throttle_tally`). Invite-class stamps never
# block an engaged candidate's DM (sibling-invite exemption); DM-class stamps
# from a sibling do. Anything outside the union is corrupt → fail closed.
_INVITE_CLASS_STAMP_STEPS = frozenset({"CONNECTION_SENT"})
_KNOWN_STAMP_STEPS = _INVITE_CLASS_STAMP_STEPS | frozenset(_SELF_EXEMPT_REQUIRED_DM_STEP)


def _extract_step_title(raw: object) -> str | None:
    """Read the select-attr title from `companies.last_outreach_step`.

    Tolerates every degraded shape Attio (or a manual edit) can produce;
    returns None when unreadable so the caller fails closed."""
    items = raw if isinstance(raw, list) else [raw]
    first = items[0] if items else None
    if not isinstance(first, dict):
        return None
    option = first.get("option")
    if isinstance(option, dict) and option.get("title"):
        return str(option["title"])
    # Flat shapes: {"value": "DM1"} or {"status": "DM1"}.
    for key in ("value", "status", "title"):
        val = first.get(key)
        if isinstance(val, str) and val:
            return val
    return None


def ensure_throttle_policy_decision_opened(attio: AttioClient) -> None:
    """Idempotently open the `throttle_ttl_policy` configuration_decision
    row so the operator can revise the 14-day default before the next
    daily run.

    Safe to call once per daily run — escalate()'s `(type, decision_key,
    idempotency_key)` uniqueness ensures repeated calls return the
    existing row without mutation. Should be invoked at top of
    `run_connection_requests` and `run_dm_sequencing` before any
    throttle check fires. **Failures propagate by design** — a daily
    run without throttle policy enforcement violates §3.8.

    The row carries a 14-day deadline; if the operator doesn't decide
    by then, the resolver (lands in PR-30 / PR-39) flips
    `status="auto_resolved_default"` with the documented 14-day window
    value. v1 has no resolver — see module docstring for the
    "operator-facing config-capture only" caveat.

    Do NOT call from backfill scripts or ad-hoc operator tooling — the
    daily-check loop is the only legitimate caller.
    """
    escalate(
        type="configuration_decision",
        decision_key="throttle_ttl_policy",
        idempotency_key="default-14d-v2",
        payload={
            "question": (
                "Per-company outreach throttle window. After ANY DM "
                "or invite to ANY person at a company, no further "
                "outbound to that company for N days. Default 14."
            ),
            "options": [
                {
                    "key": "14d",
                    "label": "14 days (default)",
                    "description": (
                        "Aggressive — allows ~26 outreach windows "
                        "per company per year. May annoy contacts."
                    ),
                },
                {
                    "key": "30d",
                    "label": "30 days",
                    "description": (
                        "Conservative — allows ~12 outreach windows "
                        "per company per year. Matches B-PD-002."
                    ),
                },
                {
                    "key": "60d",
                    "label": "60 days",
                    "description": (
                        "Patient — allows ~6 outreach windows per "
                        "company per year. Minimizes contact fatigue."
                    ),
                },
            ],
            "recommended_option": "14d",
            "default_on_expiry": "14d",
            "rationale": (
                "Plan §3.8 specifies a 14d person-scope-aware window. "
                "Operator may revise via Attio UI."
            ),
        },
        deadline=date.today() + timedelta(days=14),
        attio=attio,
    )


def _extract_datetime_value(
    raw: object,
    *,
    audit_logger: AuditLogger | None = None,
    company_id: str | None = None,
) -> date | None:
    """Best-effort parse of an Attio datetime/date value into a date.

    Attio returns datetime attributes as ISO 8601 strings (with or
    without timezone). `last_outreach_at` is a datetime in the schema
    but the throttle only needs the date — the comparison window is
    in whole days, so sub-day precision doesn't affect the verdict.

    Returns None when the value is missing/null/empty/malformed.

    On parse failure (`ValueError` from `datetime.fromisoformat`),
    emits a `malformed_company_last_outreach_at` audit event when an
    `audit_logger` is provided. Per §0 #9, the audit event makes the
    otherwise-silent fall-through observable so operators can triage
    the upstream writer.
    """
    if raw is None or raw == "" or raw == []:
        return None
    # Attio v2 wraps select/datetime values in a list of dicts; the
    # actual ISO string lives under either "value" or the top level
    # depending on the attribute type.
    if isinstance(raw, list):
        if not raw:
            return None
        first = raw[0]
        raw = (
            first.get("value") or first.get("date_value")
            if isinstance(first, dict)
            else first
        )
    if not isinstance(raw, str):
        return None
    try:
        # Strip a trailing 'Z' to fit datetime.fromisoformat in 3.10.
        cleaned = raw.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned).date()
    except ValueError:
        if audit_logger is not None:
            audit_logger.event(
                "malformed_company_last_outreach_at",
                company_id=company_id or "",
                raw_value=str(raw)[:50],
            )
        return None
