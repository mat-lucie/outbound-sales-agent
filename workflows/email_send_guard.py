"""Fresh email-stage checks before sends and sent-ledger repairs."""
from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clients.attio import AttioClient
    from workflows.audit import AuditLogger


@dataclass(frozen=True)
class GuardResult:
    """A fresh email-stage observation; failures never authorize delivery."""

    allowed: bool
    reason: str = ""
    actual_stage: str = ""


def _extract_select_value_from_person(values: dict, field: str) -> str:
    """Extract a select attribute value from an Attio person record's ``values`` dict.

    Handles common shapes returned by ``GET /v2/objects/people/records/{id}``:
    - ``{field: [{"value": "queued"}]}``                        (plain value)
    - ``{field: [{"option": {"title": "queued"}}]}``            (select option)
    - ``{field: "queued"}``                                     (scalar, test fakes)

    NOTE — semantics intentionally differ from ``AttioClient._extract_value``
    (``clients/attio.py``): that helper targets list-entry values and routes
    select-typed attributes via ``attribute_type==select``; this helper targets
    person-record values which carry a different shape and have no
    ``attribute_type`` discriminator.  Do not merge them.

    Also distinct from the module-level ``parse_stage_value`` / ``_extract_stage_from_raw``
    which targets the ``stage`` attribute on list entries, not person-record
    attributes.

    Thin delegate: the implementation moved to
    ``AttioClient.extract_person_select_value`` (the client layer owns Attio
    response shapes) when the person-level language override needed the same
    parser. Kept as a named function so this module's call sites and tests
    read unchanged.
    """
    from clients.attio import AttioClient

    return AttioClient.extract_person_select_value(values, field)



def verify_email_send_preconditions(
    attio: AttioClient,
    record_id: str,
    expected_email_stage: str,
    *,
    audit_logger: AuditLogger | None = None,
) -> GuardResult:
    """Re-read the contact's ``email_campaign_stage`` from the Attio person
    record and confirm it still matches what the run assumed when it queued
    this email send.

    This is the email-channel equivalent of ``verify_send_preconditions``
    (which targets LinkedIn list entries). Email contacts are stored on the
    *person* object (not in the ``linkedin_outreach`` list), so there is no
    ``entry_id`` and no ``owner`` field to check — only the stage guard runs.

    Args:
        attio:                  AttioClient instance.
        record_id:              Person record id (``GET /v2/objects/people/records/{id}``).
        expected_email_stage:   The ``email_campaign_stage`` value the caller
                                assumed (e.g. ``"queued"``, ``"email1_sent"``).
                                A mismatch means the contact was already
                                advanced (by another process or a manual
                                Attio edit) → skip to avoid duplicate send.
        audit_logger:           Optional audit logger; emits an audit event on
                                every non-pass outcome.

    Returns:
        ``GuardResult(allowed=True)``  when the stage still matches.
        ``GuardResult(allowed=False)`` when the stage has moved OR when the
        re-read itself fails (fail-closed, reason ``"reread_failed"``).

    Residual window: time between this re-read and ``resend.send_email(...)``
    (per-prospect, no batching) — typically ~100–300 ms (Attio GET + network RTT).
    """
    try:
        raw_data = attio._request("GET", f"/objects/people/records/{record_id}")
        if not isinstance(raw_data, dict):
            raise ValueError("Person response is not an object")
        person = raw_data.get("data", raw_data)
        if not isinstance(person, dict) or not isinstance(person.get("values"), dict):
            raise ValueError("Person response has no values")
    except Exception as exc:  # noqa: BLE001 — fail-closed on ANY network/API error
        print(
            f"[send_guard] reread failed record_id={record_id} channel=email"
            f" error={exc!s}",
            file=sys.stderr,
        )
        if audit_logger is not None:
            try:  # noqa: SIM105 — audit_logger.event raising must not crash batch assembly
                audit_logger.event(
                    "send_guard_reread_failed",
                    record_id=record_id,
                    expected_stage=expected_email_stage,
                    channel="email",
                    error=str(exc),
                )
            except Exception:  # noqa: BLE001
                pass
        return GuardResult(allowed=False, reason="reread_failed")

    values = person.get("values") or {}
    actual_stage = _extract_select_value_from_person(values, "email_campaign_stage")

    if actual_stage != expected_email_stage:
        if audit_logger is not None:
            audit_logger.event(
                "send_guard_stage_moved",
                record_id=record_id,
                expected_stage=expected_email_stage,
                actual_stage=actual_stage,
                channel="email",
            )
        return GuardResult(
            allowed=False,
            reason="stage_moved",
            actual_stage=actual_stage,
        )

    return GuardResult(allowed=True, reason="", actual_stage=actual_stage)

