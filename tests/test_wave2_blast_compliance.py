"""Wave-2 blast compliance + idempotency parity with the email1/2/3 path.

The 2026-08-24 review found run_wave2_blast sending the raw body (no CAN-SPAM
footer, no List-Unsubscribe header, no text/plain part) and PATCHing the Attio
stage with no sent-ledger write in between — the exact crash window
email_campaign.py documents ("Ledger BEFORE the CRM stage write"). Wave-2
targets already-idle contacts, the highest-complaint-rate cohort, so both gaps
matter most here. These tests pin the mirrored pattern, plus the wave-2
hardenings on top of it: the stage-repair branch is send-guarded (a stage that
moved mid-run is never overwritten) and fail-soft (a failing repair PATCH
cannot abort the rest of the blast).
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import httpx
import pytest

from tests.fakes import email_guard_pass_response, make_wave2_person
from workflows.email_compliance import ComplianceError, already_sent, mark_sent
from workflows.wave2_blast import (
    WAVE2_LEDGER_STEP,
    WAVE2_STAGE,
    WAVE2_UNKNOWN_LEDGER_STEP,
    run_wave2_blast,
)


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_send_includes_footer_header_and_plaintext(
    mock_supp, mock_coll, mock_date, monkeypatch
):
    monkeypatch.setenv("EMAIL_UNSUBSCRIBE_MAILTO", "unsub@acme.com")
    monkeypatch.setenv("EMAIL_PHYSICAL_ADDRESS", "55 Market St, City")
    mock_date.today.return_value = date(2026, 5, 5)  # Tuesday
    mock_coll.return_value = set()
    mock_supp.return_value = set()

    attio = MagicMock()
    attio.search_people.side_effect = [
        [make_wave2_person("rec-1", "John", "Doe", "john@acme.com", "email1_sent")],
        [],
    ]
    attio._request.return_value = email_guard_pass_response("email1_sent")
    resend = MagicMock()
    resend.send_email.return_value = {"id": "w2-1"}

    run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    kwargs = resend.send_email.call_args[1]
    assert kwargs["headers"]["List-Unsubscribe"] == "<mailto:unsub@acme.com?subject=unsubscribe>"
    assert "55 Market St, City" in kwargs["html"]
    assert kwargs["text"] and "55 Market St, City" in kwargs["text"]


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_already_sent_does_not_resend_and_repairs_stage(
    mock_supp, mock_coll, mock_date
):
    mock_date.today.return_value = date(2026, 5, 5)
    mock_coll.return_value = set()
    mock_supp.return_value = set()

    # Simulate a crash on a PRIOR day: wave2 sent (recorded yesterday), stage
    # never advanced past email1_sent. The cross-day re-run must NOT re-send —
    # the Phase-3 send guard can't catch this, the stage is exactly what it
    # expects.
    mark_sent("rec-1", WAVE2_LEDGER_STEP, date(2026, 5, 4))

    attio = MagicMock()
    attio.search_people.side_effect = [
        [make_wave2_person("rec-1", "John", "Doe", "john@acme.com", "email1_sent")],
        [],
    ]
    # Repair is guarded too: the re-read must confirm the stage is unmoved.
    attio._request.return_value = email_guard_pass_response("email1_sent")
    resend = MagicMock()

    result = run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_not_called()
    attio.update_person.assert_called_once()
    repaired = attio.update_person.call_args[0]
    assert repaired[0] == "rec-1"
    assert repaired[1]["email_campaign_stage"] == WAVE2_STAGE
    assert result["sent"] == 0
    assert result["already_sent_repaired"] == 1
    assert result["repair_failed"] == 0


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_repair_skipped_when_stage_moved(
    mock_supp, mock_coll, mock_date
):
    """A stage that moved after the batch snapshot (reply detected, operator
    ran email-unsubscribe) must never be overwritten by the stage repair."""
    mock_date.today.return_value = date(2026, 5, 5)
    mock_coll.return_value = set()
    mock_supp.return_value = set()

    mark_sent("rec-1", WAVE2_LEDGER_STEP, date(2026, 5, 4))

    attio = MagicMock()
    attio.search_people.side_effect = [
        [make_wave2_person("rec-1", "John", "Doe", "john@acme.com", "email1_sent")],
        [],
    ]
    attio._request.return_value = email_guard_pass_response("unsubscribed")
    resend = MagicMock()

    result = run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_not_called()
    attio.update_person.assert_not_called()
    assert result["send_guard_skipped"] == 1
    assert result["already_sent_repaired"] == 0


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_repair_failure_reports_batch_failure_after_other_contacts(
    mock_supp, mock_coll, mock_date
):
    """A failing repair PATCH (e.g. 404 after a record merge) is loud but
    non-fatal: the ledger already blocks a re-send, and the remaining
    contacts must still be processed."""
    mock_date.today.return_value = date(2026, 5, 5)
    mock_coll.return_value = set()
    mock_supp.return_value = set()

    mark_sent("rec-merged", WAVE2_LEDGER_STEP, date(2026, 5, 4))

    attio = MagicMock()
    attio.search_people.side_effect = [
        [
            make_wave2_person("rec-merged", "Gone", "Person", "gone@corp.com", "email1_sent"),
            make_wave2_person("rec-clean", "Clean", "Sweep", "c@corp.com", "email1_sent"),
        ],
        [],
    ]
    attio._request.return_value = email_guard_pass_response("email1_sent")

    def _update(record_id, attrs):
        if record_id == "rec-merged":
            raise RuntimeError("Attio 404: record merged")
        return {}

    attio.update_person.side_effect = _update
    resend = MagicMock()
    resend.send_email.return_value = {"id": "w2-2"}

    with pytest.raises(RuntimeError, match="stage repair failed.*rec-merged"):
        run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    # rec-merged: repair failed, no re-send; rec-clean: sent normally.
    resend.send_email.assert_called_once()
    assert resend.send_email.call_args[1]["to"] == "c@corp.com"
    assert already_sent("rec-clean", WAVE2_LEDGER_STEP)


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set", return_value=set())
@patch("workflows.wave2_blast.build_suppression_set", return_value=set())
def test_wave2_read_timeout_blocks_batch_and_rerun(
    _supp, _coll, mock_date, monkeypatch
):
    monkeypatch.setenv("EMAIL_UNSUBSCRIBE_MAILTO", "unsub@acme.com")
    monkeypatch.setenv("EMAIL_PHYSICAL_ADDRESS", "55 Market St, City")
    mock_date.today.return_value = date(2026, 5, 5)
    people = [
        make_wave2_person("rec-1", "John", "Doe", "john@acme.com", "email1_sent"),
        make_wave2_person("rec-2", "Jane", "Doe", "jane@acme.com", "email1_sent"),
    ]
    attio = MagicMock()
    attio.search_people.side_effect = [people, [], people, []]
    attio._request.return_value = email_guard_pass_response("email1_sent")
    resend = MagicMock()
    resend.send_email.side_effect = httpx.ReadTimeout("response lost")

    with pytest.raises(RuntimeError, match="delivery outcome.*unknown"):
        run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)
    assert already_sent("rec-1", WAVE2_UNKNOWN_LEDGER_STEP)
    assert not already_sent("rec-1", WAVE2_LEDGER_STEP)
    assert resend.send_email.call_count == 1

    with pytest.raises(RuntimeError, match="unknown outcome"):
        run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)
    assert resend.send_email.call_count == 1
    attio.update_person.assert_not_called()


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set", return_value=set())
@patch("workflows.wave2_blast.build_suppression_set", return_value=set())
def test_wave2_repair_only_ignores_blank_template(
    _supp, _coll, mock_date, monkeypatch
):
    monkeypatch.setenv("EMAIL_PHYSICAL_ADDRESS", "55 Market St, City")
    mock_date.today.return_value = date(2026, 5, 5)
    mark_sent("rec-1", WAVE2_LEDGER_STEP, date(2026, 5, 4))
    attio = MagicMock()
    attio.search_people.side_effect = [
        [make_wave2_person("rec-1", "John", "Doe", "john@acme.com", "email1_sent")],
        [],
    ]
    attio._request.return_value = email_guard_pass_response("email1_sent")
    resend = MagicMock()

    with patch("workflows.wave2_blast._load_wave2_template", return_value={
        "subject": "", "body_html": ""
    }):
        result = run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    assert result["already_sent_repaired"] == 1
    resend.send_email.assert_not_called()
    attio.update_person.assert_called_once()


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_ledger_written_before_stage_write(
    mock_supp, mock_coll, mock_date
):
    """Crash-window simulation: the Attio stage PATCH fails AFTER the Resend
    send. The ledger must already hold the (record_id, wave2) entry so the next
    run skip-repairs instead of re-sending."""
    mock_date.today.return_value = date(2026, 5, 5)
    mock_coll.return_value = set()
    mock_supp.return_value = set()

    attio = MagicMock()
    attio.search_people.side_effect = [
        [make_wave2_person("rec-1", "John", "Doe", "john@acme.com", "email1_sent")],
        [],
    ]
    attio._request.return_value = email_guard_pass_response("email1_sent")
    attio.update_person.side_effect = RuntimeError("Attio 500 mid-blast")
    resend = MagicMock()
    resend.send_email.return_value = {"id": "w2-1"}

    with pytest.raises(RuntimeError, match="Attio 500 mid-blast"):
        run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_called_once()
    assert already_sent("rec-1", WAVE2_LEDGER_STEP)


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_live_send_blocked_without_compliance_config(
    mock_supp, mock_coll, mock_date, monkeypatch
):
    monkeypatch.delenv("EMAIL_PHYSICAL_ADDRESS", raising=False)
    mock_date.today.return_value = date(2026, 5, 5)
    mock_coll.return_value = set()
    mock_supp.return_value = set()

    attio = MagicMock()
    resend = MagicMock()

    with pytest.raises(ComplianceError):
        run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_not_called()


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_dry_run_exempt_from_compliance_gate(
    mock_supp, mock_coll, mock_date, monkeypatch
):
    monkeypatch.delenv("EMAIL_PHYSICAL_ADDRESS", raising=False)
    mock_date.today.return_value = date(2026, 5, 5)
    mock_coll.return_value = set()
    mock_supp.return_value = set()

    attio = MagicMock()
    attio.search_people.side_effect = [
        [make_wave2_person("rec-1", "John", "Doe", "john@acme.com", "email1_sent")],
        [],
    ]

    result = run_wave2_blast(attio, None, dry_run=True, auto_confirm=True)

    assert result["sent"] == 1
    attio.update_person.assert_not_called()
