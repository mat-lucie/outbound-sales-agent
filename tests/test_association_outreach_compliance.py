"""Association-outreach compliance + idempotency parity with the email1/2/3 path.

The 2026-08-24 review found run_association_outreach sending the raw body (no
CAN-SPAM footer, no List-Unsubscribe header, no text/plain part, no explicit
from/reply-to identity) with no assert_email_compliance_ready gate, and saving
its bespoke sent-state with best-effort semantics only — no crash-safe shared
sent-ledger write between the Resend send and the state writes. Cold outreach
to associations is a commercial email under CAN-SPAM like every other lane.
These tests pin the mirrored pattern (same bug class as wave2_blast).
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from workflows import association_outreach
from workflows.association_outreach import (
    ASSOCIATION_LEDGER_STEP,
    run_association_outreach,
)
from workflows.email_compliance import ComplianceError, already_sent, mark_sent


def _make_contact(**overrides) -> dict:
    base = {
        "id": "assoc-1",
        "name": "Test Person",
        "email": "test@assoc.org",
        "subject": "Partnership",
        "body_html": "<p>Hello</p>",
        "organization": "Test Org",
    }
    base.update(overrides)
    return base


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_send_includes_footer_header_plaintext_and_identity(
    mock_pending, _send_day, monkeypatch
):
    monkeypatch.setenv("EMAIL_UNSUBSCRIBE_MAILTO", "unsub@acme.com")
    monkeypatch.setenv("EMAIL_PHYSICAL_ADDRESS", "55 Market St, City")
    mock_pending.return_value = [_make_contact()]

    resend = MagicMock()
    resend.send_email.return_value = {"id": "assoc-resend-1"}

    result = run_association_outreach(resend, dry_run=False, auto_confirm=True)

    assert result["sent"] == 1
    kwargs = resend.send_email.call_args[1]
    assert kwargs["headers"]["List-Unsubscribe"] == "<mailto:unsub@acme.com?subject=unsubscribe>"
    assert "55 Market St, City" in kwargs["html"]
    assert kwargs["text"] and "55 Market St, City" in kwargs["text"]
    # Original body is preserved ahead of the footer.
    assert "Hello" in kwargs["html"]
    # Explicit operator identity — never the ResendClient default.
    assert kwargs["from_address"]
    assert kwargs["reply_to"]


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_live_send_blocked_without_compliance_config(mock_pending, _send_day, monkeypatch):
    monkeypatch.delenv("EMAIL_PHYSICAL_ADDRESS", raising=False)
    mock_pending.return_value = [_make_contact()]

    resend = MagicMock()

    with pytest.raises(ComplianceError):
        run_association_outreach(resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_not_called()


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_no_pending_is_clean_noop_even_unconfigured(mock_pending, _send_day, monkeypatch):
    """A live run with nothing to send must stay a clean no-op on a box with
    no compliance env — the gate guards sends, not empty runs (the association
    lane is one-shot, so post-completion every run takes this path)."""
    monkeypatch.delenv("EMAIL_PHYSICAL_ADDRESS", raising=False)
    mock_pending.return_value = []

    result = run_association_outreach(MagicMock(), dry_run=False, auto_confirm=True)

    assert result["pending"] == 0
    assert result["sent"] == 0


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_dry_run_exempt_from_compliance_gate(mock_pending, _send_day, monkeypatch):
    monkeypatch.delenv("EMAIL_PHYSICAL_ADDRESS", raising=False)
    mock_pending.return_value = [_make_contact()]

    result = run_association_outreach(resend=None, dry_run=True, auto_confirm=True)

    assert result["pending"] == 1
    assert result["sent"] == 0


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_shared_ledger_written_before_local_state_write(mock_pending, _send_day):
    """Crash-window simulation: the bespoke local state save fails AFTER the
    Resend send. The shared sent-ledger must already hold the entry so the next
    run skip-repairs instead of re-sending."""
    mock_pending.return_value = [_make_contact()]

    resend = MagicMock()
    resend.send_email.return_value = {"id": "assoc-resend-2"}

    with patch(
        "workflows.association_outreach._save_sent_state",
        side_effect=OSError("disk full"),
    ):
        result = run_association_outreach(resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_called_once()
    assert result["sent"] == 1
    assert already_sent("assoc-1", ASSOCIATION_LEDGER_STEP)


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_already_sent_does_not_resend_and_repairs_state(mock_pending, _send_day):
    """Cross-run crash recovery: the shared ledger says sent, but the crash hit
    before the local state save / Attio stamp. The re-run must NOT re-send and
    must repair the local state + Attio stamp so future runs converge."""
    mark_sent("assoc-1", ASSOCIATION_LEDGER_STEP, date(2026, 8, 21))
    mock_pending.return_value = [_make_contact()]

    resend = MagicMock()
    attio = MagicMock()
    attio.search_people.return_value = [{"id": {"record_id": "person-rec-1"}}]
    writer = MagicMock()

    with patch("workflows.association_outreach.stamp_outreach_channel") as mock_stamp:
        result = run_association_outreach(
            resend,
            dry_run=False,
            auto_confirm=True,
            attio=attio,
            writer=writer,
        )

    resend.send_email.assert_not_called()
    assert result["sent"] == 0
    assert result["skipped"] == 1
    # Local state repaired (conftest points SENT_STATE_FILE at a temp path),
    # with the backfill entry shape — repaired_at, no fabricated sent_at or
    # resend_id (the true send date lives in the shared ledger).
    saved = json.loads(association_outreach.SENT_STATE_FILE.read_text())
    assert saved["assoc-1"]["repaired_from_shared_ledger"] is True
    assert "repaired_at" in saved["assoc-1"]
    assert "sent_at" not in saved["assoc-1"]
    assert "resend_id" not in saved["assoc-1"]
    # Attio stamp repaired too.
    mock_stamp.assert_called_once()


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_dry_run_preview_marks_repair_only_contacts(mock_pending, _send_day, capsys):
    """The preview must not claim a contact will be emailed when the shared
    ledger says the live run will only repair its state (dry-run honesty)."""
    mark_sent("assoc-1", ASSOCIATION_LEDGER_STEP, date(2026, 8, 21))
    mock_pending.return_value = [_make_contact()]

    run_association_outreach(resend=None, dry_run=True, auto_confirm=True)

    out = capsys.readouterr().out
    assert "[repair-only: already sent on a prior run]" in out


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_shared_ledger_write_failure_halts_batch_after_state_writes(
    mock_pending, _send_day
):
    """A shared-ledger write failure after a successful send must halt the
    batch — but only AFTER the local state save and Attio stamp land, because
    with the ledger unwritable they are the only dedup records of the send."""
    mock_pending.return_value = [
        _make_contact(),
        _make_contact(id="assoc-2", email="two@assoc.org"),
    ]

    resend = MagicMock()
    resend.send_email.return_value = {"id": "assoc-resend-3"}
    attio = MagicMock()
    attio.search_people.return_value = [{"id": {"record_id": "person-rec-9"}}]
    writer = MagicMock()

    with patch(
        "workflows.association_outreach.mark_sent",
        side_effect=OSError("ledger write failed"),
    ), patch(
        "workflows.association_outreach.stamp_outreach_channel"
    ) as mock_stamp, pytest.raises(RuntimeError, match="ledger write failed"):
        run_association_outreach(
            resend, dry_run=False, auto_confirm=True, attio=attio, writer=writer
        )

    # Halted after the first send — the second contact was never attempted.
    resend.send_email.assert_called_once()
    # Both remaining dedup records were still written before the halt.
    saved = json.loads(association_outreach.SENT_STATE_FILE.read_text())
    assert saved["assoc-1"]["resend_id"] == "assoc-resend-3"
    mock_stamp.assert_called_once()


# ---------------------------------------------------------------------------
# Silent-failure audit (2026-08-24): unknown send outcomes, corrupt-ledger
# fail-closed, and halt-message truthfulness.
# ---------------------------------------------------------------------------


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_unknown_send_outcome_fails_closed_and_halts(mock_pending, _send_day):
    """A send error that may have reached Resend (read timeout) must NOT be
    reported as 'not sent': the contact is recorded in the dedup stores as
    sent (fail closed) and the batch halts with a MAY-have-been-delivered
    error, so a blind re-run cannot double-send."""
    import httpx

    mock_pending.return_value = [
        _make_contact(),
        _make_contact(id="assoc-2", email="two@assoc.org"),
    ]

    resend = MagicMock()
    resend.send_email.side_effect = httpx.ReadTimeout("30s elapsed")

    with pytest.raises(RuntimeError, match="MAY have been delivered"):
        run_association_outreach(resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_called_once()
    assert already_sent("assoc-1", ASSOCIATION_LEDGER_STEP)
    saved = json.loads(association_outreach.SENT_STATE_FILE.read_text())
    assert saved["assoc-1"]["send_outcome_unknown"] is True


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_http_408_fails_closed_and_halts(mock_pending, _send_day):
    """A gateway timeout can follow an accepted send; never retry blindly."""
    import httpx

    mock_pending.return_value = [
        _make_contact(),
        _make_contact(id="assoc-2", email="two@assoc.org"),
    ]
    resend = MagicMock()
    request = httpx.Request("POST", "https://api.resend.com/emails")
    response = httpx.Response(408, request=request)
    resend.send_email.side_effect = httpx.HTTPStatusError(
        "gateway timeout", request=request, response=response
    )

    with pytest.raises(RuntimeError, match="MAY have been delivered"):
        run_association_outreach(resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_called_once()
    assert already_sent("assoc-1", ASSOCIATION_LEDGER_STEP)
    saved = json.loads(association_outreach.SENT_STATE_FILE.read_text())
    assert saved["assoc-1"]["send_outcome_unknown"] is True
    assert "assoc-2" not in saved


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_definite_send_failure_counts_error_and_continues(mock_pending, _send_day):
    """A connect error provably never reached Resend: count it as an error,
    record nothing, and continue with the rest of the batch."""
    import httpx

    mock_pending.return_value = [
        _make_contact(),
        _make_contact(id="assoc-2", email="two@assoc.org"),
    ]

    resend = MagicMock()
    resend.send_email.side_effect = [
        httpx.ConnectError("connection refused"),
        {"id": "assoc-resend-5"},
    ]

    result = run_association_outreach(resend, dry_run=False, auto_confirm=True)

    assert resend.send_email.call_count == 2
    assert result["errors"] == 1
    assert result["sent"] == 1
    assert not already_sent("assoc-1", ASSOCIATION_LEDGER_STEP)
    assert already_sent("assoc-2", ASSOCIATION_LEDGER_STEP)


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_non_dict_ledger_blocks_live_send(mock_pending, _send_day):
    """A ledger that parses as valid JSON but not an object ([]) must block
    live sends instead of being silently treated as empty history."""
    from workflows import email_compliance
    from workflows.email_compliance import LedgerCorruptError

    email_compliance.LEDGER_FILE.write_text("[]")
    mock_pending.return_value = [_make_contact()]

    resend = MagicMock()

    with pytest.raises(LedgerCorruptError):
        run_association_outreach(resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_not_called()


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_corrupt_ledger_dry_run_warns_instead_of_crashing(
    mock_pending, _send_day, capsys
):
    """Dry-run stays exempt from the ledger gate: a corrupt ledger degrades
    the preview with a warning, never a traceback."""
    from workflows import email_compliance

    email_compliance.LEDGER_FILE.write_text("{")
    mock_pending.return_value = [_make_contact()]

    result = run_association_outreach(resend=None, dry_run=True, auto_confirm=True)

    assert result["pending"] == 1
    assert "shared sent-ledger is unreadable" in capsys.readouterr().err


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_halt_survives_audit_and_save_failures_and_reports_no_records(
    mock_pending, _send_day
):
    """Correlated disk-full: mark_sent, _save_sent_state, and the audit write
    all fail. The delivery-safety RuntimeError must still surface (not be
    pre-empted by the audit OSError) and must NOT claim any surviving dedup
    record."""
    mock_pending.return_value = [_make_contact()]

    resend = MagicMock()
    resend.send_email.return_value = {"id": "assoc-resend-6"}
    audit = MagicMock()
    audit.event.side_effect = OSError("no space left on device")

    with patch(
        "workflows.association_outreach.mark_sent",
        side_effect=OSError("ledger write failed"),
    ), patch(
        "workflows.association_outreach._save_sent_state",
        side_effect=OSError("disk full"),
    ), pytest.raises(RuntimeError, match="NO dedup record of this send survives"):
        run_association_outreach(
            resend, dry_run=False, auto_confirm=True, audit_logger=audit
        )


@patch("workflows.association_outreach.is_send_day", return_value=True)
@patch("workflows.association_outreach.get_pending_association_emails")
def test_repair_only_batch_proceeds_without_resend_client(mock_pending, _send_day):
    """A live batch where every contact is repair-only never touches Resend,
    so it must repair state even when old copy is blank and resend is None."""
    mark_sent("assoc-1", ASSOCIATION_LEDGER_STEP, date(2026, 8, 21))
    mock_pending.return_value = [_make_contact(subject="   ", body_html="<p>&nbsp;</p>")]

    result = run_association_outreach(None, dry_run=False, auto_confirm=True)

    assert result["errors"] == 0
    assert result["skipped"] == 1
    saved = json.loads(association_outreach.SENT_STATE_FILE.read_text())
    assert saved["assoc-1"]["repaired_from_shared_ledger"] is True
