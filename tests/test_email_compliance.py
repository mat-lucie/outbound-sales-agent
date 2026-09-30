"""Tests for the email-compliance layer (workflows/email_compliance.py) and its
wiring into run_email_daily: suppression-on-send, CAN-SPAM gate + footer,
List-Unsubscribe header, plaintext, idempotency ledger, and the unsubscribe path.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from clients.resend_client import ResendClient
from workflows import email_compliance
from workflows.email_campaign import run_email_daily
from workflows.email_compliance import (
    ComplianceError,
    LedgerCorruptError,
    already_sent,
    append_footer,
    assert_email_compliance_ready,
    build_footer,
    html_to_text,
    list_unsubscribe_header,
    mark_sent,
    unsubscribe_email,
)


def _person(record_id, first, last, email, stage):
    return {
        "id": {"record_id": record_id},
        "values": {
            "name": [{"first_name": first, "last_name": last}],
            "email_addresses": [{"email_address": email}],
            "email_campaign_stage": [{"value": stage}],
            "email_campaign_last_sent": [],
            "primary_location": [{"country_code": "US"}],
            "company": [],
        },
    }


# ── ResendClient headers/text passthrough ─────────────────────────────────────


def _stub_resend():
    rc = ResendClient(api_key="test-key")
    rc._client = MagicMock()
    resp = MagicMock()
    resp.json.return_value = {"id": "e1"}
    rc._client.post.return_value = resp
    return rc


def test_resend_passes_headers_and_text():
    rc = _stub_resend()
    rc.send_email(
        "a@b.com", "subj", "<p>hi</p>",
        text="hi", headers={"List-Unsubscribe": "<mailto:u@x.com>"},
    )
    payload = rc._client.post.call_args[1]["json"]
    assert payload["text"] == "hi"
    assert payload["headers"]["List-Unsubscribe"] == "<mailto:u@x.com>"


def test_resend_omits_headers_text_when_absent():
    rc = _stub_resend()
    rc.send_email("a@b.com", "subj", "<p>hi</p>")
    payload = rc._client.post.call_args[1]["json"]
    assert "text" not in payload
    assert "headers" not in payload


# ── Compliance send-gate ──────────────────────────────────────────────────────


def test_gate_raises_without_physical_address(monkeypatch):
    monkeypatch.delenv("EMAIL_PHYSICAL_ADDRESS", raising=False)
    with pytest.raises(ComplianceError, match="EMAIL_PHYSICAL_ADDRESS"):
        assert_email_compliance_ready(dry_run=False)


def test_gate_dry_run_exempt(monkeypatch):
    monkeypatch.delenv("EMAIL_PHYSICAL_ADDRESS", raising=False)
    assert_email_compliance_ready(dry_run=True)  # must not raise


def test_gate_passes_with_full_config():
    # conftest._email_compliance_baseline_env sets address + sender org +
    # unsubscribe mailto; the ledger tmp file doesn't exist yet (→ empty).
    assert_email_compliance_ready(dry_run=False)  # must not raise


def test_gate_raises_without_unsubscribe_address(monkeypatch):
    # A live send needs a working opt-out. Remove both the mailto and the
    # reply-to fallback so list_unsubscribe_header() returns None.
    monkeypatch.delenv("EMAIL_UNSUBSCRIBE_MAILTO", raising=False)
    monkeypatch.delenv("EMAIL_REPLY_TO", raising=False)
    with pytest.raises(ComplianceError, match="unsubscribe"):
        assert_email_compliance_ready(dry_run=False)


def test_gate_raises_on_unresolvable_sender_org(monkeypatch):
    # _sender_org() must resolve non-empty or the footer ships the literal
    # placeholder. Clear the explicit org AND the EMAIL_FROM fallback.
    monkeypatch.delenv("EMAIL_SENDER_ORG", raising=False)
    monkeypatch.delenv("EMAIL_FROM", raising=False)
    with pytest.raises(ComplianceError, match="[Ss]ender org"):
        assert_email_compliance_ready(dry_run=False)


def test_gate_blocks_live_send_on_corrupt_ledger():
    # A corrupt ledger must fail loud on a live send (not silently reset to
    # empty history, which would re-email the crash window).
    email_compliance.LEDGER_FILE.parent.mkdir(parents=True, exist_ok=True)
    email_compliance.LEDGER_FILE.write_text("{ this is not valid json")
    with pytest.raises(LedgerCorruptError, match="corrupt"):
        assert_email_compliance_ready(dry_run=False)


def test_gate_corrupt_ledger_dry_run_exempt():
    # dry-run never touches the ledger, so a corrupt file must NOT block it.
    email_compliance.LEDGER_FILE.parent.mkdir(parents=True, exist_ok=True)
    email_compliance.LEDGER_FILE.write_text("{ this is not valid json")
    assert_email_compliance_ready(dry_run=True)  # must not raise


# ── List-Unsubscribe header ──────────────────────────────────────────────────


def test_list_unsubscribe_header_present_with_mailto(monkeypatch):
    monkeypatch.setenv("EMAIL_UNSUBSCRIBE_MAILTO", "unsub@acme.com")
    hdr = list_unsubscribe_header()
    assert hdr == {"List-Unsubscribe": "<mailto:unsub@acme.com?subject=unsubscribe>"}


def test_list_unsubscribe_falls_back_to_reply_to(monkeypatch):
    monkeypatch.delenv("EMAIL_UNSUBSCRIBE_MAILTO", raising=False)
    monkeypatch.setenv("EMAIL_REPLY_TO", "reply@acme.com")
    assert list_unsubscribe_header()["List-Unsubscribe"] == "<mailto:reply@acme.com?subject=unsubscribe>"


def test_list_unsubscribe_none_when_unconfigured(monkeypatch):
    monkeypatch.delenv("EMAIL_UNSUBSCRIBE_MAILTO", raising=False)
    monkeypatch.delenv("EMAIL_REPLY_TO", raising=False)
    assert list_unsubscribe_header() is None


# ── Footer + plaintext ───────────────────────────────────────────────────────


def test_footer_contains_org_address_optout(monkeypatch):
    monkeypatch.setenv("EMAIL_SENDER_ORG", "Acme Inc")
    monkeypatch.setenv("EMAIL_PHYSICAL_ADDRESS", "1 Main St, Townsville")
    html, text = build_footer()
    for blob in (html, text):
        assert "Acme Inc" in blob
        assert "1 Main St, Townsville" in blob
        assert "UNSUBSCRIBE" in blob.upper()


def test_footer_html_escapes_org_and_address(monkeypatch):
    # A legitimate "&" in an org/address must not render as a broken entity in
    # the HTML part — but the plaintext part stays literal.
    monkeypatch.setenv("EMAIL_SENDER_ORG", "Smith & Co")
    monkeypatch.setenv("EMAIL_PHYSICAL_ADDRESS", "1 Main St <Suite 2>, Townsville")
    html, text = build_footer()
    assert "Smith &amp; Co" in html
    assert "&lt;Suite 2&gt;" in html
    assert "Smith & Co" in text
    assert "1 Main St <Suite 2>, Townsville" in text


def test_footer_optout_line_is_bilingual(monkeypatch):
    # ES/EN prospects both get an intelligible instruction; the UNSUBSCRIBE
    # keyword stays constant so an inbox filter can match it.
    monkeypatch.setenv("EMAIL_PHYSICAL_ADDRESS", "1 Main St")
    html, text = build_footer()
    for blob in (html, text):
        assert "Reply with UNSUBSCRIBE" in blob
        assert "Responde UNSUBSCRIBE" in blob


def test_html_to_text_strips_tags_and_keeps_breaks():
    out = html_to_text("<p>Hello &amp; welcome</p><br><p>Line two</p>")
    assert "Hello & welcome" in out
    assert "<" not in out
    assert "\n" in out


def test_append_footer_returns_html_and_text(monkeypatch):
    monkeypatch.setenv("EMAIL_PHYSICAL_ADDRESS", "9 Elm Rd")
    html, text = append_footer("<p>Body</p>")
    assert html.startswith("<p>Body</p>")
    assert "9 Elm Rd" in html and "9 Elm Rd" in text
    assert "Body" in text and "<" not in text.split("--")[0]


# ── Idempotency ledger ───────────────────────────────────────────────────────


def test_ledger_roundtrip():
    assert not already_sent("rX", "email1")
    mark_sent("rX", "email1", date(2026, 4, 7))
    assert already_sent("rX", "email1")
    # step-scoped: email2 is independent of email1.
    assert not already_sent("rX", "email2")
    # NOT date-scoped: a send recorded on ANY date suppresses re-send forever
    # (each step is a once-ever event per contact) — cross-day idempotency.
    mark_sent("rY", "email1", date(2026, 1, 1))
    assert already_sent("rY", "email1")


def test_mark_sent_writes_atomically_no_tmp_left_behind():
    # mark_sent must write via a temp file + os.replace so an interrupted
    # write can't corrupt the ledger. After a clean write, the ledger exists and
    # no stray .tmp remains.
    mark_sent("rZ", "email1", date(2026, 4, 7))
    ledger = email_compliance.LEDGER_FILE
    assert ledger.exists()
    tmp = ledger.with_name(ledger.name + ".tmp")
    assert not tmp.exists()
    # And the file is valid JSON (not truncated).
    import json

    assert json.loads(ledger.read_text())["rZ|email1"] == "2026-04-07"


def test_already_sent_fails_loud_on_corrupt_ledger():
    # The low-level load must raise, not silently return {}.
    email_compliance.LEDGER_FILE.parent.mkdir(parents=True, exist_ok=True)
    email_compliance.LEDGER_FILE.write_text("]not json[")
    with pytest.raises(LedgerCorruptError, match="corrupt"):
        already_sent("rX", "email1")


# ── unsubscribe_email ────────────────────────────────────────────────────────


def test_unsubscribe_email_sets_stage():
    attio = MagicMock()
    attio.search_people.return_value = [{"id": {"record_id": "r9"}}]
    updated, maybe_more = unsubscribe_email(attio, "x@y.com")
    assert updated == ["r9"]
    assert maybe_more is False
    attio.update_person.assert_called_once_with(
        "r9", {"email_campaign_stage": "unsubscribed"}
    )


def test_unsubscribe_email_updates_all_duplicates():
    # CRM workspaces routinely carry duplicate person records — every match
    # must be updated, not just the first.
    attio = MagicMock()
    attio.search_people.return_value = [
        {"id": {"record_id": "r1"}},
        {"id": {"record_id": "r2"}},
        {"id": {"record_id": ""}},  # malformed → skipped, no blind update
        {"id": {"record_id": "r3"}},
    ]
    updated, maybe_more = unsubscribe_email(attio, "dup@y.com")
    assert updated == ["r1", "r2", "r3"]
    assert maybe_more is False
    assert attio.update_person.call_count == 3


def test_unsubscribe_email_flags_maybe_more_at_limit():
    # When the search returns exactly `limit` rows, more duplicates may exist
    # beyond the cap — signal it so the CLI can warn.
    attio = MagicMock()
    attio.search_people.return_value = [{"id": {"record_id": f"r{i}"}} for i in range(3)]
    updated, maybe_more = unsubscribe_email(attio, "many@y.com", limit=3)
    assert len(updated) == 3
    assert maybe_more is True


def test_unsubscribe_email_not_found():
    attio = MagicMock()
    attio.search_people.return_value = []
    updated, maybe_more = unsubscribe_email(attio, "no@one.com")
    assert updated == []
    assert maybe_more is False
    attio.update_person.assert_not_called()


# ── run_email_daily integration ──────────────────────────────────────────────


@patch("workflows.email_campaign.date")
@patch("workflows.email_campaign._build_linkedin_collision_set")
@patch("workflows.email_campaign.build_suppression_set")
def test_suppressed_contact_is_skipped(mock_supp, mock_coll, mock_date):
    mock_date.today.return_value = date(2026, 4, 7)
    mock_date.fromisoformat = date.fromisoformat
    mock_coll.return_value = set()
    mock_supp.return_value = {"r-supp"}

    attio = MagicMock()
    attio.search_people.side_effect = [
        [_person("r-supp", "A", "B", "a@x.com", "queued")],
        [],
        [],
    ]
    resend = MagicMock()
    resend.send_email.return_value = {"id": "e1"}

    result = run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_not_called()
    assert result["suppressed"] == 1


@patch("workflows.email_campaign.date")
@patch("workflows.email_campaign._build_linkedin_collision_set")
@patch("workflows.email_campaign.build_suppression_set")
def test_record_with_blank_id_is_skipped(mock_supp, mock_coll, mock_date):
    """A malformed record with a blank record_id can't be stage-tracked or
    suppression-matched — it must be skipped, never blind-sent."""
    mock_date.today.return_value = date(2026, 4, 7)
    mock_date.fromisoformat = date.fromisoformat
    mock_coll.return_value = set()
    mock_supp.return_value = set()

    attio = MagicMock()
    attio.search_people.side_effect = [
        [_person("", "No", "Id", "noid@x.com", "queued")],
        [],
        [],
    ]
    resend = MagicMock()

    run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

    resend.send_email.assert_not_called()
    attio.update_person.assert_not_called()


@patch("workflows.email_campaign.date")
@patch("workflows.email_campaign._build_linkedin_collision_set")
@patch("workflows.email_campaign.build_suppression_set")
def test_send_includes_footer_header_and_plaintext(
    mock_supp, mock_coll, mock_date, monkeypatch
):
    monkeypatch.setenv("EMAIL_UNSUBSCRIBE_MAILTO", "unsub@acme.com")
    monkeypatch.setenv("EMAIL_PHYSICAL_ADDRESS", "55 Market St, City")
    mock_date.today.return_value = date(2026, 4, 7)
    mock_date.fromisoformat = date.fromisoformat
    mock_coll.return_value = set()
    mock_supp.return_value = set()

    attio = MagicMock()
    attio.search_people.side_effect = [
        [_person("r1", "John", "Doe", "john@acme.com", "queued")],
        [],
        [],
    ]
    resend = MagicMock()
    resend.send_email.return_value = {"id": "e1"}

    run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

    kwargs = resend.send_email.call_args[1]
    assert kwargs["headers"]["List-Unsubscribe"] == "<mailto:unsub@acme.com?subject=unsubscribe>"
    assert "55 Market St, City" in kwargs["html"]
    assert kwargs["text"] and "55 Market St, City" in kwargs["text"]


@patch("workflows.email_campaign.date")
@patch("workflows.email_campaign._build_linkedin_collision_set")
@patch("workflows.email_campaign.build_suppression_set")
def test_already_sent_does_not_resend_and_repairs_stage(
    mock_supp, mock_coll, mock_date
):
    mock_date.today.return_value = date(2026, 4, 7)
    mock_date.fromisoformat = date.fromisoformat
    mock_coll.return_value = set()
    mock_supp.return_value = set()

    # Simulate a crash on a PRIOR day: email already sent (recorded yesterday),
    # stage not advanced. The cross-day re-run must NOT re-send.
    mark_sent("r1", "email1", date(2026, 4, 6))

    attio = MagicMock()
    attio.search_people.side_effect = [
        [_person("r1", "John", "Doe", "john@acme.com", "queued")],
        [],
        [],
    ]
    resend = MagicMock()

    run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

    # No duplicate send; stage write is repaired.
    resend.send_email.assert_not_called()
    attio.update_person.assert_called_once()
    assert attio.update_person.call_args[0][1]["email_campaign_stage"] == "email1_sent"


@pytest.fixture(autouse=True)
def _fresh_state_boundary():
    from unittest.mock import patch

    from workflows.email_send_guard import GuardResult
    with patch("workflows.daily_check.verify_send_preconditions", return_value=GuardResult(True)), patch("workflows.email_campaign.verify_email_send_preconditions", return_value=GuardResult(True)), patch("workflows.dm_quality_gate.require_clear_dm_quality_queue"):
        yield
