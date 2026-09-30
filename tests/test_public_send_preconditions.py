"""Fresh CRM state must prevent stale delivery and destructive ledger repair."""
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from tests.fakes import fake_daily_run
from tests.test_email_campaign import _make_attio_person
from tests.test_integration import _attio_with_full_schema, _make_attio_entry
from workflows.email_send_guard import verify_email_send_preconditions
from workflows.send_preconditions import verify_send_preconditions


@pytest.mark.parametrize("response,reason", [
    ({"data": {"entry_values": {"stage": [{"value": "Not Interested"}]}}}, "stage_moved"),
    ({"data": {"entry_values": {"stage": [{"value": "Accepted"}],
                               "merged_into": [{"value": "winner"}]}}}, "suppressed"),
    ({"data": {}}, "reread_failed"),
    ({"data": []}, "reread_failed"),
])
def test_entry_fresh_state_blocks(response, reason, monkeypatch):
    monkeypatch.setenv("ATTIO_LIST_ID", "list")
    attio = MagicMock()
    attio._request.return_value = response
    result = verify_send_preconditions(attio, "entry", "Accepted")
    assert not result.allowed
    assert result.reason == reason
    attio._request.assert_called_once_with("GET", "/lists/list/entries/entry")


def test_fresh_stage_uses_operator_mapping(monkeypatch, tmp_path):
    monkeypatch.setenv("ATTIO_LIST_ID", "list")
    monkeypatch.setenv("OUTBOUND_CONFIG_DIR", str(tmp_path))
    (tmp_path / "crm.yaml").write_text('vendor: attio\nstage_mapping:\n  ACCEPTED: {name: "Connected", rank: 2}\n')
    attio = MagicMock()
    attio._request.return_value = {"data": {"entry_values": {"stage": [{"value": "Connected"}]}}}
    assert verify_send_preconditions(attio, "e", "Accepted").allowed


@pytest.mark.parametrize("response", [{}, {"data": []}, {"data": {"values": {"email_campaign_stage": [{"value": "unsubscribed"}]}}}])
def test_email_fresh_state_fails_closed(response):
    attio = MagicMock()
    attio._request.return_value = response
    assert not verify_email_send_preconditions(attio, "person", "queued").allowed


def test_stale_invite_never_reaches_provider(monkeypatch):
    from workflows.daily_check import run_connection_requests
    monkeypatch.setenv("ATTIO_LIST_ID", "list")
    entry = _make_attio_entry("entry", "person", "Prospect", quality_score=80)
    attio = _attio_with_full_schema()
    attio.query_list_entries.return_value = [entry]
    attio.person_language_override.return_value = None
    attio._request.return_value = {"data": {"entry_values": {"stage": [{"value": "Not Interested"}]}}}
    cache = MagicMock()
    cache.get.return_value = ("Alex Example", "Acme", "https://linkedin.com/in/example", "manufacturing", "Director")
    pb = MagicMock()
    with patch("workflows.daily_check.ensure_throttle_policy_decision_opened"), patch("workflows.daily_check.can_send_connections", return_value=True), patch(
        "workflows.daily_check.get_remaining", return_value={"connections": 25, "messages": 30, "visits": 50}
    ):
        result = run_connection_requests(attio, pb, "invite-agent", daily_run=fake_daily_run(),
                                         auto_confirm=True, cache=cache)
    assert result["sent"] == 0
    pb.launch_agent.assert_not_called()
    attio.update_list_entry.assert_not_called()


@pytest.mark.parametrize("ledger_hit", [False, True])
def test_email_unsubscribe_after_selection_prevents_send_and_repair(ledger_hit):
    from workflows.email_campaign import run_email_daily
    attio = MagicMock()
    attio.search_people.side_effect = [[_make_attio_person("p", "Alex", "Example", "alex@example.com", "queued")], [], []]
    attio._request.return_value = {"data": {"values": {"email_campaign_stage": [{"value": "unsubscribed"}]}}}
    resend = MagicMock()
    with patch("workflows.email_campaign.date") as day, patch(
        "workflows.email_campaign._build_linkedin_collision_set", return_value=set()
    ), patch("workflows.email_campaign.build_suppression_set", return_value=set()), patch(
        "workflows.email_campaign.already_sent", return_value=ledger_hit
    ):
        day.today.return_value = date(2026, 9, 29)
        day.fromisoformat = date.fromisoformat
        result = run_email_daily(attio, resend, auto_confirm=True)
    assert result["send_guard_skipped"] == 1
    assert result["sent"] == 0
    resend.send_email.assert_not_called()
    attio.update_person.assert_not_called()


def test_later_invite_refusal_preserves_prior_delivery():
    from workflows.daily_check import BlankMessageError, drain_connection_invites
    run = MagicMock()
    run.remaining.return_value = 25
    one = MagicMock(side_effect=[{"sent": 10, "pb_queued": 10, "attio_updated": 10},
                                BlankMessageError("invite", ["e"] )])
    with pytest.raises(BlankMessageError) as failure:
        drain_connection_invites(one, batch_size=25, daily_run=run)
    assert failure.value.partial_results["sent"] == 10
    assert failure.value.partial_results["attio_updated"] == 10
