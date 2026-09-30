"""Task 1 sender seam: `PBSender` conformance to the `Sender` protocol.

The drain code in `workflows/daily_check.py` calls the PB-shaped
per-launch methods (`launch_invite_batch` / `launch_dm_batch`) directly
— those are pinned by the existing send-path suites. This file pins the
protocol surface itself: the three `Sender` methods exist, delegate to
the same transport flow, and `fetch_events` stays empty on PB (its
detection is scrape-based).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from clients.google_sheets import write_prospects_to_sheet
from clients.pb_envelope import SendOutcome
from clients.sender import LeadEvent, PBSender, PBSendResult, Sender


def _pb(csv_text: str | None, log_output: str = "") -> MagicMock:
    pb = MagicMock()
    pb.launch_agent.return_value = MagicMock(container_id="cid-1")
    pb.wait_for_completion.return_value = MagicMock(
        status="finished", log_output=log_output
    )
    pb.download_result_csv.return_value = csv_text
    return pb


def _sender(pb: MagicMock) -> PBSender:
    return PBSender(
        pb,
        network_booster_id="nb-id",
        message_sender_id="ms-id",
        write_sheet=lambda rows: "https://sheet/x",
        session_args=lambda: {"sessionCookie": "c", "userAgent": "ua"},
    )


def test_pbsender_satisfies_sender_protocol():
    assert isinstance(_sender(_pb(None)), Sender)


def test_fetch_events_is_empty_on_pb():
    # PB detection stays scrape-based (Phase 0 / 0.5); the event feed is
    # a Botdog capability (Task 5).
    assert _sender(_pb(None)).fetch_events(None) == []
    assert _sender(_pb(None)).fetch_events(None) == []  # idempotent no-op


def test_send_invites_delegates_to_invite_launch_flow():
    pb = _pb(
        "query,error\n",
        log_output="Invitation sent to alice\nInvitation sent to bob",
    )  # NB CSV has no usable status; per-person log lines confirm delivery
    sender = _sender(pb)
    batch = [
        {"linkedInUrl": "https://linkedin.com/in/alice", "message": "hola"},
        {"linkedInUrl": "https://linkedin.com/in/bob", "message": "hola"},
    ]

    outcome = sender.send_invites(batch)

    assert isinstance(outcome, SendOutcome)
    pb.launch_agent.assert_called_once()
    agent_id, args = pb.launch_agent.call_args.args
    assert agent_id == "nb-id"
    assert args["spreadsheetUrl"] == "https://sheet/x"
    # batch + 1 sheet header line (PB counts the header as processable).
    assert args["numberOfAddsPerLaunch"] == 2
    assert args["sessionCookie"] == "c"
    assert outcome.drift_skipped_reason is None
    assert outcome.sent_count == 2


@pytest.mark.parametrize("count", [0, 10, 11])
def test_network_booster_add_cap_is_checked_before_sheet_write(count):
    pb = _pb("query,error\n")
    write_sheet = MagicMock(return_value="https://sheet/x")
    sender = PBSender(
        pb, network_booster_id="nb-id", write_sheet=write_sheet,
        session_args=lambda: {"sessionCookie": "c", "userAgent": "ua"},
    )
    rows = [
        {"linkedInUrl": f"https://linkedin.com/in/person{i}", "message": "hola"}
        for i in range(count)
    ]
    urls = {row["linkedInUrl"] for row in rows}
    if count != 10:
        with pytest.raises(ValueError, match="1-10 rows"):
            sender.launch_invite_batch(rows, urls)
        write_sheet.assert_not_called()
        pb.launch_agent.assert_not_called()
    else:
        sender.launch_invite_batch(rows, urls)
        write_sheet.assert_called_once_with(rows)
        args = pb.launch_agent.call_args.args[1]
        assert args["numberOfAddsPerLaunch"] == 10
        assert "numberOfProfilesPerLaunch" not in args


def test_send_dm_delegates_to_single_row_dm_launch():
    url = "https://linkedin.com/in/alice"
    pb = _pb(f"query,status\n{url},Message sent\n")
    sender = _sender(pb)

    outcome = sender.send_dm({"linkedInUrl": url, "name": "Alice"}, "hola Alice")

    assert isinstance(outcome, SendOutcome)
    pb.launch_agent.assert_called_once()
    agent_id, args = pb.launch_agent.call_args.args
    assert agent_id == "ms-id"
    assert args["message"] == "#message#"  # per-row from sheet column
    assert args["numberOfProfilesPerLaunch"] == 2  # 1 row + header
    assert outcome.csv_status == "Message sent"
    assert outcome.sent_count == 1


def test_launch_dm_batch_returns_full_pb_envelope():
    """The drain code needs launch + completion alongside the outcome
    (advance gate keys on the container id; queue rows embed the launch)."""
    url = "https://linkedin.com/in/alice"
    pb = _pb(f"query,status\n{url},Message sent\n")
    sender = _sender(pb)

    result = sender.launch_dm_batch(
        [{"linkedInUrl": url, "message": "hola"}],
        {"https://linkedin.com/in/alice"},
        step_label="dm1",
    )

    assert isinstance(result, PBSendResult)
    assert result.launch is pb.launch_agent.return_value
    assert result.completion is pb.wait_for_completion.return_value
    assert result.outcome.container_id == "cid-1"


@pytest.mark.parametrize("kind", ["invite", "dm"])
def test_send_launch_preserves_header_and_last_recipient(kind, monkeypatch):
    """The real sheet writer and PB launch cap must agree for send paths."""
    monkeypatch.setenv("GSHEET_AUTOCONNECT_ID", "sheet-id")
    ws = MagicMock()
    sh = MagicMock()
    sh.worksheet.return_value = ws
    gc = MagicMock()
    gc.open_by_key.return_value = sh
    pb = _pb(None)
    sender = PBSender(
        pb,
        network_booster_id="nb-id",
        message_sender_id="ms-id",
        write_sheet=write_prospects_to_sheet,
        session_args=lambda: {"sessionCookie": "c", "userAgent": "ua"},
    )
    rows = [
        {"linkedInUrl": "https://linkedin.com/in/alice", "message": "hola Alice"},
        {"linkedInUrl": "https://linkedin.com/in/bob", "message": "hola Bob"},
    ]

    with patch("clients.google_sheets.get_client", return_value=gc):
        if kind == "invite":
            sender.launch_invite_batch(rows, {row["linkedInUrl"] for row in rows})
        else:
            sender.launch_dm_batch(
                rows, {row["linkedInUrl"] for row in rows}, step_label="dm1"
            )

    assert ws.update.call_args.args[0] == [
        ["linkedInUrl", "message"],
        [rows[0]["linkedInUrl"], rows[0]["message"]],
        [rows[1]["linkedInUrl"], rows[1]["message"]],
    ]
    expected_limit = 2 if kind == "invite" else 3
    limit_key = "numberOfAddsPerLaunch" if kind == "invite" else "numberOfProfilesPerLaunch"
    assert pb.launch_agent.call_args.args[1][limit_key] == expected_limit


def test_missing_agent_id_raises_instead_of_launching():
    pb = _pb(None)
    sender = PBSender(pb, write_sheet=lambda rows: "https://sheet/x")
    with pytest.raises(ValueError, match="network_booster_id"):
        sender.launch_invite_batch([{"linkedInUrl": "https://x/in/a"}], set())
    with pytest.raises(ValueError, match="message_sender_id"):
        sender.launch_dm_batch(
            [{"linkedInUrl": "https://x/in/a"}], set(), step_label="dm1"
        )
    pb.launch_agent.assert_not_called()


def test_lead_event_is_frozen():
    ev = LeadEvent(
        event_type="invitation-accepted",
        lead_linkedin_url="https://linkedin.com/in/alice",
        occurred_at=None,
        raw={},
    )
    with pytest.raises(AttributeError):
        ev.event_type = "message-replied"  # type: ignore[misc]
