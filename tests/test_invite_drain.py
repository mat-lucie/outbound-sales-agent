"""Daily invite target drains through bounded Network Booster containers."""

from unittest.mock import MagicMock, patch

import pytest

from workflows.daily_check import (
    _hold_invite_batch_before_launch,
    drain_connection_invites,
)


def test_twenty_five_new_invites_use_three_disjoint_containers():
    daily_run = MagicMock()
    daily_run.remaining.return_value = 25
    calls = []

    def run_one(target, us_cap, excluded, companies, urls):
        calls.append((target, us_cap, set(excluded)))
        index = len(calls)
        count = min(target, 10)
        return {
            "sent": count, "pb_queued": count, "attio_updated": count,
            "sent_us_mode": min(us_cap or 0, count),
            "attempted_entry_ids": [f"batch{index}-row{i}" for i in range(count)],
        }

    result = drain_connection_invites(
        run_one, batch_size=25, daily_run=daily_run,
        us_mode_daily_cap=10,
    )
    assert [target for target, _, _ in calls] == [25, 15, 5]
    assert [us_cap for _, us_cap, _ in calls] == [10, 0, 0]
    assert len(calls[1][2]) == 10
    assert len(calls[2][2]) == 20
    assert result["sent"] == 25
    assert result["pb_queued"] == 25
    assert result["sent_us_mode"] == 10


def test_pending_rows_do_not_consume_daily_send_target_or_repeat():
    daily_run = MagicMock()
    daily_run.remaining.return_value = 25
    calls = []

    def run_one(target, us_cap, excluded, companies, urls):
        calls.append((target, set(excluded)))
        if len(calls) == 1:
            return {"sent": 0, "pb_queued": 10, "attio_updated": 10,
                    "attempted_entry_ids": [f"pending-{i}" for i in range(10)]}
        return {"sent": 5, "pb_queued": 5, "attio_updated": 5,
                "attempted_entry_ids": [f"new-{i}" for i in range(5)]}

    result = drain_connection_invites(
        run_one, batch_size=25, daily_run=daily_run,
        us_mode_daily_cap=None,
    )
    assert calls[1] == (25, {f"pending-{i}" for i in range(10)})
    assert result["sent"] == 20
    assert result["pb_queued"] == 30


def test_ambiguous_provider_result_stops_drain():
    daily_run = MagicMock()
    daily_run.remaining.return_value = 25
    calls = []

    def run_one(target, us_cap, excluded, companies, urls):
        calls.append(target)
        return {"sent": 2, "pb_queued": 10, "unconfirmed": 8,
                "attempted_entry_ids": ["held"]}

    result = drain_connection_invites(
        run_one, batch_size=25, daily_run=daily_run, us_mode_daily_cap=None,
    )
    assert calls == [25]
    assert result["sent"] == 2


def test_failed_second_prelaunch_hold_rolls_back_first():
    writer = MagicMock()
    writer.apply.side_effect = [None, RuntimeError("second hold failed"), None]
    rows = [
        {"entry_id": "entry-1", "record_id": "person-1",
         "linkedInUrl": "https://linkedin.com/in/person-1",
         "invite_eligible_after": "2026-09-20"},
        {"entry_id": "entry-2", "record_id": "person-2",
         "linkedInUrl": "https://linkedin.com/in/person-2",
         "invite_eligible_after": "2026-09-20"},
    ]
    with patch("clients.attio_writer.AttioWriter", return_value=writer), \
         pytest.raises(RuntimeError, match="second hold failed"):
        _hold_invite_batch_before_launch(
            rows, attio=MagicMock(), list_id="list-1",
        )
    assert writer.apply.call_count == 3
    first, second, rollback = [c.args[0] for c in writer.apply.call_args_list]
    assert first.record_id == "entry-1"
    assert second.record_id == "entry-2"
    assert rollback.record_id == "entry-1"
    assert rollback.updates == {"invite_eligible_after": "2026-09-20"}
