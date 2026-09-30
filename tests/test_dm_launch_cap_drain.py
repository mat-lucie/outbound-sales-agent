"""2026-06-10 cap-trickle fix: Message Sender per-launch cap resilience.

PB API launches that pass ``arguments`` REPLACE the phantom's saved console
argument, so the saved ``numberOfProfilesPerLaunch`` (30) never applied to
API launches — the phantom fell back to its built-in default (10) and a
17-row dm1 batch silently trickled at 10/launch on 2026-06-10.

These tests lock in the two-layer fix in ``run_dm_sequencing``:

1. ``numberOfProfilesPerLaunch=len(batch)+1 header line`` is passed explicitly on every
   launch.
2. If PB still truncates (the cap signature: a strict prefix of the sheet
   reported, the ENTIRE remainder unreported), the unprocessed tail is
   relaunched in the same run — fresh sheet write, fresh PR-17 lease per
   launch, sequential launches, bounded by ``MAX_DM_LAUNCHES_PER_STEP``.
   Mid-batch reporting holes do NOT relaunch (retry-tomorrow, §3.1).
"""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from models.pipeline import PipelineStage
from tests.test_integration import _attio_with_full_schema


def _url(i: int) -> str:
    return f"https://linkedin.com/in/alice{i}"


def _csv(sent_urls: list[str], skipped_urls: list[str] | None = None) -> str:
    """Minimal Message Sender result CSV (``query`` + ``status`` columns)."""
    lines = ["query,status"]
    lines += [f"{u},Message sent" for u in sent_urls]
    lines += [f"{u},Error: InMail required" for u in (skipped_urls or [])]
    return "\n".join(lines) + "\n"


class _LeaseRecorder:
    """Records reserve/confirm/release calls on the daily_run mock."""

    def __init__(self) -> None:
        self.ops: list[tuple[str, object]] = []
        self._n = 0

    def reserve(self, kind, count):
        self._n += 1
        self.ops.append(("reserve", count))
        return f"lease-{self._n}"

    def confirm(self, token, confirmed_count=None):
        self.ops.append(("confirm", confirmed_count))

    def release(self, token):
        self.ops.append(("release", token))


def _run(monkeypatch, n_rows: int, csv_per_launch: list[str | None],
         wait_side_effect: list | None = None,
         leases: _LeaseRecorder | None = None,
         dm2_ids: set[int] | None = None, dry_run: bool = False):
    """Drive run_dm_sequencing with ``n_rows`` due DM1s and a scripted PB.

    ``csv_per_launch[i]`` is the result CSV text the i-th launch returns
    (None = PB returned no CSV → advance gate fails for that launch).
    Returns (result, pb, lease_recorder, sheet_batches, audit_logger).
    """
    for key, value in {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake", "PB_LI_SESSION_COOKIE": "fake-cookie",
        "PB_LI_USER_AGENT": "TestAgent/1.0", "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
    }.items():
        monkeypatch.setenv(key, value)
    from workflows import daily_check

    last_contact = (date.today() - timedelta(days=3)).isoformat()
    entries = [
        {
            "record_id": f"rec{i}", "entry_id": f"ent{i}",
            "stage": PipelineStage.ACCEPTED.value,
            "last_contact_date": last_contact,
            "quality_score": 75, "persona": "operations_leaders",
            "language": "es", "invite_eligible_after": None,
            "dm_step": 0, "experiment_id": None,
            "experiment_id_frozen_at": None,
            "next_eligible_send_date": None,
        }
        for i in range(n_rows)
    ]

    for i in dm2_ids or set():
        entries[i].update(stage=PipelineStage.DM1_SENT.value, dm_step=1,
                          last_contact_date=(date.today() - timedelta(days=10)).isoformat())

    monkeypatch.setattr(daily_check, "escalate", MagicMock(return_value={"id": "r"}))
    monkeypatch.setattr(daily_check, "_get_all_entries_with_raw", lambda _: ([], entries))
    monkeypatch.setattr(daily_check, "can_send_messages", lambda n: True)
    monkeypatch.setattr(daily_check, "is_send_eligible", lambda attrs: True)
    monkeypatch.setattr(daily_check, "_is_blocked_by_stored_floor", lambda *a, **kw: False)
    monkeypatch.setattr(daily_check, "_check_company_throttle_or_skip", lambda *a, **kw: True)
    monkeypatch.setattr(daily_check, "_assert_no_unresolved_placeholders", MagicMock())
    monkeypatch.setattr(daily_check, "resolve_language", lambda *a, **kw: "es")
    monkeypatch.setattr(daily_check, "get_message", lambda *a, **kw: MagicMock())
    monkeypatch.setattr(daily_check, "get_industry_label", lambda *a, **kw: "Manufacturing")
    monkeypatch.setattr(daily_check, "personalize", lambda tpl, *a, **kw: "Hello Alice")
    monkeypatch.setattr(daily_check, "get_current_experiment_id", lambda: None)
    monkeypatch.setattr(daily_check, "_write_company_throttle_tally", MagicMock())
    monkeypatch.setattr(daily_check, "emit_pb_inmail_dead_end", MagicMock())
    monkeypatch.setattr(daily_check, "emit_pb_silent_no_op", MagicMock())
    monkeypatch.setattr(daily_check, "_attio_advance_with_escalation", lambda **kw: True)
    monkeypatch.setattr(daily_check, "_finalize_confirmed_dm_send", MagicMock(return_value=1))

    sheet_batches: list[list[str]] = []

    def _capture_sheet(rows):
        sheet_batches.append([r["linkedInUrl"] for r in rows])
        return "https://sheet/x"

    monkeypatch.setattr(daily_check, "write_prospects_to_sheet", _capture_sheet)
    monkeypatch.setattr(daily_check, "_pb_session_args", lambda: {})

    cache = MagicMock()
    cache.get.side_effect = lambda rid: (
        f"Alice{rid[3:]}", "Acme", _url(int(rid[3:])), "food", "VP",
    )

    pb = MagicMock()
    launch_n = {"n": 0}

    def _launch(agent_id, args):
        launch_n["n"] += 1
        return MagicMock(container_id=f"cid{launch_n['n']}")

    pb.launch_agent.side_effect = _launch
    pb.wait_for_completion.side_effect = (
        wait_side_effect
        if wait_side_effect is not None
        else [MagicMock(status="finished", log_output="") for _ in csv_per_launch]
    )
    pb.download_result_csv.side_effect = csv_per_launch

    leases = leases if leases is not None else _LeaseRecorder()
    from workflows.email_send_guard import GuardResult

    daily_run = MagicMock()
    daily_run.remaining.return_value = 30
    daily_run.get_reply_detection_status.return_value = "ok"
    daily_run.reserve_send.side_effect = leases.reserve
    daily_run.confirm_lease.side_effect = leases.confirm
    daily_run.release_lease.side_effect = leases.release

    audit_logger = MagicMock()
    with patch("workflows.daily_check.verify_send_preconditions", return_value=GuardResult(True)):
        result = daily_check.run_dm_sequencing(
            _attio_with_full_schema(), pb, "msg_sender_id", daily_run,
            dry_run=dry_run, auto_confirm=True, cache=cache,
            audit_logger=audit_logger,
        )
    return result, pb, leases, sheet_batches, audit_logger


def _audit_events(audit_logger, name: str) -> list:
    return [c for c in audit_logger.event.call_args_list if c.args[0] == name]


# ── layer 1: explicit per-launch count ──────────────────────────────────


def test_launch_args_carry_explicit_per_launch_count(monkeypatch):
    """Every launch must pass numberOfProfilesPerLaunch = len(batch) + 1
    sheet header line (PB counts the header as a processable line): PB launch
    `arguments` replace the saved console config, so omitting the key falls
    back to the phantom default (10) — the 2026-06-10 trickle — and a count
    of exactly len(batch) drops the last row (2026-06-12 incident)."""
    result, pb, _, _, _ = _run(
        monkeypatch, n_rows=17,
        csv_per_launch=[_csv([_url(i) for i in range(17)])],
    )

    assert pb.launch_agent.call_count == 1
    args = pb.launch_agent.call_args.args[1]
    # 17 data rows + 1 sheet header line — PB counts the header as a
    # processable line (2026-06-12 last-row-dropped incident).
    assert args["numberOfProfilesPerLaunch"] == 18
    assert result["dm1"] == 17
    assert result["dm1_queued"] == 17


# ── layer 2: truncation-tail drain ──────────────────────────────────────


def test_truncated_tail_relaunched_same_run(monkeypatch):
    """The 2026-06-10 incident shape: 17-row dm1 batch, PB processes only
    the first 10. The remaining 7 must be relaunched in the SAME run with
    a fresh sheet write and their own lease — not wait for tomorrow."""
    result, pb, leases, sheet_batches, audit_logger = _run(
        monkeypatch, n_rows=17,
        csv_per_launch=[
            _csv([_url(i) for i in range(10)]),       # launch 1: truncated at 10
            _csv([_url(i) for i in range(10, 17)]),   # launch 2: drains the tail
        ],
    )

    assert pb.launch_agent.call_count == 2
    first_args = pb.launch_agent.call_args_list[0].args[1]
    second_args = pb.launch_agent.call_args_list[1].args[1]
    # batch + 1 header line per launch (PB counts the sheet header as a
    # processable line; each relaunch rewrites the sheet, so each gets +1).
    assert first_args["numberOfProfilesPerLaunch"] == 18
    assert second_args["numberOfProfilesPerLaunch"] == 8

    # Sheet rewritten per launch with ONLY the unprocessed tail (§3.1: a
    # row PB already processed is never re-fed to the phantom).
    assert sheet_batches == [
        [_url(i) for i in range(17)],
        [_url(i) for i in range(10, 17)],
    ]

    # PR-17 lease per launch: reserve batch → confirm what PB confirmed.
    assert leases.ops == [
        ("reserve", 17), ("confirm", 10),
        ("reserve", 7), ("confirm", 7),
    ]

    assert result["dm1"] == 17
    assert result["dm1_queued"] == 17
    assert len(_audit_events(audit_logger, "pb_launch_cap_truncation_relaunch")) == 1
    assert _audit_events(audit_logger, "pb_url_unreported") == []


def test_midbatch_unreported_hole_is_not_relaunched(monkeypatch):
    """An unreported row FOLLOWED by reported rows is a reporting hole, not
    cap truncation — same-run relaunch must not fire (it could re-send a
    row PB processed but failed to record). Retry-tomorrow per §3.1."""
    sent = [_url(i) for i in range(7) if i != 4]  # alice4 is a mid-batch hole
    result, pb, leases, _, audit_logger = _run(
        monkeypatch, n_rows=7, csv_per_launch=[_csv(sent)],
    )

    assert pb.launch_agent.call_count == 1
    assert leases.ops == [("reserve", 7), ("confirm", 6)]
    assert result["dm1"] == 6
    unreported = _audit_events(audit_logger, "pb_url_unreported")
    assert len(unreported) == 1
    assert unreported[0].kwargs["linkedin_url"] == _url(4)
    assert _audit_events(audit_logger, "pb_launch_cap_truncation_relaunch") == []


def test_drain_bounded_by_max_launches_per_step(monkeypatch):
    """A phantom that truncates every launch must not loop forever: stop at
    MAX_DM_LAUNCHES_PER_STEP and route the remainder to retry-tomorrow."""
    from workflows.daily_check import MAX_DM_LAUNCHES_PER_STEP

    assert MAX_DM_LAUNCHES_PER_STEP == 3
    # Each launch processes only the FIRST row of its batch (worst case).
    result, pb, leases, _, audit_logger = _run(
        monkeypatch, n_rows=5,
        csv_per_launch=[_csv([_url(0)]), _csv([_url(1)]), _csv([_url(2)])],
    )

    assert pb.launch_agent.call_count == 3
    assert leases.ops == [
        ("reserve", 5), ("confirm", 1),
        ("reserve", 4), ("confirm", 1),
        ("reserve", 3), ("confirm", 1),
    ]
    assert result["dm1"] == 3
    # alice3 + alice4 fall back to the retry-tomorrow path.
    unreported = _audit_events(audit_logger, "pb_url_unreported")
    assert {c.kwargs["linkedin_url"] for c in unreported} == {_url(3), _url(4)}
    assert len(_audit_events(audit_logger, "pb_launch_cap_truncation_relaunch")) == 2


def test_gate_failure_on_relaunch_keeps_earlier_advances(monkeypatch):
    """If the tail relaunch fails the advance gate (e.g. PB returns no CSV),
    the rows already advanced by launch 1 stay counted; the remainder takes
    the pb_silent_no_op path with an explicit failure."""
    from workflows import daily_check

    result, pb, leases, _, _ = _run(
        monkeypatch, n_rows=17,
        csv_per_launch=[
            _csv([_url(i) for i in range(10)]),  # launch 1: truncated at 10
            None,                                # launch 2: no CSV → gate fails
        ],
    )

    assert pb.launch_agent.call_count == 2
    # Launch 2's lease confirms 0 (nothing of its batch confirmed sent).
    assert leases.ops == [
        ("reserve", 17), ("confirm", 10),
        ("reserve", 7), ("confirm", 0),
    ]
    assert daily_check.emit_pb_silent_no_op.call_count == 1
    assert result["dm1"] == 10
    assert result["dm1_queued"] == 17
    assert result["failed_batches"][0]["requested"] == 7


def test_stale_csv_on_relaunch_fails_gate_not_soft_path(monkeypatch):
    """Review F1: the tail relaunch reuses the same agent + csvName, so a
    no-op relaunch can return a CSV still carrying launch 1's sent rows —
    nominally satisfying the raw advance gate (sent_count >= 1) while ZERO
    of this launch's own batch was confirmed. That is a batch-level no-op
    and must take the pb_silent_no_op path, not the soft audit-only
    pb_url_unreported path."""
    from workflows import daily_check

    stale_csv = _csv([_url(i) for i in range(10)])  # launch 1's rows only
    result, pb, leases, _, audit_logger = _run(
        monkeypatch, n_rows=17,
        csv_per_launch=[
            stale_csv,   # launch 1: truncated at 10 → relaunch tail
            stale_csv,   # launch 2: no-op, CSV is launch 1 leftovers
        ],
    )

    assert pb.launch_agent.call_count == 2
    assert leases.ops == [
        ("reserve", 17), ("confirm", 10),
        ("reserve", 7), ("confirm", 0),
    ]
    assert daily_check.emit_pb_silent_no_op.call_count == 1
    assert result["dm1"] == 10
    assert result["failed_batches"][0]["requested"] == 7
    # The remainder is covered by the queue row, not double-reported as
    # per-URL unreported audit events.
    assert _audit_events(audit_logger, "pb_url_unreported") == []


def test_gate_failure_on_first_launch_matches_legacy_behavior(monkeypatch):
    """Parity with the pre-drain-loop code: a first-launch batch-level PB
    no-op emits pb_silent_no_op + failed_batches and reports nothing sent."""
    from workflows import daily_check

    result, pb, leases, _, _ = _run(
        monkeypatch, n_rows=5, csv_per_launch=[None],
    )

    assert pb.launch_agent.call_count == 1
    assert leases.ops == [("reserve", 5), ("confirm", 0)]
    assert daily_check.emit_pb_silent_no_op.call_count == 1
    assert result["dm1"] == 0
    assert result["failed_batches"][0]["requested"] == 5
    assert "dm1_queued" not in result


def test_relaunch_exception_releases_only_its_own_lease(monkeypatch):
    """PR-17 lease lifecycle across drain launches: a wait_for_completion
    failure on the RELAUNCH releases that launch's lease (refund) while
    launch 1's confirmed lease stays committed; the error propagates."""
    completion = MagicMock(status="finished", log_output="")
    leases = _LeaseRecorder()
    with pytest.raises(RuntimeError, match="pb timeout"):
        _run(
            monkeypatch, n_rows=17,
            csv_per_launch=[
                _csv([_url(i) for i in range(10)]),
                _csv([_url(i) for i in range(10, 17)]),
            ],
            wait_side_effect=[completion, RuntimeError("pb timeout")],
            leases=leases,
        )

    assert leases.ops == [
        ("reserve", 17), ("confirm", 10),
        ("reserve", 7), ("release", "lease-2"),
    ]

@pytest.mark.parametrize("prior_sent", [0, 10])
def test_failed_approved_batch_is_not_an_intentional_skip(monkeypatch, prior_sent):
    from workflows import daily_check

    csvs = ([_csv([_url(i) for i in range(prior_sent)])] if prior_sent else []) + [None]
    result, pb, leases, _, _ = _run(
        monkeypatch, n_rows=prior_sent + 5, csv_per_launch=csvs,
        wait_side_effect=[MagicMock(status="finished", log_output="Can't connect to profile. Process exited with code 1") for _ in csvs],
    )
    assert result["dm1"] == prior_sent
    assert result["failed_batches"] == [{
        "step": "dm1", "container_id": f"cid{len(csvs)}",
        "requested": 5, "confirmed_sent": 0, "reason": "advance_gate_failed",
    }]
    assert "dm1" not in result.get("dry_skipped", {})
    assert daily_check._finalize_confirmed_dm_send.call_count == prior_sent
    assert pb.launch_agent.call_count == len(csvs)
    assert leases.ops[-1] == ("confirm", 0)
    assert daily_check.emit_pb_silent_no_op.call_count == 1


def test_successful_dm1_then_failed_dm2_preserves_confirmed_stage_and_count(monkeypatch):
    from workflows import daily_check

    result, pb, leases, _, _ = _run(
        monkeypatch, n_rows=3, dm2_ids={2},
        csv_per_launch=[_csv([_url(0), _url(1)]), None],
    )
    assert result["dm1"] == 2
    assert result["dm2"] == 0
    assert result["failed_batches"][0]["step"] == "dm2"
    assert daily_check._finalize_confirmed_dm_send.call_count == 2
    assert pb.launch_agent.call_count == 2
    assert leases.ops == [("reserve", 2), ("confirm", 2), ("reserve", 1), ("confirm", 0)]


@pytest.mark.parametrize("n_rows,dry_run", [(0, False), (3, True), (3, False)])
def test_no_failed_batches_for_empty_dry_run_or_success(monkeypatch, n_rows, dry_run):
    result, pb, leases, _, _ = _run(
        monkeypatch, n_rows=n_rows, dry_run=dry_run,
        csv_per_launch=[_csv([_url(i) for i in range(n_rows)])],
    )
    assert not result.get("failed_batches")
    if dry_run or not n_rows:
        pb.launch_agent.assert_not_called()
        assert not leases.ops
    else:
        assert result["dm1"] == n_rows


@pytest.fixture(autouse=True)
def _fresh_stage_unit_boundary():
    from workflows.email_send_guard import GuardResult
    with patch("workflows.daily_check.verify_send_preconditions", return_value=GuardResult(True)), patch("workflows.dm_quality_gate.require_clear_dm_quality_queue"):
        yield
