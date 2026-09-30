"""PR-17 B-SD-001: daily_run_collision escalation + exit EX_TEMPFAIL.

When ``open_daily_run`` raises ``ConcurrentRunInAttio`` (F-PR-8's
uniqueness collision on ``(run_date, machine_id)``), the daily CLI must:
1. Open a ``daily_run_collision`` Operator Review Queue row (idempotent
   on ``f"{run_date}|{machine_id}"`` — re-running the colliding command
   shouldn't open N queue rows).
2. Print an operator-visible breadcrumb.
3. Exit with code 75 (EX_TEMPFAIL) so a wrapping retry/launchd backs off
   without overwriting state.

Also locks in the lease-lifecycle invariants: a reservation held when
``pb.launch_agent`` or ``pb.wait_for_completion`` raises is released —
no quota gets leaked when a batch fails before PB confirmed any send.
"""
from __future__ import annotations

import os
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tests.test_integration import _attio_with_full_schema
from workflows.daily_run import ConcurrentRunInAttio, DailyRun

# ── lease lifecycle on pb.launch_agent failure ──────────────────────────


@patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
                          "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0",
                          "GSHEET_AUTOCONNECT_ID": "fake-sheet-id"})
@patch("workflows.daily_check._get_all_entries_with_raw")
@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet/x")
@patch("workflows.daily_check._pb_session_args", return_value={})
def test_lease_released_when_pb_launch_agent_raises(
    _session, _sheet, _entries,
):
    """Reserve N → pb.launch_agent raises → lease released → capacity
    refunded to the daily_run row. The §3.1 invariant: quota consumed
    must equal sends actually executed (here, zero)."""
    from workflows.daily_check import run_dm_sequencing

    today = date(2026, 5, 20)
    _entries.return_value = ([], [
        {
            "record_id": "a1", "entry_id": "ent-a1",
            "stage": "Accepted", "last_contact_date": "2026-05-18",
            "dm_step": 0, "persona": "operations_leaders", "language": "en",
        }
    ])

    attio = _attio_with_full_schema()
    cache = MagicMock()
    cache.get.side_effect = lambda rid: (
        f"Name-{rid}", f"Company-{rid}", f"https://linkedin.com/in/{rid}",
        "manufacturing", "Plant Director",
    )

    pb = MagicMock()
    pb.launch_agent.side_effect = RuntimeError("phantombuster 502")

    daily_run = MagicMock(spec=DailyRun)
    daily_run.remaining.return_value = 30
    captured_leases: list[tuple[str, object]] = []

    def _reserve(kind, count):
        token = f"lease-{kind}-{count}"
        captured_leases.append(("reserve", count))
        return token

    def _release(token):
        captured_leases.append(("release", token))

    def _confirm(token, confirmed_count=None):
        captured_leases.append(("confirm", confirmed_count))

    daily_run.reserve_send.side_effect = _reserve
    daily_run.release_lease.side_effect = _release
    daily_run.confirm_lease.side_effect = _confirm

    with patch("workflows.daily_check.date") as mock_date:
        mock_date.today.return_value = today
        mock_date.fromisoformat = date.fromisoformat
        with pytest.raises(RuntimeError, match="phantombuster 502"):
            run_dm_sequencing(
                attio, pb, "sender-id",
                dry_run=False, auto_confirm=True,
                cache=cache, daily_run=daily_run,
            )

    actions = [act for act, _ in captured_leases]
    assert "reserve" in actions
    assert "release" in actions
    assert "confirm" not in actions


@patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
                          "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0",
                          "GSHEET_AUTOCONNECT_ID": "fake-sheet-id"})
@patch("workflows.daily_check._get_all_entries_with_raw")
@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet/x")
@patch("workflows.daily_check._pb_session_args", return_value={})
def test_lease_released_when_pb_wait_for_completion_raises(
    _session, _sheet, _entries,
):
    """PR-17 fold-in (cross-agent BLOCKING from salesman-daily + engineer +
    silent-failure-hunter): the try/finally must span from reserve through
    confirm — a PBRunFailed / PBRunTimeout / network error from
    pb.wait_for_completion (NOT pb.launch_agent) after a successful launch
    must still release the lease so capacity refunds.

    Without this guard, a wait_for_completion timeout (max_wait=1800s, a
    real PB failure mode) would leave the reservation permanently held for
    the rest of the run — silencing DM2/DM3 with §3.1 quota leak.
    """
    from workflows.daily_check import run_dm_sequencing

    today = date(2026, 5, 20)
    _entries.return_value = ([], [
        {
            "record_id": "a1", "entry_id": "ent-a1",
            "stage": "Accepted", "last_contact_date": "2026-05-18",
            "dm_step": 0, "persona": "operations_leaders", "language": "en",
        }
    ])

    attio = _attio_with_full_schema()
    cache = MagicMock()
    cache.get.side_effect = lambda rid: (
        f"Name-{rid}", f"Company-{rid}", f"https://linkedin.com/in/{rid}",
        "manufacturing", "Plant Director",
    )

    # launch_agent succeeds, but wait_for_completion times out.
    pb = MagicMock()
    pb.launch_agent.return_value = MagicMock(container_id="c_test")
    pb.wait_for_completion.side_effect = TimeoutError("PB run exceeded max_wait=1800s")

    daily_run = MagicMock(spec=DailyRun)
    daily_run.remaining.return_value = 30
    captured: list[tuple[str, object]] = []

    def _reserve(kind, count):
        captured.append(("reserve", count))
        return f"lease-{count}"

    def _release(token):
        captured.append(("release", token))

    def _confirm(token, confirmed_count=None):
        captured.append(("confirm", confirmed_count))

    daily_run.reserve_send.side_effect = _reserve
    daily_run.release_lease.side_effect = _release
    daily_run.confirm_lease.side_effect = _confirm

    with patch("workflows.daily_check.date") as mock_date:
        mock_date.today.return_value = today
        mock_date.fromisoformat = date.fromisoformat
        with pytest.raises(TimeoutError, match="exceeded max_wait"):
            run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=daily_run,
                dry_run=False, auto_confirm=True, cache=cache,
            )

    actions = [act for act, _ in captured]
    assert "reserve" in actions
    assert "release" in actions, (
        f"lease NOT released after wait_for_completion failure — actions: {captured}. "
        f"This is the §3.1 quota-leak the try/finally fold-in must prevent."
    )
    assert "confirm" not in actions


@patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
                          "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0",
                          "GSHEET_AUTOCONNECT_ID": "fake-sheet-id"})
@patch("workflows.daily_check._get_all_entries_with_raw")
@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet/x")
@patch("workflows.daily_check._pb_session_args", return_value={})
def test_dm_confirm_failure_after_send_echoes_uncharged(
    _session, _sheet, _entries, capsys,
):
    """C-fix: PB Message Sender launch + wait_for_completion succeed (DMs
    physically sent), but the post-send confirm_lease PATCH raises. The lease
    refunds in `finally` so the cap is uncharged for real sends — a loud
    operator ERROR echo must name the physically-sent-but-uncharged DMs, and
    the exception must propagate."""
    from tests.test_integration import _typed_pb_mock
    from workflows.daily_check import run_dm_sequencing

    today = date(2026, 5, 20)
    _entries.return_value = ([], [
        {
            "record_id": "a1", "entry_id": "ent-a1",
            "stage": "Accepted", "last_contact_date": "2026-05-18",
            "dm_step": 0, "persona": "operations_leaders", "language": "en",
        }
    ])

    attio = _attio_with_full_schema()
    cache = MagicMock()
    cache.get.side_effect = lambda rid: (
        f"Name-{rid}", f"Company-{rid}", "https://linkedin.com/in/a1",
        "manufacturing", "Plant Director",
    )

    # Clean launch whose CSV confirms the DM was sent → gate would pass.
    pb = _typed_pb_mock(
        csv_text="query,status\nhttps://linkedin.com/in/a1,Message sent\n",
        container_id="c_dm_confirm",
    )

    daily_run = MagicMock(spec=DailyRun)
    daily_run.remaining.return_value = 30
    captured: list[tuple[str, object]] = []

    def _reserve(kind, count):
        captured.append(("reserve", count))
        return f"lease-{count}"

    def _release(token):
        captured.append(("release", token))

    def _confirm(token, confirmed_count=None):
        captured.append(("confirm", confirmed_count))
        raise ValueError("confirm PATCH parse error")

    daily_run.reserve_send.side_effect = _reserve
    daily_run.release_lease.side_effect = _release
    daily_run.confirm_lease.side_effect = _confirm

    with patch("workflows.daily_check.date") as mock_date:
        mock_date.today.return_value = today
        mock_date.fromisoformat = date.fromisoformat
        with pytest.raises(ValueError, match="confirm PATCH parse error"):
            run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=daily_run,
                dry_run=False, auto_confirm=True, cache=cache,
            )

    err = capsys.readouterr().err
    assert "PHYSICALLY SENT" in err, f"Expected post-send echo in stderr, got:\n{err}"
    assert "UNCHARGED" in err, f"Expected 'UNCHARGED' in stderr, got:\n{err}"
    # Lease refunded (released) after the confirm failure.
    actions = [act for act, _ in captured]
    assert "release" in actions


# ── daily_run_collision escalation on CLI ────────────────────────────────


@patch.dict("os.environ", {
    "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
    "ATTIO_LIST_ID": "list-001",
})
def test_cli_daily_emits_daily_run_collision_and_exits_75():
    """CLI daily command catches ``ConcurrentRunInAttio``, opens a
    ``daily_run_collision`` operator queue row, exits 75."""
    runner = CliRunner()

    with patch("workflows.run_lock.acquire_run_lock") as mock_lock, \
         patch("workflows.audit.AuditLogger") as mock_audit, \
         patch("clients.attio.AttioClient") as mock_attio_cls, \
         patch("clients.phantombuster.PhantomBusterClient") as mock_pb_cls, \
         patch("workflows.daily_run.open_daily_run") as mock_open_dr, \
         patch("workflows.escalation.escalate") as mock_escalate, \
         patch("workflows.record_cache.preload_pipeline_persons", return_value=0), \
         patch("workflows.record_cache.RecordCache"):
        mock_lock.return_value.__enter__ = lambda self: None
        mock_lock.return_value.__exit__ = lambda self, *a: None
        mock_audit.return_value.__enter__ = lambda self: MagicMock()
        mock_audit.return_value.__exit__ = lambda self, *a: None

        attio_instance = MagicMock()
        attio_instance.query_list_entries.return_value = []
        mock_attio_cls.return_value.query_list_entries.return_value = []
        mock_attio_cls.return_value.__enter__ = lambda self: attio_instance
        mock_attio_cls.return_value.__exit__ = lambda self, *a: None
        mock_pb_cls.return_value.__enter__ = lambda self: MagicMock()
        mock_pb_cls.return_value.__exit__ = lambda self, *a: None

        collision = ConcurrentRunInAttio(
            run_date="2026-05-21",
            machine_id="host-A",
            existing={"hostname": "host-A", "started_at": "2026-05-21T00:00:00Z", "run_id": "prior"},
        )
        cm = MagicMock()
        cm.__enter__.side_effect = collision
        cm.__exit__.return_value = False
        mock_open_dr.return_value = cm

        from cli import cli
        result = runner.invoke(cli, ["daily", "--yes"], catch_exceptions=False)

    assert result.exit_code == 75, (
        f"expected exit 75 (EX_TEMPFAIL), got {result.exit_code}; "
        f"output={result.output!r}"
    )
    # PR-17 fold-in (engineer-QA IMPORTANT): idempotency key must use
    # exc.run_date (the string Attio's uniqueness_key was opened with),
    # NOT today.isoformat() — guards against the microsecond midnight
    # skew between cli.py's `today = date.today()` and the collision
    # catch.
    assert mock_escalate.called, "escalate() was not called"
    call_kwargs = mock_escalate.call_args.kwargs
    assert call_kwargs["type"] == "daily_run_collision"
    assert call_kwargs["idempotency_key"] == "2026-05-21|host-A"
    payload = call_kwargs["payload"]
    assert payload["run_date"] == "2026-05-21"
    assert payload["machine_id"] == "host-A"
    assert payload["existing"]["hostname"] == "host-A"


@patch.dict("os.environ", {
    "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
    "ATTIO_LIST_ID": "list-001",
})
def test_cli_daily_malformed_prior_row_exits_75():
    """CLI daily catches ``MalformedDailyRunRow`` — raisable from
    open_daily_run's pre-open same-day scan when a prior duplicate row has
    a corrupt counter (multi-row incident guard) — and exits EX_TEMPFAIL
    (75) so launchd keeps its retry/backoff semantics, instead of dying
    with a raw traceback and exit 1."""
    from workflows.daily_run import MalformedDailyRunRow

    runner = CliRunner()

    with patch("workflows.run_lock.acquire_run_lock") as mock_lock, \
         patch("workflows.audit.AuditLogger") as mock_audit, \
         patch("clients.attio.AttioClient") as mock_attio_cls, \
         patch("clients.phantombuster.PhantomBusterClient") as mock_pb_cls, \
         patch("workflows.daily_run.open_daily_run") as mock_open_dr, \
         patch("workflows.record_cache.preload_pipeline_persons", return_value=0), \
         patch("workflows.record_cache.RecordCache"):
        mock_lock.return_value.__enter__ = lambda self: None
        mock_lock.return_value.__exit__ = lambda self, *a: None
        mock_audit.return_value.__enter__ = lambda self: MagicMock()
        mock_audit.return_value.__exit__ = lambda self, *a: None

        attio_instance = MagicMock()
        attio_instance.query_list_entries.return_value = []
        mock_attio_cls.return_value.query_list_entries.return_value = []
        mock_attio_cls.return_value.__enter__ = lambda self: attio_instance
        mock_attio_cls.return_value.__exit__ = lambda self, *a: None
        mock_pb_cls.return_value.__enter__ = lambda self: MagicMock()
        mock_pb_cls.return_value.__exit__ = lambda self, *a: None

        cm = MagicMock()
        cm.__enter__.side_effect = MalformedDailyRunRow("messages_sent", "rec_bad")
        cm.__exit__.return_value = False
        mock_open_dr.return_value = cm

        from cli import cli
        result = runner.invoke(cli, ["daily", "--yes"], catch_exceptions=False)

    assert result.exit_code == 75, (
        f"expected exit 75 (EX_TEMPFAIL), got {result.exit_code}; "
        f"output={result.output!r}"
    )
    assert "REFUSE" in result.output


@patch.dict("os.environ", {
    "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
    "ATTIO_LIST_ID": "list-001",
})
def test_cli_daily_dry_run_skips_daily_run_open():
    """Dry-run mode should not touch Attio's daily_run uniqueness key —
    the dry-run is purely exploratory and burning a uniqueness slot
    would block the real same-day invocation."""
    runner = CliRunner()

    with patch("workflows.run_lock.acquire_run_lock") as mock_lock, \
         patch("workflows.audit.AuditLogger") as mock_audit, \
         patch("workflows.daily_preview.ReadOnlyPipelineClient") as mock_attio_cls, \
         patch("clients.phantombuster.PhantomBusterClient") as mock_pb_cls, \
         patch("workflows.daily_run.open_daily_run") as mock_open_dr, \
         patch("workflows.record_cache.preload_pipeline_persons", return_value=0), \
         patch("workflows.record_cache.RecordCache"):
        mock_lock.return_value.__enter__ = lambda self: None
        mock_lock.return_value.__exit__ = lambda self, *a: None
        mock_audit.return_value.__enter__ = lambda self: MagicMock()
        mock_audit.return_value.__exit__ = lambda self, *a: None
        attio_instance = MagicMock()
        attio_instance.query_list_entries.return_value = []
        mock_attio_cls.return_value.query_list_entries.return_value = []
        mock_attio_cls.return_value.__enter__ = lambda self: attio_instance
        mock_attio_cls.return_value.__exit__ = lambda self, *a: None
        mock_pb_cls.return_value.__enter__ = lambda self: MagicMock()
        mock_pb_cls.return_value.__exit__ = lambda self, *a: None

        from cli import cli
        result = runner.invoke(cli, ["daily", "--dry-run", "--yes"], catch_exceptions=False)

    assert not mock_open_dr.called, (
        "dry-run consumed a daily_run uniqueness slot — that's the bug "
        "this test exists to prevent."
    )
    assert result.exit_code == 0, result.output


@patch.dict("os.environ", {
    "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
    "ATTIO_LIST_ID": "list-001",
})
def test_cli_daily_still_exits_75_when_escalate_itself_fails():
    """PR-17 fold-in (silent-failure-hunter IMPORTANT): if escalate()
    raises (Attio 5xx, schema regression, missing creds), the collision
    must still propagate as EX_TEMPFAIL=75 so a wrapping launchd backs
    off. Without the try/except wrapper around escalate, the original
    collision would land as a raw traceback with exit 1, breaking the
    retry-on-EX_TEMPFAIL invariant.
    """
    runner = CliRunner()

    with patch("workflows.run_lock.acquire_run_lock") as mock_lock, \
         patch("workflows.audit.AuditLogger") as mock_audit, \
         patch("clients.attio.AttioClient") as mock_attio_cls, \
         patch("clients.phantombuster.PhantomBusterClient") as mock_pb_cls, \
         patch("workflows.daily_run.open_daily_run") as mock_open_dr, \
         patch("workflows.escalation.escalate") as mock_escalate, \
         patch("workflows.record_cache.preload_pipeline_persons", return_value=0), \
         patch("workflows.record_cache.RecordCache"):
        mock_lock.return_value.__enter__ = lambda self: None
        mock_lock.return_value.__exit__ = lambda self, *a: None
        mock_audit.return_value.__enter__ = lambda self: MagicMock()
        mock_audit.return_value.__exit__ = lambda self, *a: None

        attio_instance = MagicMock()
        attio_instance.query_list_entries.return_value = []
        mock_attio_cls.return_value.query_list_entries.return_value = []
        mock_attio_cls.return_value.__enter__ = lambda self: attio_instance
        mock_attio_cls.return_value.__exit__ = lambda self, *a: None
        mock_pb_cls.return_value.__enter__ = lambda self: MagicMock()
        mock_pb_cls.return_value.__exit__ = lambda self, *a: None

        collision = ConcurrentRunInAttio(
            run_date="2026-05-21",
            machine_id="host-B",
            existing=None,
        )
        cm = MagicMock()
        cm.__enter__.side_effect = collision
        cm.__exit__.return_value = False
        mock_open_dr.return_value = cm

        mock_escalate.side_effect = RuntimeError("Attio 503 during escalation")

        from cli import cli
        result = runner.invoke(cli, ["daily", "--yes"], catch_exceptions=False)

    assert result.exit_code == 75, (
        f"expected exit 75 (EX_TEMPFAIL) even when escalate() fails, got "
        f"{result.exit_code}; output={result.output!r}"
    )


@pytest.fixture(autouse=True)
def _fresh_state_boundary():
    from unittest.mock import patch

    from workflows.email_send_guard import GuardResult
    with patch("workflows.daily_check.verify_send_preconditions", return_value=GuardResult(True)), patch("workflows.email_campaign.verify_email_send_preconditions", return_value=GuardResult(True)), patch("workflows.dm_quality_gate.require_clear_dm_quality_queue"):
        yield
