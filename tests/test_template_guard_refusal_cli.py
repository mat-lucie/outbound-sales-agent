"""CLI-level curated refusal for the pre-send template guards.

2026-08-24 review of PR #286, Finding 1: ``UnresolvedPlaceholderError`` /
``BlankMessageError`` raised by the pre-send guards inside
``run_connection_requests`` / ``run_dm_sequencing`` used to escape the
``daily`` and ``send-dms`` commands as raw tracebacks with a generic
exit 1 — skipping the run-end summary write, the "Daily Check FAILED"
rollup (including Part A realized send counts), and Phase C, after
Part A invites had already gone out.

Contract under test:
- both commands convert the guard raise into the curated ``⚠ REFUSE``
  treatment and exit EX_CONFIG (78);
- the daily command still emits the rollup / run-end summary / Phase C
  for the parts that DID run, and the rollup reports the DM counts that
  actually shipped before the halt (via the exception's
  ``partial_results``), never a fabricated zero;
- the daily_run row still closes as ``failed`` (the ORIGINAL exception —
  not a SystemExit — traverses the open_daily_run context manager);
- a Part A guard halt skips Part B (a systemic template break must not
  keep sending on the other lane of the same content pipeline);
- a transient failure inside the run-end summary block must not discard
  the guard halt (the curated refusal still wins the exit).
"""

from __future__ import annotations

import contextlib
import os
from datetime import date
from unittest.mock import MagicMock

from click.testing import CliRunner

from cli import EXIT_TEMPLATE_REFUSE, cli
from tests.test_send_dms_cli import (
    _FakeRun,
    _install_attach,
    _patch_clients_and_lock,
)
from workflows.daily_check import BlankMessageError, UnresolvedPlaceholderError

# Overlay env: Part A enabled (network booster), Part B enabled (message
# sender), every side phase pinned off so the run reaches Part A/B without
# live services. BOTDOG_SEND_ENABLED="" keeps Phase 0.7 off even when the
# developer's .env sets it.
_ENV = {
    "ATTIO_LIST_ID": "lst",
    "PB_NETWORK_BOOSTER_ID": "nb",
    "PB_MESSAGE_SENDER_ID": "ms",
    "PB_INBOX_SCRAPER_ID": "",
    "PB_PROFILE_SCRAPER_ID": "",
    "PB_SALES_NAV_PROFILE_SCRAPER_ID": "",
    "PRE_INVITE_DEGREE_CHECK_BACKEND": "regular",
    "DISABLE_EMAIL_RESPONSE_DETECTION": "1",
    "BOTDOG_SEND_ENABLED": "",
}


def _patch_daily_harness(monkeypatch):
    """Client/lock/pre-flight stubs for the daily command: the shared
    send-dms harness plus the daily-only phases."""
    _patch_clients_and_lock(monkeypatch)
    # 2026-06-09 is a Tuesday — keeps Part B off the weekend-skip branch.
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    # The RC3 staleness gate would refuse a wet run from any dev checkout,
    # so it returns a benign provenance here.
    monkeypatch.setattr(
        "workflows.run_provenance.assert_checkout_current",
        lambda **_k: {
            "branch": "test", "sha": "abc1234",
            "dirty": False, "behind_origin_main": False,
        },
    )
    monkeypatch.setattr(
        "workflows.pb_send_recovery.recover_unrecorded_dm_sends",
        lambda *a, **k: {"recovered": 0, "skipped_write_failed": 0},
    )
    monkeypatch.setattr(
        "workflows.starvation.evaluate_pipeline_starvation",
        lambda *a, **k: {
            "triggers_fired": [],
            "invite_eligible_pool": 9,
            "runway_bdays_remaining": 9,
        },
    )
    monkeypatch.setattr(
        "workflows.followup_radar.run_followup_radar",
        lambda *a, **k: {
            "digest": "  (radar stub)", "surfaced": 0,
            "partner": 0, "owed": 0, "waiting": 0,
            "linkedin_warm": 0, "nudge": 0,
        },
    )
    # These tests now exercise the live branch with all sends mocked. A fake
    # daily_run keeps the harness offline before the template guard fires.
    _wet_harness_with_failed_row_capture(monkeypatch, {})


def _combined(result) -> str:
    return (result.output or "") + (result.stderr or "")


def test_successful_daily_cohort_uses_raw_attio_boundary(monkeypatch):
    from clients.attio import AttioClient
    from tests.test_integration import _make_attio_entry
    from workflows.daily_check import compute_dm1_sent_cohort_by_date

    _patch_daily_harness(monkeypatch)
    monkeypatch.setattr("workflows.daily_check.run_connection_requests", lambda *a, **k: {"sent": 0})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", lambda *a, **k: {"dm1": 0, "dm2": 0, "dm3": 0})
    counts = {"prospect_pool_size": 1, "due_dm1_count": 0, "due_dm2_count": 0, "due_dm3_count": 0}
    monkeypatch.setattr("workflows.daily_check.compute_due_dm_counts", lambda *a, **k: counts)
    monkeypatch.setattr("workflows.daily_check.run_end_summary", lambda *a, **k: {**counts, "degree_unknown_count": 0, "starvation_signal": "healthy"})
    entry = _make_attio_entry("cohort-e", "cohort-p", "DM1 Sent", last_contact_date="2026-06-09")
    monkeypatch.setattr("clients.attio.AttioClient.query_list_entries", lambda self, **k: [entry])
    observed = []

    def cohort(client, **kwargs):
        assert isinstance(client, AttioClient)
        value = compute_dm1_sent_cohort_by_date(client, **kwargs)
        observed.append(value)
        return value

    monkeypatch.setattr("workflows.daily_check.compute_dm1_sent_cohort_by_date", cohort)
    result = CliRunner().invoke(cli, ["daily", "--yes"], env={**os.environ, **_ENV})
    assert result.exit_code == 0, result.output + repr(result.exception)
    assert observed and ("2026-06-09", 1) in observed[0]


def test_daily_dm_guard_halt_curated_refusal_keeps_rollup(monkeypatch):
    """Part B guard raise → ⚠ REFUSE + exit 75, with the Part A rollup and
    Phase C still emitted (invites already went out by then)."""
    _patch_daily_harness(monkeypatch)
    monkeypatch.setattr(
        "workflows.daily_check.run_connection_requests",
        lambda *a, **k: {"sent": 3},
    )
    seq = MagicMock(
        side_effect=BlankMessageError(
            "Refusing to send dm2: 5/5 rendered message(s) are blank."
        )
    )
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli, ["daily", "--yes"], env={**os.environ, **_ENV}
    )

    combined = _combined(result)
    assert result.exit_code == EXIT_TEMPLATE_REFUSE, combined
    seq.assert_called_once()
    assert "REFUSE" in combined
    assert "Refusing to send dm2" in combined
    assert "EX_CONFIG (78)" in combined
    # The parts that DID run still get their rollup + Phase C.
    assert "Daily Check FAILED" in combined
    assert "Connections sent: 3" in combined
    assert "Follow-up Radar" in combined
    # The halt lands in the metrics footer, not just the scrollback.
    assert "template_guard_halt" in combined
    assert "Traceback" not in combined


def test_daily_dm_guard_halt_reports_partial_sends(monkeypatch):
    """A dm2 halt after dm1 shipped reports realized counts, never zero."""
    _patch_daily_harness(monkeypatch)
    monkeypatch.setattr(
        "workflows.daily_check.run_connection_requests",
        lambda *a, **k: {"sent": 2},
    )
    halt = BlankMessageError(
        "Refusing to send dm2: 3/3 rendered message(s) are blank."
    )
    halt.partial_results = {
        "dm1": 8, "dm2": 0, "dm3": 0,
    }
    seq = MagicMock(side_effect=halt)
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli, ["daily", "--yes"], env={**os.environ, **_ENV}
    )

    combined = _combined(result)
    assert result.exit_code == EXIT_TEMPLATE_REFUSE, combined
    assert "DMs sent: 8" in combined
    assert "REFUSE" in combined


def test_daily_invite_guard_halt_skips_dms_and_exits_tempfail(monkeypatch):
    """Part A guard raise → curated refusal, and Part B never sends on a
    run whose content pipeline is in a known-broken state."""
    _patch_daily_harness(monkeypatch)
    conn = MagicMock(
        side_effect=UnresolvedPlaceholderError(
            "Refusing to send connection_note: 1 message(s) contain "
            "unresolved placeholders. Examples: https://linkedin.com/in/a → [Name]"
        )
    )
    monkeypatch.setattr("workflows.daily_check.run_connection_requests", conn)
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli, ["daily", "--yes"], env={**os.environ, **_ENV}
    )

    combined = _combined(result)
    assert result.exit_code == EXIT_TEMPLATE_REFUSE, combined
    conn.assert_called_once()
    seq.assert_not_called()
    assert "REFUSE" in combined
    assert "connection_note" in combined
    assert "Daily Check FAILED" in combined
    assert "Traceback" not in combined


def _wet_harness_with_failed_row_capture(monkeypatch, captured: dict):
    """Wet-run additions: a fake open_daily_run yielding a real DailyRun and
    capturing any exception that traverses it, plus summary-path stubs."""
    from workflows.daily_run import DailyRun

    @contextlib.contextmanager
    def _fake_open(attio, *, run_id, run_date):
        run = DailyRun(
            crm=MagicMock(),
            record_id="rec1",
            run_date=run_date.isoformat(),
            machine_id="m1",
            run_id=run_id,
            initial_counters={"connections": 0, "messages": 0, "visits": 0},
        )
        try:
            yield run
        except BaseException as exc:
            captured["exc"] = exc
            raise

    monkeypatch.setattr("workflows.daily_run.open_daily_run", _fake_open)


def test_daily_wet_guard_halt_marks_row_failed_and_writes_summary(monkeypatch):
    """Wet run: the ORIGINAL guard exception (an Exception, not SystemExit)
    traverses the daily_run context manager — so the real open_daily_run
    closes the row as ``failed`` — and the run-end summary write for the
    parts that ran still happens before the curated exit."""
    _patch_daily_harness(monkeypatch)
    monkeypatch.setattr(
        "workflows.daily_check.run_connection_requests",
        lambda *a, **k: {"sent": 1},
    )
    seq = MagicMock(
        side_effect=BlankMessageError(
            "Refusing to send dm1: 2/2 rendered message(s) are blank."
        )
    )
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    captured: dict = {}
    _wet_harness_with_failed_row_capture(monkeypatch, captured)
    summary = MagicMock(
        return_value={
            "prospect_pool_size": 10, "due_dm1_count": 1, "due_dm2_count": 2,
            "due_dm3_count": 3, "degree_unknown_count": 0,
            "starvation_signal": "healthy",
        }
    )
    monkeypatch.setattr("workflows.daily_check.run_end_summary", summary)
    monkeypatch.setattr(
        "workflows.daily_check.compute_due_dm_counts",
        lambda *a, **k: {
            "prospect_pool_size": 10, "due_dm1_count": 1,
            "due_dm2_count": 2, "due_dm3_count": 3,
        },
    )
    monkeypatch.setattr(
        "workflows.daily_check.compute_dm1_sent_cohort_by_date",
        lambda *a, **k: [],
    )

    result = CliRunner().invoke(
        cli, ["daily", "--yes"], env={**os.environ, **_ENV}
    )

    combined = _combined(result)
    assert result.exit_code == EXIT_TEMPLATE_REFUSE, combined
    assert isinstance(captured.get("exc"), BlankMessageError), captured
    summary.assert_called_once()
    assert "Run-end summary" in combined
    assert "Daily Check FAILED" in combined
    assert "REFUSE" in combined


def test_daily_wet_summary_failure_does_not_discard_guard_halt(monkeypatch):
    """A transient Attio failure inside the run-end summary block must not
    replace the guard halt: the curated ⚠ REFUSE + EX_CONFIG still wins,
    and the daily_run row still records the guard exception."""
    _patch_daily_harness(monkeypatch)
    monkeypatch.setattr(
        "workflows.daily_check.run_connection_requests",
        lambda *a, **k: {"sent": 1},
    )
    seq = MagicMock(
        side_effect=BlankMessageError(
            "Refusing to send dm1: 2/2 rendered message(s) are blank."
        )
    )
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    captured: dict = {}
    _wet_harness_with_failed_row_capture(monkeypatch, captured)
    monkeypatch.setattr(
        "workflows.daily_check.compute_due_dm_counts",
        MagicMock(side_effect=RuntimeError("attio 502 mid-summary")),
    )

    result = CliRunner().invoke(
        cli, ["daily", "--yes"], env={**os.environ, **_ENV}
    )

    combined = _combined(result)
    assert result.exit_code == EXIT_TEMPLATE_REFUSE, combined
    assert isinstance(captured.get("exc"), BlankMessageError), captured
    assert "Run-end summary failed after a send hold" in combined
    assert "REFUSE" in combined
    assert "Refusing to send dm1" in combined


def test_send_dms_guard_halt_curated_refusal(monkeypatch):
    """send-dms: same curated ⚠ REFUSE + EX_CONFIG treatment as the other
    reattach refusals, instead of a raw traceback."""
    _patch_clients_and_lock(monkeypatch)
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    _install_attach(
        monkeypatch, _FakeRun(run_date="2026-06-09", reply_status="ok")
    )
    seq = MagicMock(
        side_effect=UnresolvedPlaceholderError(
            "Refusing to send dm3: 1 message(s) contain unresolved "
            "placeholders. Examples: https://linkedin.com/in/b → [industria similar]"
        )
    )
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli,
        ["send-dms", "--dry-run"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst"},
    )

    combined = _combined(result)
    assert result.exit_code == EXIT_TEMPLATE_REFUSE, combined
    seq.assert_called_once()
    assert "REFUSE" in combined
    assert "Refusing to send dm3" in combined
    assert "EX_CONFIG (78)" in combined
    assert "Traceback" not in combined
