"""Tests for the send-dms subcommand (wet-run DM approval gate).

Covers:
- reply-status fail-closed refuse
- weekend refuse
- --dry-run zero CRM mutations
- staleness refuse
- review-gap re-detect + post-re-detect guard re-check
- MalformedDailyRunRow / NoDailyRunRow / ReopenCollision from reattach → fail-closed

The send-dms command obtains its CRM through the factory seam
(``_crm_provider`` → ``get_crm_provider``), which builds an ``AttioProvider``
over an ``AttioClient``. The tests stub the ``AttioClient`` constructor +
``query_list_entries`` (no creds, no network) and monkeypatch the reattach at
``cli.attach_daily_run`` so the gate logic runs against an in-memory fake run.
"""

from __future__ import annotations

import contextlib
import os
from datetime import date
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from cli import cli


def _patch_clients_and_lock(monkeypatch):
    """Stub AttioClient + PhantomBusterClient constructors/ctx-managers and
    disable the run lock. The CRM factory builds an AttioProvider over the
    stubbed AttioClient, so _crm_provider() yields a working provider with no
    creds and no network."""
    monkeypatch.setattr("clients.phantombuster.PhantomBusterClient.reconcile_workspace", lambda self: None)
    monkeypatch.setattr("clients.attio.AttioClient.__init__", lambda self, *a, **k: None)
    monkeypatch.setattr("clients.attio.AttioClient.__enter__", lambda self: self)
    monkeypatch.setattr("clients.attio.AttioClient.__exit__", lambda self, *a: False)
    monkeypatch.setattr("clients.attio.AttioClient.close", lambda self: None)
    monkeypatch.setattr(
        "clients.attio.AttioClient.query_list_entries", lambda self, **_k: []
    )
    monkeypatch.setattr(
        "clients.phantombuster.PhantomBusterClient.__init__", lambda self: None
    )
    monkeypatch.setattr(
        "clients.phantombuster.PhantomBusterClient.__enter__", lambda self: self
    )
    monkeypatch.setattr(
        "clients.phantombuster.PhantomBusterClient.__exit__", lambda self, *a: False
    )
    monkeypatch.setattr(
        "workflows.record_cache.preload_pipeline_persons", lambda *a, **k: 0
    )
    monkeypatch.setattr(
        "workflows.run_lock.acquire_run_lock",
        lambda *_a, **_k: contextlib.nullcontext(),
    )
    # Neutralize the experiment + schema pre-flights (exercised elsewhere).
    # send-dms imports these function-locally, so patch the source-module attrs.
    monkeypatch.setattr("models.experiment.get_current_experiment_id", lambda: "exp-1")
    monkeypatch.setattr(
        "scripts.validate_attio_schema_deltas.load_manifest", lambda *_a, **_k: {}
    )
    monkeypatch.setattr(
        "scripts.validate_attio_schema_deltas.validate", lambda *_a, **_k: []
    )
    monkeypatch.setattr(
        "scripts.validate_attio_schema_deltas.check_attio_shipped",
        lambda *_a, **_k: [],
    )


class _FakeRun:
    """A DailyRun stand-in the reattach yields. Records run_dm_sequencing reach
    (or not) and exposes the reply-status the guard reads."""

    record_id = "rec_today"

    def __init__(self, run_date: str, reply_status: str | None):
        self.run_date = run_date
        self._reply_status = reply_status

    def get_reply_detection_status(self):
        return self._reply_status


def _install_attach(monkeypatch, run):
    """Make cli's attach_daily_run yield `run` as a context manager."""

    @contextlib.contextmanager
    def _fake_attach(*_a, **_k):
        yield run

    monkeypatch.setattr("cli.attach_daily_run", _fake_attach)


def test_send_dms_refuses_when_reply_status_not_ok(monkeypatch):
    """reply_detection_status is None (detection never ran in this process) →
    fail-closed refuse, run_dm_sequencing NEVER called.

    Critical: the guard is `!= "ok"`, which covers None — NOT the `== "failed"`
    form that the in-function short-circuit uses."""
    _patch_clients_and_lock(monkeypatch)
    _install_attach(monkeypatch, _FakeRun(run_date="2026-06-09", reply_status=None))
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli,
        ["send-dms", "--yes"],
        env={
            **os.environ,
            "PB_MESSAGE_SENDER_ID": "ms",
            "ATTIO_LIST_ID": "lst",
            "PB_INBOX_SCRAPER_ID": "inbox",
        },
    )
    assert result.exit_code != 0, result.output
    seq.assert_not_called()
    assert "reply" in result.output.lower()


def test_send_dms_refuses_stale_row(monkeypatch):
    """Bound row run_date=yesterday → refuse, never send."""
    _patch_clients_and_lock(monkeypatch)
    _install_attach(monkeypatch, _FakeRun(run_date="2026-06-08", reply_status="ok"))
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli, ["send-dms", "--dry-run"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst"},
    )
    assert result.exit_code != 0
    seq.assert_not_called()
    assert "not today" in result.output.lower()


def test_send_dms_refuses_on_weekend_without_force(monkeypatch):
    """Saturday + no --force-weekend → refuse. 2026-06-13 is a Saturday."""
    _patch_clients_and_lock(monkeypatch)
    _install_attach(monkeypatch, _FakeRun(run_date="2026-06-13", reply_status="ok"))
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 13)
    )
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli, ["send-dms", "--dry-run"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst"},
    )
    assert result.exit_code != 0
    seq.assert_not_called()
    assert "weekend" in result.output.lower()


def test_send_dms_dry_run_resolves_without_re_detect_or_mutation(monkeypatch):
    """--dry-run: no re-detect, run_dm_sequencing called with dry_run=True (its
    dry-run branch makes zero CRM writes)."""
    _patch_clients_and_lock(monkeypatch)
    _install_attach(monkeypatch, _FakeRun(run_date="2026-06-09", reply_status="ok"))
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    detect = MagicMock()
    monkeypatch.setattr("workflows.detect_responses.detect_responses", detect)
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0, "dry_run": {"dm1": 0}})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli, ["send-dms", "--dry-run"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst",
             "PB_INBOX_SCRAPER_ID": "inbox"},
    )
    assert result.exit_code == 0, result.output
    detect.assert_not_called()  # --dry-run NEVER re-runs detection
    seq.assert_called_once()
    assert seq.call_args.kwargs["dry_run"] is True
    assert seq.call_args.kwargs["auto_confirm"] is False


def test_send_dms_dry_run_attaches_read_only(monkeypatch):
    """G(iv): --dry-run must attach the daily_run row read-only so a preview
    never reopens a terminal row and re-closes it 'completed', silently
    rewriting a prior 'failed' status."""
    _patch_clients_and_lock(monkeypatch)
    captured: dict = {}

    @contextlib.contextmanager
    def _capturing_attach(*_a, **kwargs):
        captured.update(kwargs)
        yield _FakeRun(run_date="2026-06-09", reply_status="ok")

    monkeypatch.setattr("cli.attach_daily_run", _capturing_attach)
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    monkeypatch.setattr(
        "workflows.daily_check.run_dm_sequencing",
        MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0, "dry_run": {"dm1": 0}}),
    )

    result = CliRunner().invoke(
        cli, ["send-dms", "--dry-run"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst",
             "PB_INBOX_SCRAPER_ID": "inbox"},
    )
    assert result.exit_code == 0, result.output
    assert captured.get("read_only") is True


def test_send_dms_wet_attaches_not_read_only(monkeypatch):
    """A wet send-dms attaches read_only=False so the normal reopen/close
    state-machine runs."""
    _patch_clients_and_lock(monkeypatch)
    captured: dict = {}

    @contextlib.contextmanager
    def _capturing_attach(*_a, **kwargs):
        captured.update(kwargs)
        yield _FakeRun(run_date="2026-06-09", reply_status="ok")

    monkeypatch.setattr("cli.attach_daily_run", _capturing_attach)
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    monkeypatch.setattr(
        "workflows.daily_check.run_dm_sequencing",
        MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0}),
    )

    result = CliRunner().invoke(
        cli, ["send-dms", "--yes"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst"},
    )
    assert result.exit_code == 0, result.output
    assert captured.get("read_only") is False


def test_send_dms_dry_run_yes_never_re_detects(monkeypatch):
    """`--dry-run --yes` must NOT trigger a live inbox scrape. The re-detect
    block is gated on `not mode.is_dry_run()`, so even with --yes set,
    detect_responses is never called under --dry-run; run_dm_sequencing still
    receives dry_run=True."""
    _patch_clients_and_lock(monkeypatch)
    _install_attach(monkeypatch, _FakeRun(run_date="2026-06-09", reply_status="ok"))
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    detect = MagicMock()
    monkeypatch.setattr("workflows.detect_responses.detect_responses", detect)
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0, "dry_run": {"dm1": 0}})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli, ["send-dms", "--dry-run", "--yes"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst",
             "PB_INBOX_SCRAPER_ID": "inbox"},
    )
    assert result.exit_code == 0, result.output
    detect.assert_not_called()  # --dry-run NEVER re-runs detection, even with --yes
    seq.assert_called_once()
    assert seq.call_args.kwargs["dry_run"] is True


def test_send_dms_wet_yes_without_inbox_scraper_warns_loudly(monkeypatch):
    """A WET --yes send with no inbox-scraper-id can't run the review-gap
    race-closer. The skip must be LOUD on stderr (not silent), and the send must
    still proceed (sending on step-1 reply status, guarded by the early
    check)."""
    _patch_clients_and_lock(monkeypatch)
    _install_attach(monkeypatch, _FakeRun(run_date="2026-06-09", reply_status="ok"))
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    detect = MagicMock()
    monkeypatch.setattr("workflows.detect_responses.detect_responses", detect)
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    # Empty string (not pop): CliRunner falls through to os.environ for keys it
    # doesn't override, and the real env may carry PB_INBOX_SCRAPER_ID — an empty
    # value is what click's envvar resolves to falsy.
    env = {**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst",
           "PB_INBOX_SCRAPER_ID": ""}
    result = CliRunner().invoke(cli, ["send-dms", "--yes"], env=env)

    assert result.exit_code == 0, result.output
    detect.assert_not_called()  # no scraper id → re-detect cannot run
    seq.assert_called_once()  # but the send still proceeds
    combined = result.output.lower()
    assert "no inbox-scraper-id" in combined
    assert "review-gap race not re-closed" in combined


def test_send_dms_rechecks_guard_after_re_detect(monkeypatch):
    """Early guard sees reply_status='ok' and passes; the --yes re-detect flips
    it to 'failed'; the POST-re-detect re-check refuses BEFORE
    run_dm_sequencing is ever called."""
    _patch_clients_and_lock(monkeypatch)
    run = _FakeRun(run_date="2026-06-09", reply_status="ok")
    _install_attach(monkeypatch, run)
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )

    def _flip_to_failed(*_a, **_k):
        run._reply_status = "failed"  # simulate detect_responses' CRM write
        return {"detected": 0}

    monkeypatch.setattr("workflows.detect_responses.detect_responses", _flip_to_failed)
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli, ["send-dms", "--yes"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst",
             "PB_INBOX_SCRAPER_ID": "inbox"},
    )
    assert result.exit_code != 0
    seq.assert_not_called()
    assert "post-re-detect" in result.output.lower()


def test_send_dms_yes_redetects_then_resolves(monkeypatch):
    """--yes runs re-detect, THEN run_dm_sequencing with the current state.
    Re-detect can only shrink the set (RESPONDED dropped downstream), never add
    an un-reviewed DM."""
    _patch_clients_and_lock(monkeypatch)
    run = _FakeRun(run_date="2026-06-09", reply_status="ok")
    _install_attach(monkeypatch, run)
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    order = []
    monkeypatch.setattr(
        "workflows.detect_responses.detect_responses",
        lambda *a, **k: order.append("detect") or {"detected": 1},
    )
    monkeypatch.setattr(
        "workflows.daily_check.run_dm_sequencing",
        lambda *a, **k: order.append("send") or {"dm1": 0, "dm2": 0, "dm3": 0},
    )
    result = CliRunner().invoke(
        cli, ["send-dms", "--yes"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst",
             "PB_INBOX_SCRAPER_ID": "inbox"},
    )
    assert result.exit_code == 0, result.output
    assert order == ["detect", "send"], f"re-detect must precede send; got {order}"


def test_responded_prospect_is_send_ineligible():
    """A prospect flipped to RESPONDED between resolve and send is dropped by
    is_send_eligible (terminal stage). This is the mechanism send-dms relies on
    after the re-detect flips repliers."""
    from models.pipeline import PipelineStage, is_send_eligible
    entry = {"stage": PipelineStage.RESPONDED.value}
    assert is_send_eligible(entry) is False
    # Sanity: an ACCEPTED prospect is still eligible.
    accepted = {"stage": PipelineStage.ACCEPTED.value}
    assert is_send_eligible(accepted) is True


def test_send_dms_malformed_row_fails_closed(monkeypatch):
    """A reattach that raises MalformedDailyRunRow (an existing row with a
    missing/unparseable counter) makes send-dms exit non-zero WITHOUT calling
    run_dm_sequencing. Defaulting the counter to 0 would silently reset the cap
    and permit a 60-DM breach — fail closed."""
    from workflows.daily_run import MalformedDailyRunRow

    _patch_clients_and_lock(monkeypatch)
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )

    @contextlib.contextmanager
    def _raising_attach(*_a, **_k):
        raise MalformedDailyRunRow("messages_sent", "rec_today")
        yield  # pragma: no cover

    monkeypatch.setattr("cli.attach_daily_run", _raising_attach)
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli, ["send-dms", "--dry-run"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst"},
    )
    assert result.exit_code != 0
    seq.assert_not_called()
    assert "untrustworthy" in result.output.lower()


@pytest.mark.parametrize(
    "make_exc",
    [
        lambda: __import__(
            "workflows.daily_run", fromlist=["NoDailyRunRow"]
        ).NoDailyRunRow("2026-06-09", "machine-1"),
        lambda: __import__(
            "workflows.daily_run", fromlist=["ReopenCollision"]
        ).ReopenCollision("2026-06-09", "machine-1", "rec_today"),
    ],
    ids=["NoDailyRunRow", "ReopenCollision"],
)
def test_send_dms_reattach_refusals_fail_closed(monkeypatch, make_exc):
    """The other reattach-refusal branches (NoDailyRunRow, ReopenCollision) each
    exit non-zero and never call run_dm_sequencing — same fail-closed treatment
    as MalformedDailyRunRow."""
    _patch_clients_and_lock(monkeypatch)
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )

    @contextlib.contextmanager
    def _raising_attach(*_a, **_k):
        raise make_exc()
        yield  # pragma: no cover

    monkeypatch.setattr("cli.attach_daily_run", _raising_attach)
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)

    result = CliRunner().invoke(
        cli, ["send-dms", "--dry-run"],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst"},
    )
    assert result.exit_code != 0
    seq.assert_not_called()


# ── PR-237: --exclude passthrough (send-dms → run_dm_sequencing) ─────────────


def _invoke_send_dms_dry(monkeypatch, seq, extra_args):
    """Drive `send-dms --dry-run` past the preflights with run_dm_sequencing
    stubbed, returning the CliRunner result. Shared by the --exclude passthrough
    tests below."""
    _patch_clients_and_lock(monkeypatch)
    _install_attach(monkeypatch, _FakeRun(run_date="2026-06-09", reply_status="ok"))
    monkeypatch.setattr(
        "models.business_calendar.operator_today", lambda: date(2026, 6, 9)
    )
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", seq)
    return CliRunner().invoke(
        cli, ["send-dms", "--dry-run", *extra_args],
        env={**os.environ, "PB_MESSAGE_SENDER_ID": "ms", "ATTIO_LIST_ID": "lst"},
    )


def test_send_dms_exclude_entry_id_threaded_as_set(monkeypatch):
    """A single --exclude reaches run_dm_sequencing as exclude_ids={"e1"}."""
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    result = _invoke_send_dms_dry(monkeypatch, seq, ["--exclude", "e1"])
    assert result.exit_code == 0, result.output
    seq.assert_called_once()
    assert seq.call_args.kwargs["exclude_ids"] == {"e1"}


def test_send_dms_exclude_record_id_threaded_as_set(monkeypatch):
    """--exclude also accepts a record_id (opaque to the CLI — same passthrough)."""
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    result = _invoke_send_dms_dry(monkeypatch, seq, ["--exclude", "rec_123"])
    assert result.exit_code == 0, result.output
    assert seq.call_args.kwargs["exclude_ids"] == {"rec_123"}


def test_send_dms_multiple_exclude_flags_collected(monkeypatch):
    """Repeated --exclude flags collect into one set."""
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    result = _invoke_send_dms_dry(
        monkeypatch, seq, ["--exclude", "e1", "--exclude", "rec_2"]
    )
    assert result.exit_code == 0, result.output
    assert seq.call_args.kwargs["exclude_ids"] == {"e1", "rec_2"}


def test_send_dms_no_exclude_passes_empty_set(monkeypatch):
    """Absent --exclude → empty set (never None), so the filter is a no-op."""
    seq = MagicMock(return_value={"dm1": 0, "dm2": 0, "dm3": 0})
    result = _invoke_send_dms_dry(monkeypatch, seq, [])
    assert result.exit_code == 0, result.output
    assert seq.call_args.kwargs["exclude_ids"] == set()
