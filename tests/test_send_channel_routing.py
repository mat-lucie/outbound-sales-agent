"""`send_channel` hold-out semantics + the blank-rendered-copy guards.

PhantomBuster is the only wired send transport (`clients.sender.PBSender`).
No code path in this engine stamps `send_channel=botdog`, and no send path
routes to Botdog. The stamp exists as a SAFETY INTERLOCK: a row an operator
stamped `botdog` (their own migration, another deployment, a Botdog campaign
that may still hold the lead) must get NOTHING from PB — no invite, no DM, no
lease charge — rather than a second first-touch from a second transport. The
remediation is always the same: re-stamp the row `send_channel=pb` once the
Botdog campaigns are paused and their leads removed.

Covered here:

  * ``_resolve_send_channel`` — missing / None / empty resolve to ``pb``;
    an unknown stored value passes through verbatim and is treated as
    non-botdog (the safe direction — never route to an unwired transport);
  * the invite path (``run_connection_requests``): the residual census counts
    stamped rows at ANY stage, and stamped PROSPECTs are excluded from the PB
    invite batch;
  * the DM path (``run_dm_sequencing``): stamped DM-due rows are held out at
    QUEUE BUILD — before the cap trim, the wet confirm and composition — and
    reported identically on dry and wet runs; a stamp on a SIBLING entry of
    the same LinkedIn identity holds the whole prospect out;
  * ``compute_due_dm_counts`` — held-out rows are not counted as due;
  * Phase 0 (``detect_accepted_connections``) and Phase 0.5
    (``detect_responses``) scope skips;
  * the blank-render halt on BOTH the DM step batch and the invite batch,
    including on a dry run (blank copy must scream in content QA, not preview
    as an empty message line), with whitespace-only counting as blank;
  * ``BOTDOG_SEND_ENABLED`` — the flag gates ONLY the optional event-ingest
    drain, never a send, and a failure in that drain must not take the PB
    sends down with it.

DM harness idiom follows tests/test_run_dm_sequencing_trim.py; invite harness
follows tests/test_invite_cap_attio_routing.py.
"""
from __future__ import annotations

import contextlib
import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from clients.attio import AttioClient
from models.pipeline import PipelineStage
from tests.fakes import fake_daily_run
from tests.test_integration import _attio_with_full_schema, _make_attio_entry
from workflows.daily_check import (
    SEND_CHANNEL_BOTDOG,
    SEND_CHANNEL_DEFAULT,
    SEND_CHANNEL_PB,
    _resolve_send_channel,
)

# `BlankMessageError` lives in daily_check_helpers and — unlike its sibling
# `UnresolvedPlaceholderError` — is NOT re-exported from `workflows.daily_check`,
# so it is imported from its home module here.
from workflows.daily_check_helpers import BlankMessageError


class TestResolveSendChannel:
    def test_missing_key_resolves_to_default(self):
        assert _resolve_send_channel({}) == SEND_CHANNEL_DEFAULT

    def test_none_and_empty_resolve_to_default(self):
        assert _resolve_send_channel({"send_channel": None}) == SEND_CHANNEL_DEFAULT
        assert _resolve_send_channel({"send_channel": ""}) == SEND_CHANNEL_DEFAULT

    def test_explicit_channels_pass_through(self):
        assert _resolve_send_channel({"send_channel": "pb"}) == SEND_CHANNEL_PB
        assert _resolve_send_channel({"send_channel": "botdog"}) == SEND_CHANNEL_BOTDOG

    def test_unknown_value_passes_through_verbatim(self):
        """An unexpected stored value is returned as-is. Callers compare
        against SEND_CHANNEL_BOTDOG, so an unknown channel lands on the PB
        path — the safe direction: a typo must never route a send to an
        unwired transport."""
        assert _resolve_send_channel({"send_channel": "smoke-signal"}) == "smoke-signal"
        assert (
            _resolve_send_channel({"send_channel": "smoke-signal"})
            != SEND_CHANNEL_BOTDOG
        )

    def test_default_is_pb(self):
        """Every row this engine writes leaves `send_channel` unset, so the
        default IS the routing decision for the whole live pipeline."""
        assert SEND_CHANNEL_DEFAULT == SEND_CHANNEL_PB


class TestSharedChannelResolver:
    """ONE definition, three consumers.

    The send path (daily_check), the Phase-0.5 reply skip (detect_responses)
    and the event-ingest scope guard (botdog_ingest) must all answer "which
    transport owns this row?" identically. A drifted copy would move the send
    hold-out and leave a detection path behind, splitting one prospect's
    cadence across two transports.
    """

    def test_canonical_home_is_daily_check_helpers(self):
        from workflows import daily_check_helpers

        assert daily_check_helpers.SEND_CHANNEL_BOTDOG == "botdog"
        assert daily_check_helpers.SEND_CHANNEL_DEFAULT == "pb"

    def test_daily_check_reexports_the_same_objects(self):
        from workflows import daily_check, daily_check_helpers

        assert (
            daily_check._resolve_send_channel
            is daily_check_helpers._resolve_send_channel
        )
        assert (
            daily_check.SEND_CHANNEL_BOTDOG == daily_check_helpers.SEND_CHANNEL_BOTDOG
        )

    def test_detect_responses_uses_the_shared_resolver(self):
        from workflows import daily_check_helpers, detect_responses

        assert (
            detect_responses._resolve_send_channel
            is daily_check_helpers._resolve_send_channel
        )

    def test_botdog_ingest_uses_the_shared_resolver(self):
        from workflows import botdog_ingest, daily_check_helpers

        assert (
            botdog_ingest._resolve_send_channel
            is daily_check_helpers._resolve_send_channel
        )

    def test_no_inline_botdog_literal_in_the_guarded_paths(self):
        """Guards against a copy of the literal creeping back in — in ANY of
        the resolver's consumers, not just one."""
        import pathlib

        root = pathlib.Path(__file__).resolve().parent.parent
        for name in (
            "workflows/detect_responses.py",
            "workflows/botdog_ingest.py",
            "workflows/daily_check.py",
        ):
            text = (root / name).read_text()
            # Only comments/docstrings may mention the raw value.
            offenders = [
                line
                for line in text.splitlines()
                if '== "botdog"' in line and not line.strip().startswith("#")
            ]
            assert not offenders, f"{name}: {offenders}"


class TestSendPathsTakeNoBotdogArguments:
    """PB owns sending. If someone re-adds a ``botdog_send_enabled`` /
    ``botdog_sender`` parameter to a send entry point, this fails before any
    behavior test can be fooled by a default. The injectable ``sender`` seam
    IS the supported extension point."""

    def test_run_connection_requests_signature(self):
        import inspect

        from workflows.daily_check import run_connection_requests

        params = inspect.signature(run_connection_requests).parameters
        assert "botdog_send_enabled" not in params
        assert "botdog_sender" not in params
        assert "sender" in params

    def test_run_dm_sequencing_signature(self):
        import inspect

        from workflows.daily_check import run_dm_sequencing

        params = inspect.signature(run_dm_sequencing).parameters
        assert "botdog_send_enabled" not in params
        assert "botdog_sender" not in params
        assert "sender" in params


# ---------------------------------------------------------------------------
# DM hold-out (run_dm_sequencing)
# ---------------------------------------------------------------------------


def _url(i: int) -> str:
    return f"https://linkedin.com/in/acme-{i}"


def _csv(sent_urls: list[str]) -> str:
    lines = ["query,status"] + [f"{u},Message sent" for u in sent_urls]
    return "\n".join(lines) + "\n"


class _LeaseRecorder:
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


def _run_dm(
    monkeypatch,
    entries: list[dict],
    csv_per_launch: list[str | None],
    dry_run: bool = False,
    personalized_message: str = "Hola Alice",
    remaining_messages: int = 30,
):
    """Drive run_dm_sequencing with caller-supplied parsed entries and a
    scripted PB. Returns (result, pb, leases, sheet_batches, builder).

    ``builder`` is the patched ``_build_botdog_sender`` — it raises if called,
    because no send path may construct a Botdog sender (its only legitimate
    caller is the optional event-ingest drain, which sends nothing).
    """
    for key, value in {
        "ATTIO_LIST_ID": "list-001",
        "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake",
        "PB_LI_SESSION_COOKIE": "fake-cookie",
        "PB_LI_USER_AGENT": "TestAgent/1.0",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
    }.items():
        monkeypatch.setenv(key, value)
    from workflows import daily_check

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
    monkeypatch.setattr(
        daily_check, "personalize", lambda tpl, *a, **kw: personalized_message
    )
    monkeypatch.setattr(daily_check, "get_current_experiment_id", lambda: None)
    monkeypatch.setattr(daily_check, "_write_company_throttle_tally", MagicMock())
    monkeypatch.setattr(daily_check, "emit_pb_inmail_dead_end", MagicMock())
    monkeypatch.setattr(daily_check, "emit_pb_silent_no_op", MagicMock())
    monkeypatch.setattr(daily_check, "_attio_advance_with_escalation", lambda **kw: True)
    monkeypatch.setattr(daily_check, "_finalize_confirmed_dm_send", lambda **kw: 1)

    builder = MagicMock(
        side_effect=AssertionError(
            "_build_botdog_sender must never be called from the DM path — "
            "PhantomBuster owns sending"
        )
    )
    monkeypatch.setattr(daily_check, "_build_botdog_sender", builder)

    sheet_batches: list[list[str]] = []

    def _capture_sheet(rows):
        sheet_batches.append([r["linkedInUrl"] for r in rows])
        return "https://sheet/x"

    monkeypatch.setattr(daily_check, "write_prospects_to_sheet", _capture_sheet)
    monkeypatch.setattr(daily_check, "_pb_session_args", lambda: {})

    cache = MagicMock()
    cache.get.side_effect = lambda rid: (
        f"Alice {rid[3:]}",
        "Acme Foods",
        _url(int(rid[3:])),
        "food",
        "VP Operations",
    )

    pb = MagicMock()
    pb.launch_agent.side_effect = lambda agent_id, args: MagicMock(container_id="cid1")
    pb.wait_for_completion.side_effect = [
        MagicMock(status="finished", log_output="") for _ in csv_per_launch
    ]
    pb.download_result_csv.side_effect = csv_per_launch

    leases = _LeaseRecorder()
    daily_run = MagicMock()
    daily_run.remaining.return_value = remaining_messages
    daily_run.get_reply_detection_status.return_value = "ok"
    daily_run.reserve_send.side_effect = leases.reserve
    daily_run.confirm_lease.side_effect = leases.confirm
    daily_run.release_lease.side_effect = leases.release

    result = daily_check.run_dm_sequencing(
        _attio_with_full_schema(),
        pb,
        "msg_sender_id",
        daily_run,
        dry_run=dry_run,
        auto_confirm=True,
        cache=cache,
        audit_logger=MagicMock(),
    )
    return result, pb, leases, sheet_batches, builder


def _dm_entry(i: int, send_channel: str | None = None, *, omit_key: bool = False) -> dict:
    last_contact = (date.today() - timedelta(days=3)).isoformat()
    entry = {
        "record_id": f"rec{i}",
        "entry_id": f"ent{i}",
        "stage": PipelineStage.ACCEPTED.value,
        "last_contact_date": last_contact,
        "quality_score": 75,
        "persona": "operations_leaders",
        "language": "es",
        "invite_eligible_after": None,
        "dm_step": 0,
        "experiment_id": None,
        "experiment_id_frozen_at": None,
        "next_eligible_send_date": None,
    }
    if not omit_key:
        entry["send_channel"] = send_channel
    return entry


class TestBlankRenderBatchHalt:
    """Systemic empty-render halt: when template rendering breaks upstream and
    EVERY DM composes blank, the batch must abort loudly at the pre-send guard
    — before the dry-run preview, before any transport launch and before the
    lease reservation — instead of surfacing as N per-prospect transport
    failures (or a "successful" run with 0 sends).
    """

    def test_blank_renders_halt_batch_before_any_transport(self, monkeypatch):
        """Botdog-stamped rows leave the queue at build time, so only the pb
        rows reach composition — and a systemic blank still halts."""
        entries = [_dm_entry(0, "pb"), _dm_entry(1, "botdog")]
        with pytest.raises(BlankMessageError) as exc:
            _run_dm(monkeypatch, entries, csv_per_launch=[], personalized_message="")
        # The one composed (pb) row flagged systemic; no transport touched.
        assert "1/1" in str(exc.value)

    def test_whitespace_only_counts_as_blank(self, monkeypatch):
        """A render that emits only spaces/newlines is just as broken as an
        empty string — PB would inject it verbatim as an empty message."""
        entries = [_dm_entry(0, "pb")]
        with pytest.raises(BlankMessageError):
            _run_dm(
                monkeypatch, entries, csv_per_launch=[], personalized_message=" \n\t "
            )

    def test_blank_renders_halt_dry_run_too(self, monkeypatch):
        """Dry-run is the content-QA pass — blank copy must scream there, not
        preview as an empty message line the operator then approves."""
        entries = [_dm_entry(0, "pb")]
        with pytest.raises(BlankMessageError):
            _run_dm(
                monkeypatch,
                entries,
                csv_per_launch=[],
                dry_run=True,
                personalized_message="   ",
            )

    def test_blank_halt_never_reserved_a_lease(self, monkeypatch):
        """The guard runs before ``reserve_send``, so an aborted batch leaves
        the day's message budget untouched — the operator can re-run after
        fixing the template without a phantom charge."""
        entries = [_dm_entry(0, "pb")]
        leases = _LeaseRecorder()
        with pytest.raises(BlankMessageError), patch.object(
            _LeaseRecorder, "reserve", leases.reserve
        ):
            _run_dm(monkeypatch, entries, csv_per_launch=[], personalized_message="")
        assert leases.ops == []


class TestDmChannelHoldOut:
    """Rows stamped ``send_channel=botdog`` get NOTHING: no Botdog call (no
    such send path exists), no PB fallback (a Botdog campaign may still hold
    the lead; PB sending it too would double-message), no lease charge — until
    they are re-stamped ``send_channel=pb``."""

    def test_mixed_queue_pb_drains_and_botdog_rows_held_out(self, monkeypatch, capsys):
        """2 pb-channel rows (one explicit `pb`, one missing the attr →
        default) flow through the PB send loop; the botdog-stamped row is
        skipped loudly — never on the sheet, never leased."""
        entries = [
            _dm_entry(0, "pb"),
            _dm_entry(1, omit_key=True),  # unset → SEND_CHANNEL_DEFAULT (pb)
            _dm_entry(2, "botdog"),
        ]
        result, pb, leases, sheet_batches, builder = _run_dm(
            monkeypatch, entries, csv_per_launch=[_csv([_url(0), _url(1)])]
        )

        assert result["dm1"] == 2  # pb delivery-confirmed
        assert result["botdog_channel_skipped"]["dm1"] == 1
        # The botdog row never reached the sheet or PB.
        assert sheet_batches == [[_url(0), _url(1)]]
        assert pb.launch_agent.call_count == 1
        # Only the pb rows reserve on the messages budget.
        assert ("reserve", 2) in leases.ops
        assert all(op != ("reserve", 1) for op in leases.ops)
        assert all(op != ("reserve", 3) for op in leases.ops)
        builder.assert_not_called()
        err = capsys.readouterr().err
        assert "botdog-stamped DM-due row(s) held out of the" in err
        assert "re-stamped send_channel=pb" in err

    def test_all_botdog_queue_sends_nothing_anywhere(self, monkeypatch):
        entries = [_dm_entry(0, "botdog"), _dm_entry(1, "botdog")]
        result, pb, leases, sheet_batches, builder = _run_dm(
            monkeypatch, entries, csv_per_launch=[]
        )

        assert result["dm1"] == 0
        assert result["botdog_channel_skipped"]["dm1"] == 2
        assert pb.launch_agent.call_count == 0
        assert sheet_batches == []
        assert leases.ops == []
        builder.assert_not_called()

    def test_env_flag_cannot_reroute_dms_to_botdog(self, monkeypatch):
        """BOTDOG_SEND_ENABLED in the environment must have ZERO effect on the
        DM path — the flag gates only the optional event-ingest drain. A stale
        `true` in .env must not resurrect a send path that does not exist."""
        monkeypatch.setenv("BOTDOG_SEND_ENABLED", "1")
        entries = [_dm_entry(0, "botdog"), _dm_entry(1, "pb")]
        result, pb, leases, sheet_batches, builder = _run_dm(
            monkeypatch, entries, csv_per_launch=[_csv([_url(1)])]
        )

        assert result["botdog_channel_skipped"]["dm1"] == 1
        assert result["dm1"] == 1
        assert sheet_batches == [[_url(1)]]
        builder.assert_not_called()

    def test_missing_key_cannot_crash_the_hold_out(self, monkeypatch):
        """No BOTDOG_API_KEY + botdog rows in the queue == a clean run, not a
        RuntimeError mid-phase: the send path never builds a Botdog sender."""
        monkeypatch.delenv("BOTDOG_API_KEY", raising=False)
        result, _pb, leases, _sheets, builder = _run_dm(
            monkeypatch, [_dm_entry(0, "botdog")], csv_per_launch=[]
        )
        builder.assert_not_called()
        assert result["botdog_channel_skipped"]["dm1"] == 1
        assert leases.ops == []

    def test_dry_run_reports_hold_out_not_would_send(self, monkeypatch, capsys):
        """Dry-run honesty: a botdog-stamped row must NOT preview as "would
        send". It is held out at queue build, counted, and warned about exactly
        like a wet run — otherwise an operator approves a batch containing
        sends that cannot happen."""
        entries = [_dm_entry(0, "botdog")]
        result, pb, leases, sheet_batches, builder = _run_dm(
            monkeypatch, entries, csv_per_launch=[], dry_run=True
        )
        assert result["dry_run"]["dm1"] == 0
        assert result["botdog_channel_skipped"]["dm1"] == 1
        assert leases.ops == []
        assert pb.launch_agent.call_count == 0
        builder.assert_not_called()
        err = capsys.readouterr().err
        assert "botdog-stamped DM-due row(s) held out of the" in err

    def test_sibling_botdog_stamp_holds_out_the_pb_entry(self, monkeypatch):
        """Duplicate entries for the SAME LinkedIn identity with divergent
        stamps: if any sibling is botdog-stamped, a Botdog campaign may still
        hold the lead, so the pb-stamped entry must be held out too — resolving
        the channel on just the kept dedupe row would let it double-send."""
        sibling = {
            # record "rec05" → int("05") == 5 → same URL as rec5.
            "record_id": "rec05",
            "entry_id": "ent05",
            "stage": PipelineStage.PROSPECT.value,
            "send_channel": "botdog",
            "last_contact_date": None,
            "quality_score": 75,
            "persona": "operations_leaders",
            "language": "es",
            "invite_eligible_after": None,
            "dm_step": 0,
            "experiment_id": None,
            "experiment_id_frozen_at": None,
            "next_eligible_send_date": None,
        }
        entries = [_dm_entry(5, "pb"), sibling]
        result, pb, leases, sheet_batches, builder = _run_dm(
            monkeypatch, entries, csv_per_launch=[]
        )
        assert result["botdog_channel_skipped"]["dm1"] == 1
        assert result["dm1"] == 0
        assert sheet_batches == []
        assert pb.launch_agent.call_count == 0
        builder.assert_not_called()

    def test_hold_out_rows_do_not_consume_the_message_cap(self, monkeypatch):
        """Held-out rows leave the queue BEFORE the cap trim, so they can no
        longer displace sendable pb rows on a cap-bound day."""
        entries = [_dm_entry(i, "botdog") for i in range(3)] + [
            _dm_entry(3, "pb"),
            _dm_entry(4, "pb"),
        ]
        result, pb, leases, sheet_batches, builder = _run_dm(
            monkeypatch,
            entries,
            csv_per_launch=[_csv([_url(3), _url(4)])],
            remaining_messages=2,
        )
        # Cap 2, 2 pb rows: both sent — the 3 botdog rows did not eat slots.
        assert result["dm1"] == 2
        assert result["botdog_channel_skipped"]["dm1"] == 3
        assert sheet_batches == [[_url(3), _url(4)]]

    def test_pb_only_queue_is_unchanged(self, monkeypatch):
        entries = [_dm_entry(0, "pb"), _dm_entry(1, None)]
        result, pb, leases, sheet_batches, builder = _run_dm(
            monkeypatch, entries, csv_per_launch=[_csv([_url(0), _url(1)])]
        )
        assert result["dm1"] == 2
        assert result["botdog_channel_skipped"] == {"dm1": 0, "dm2": 0, "dm3": 0}
        builder.assert_not_called()

    def test_unknown_channel_value_still_sends_via_pb(self, monkeypatch):
        """An unrecognized stamp is NOT a hold-out: only the explicit `botdog`
        value means "another transport may hold this lead". A typo must degrade
        to the wired transport, not silently freeze the prospect."""
        entries = [_dm_entry(0, "smoke-signal")]
        result, pb, _leases, sheet_batches, builder = _run_dm(
            monkeypatch, entries, csv_per_launch=[_csv([_url(0)])]
        )
        assert result["dm1"] == 1
        assert result["botdog_channel_skipped"]["dm1"] == 0
        assert sheet_batches == [[_url(0)]]
        builder.assert_not_called()


# ---------------------------------------------------------------------------
# Invite path (run_connection_requests): PB owns every invite
# ---------------------------------------------------------------------------

_ENV = {
    "ATTIO_LIST_ID": "list-001",
    "ATTIO_API_KEY": "fake",
    "PHANTOMBUSTER_API_KEY": "fake",
    "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
    "PB_LI_SESSION_COOKIE": "fake-cookie",
    "PB_LI_USER_AGENT": "TestAgent/1.0",
    "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
}


def _parsed(idx: int, *, stage: str = "Prospect", channel: str | None = None) -> dict:
    """A parsed list-entry attrs dict, optionally carrying a channel stamp.

    Built through the real ``AttioClient.parse_entry`` so the shape matches
    production, then stamped: ``parse_entry`` does not extract ``send_channel``
    (see the module-level note in the port report), so the attribute has to be
    injected for the routing gates to see it.
    """
    attrs = AttioClient.parse_entry(
        _make_attio_entry(
            entry_id=f"entry-ch-{idx:03d}",
            record_id=f"rec-ch-{idx:03d}",
            stage=stage,
            quality_score=75,
        )
    )
    if channel is not None:
        attrs["send_channel"] = channel
    return attrs


def _invite_attio(n: int) -> MagicMock:
    attio = MagicMock()
    attio.is_person_company_corrupted.return_value = False
    attio.query_list_entries.return_value = []
    attio._person_to_company = {
        f"rec-ch-{i:03d}": f"company-ch-{i:03d}" for i in range(n)
    }
    return attio


def _invite_cache(record_id):
    idx = int(record_id.rsplit("-", 1)[1])
    return (
        f"Alice {idx}",
        "Acme Foods",
        f"https://linkedin.com/in/acme-person{idx}",
        "",
        "",
    )


def _pb_invite_sender(requested_count: int = 1) -> MagicMock:
    from clients.pb_envelope import SendOutcome
    from clients.sender import PBSendResult

    sender = MagicMock()
    sender.launch_invite_batch.return_value = MagicMock(
        spec=PBSendResult,
        launch=MagicMock(container_id="c1"),
        completion=MagicMock(),
        outcome=SendOutcome(
            container_id="c1",
            csv_status="Skipped",
            sent_count=0,
            requested_count=requested_count,
            drift_skipped_reason=None,
            next_day_drift_key="k",
        ),
    )
    return sender


class TestInviteBlankNoteHalt:
    """The systemic blank-render halt covers the invite path too. Invites run
    FIRST in the daily command and PB injects the note verbatim via the sheet
    without rejecting blanks — so a template break must abort the invite batch
    before it burns the day's invite budget on note-less first-touches."""

    @patch.dict(os.environ, _ENV)
    def test_blank_notes_halt_invites_before_any_transport(self):
        from workflows.daily_check import run_connection_requests

        attio = _invite_attio(2)
        pb = MagicMock()
        dr = fake_daily_run()
        sheet = MagicMock(return_value="https://docs.google.com/spreadsheets/d/f")

        with patch.dict(os.environ, _ENV), patch(
            "workflows.daily_check._get_all_entries_parsed",
            return_value=[_parsed(0), _parsed(1)],
        ), patch(
            "workflows.daily_check.RecordCache.get", side_effect=_invite_cache
        ), patch(
            "workflows.daily_check.can_send_connections", return_value=True
        ), patch(
            "workflows.daily_check.get_remaining",
            return_value={"connections": 25, "messages": 30, "visits": 50},
        ), patch(
            "workflows.daily_check.write_prospects_to_sheet", sheet
        ), patch(
            "workflows.daily_check.personalize", return_value="   "
        ), pytest.raises(BlankMessageError, match="connection_note"):
            run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=dr,
            )

        # Nothing reached any transport or budget.
        pb.launch_agent.assert_not_called()
        sheet.assert_not_called()
        dr.reserve_send.assert_not_called()

    @patch.dict(os.environ, _ENV)
    def test_blank_notes_halt_a_dry_run_too(self):
        """The invite dry run is the note-copy QA pass; a blank note must
        abort it rather than print an empty `msg:` preview line."""
        from workflows.daily_check import run_connection_requests

        attio = _invite_attio(1)
        dr = fake_daily_run()

        with patch.dict(os.environ, _ENV), patch(
            "workflows.daily_check._get_all_entries_parsed",
            return_value=[_parsed(0)],
        ), patch(
            "workflows.daily_check.RecordCache.get", side_effect=_invite_cache
        ), patch(
            "workflows.daily_check.can_send_connections", return_value=True
        ), patch(
            "workflows.daily_check.get_remaining",
            return_value={"connections": 25, "messages": 30, "visits": 50},
        ), patch(
            "workflows.daily_check.personalize", return_value=""
        ), pytest.raises(BlankMessageError, match="connection_note"):
            run_connection_requests(
                attio=attio,
                pb=MagicMock(),
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                dry_run=True,
                daily_run=dr,
            )


def _run_invites(attio, parsed_entries, *, env_extra: dict | None = None):
    """Drive run_connection_requests over caller-supplied parsed entries with
    an injected invite sender. Returns (result, sender)."""
    from workflows.daily_check import run_connection_requests

    sender = _pb_invite_sender(requested_count=1)
    env = {**_ENV, **(env_extra or {})}
    with patch.dict(os.environ, env), patch(
        "workflows.daily_check._get_all_entries_parsed", return_value=parsed_entries
    ), patch(
        "workflows.daily_check.RecordCache.get", side_effect=_invite_cache
    ), patch(
        "workflows.daily_check.can_send_connections", return_value=True
    ), patch(
        "workflows.daily_check.get_remaining",
        return_value={"connections": 25, "messages": 30, "visits": 50},
    ), patch(
        "workflows.daily_check.write_prospects_to_sheet",
        return_value="https://sheet/x",
    ), patch(
        "workflows.daily_check.should_advance_batch", return_value=False
    ), patch(
        "workflows.daily_check.emit_pb_silent_no_op"
    ), patch(
        "workflows.daily_check.record_connections"
    ), patch(
        "workflows.daily_check.record_visits"
    ), patch(
        "workflows.daily_check.recheck_cache"
    ) as mock_rc:
        mock_rc.partition.side_effect = lambda urls: ({}, [])
        mock_rc.RECHECK_TTL_DAYS = 3
        result = run_connection_requests(
            attio=attio,
            pb=MagicMock(),
            network_booster_id="agent-nb-001",
            auto_confirm=True,
            daily_run=fake_daily_run(),
            sender=sender,
        )
    return result, sender


class TestInvitePbOwnsAllInvites:
    """The PB invite path is the ONLY invite path."""

    def test_pb_path_runs_unconditionally(self):
        """The PB launch fires for an unstamped pool with no flag involved —
        the regression anchor for PB-owned invites."""
        attio = _invite_attio(2)
        result, sender = _run_invites(attio, [_parsed(0), _parsed(1)])

        sender.launch_invite_batch.assert_called_once()
        assert "botdog_submitted" not in result

    def test_env_flag_cannot_reroute_invites(self):
        """A stale BOTDOG_SEND_ENABLED=1 in .env must not reroute the batch:
        invites still launch via PB and no Botdog sender is ever built."""
        from workflows import daily_check

        attio = _invite_attio(2)
        with patch.object(
            daily_check,
            "_build_botdog_sender",
            side_effect=AssertionError("no send path may build a Botdog sender"),
        ) as builder:
            result, sender = _run_invites(
                attio, [_parsed(0), _parsed(1)], env_extra={"BOTDOG_SEND_ENABLED": "1"}
            )

        sender.launch_invite_batch.assert_called_once()
        builder.assert_not_called()
        assert "botdog_submitted" not in result


class TestInviteAssemblyExcludesBotdogStampedRows:
    """Double-invite guard.

    A prospect stamped ``send_channel=botdog`` may already sit in a Botdog
    campaign. Until it is re-stamped ``pb``, the PB batch must exclude it —
    otherwise the prospect gets a second first-touch from a second transport.
    The exclusion is UNCONDITIONAL: no flag lifts it.
    """

    def test_stamped_rows_are_excluded_from_the_pb_batch(self, capsys):
        attio = _invite_attio(2)
        result, sender = _run_invites(
            attio, [_parsed(0, channel="botdog"), _parsed(1, channel="pb")]
        )

        assert result["botdog_excluded"] == 1
        # Residual census: counts stamped rows at ANY stage, so limbo rows
        # outside the invite slice stay visible too.
        assert result["botdog_stamped_total"] == 1
        batch = sender.launch_invite_batch.call_args.args[0]
        assert [row["record_id"] for row in batch] == ["rec-ch-001"]
        err = capsys.readouterr().err
        assert "botdog-stamped prospect(s) excluded" in err
        assert "Botdog residual" in err
        assert "re-stamped send_channel=pb" in err

    def test_census_counts_stamped_rows_outside_the_invite_slice(self, capsys):
        """A stamped CONNECTION_SENT row is invisible to the invite slice, the
        DM queue AND the Phase 0 / 0.5 scrape detectors — the census is the one
        instrument that still reports it."""
        attio = _invite_attio(2)
        result, sender = _run_invites(
            attio,
            [
                _parsed(0, stage="Connection Sent", channel="botdog"),
                _parsed(1, channel="pb"),
            ],
        )

        assert result["botdog_excluded"] == 0  # not in the invite slice
        assert result["botdog_stamped_total"] == 1  # still counted
        assert "Botdog residual" in capsys.readouterr().err

    def test_env_flag_does_not_lift_the_exclusion(self):
        attio = _invite_attio(2)
        result, sender = _run_invites(
            attio,
            [_parsed(0, channel="botdog"), _parsed(1, channel="pb")],
            env_extra={"BOTDOG_SEND_ENABLED": "1"},
        )

        assert result["botdog_excluded"] == 1
        batch = sender.launch_invite_batch.call_args.args[0]
        assert [row["record_id"] for row in batch] == ["rec-ch-001"]

    def test_an_all_botdog_pool_sends_nothing(self):
        attio = _invite_attio(2)
        result, sender = _run_invites(
            attio, [_parsed(0, channel="botdog"), _parsed(1, channel="botdog")]
        )

        assert result["botdog_excluded"] == 2
        sender.launch_invite_batch.assert_not_called()
        assert result["sent"] == 0

    def test_unstamped_and_pb_rows_are_untouched(self):
        attio = _invite_attio(2)
        result, sender = _run_invites(attio, [_parsed(0), _parsed(1, channel="pb")])

        assert result["botdog_excluded"] == 0
        assert result["botdog_stamped_total"] == 0
        batch = sender.launch_invite_batch.call_args.args[0]
        assert len(batch) == 2


class TestDueDmCountsHoldOut:
    def test_botdog_stamped_rows_do_not_count_as_due(self, monkeypatch):
        """compute_due_dm_counts feeds the run-end summary and the starvation
        signal — a held-out row counting as "due" would prop up pipeline-health
        metrics with rows no transport will touch."""
        from workflows import daily_check

        entries = [_dm_entry(0, "pb"), _dm_entry(1, "botdog")]
        monkeypatch.setattr(daily_check, "_get_all_entries_parsed", lambda _: entries)
        monkeypatch.setattr(
            "workflows.weekly_prospect._attio_inner_client", lambda crm: crm
        )
        counts = daily_check.compute_due_dm_counts(
            MagicMock(), cache=MagicMock(), today=date.today()
        )
        assert counts["due_dm1_count"] == 1


class TestPhase0ScopeSkip:
    """Phase 0 (acceptance detection) is scrape-driven. A botdog-stamped row's
    accept would arrive as a lead event instead, so scraping it wastes a
    profile visit and races the event-confirmed flip.

    Observable: the scope guard sits BEFORE the ``cache.get`` lookup in both
    Phase-0 collection loops, so the set of record ids the cache was asked
    about IS the scraped scope.
    """

    def _scoped_record_ids(self, monkeypatch, entries) -> set[str]:
        from workflows import daily_check

        monkeypatch.setenv("ATTIO_LIST_ID", "list-001")
        monkeypatch.setattr(daily_check, "_get_all_entries_parsed", lambda _: entries)
        monkeypatch.setattr(daily_check, "escalate", MagicMock(return_value={"id": "r"}))
        monkeypatch.setattr(
            daily_check,
            "write_prospects_to_sheet",
            lambda rows, **kw: "https://sheet/x",
        )
        monkeypatch.setattr(daily_check, "_pb_session_args", lambda: {})

        cache = MagicMock()
        cache.get.side_effect = lambda rid: (
            f"Alice {rid}",
            "Acme Foods",
            f"https://linkedin.com/in/acme-{rid}",
            "",
            "",
        )

        pb = MagicMock()
        pb.launch_agent.return_value = MagicMock(container_id="c0")
        pb.wait_for_completion.return_value = MagicMock(
            status="finished", log_output=""
        )
        pb.download_result_csv.return_value = ""

        daily_check.detect_accepted_connections(
            MagicMock(), pb, "scraper-id", cache=cache
        )
        return {
            rid for call in cache.get.call_args_list for rid in call.args
        }

    def _cs_row(self, idx: int, channel: str | None) -> dict:
        return {
            "record_id": f"cs{idx}",
            "entry_id": f"entcs{idx}",
            "stage": PipelineStage.CONNECTION_SENT.value,
            "last_contact_date": (date.today() - timedelta(days=2)).isoformat(),
            "quality_score": 75,
            "persona": "operations_leaders",
            "language": "es",
            "send_channel": channel,
            "prospect_committed_at": None,
        }

    def _prospect_row(self, idx: int, channel: str | None) -> dict:
        return {
            "record_id": f"pr{idx}",
            "entry_id": f"entpr{idx}",
            "stage": PipelineStage.PROSPECT.value,
            "last_contact_date": None,
            "quality_score": 75,
            "persona": "operations_leaders",
            "language": "es",
            "send_channel": channel,
            "prospect_committed_at": (date.today() - timedelta(days=30)).isoformat(),
        }

    def test_stamped_connection_sent_rows_are_out_of_scope(self, monkeypatch):
        scoped = self._scoped_record_ids(
            monkeypatch, [self._cs_row(0, "pb"), self._cs_row(1, "botdog")]
        )
        assert "cs0" in scoped
        assert "cs1" not in scoped

    def test_unstamped_connection_sent_rows_stay_in_scope(self, monkeypatch):
        """The default (`pb`) must keep the row scrape-detected — the hold-out
        is opt-in via the stamp, never the other way round."""
        row = self._cs_row(2, None)
        del row["send_channel"]
        scoped = self._scoped_record_ids(monkeypatch, [row])
        assert "cs2" in scoped

    def test_stamped_prospect_rows_are_out_of_the_already_1st_sweep(
        self, monkeypatch
    ):
        """The PROSPECT already-1st-degree sweep applies the same scope guard
        — a stamped row is event-driven, not scrape-driven."""
        scoped = self._scoped_record_ids(
            monkeypatch,
            [self._prospect_row(0, "pb"), self._prospect_row(1, "botdog")],
        )
        assert "pr0" in scoped
        assert "pr1" not in scoped


class TestPhase05ScopeSkip:
    """Phase 0.5 (reply detection) is inbox-scrape-driven. A botdog-stamped
    row's reply would arrive as a lead event, so keeping it in the SN-inbox
    scope would race / double-write the event-confirmed flip.

    Observable: the operator-facing scope line — ``Checking N prospects in DM
    stages`` (or ``No prospects in DM stages``) — is printed straight off the
    post-guard ``dm_prospects`` list. (``cache.get`` is NOT a usable probe: the
    cadence-drift index below the guard looks up every entry in the pipeline,
    stamped or not.)
    """

    def _dm_stage_entry(self, idx: int, channel: str | None) -> dict:
        entry = _make_attio_entry(
            entry_id=f"entry-r-{idx:03d}",
            record_id=f"rec-r-{idx:03d}",
            stage="DM1 Sent",
            quality_score=75,
        )
        entry["_send_channel"] = channel
        return entry

    def _scope_line(self, monkeypatch, capsys, raw_entries) -> str:
        from workflows import detect_responses

        monkeypatch.setenv("ATTIO_LIST_ID", "list-001")

        # `AttioClient.parse_entry` does not extract `send_channel`, so the
        # stamp is threaded through a sidecar key here (see the port report).
        _real_parse = AttioClient.parse_entry

        def _parse(entry):
            attrs = _real_parse(entry)
            if entry.get("_send_channel") is not None:
                attrs["send_channel"] = entry["_send_channel"]
            return attrs

        monkeypatch.setattr(
            detect_responses.AttioClient, "parse_entry", staticmethod(_parse)
        )
        attio = MagicMock()
        attio.query_list_entries.return_value = raw_entries

        cache = MagicMock()
        cache.get.side_effect = lambda rid: (
            f"Alice {rid}",
            "Acme Foods",
            f"https://linkedin.com/in/acme-{rid}",
            "",
            "",
        )

        pb = MagicMock()
        pb.launch_agent.return_value = MagicMock(container_id="c0")
        pb.wait_for_completion.return_value = MagicMock(
            status="finished", log_output=""
        )
        pb.download_result_csv.return_value = ""

        # The scope is fixed BEFORE the inbox scrape launches, so the
        # downstream no-CSV halt (PR-19) is irrelevant to what this asserts.
        with contextlib.suppress(detect_responses.NoCSVHalt):
            detect_responses.detect_responses(
                attio,
                pb,
                "inbox-scraper-id",
                cache=cache,
                daily_run=MagicMock(),
            )
        return capsys.readouterr().out

    def test_stamped_dm_rows_are_skipped(self, monkeypatch, capsys):
        out = self._scope_line(
            monkeypatch,
            capsys,
            [self._dm_stage_entry(0, "pb"), self._dm_stage_entry(1, "botdog")],
        )
        assert "Checking 1 prospects in DM stages" in out, (
            "the botdog-stamped DM row must not enter the inbox-scrape scope"
        )

    def test_an_all_stamped_pipeline_leaves_the_scope_empty(
        self, monkeypatch, capsys
    ):
        out = self._scope_line(
            monkeypatch, capsys, [self._dm_stage_entry(1, "botdog")]
        )
        assert "No prospects in DM stages" in out

    def test_unstamped_dm_rows_stay_in_scope(self, monkeypatch, capsys):
        """The default (`pb`) keeps the row inbox-scrape-detected — the
        hold-out is opt-in via the stamp, never the other way round."""
        out = self._scope_line(
            monkeypatch, capsys, [self._dm_stage_entry(2, None)]
        )
        assert "Checking 1 prospects in DM stages" in out




class TestParseEntrySurfacesTheStamp:
    """`AttioClient.parse_entry` must surface `send_channel`.

    EVERY consumer of the stamp reads the flat dict this builds — the
    invite exclusion, the DM queue hold-out, the Phase 0 / 0.5 scope skips
    and the event-ingest scope guard. If the extraction is missing, all of
    them read "pb" for every real row, the hold-out never fires, and the
    residual census prints a reassuring 0 while a stamped prospect takes a
    second first-touch from PhantomBuster. Every other test in this file
    hand-builds attrs dicts, so this is the only place that would catch it.
    """

    @staticmethod
    def _entry(stamp: str | None) -> dict:
        values: dict = {"stage": [{"status": {"title": "Prospect"}}]}
        if stamp is not None:
            values["send_channel"] = [{"value": stamp}]
        return {
            "id": {"entry_id": "entry-acme-1", "record_id": "rec-acme-1"},
            "entry_values": values,
        }

    def test_stamped_entry_round_trips_through_parse_entry(self):
        from clients.attio import AttioClient
        from workflows.daily_check_helpers import (
            SEND_CHANNEL_BOTDOG,
            _resolve_send_channel,
        )

        attrs = AttioClient.parse_entry(self._entry("botdog"))

        assert attrs["send_channel"] == "botdog"
        assert _resolve_send_channel(attrs) == SEND_CHANNEL_BOTDOG

    def test_absent_attribute_resolves_to_pb(self):
        """A workspace that never provisioned the attribute must read `pb`,
        not crash and not hold every row out of every send."""
        from clients.attio import AttioClient
        from workflows.daily_check_helpers import (
            SEND_CHANNEL_PB,
            _resolve_send_channel,
        )

        attrs = AttioClient.parse_entry(self._entry(None))

        assert attrs["send_channel"] is None
        assert _resolve_send_channel(attrs) == SEND_CHANNEL_PB


@pytest.fixture(autouse=True)
def _fresh_state_boundary():
    from unittest.mock import patch

    from workflows.email_send_guard import GuardResult
    with patch("workflows.daily_check.verify_send_preconditions", return_value=GuardResult(True)), patch("workflows.email_campaign.verify_email_send_preconditions", return_value=GuardResult(True)), patch("workflows.dm_quality_gate.require_clear_dm_quality_queue"):
        yield
