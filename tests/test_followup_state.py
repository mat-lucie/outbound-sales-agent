"""Tests for workflows.followup_state (the §3.15 writer for followup_* attrs)
and the followup-stamp/snooze/mute/callback CLI commands.

The writer must build correct WriteIntents (right object, is_list_entry, updates,
writer_module) and require list_id for the linkedin_outreach list path. The CLI
commands must route through it and exit cleanly.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from workflows import followup_state


def _intent(writer_mock):
    """The WriteIntent passed to writer.apply()."""
    return writer_mock.apply.call_args[0][0]


def test_stamp_draft_builds_deal_intent():
    writer = MagicMock()
    followup_state.stamp_draft(writer, object="deals", record_id="d1", draft_id="gm-123")
    intent = _intent(writer)
    assert intent.object == "deals"
    assert intent.is_list_entry is False
    assert intent.list_id is None
    assert intent.updates["followup_draft_id"] == "gm-123"
    assert "followup_draft_at" in intent.updates
    assert intent.writer_module == "workflows.followup_state"


def test_list_write_sets_is_list_entry_and_list_id():
    writer = MagicMock()
    followup_state.set_muted(
        writer, object="linkedin_outreach", record_id="ent-1", list_id="L-9", muted=True
    )
    intent = _intent(writer)
    assert intent.is_list_entry is True
    assert intent.list_id == "L-9"
    assert intent.updates == {"followup_muted": True}


def test_list_write_requires_list_id():
    writer = MagicMock()
    with pytest.raises(ValueError):
        followup_state.set_muted(writer, object="linkedin_outreach", record_id="ent-1")


def test_set_snooze_and_callback_serialize_dates():
    writer = MagicMock()
    followup_state.set_snooze(writer, object="deals", record_id="d1", until=date(2026, 8, 1))
    assert _intent(writer).updates == {"followup_snooze_until": "2026-08-01"}

    writer2 = MagicMock()
    followup_state.set_callback(writer2, object="deals", record_id="d1", callback=date(2026, 9, 15))
    assert _intent(writer2).updates == {"followup_callback_date": "2026-09-15"}


def test_clear_callback_builds_null_intent():
    writer = MagicMock()
    followup_state.clear_callback(writer, object="deals", record_id="d1")
    intent = _intent(writer)
    assert intent.object == "deals"
    assert intent.is_list_entry is False
    assert intent.updates == {"followup_callback_date": None}
    assert intent.writer_module == "workflows.followup_state"


def test_clear_callback_list_path_requires_and_carries_list_id():
    writer = MagicMock()
    with pytest.raises(ValueError):
        followup_state.clear_callback(writer, object="linkedin_outreach", record_id="ent-1")

    writer2 = MagicMock()
    followup_state.clear_callback(
        writer2, object="linkedin_outreach", record_id="ent-1", list_id="L-9"
    )
    intent = _intent(writer2)
    assert intent.is_list_entry is True
    assert intent.list_id == "L-9"
    assert intent.updates == {"followup_callback_date": None}


# ── stamp_referred_by ──────────────────────────────────────────────────────


def test_stamp_referred_by_builds_deal_intent():
    writer = MagicMock()
    followup_state.stamp_referred_by(writer, deal_id="d1", partner_email="harlyn@partner.com")
    intent = _intent(writer)
    assert intent.object == "deals"
    assert intent.record_id == "d1"
    assert intent.is_list_entry is False
    assert intent.list_id is None
    assert intent.updates == {"referred_by": "harlyn@partner.com"}
    assert intent.writer_module == "workflows.followup_state"


def test_stamp_referred_by_normalizes_email():
    writer = MagicMock()
    followup_state.stamp_referred_by(writer, deal_id="d1", partner_email="  Harlyn@Partner.COM  ")
    assert _intent(writer).updates == {"referred_by": "harlyn@partner.com"}


@pytest.mark.parametrize(
    "junk",
    [
        "not-an-email",          # no @
        "has space@partner.com",  # internal whitespace
        "@partner.com",          # empty local part
        "harlyn@",               # empty domain
        "harlyn@localhost",      # domain not dotted
        "a@b@c.com",             # two @
        "",                      # empty
    ],
)
def test_stamp_referred_by_rejects_junk_email(junk):
    writer = MagicMock()
    with pytest.raises(ValueError):
        followup_state.stamp_referred_by(writer, deal_id="d1", partner_email=junk)
    writer.apply.assert_not_called()


def test_stamp_referred_by_is_deals_only_by_signature():
    """stamp_referred_by takes no `object` param — it is structurally deals-only
    (there is no way to target linkedin_outreach), and always builds a deals
    intent."""
    import inspect

    params = inspect.signature(followup_state.stamp_referred_by).parameters
    assert "object" not in params
    assert set(params) >= {"writer", "deal_id", "partner_email"}


# ── mute_batch ───────────────────────────────────────────────────────────


def test_mute_batch_happy_path_builds_correct_intents():
    writer = MagicMock()
    succeeded, failed = followup_state.mute_batch(
        writer, object="linkedin_outreach", record_ids=["e1", "e2", "e3"], list_id="L-9"
    )
    assert succeeded == ["e1", "e2", "e3"]
    assert failed == []
    # One apply per id, each a correct mute intent on the list.
    assert writer.apply.call_count == 3
    intents = [call.args[0] for call in writer.apply.call_args_list]
    assert [i.record_id for i in intents] == ["e1", "e2", "e3"]
    for i in intents:
        assert i.object == "linkedin_outreach"
        assert i.is_list_entry is True
        assert i.list_id == "L-9"
        assert i.updates == {"followup_muted": True}


def test_mute_batch_partial_failure_continues():
    writer = MagicMock()

    def apply_side_effect(intent):
        if intent.record_id == "e2":
            raise RuntimeError("boom")
        return {}

    writer.apply.side_effect = apply_side_effect
    succeeded, failed = followup_state.mute_batch(
        writer, object="linkedin_outreach", record_ids=["e1", "e2", "e3"], list_id="L-9"
    )
    # First and third still processed despite the second raising.
    assert succeeded == ["e1", "e3"]
    assert len(failed) == 1
    assert failed[0][0] == "e2"
    assert "boom" in failed[0][1]
    assert writer.apply.call_count == 3


def test_mute_batch_requires_list_id_before_any_write():
    writer = MagicMock()
    with pytest.raises(ValueError):
        followup_state.mute_batch(writer, object="linkedin_outreach", record_ids=["e1", "e2"])
    writer.apply.assert_not_called()


# ── CLI ──────────────────────────────────────────────────────────────────


def test_cli_followup_mute_ok(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    # Patch AttioWriter so no network/construction happens; the state fn calls
    # .apply() on the instance (a MagicMock), which is a harmless no-op.
    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    result = CliRunner().invoke(cli, ["followup-mute", "--object", "deals", "--id", "d1"])
    assert result.exit_code == 0
    assert "ok:" in result.output


def test_cli_snooze_rejects_absurd_date(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    result = CliRunner().invoke(
        cli, ["followup-snooze", "--object", "deals", "--id", "d1", "--until", "2099-01-01"]
    )
    assert result.exit_code == 1
    assert "typo" in result.output


def test_cli_followup_callback_set_ok(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    result = CliRunner().invoke(
        cli, ["followup-callback", "--object", "deals", "--id", "d1", "--date", "2026-09-15"]
    )
    assert result.exit_code == 0
    assert "ok: set_callback deals:d1" in result.output


def test_cli_followup_callback_set_rejects_absurd_date(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    # --date optional now (for --clear) — the horizon guard must still fire.
    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    result = CliRunner().invoke(
        cli, ["followup-callback", "--object", "deals", "--id", "d1", "--date", "2099-01-01"]
    )
    assert result.exit_code == 1
    assert "typo" in result.output


def test_cli_followup_callback_clear(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    result = CliRunner().invoke(cli, ["followup-callback", "--object", "deals", "--id", "d1", "--clear"])
    assert result.exit_code == 0
    assert "ok: clear_callback deals:d1" in result.output


def test_cli_followup_callback_requires_exactly_one_of_date_or_clear(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    for args in (
        ["followup-callback", "--object", "deals", "--id", "d1"],  # neither
        ["followup-callback", "--object", "deals", "--id", "d1", "--date", "2026-09-15", "--clear"],  # both
    ):
        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 1
        assert "exactly one" in result.output


def test_cli_followup_callback_clear_linkedin_requires_list_id(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    monkeypatch.delenv("ATTIO_LIST_ID", raising=False)
    result = CliRunner().invoke(
        cli, ["followup-callback", "--object", "linkedin_outreach", "--id", "ent-1", "--clear"]
    )
    assert result.exit_code == 1
    assert "ATTIO_LIST_ID" in result.output


def test_cli_followup_stamp_linkedin_requires_list_id(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    monkeypatch.delenv("ATTIO_LIST_ID", raising=False)
    result = CliRunner().invoke(
        cli,
        ["followup-stamp", "--object", "linkedin_outreach", "--id", "ent-1", "--draft-id", "gm-1"],
    )
    assert result.exit_code == 1
    assert "ATTIO_LIST_ID" in result.output


# ── CLI: followup-refer ────────────────────────────────────────────────────


def test_cli_followup_refer_ok(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    result = CliRunner().invoke(
        cli, ["followup-refer", "--id", "d1", "--partner-email", "Harlyn@Partner.com"]
    )
    assert result.exit_code == 0
    assert "ok: stamp_referred_by deals:d1" in result.output


def test_cli_followup_refer_invalid_email_exits_1(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    result = CliRunner().invoke(
        cli, ["followup-refer", "--id", "d1", "--partner-email", "not-an-email"]
    )
    assert result.exit_code == 1
    assert "ERROR: stamp_referred_by failed" in result.output
    assert "ValueError" in result.output


# ── CLI: followup-mute-batch ───────────────────────────────────────────────


def test_cli_mute_batch_happy_path(monkeypatch, tmp_path):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    id_file = tmp_path / "ids.txt"
    id_file.write_text("d1\nd2\nd3\n")
    # click >= 8.2 always captures streams separately (no mix_stderr param);
    # result.stdout / result.stderr assert the pipe-clean contract directly.
    result = CliRunner().invoke(
        cli, ["followup-mute-batch", "--object", "deals", "--file", str(id_file)]
    )
    assert result.exit_code == 0
    assert "Muted 3 of 3" in result.stderr
    # Success case: stdout completely empty (pipe-clean — no summary on stdout).
    assert result.stdout == ""


def test_cli_mute_batch_filters_comments_and_blanks(monkeypatch, tmp_path):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    id_file = tmp_path / "ids.txt"
    # Blank lines, a comment, and a duplicate — only d1 and d2 count.
    id_file.write_text("# header comment\nd1\n\n  d2  \nd1\n")
    result = CliRunner().invoke(
        cli, ["followup-mute-batch", "--object", "deals", "--file", str(id_file)]
    )
    assert result.exit_code == 0
    assert "Muted 2 of 2" in result.stderr
    assert result.stdout == ""


def test_cli_mute_batch_empty_file_exits_1(monkeypatch, tmp_path):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    id_file = tmp_path / "ids.txt"
    id_file.write_text("# only comments\n\n   \n")
    result = CliRunner().invoke(
        cli, ["followup-mute-batch", "--object", "deals", "--file", str(id_file)]
    )
    assert result.exit_code == 1
    assert "no ids" in result.stderr
    assert result.stdout == ""


def test_cli_mute_batch_partial_failure_lists_failed_ids(monkeypatch, tmp_path):
    from click.testing import CliRunner

    from cli import cli

    # AttioWriter() returns an instance whose .apply raises for d2 only.
    instance = MagicMock()

    def apply_side_effect(intent):
        if intent.record_id == "d2":
            raise RuntimeError("boom")
        return {}

    instance.apply.side_effect = apply_side_effect
    monkeypatch.setattr("clients.attio_writer.AttioWriter", lambda *a, **k: instance)

    id_file = tmp_path / "ids.txt"
    id_file.write_text("d1\nd2\nd3\n")
    result = CliRunner().invoke(
        cli, ["followup-mute-batch", "--object", "deals", "--file", str(id_file)]
    )
    assert result.exit_code == 1
    # Stream separation: stdout is EXACTLY the failed ids (one per line) so
    # `... > retry.txt` yields a directly re-runnable file — the summary line
    # must never leak into it as a phantom record id.
    assert result.stdout == "d2\n"
    assert "Muted" not in result.stdout
    # Everything human-facing lives on stderr.
    assert "Muted 2 of 3" in result.stderr
    assert "FAILED d2: RuntimeError: boom" in result.stderr
    assert "Failed ids:" in result.stderr


# ── stamp_verified_touch / followup-touch (v2) ─────────────────────────────


def test_stamp_verified_touch_builds_deal_intent():
    writer = MagicMock()
    followup_state.stamp_verified_touch(writer, deal_id="d1", touch=date(2026, 3, 20))
    intent = _intent(writer)
    assert intent.object == "deals"
    assert intent.record_id == "d1"
    assert intent.is_list_entry is False
    assert intent.list_id is None
    assert intent.updates == {"last_verified_touch": "2026-03-20"}
    assert intent.writer_module == "workflows.followup_state"


def test_stamp_verified_touch_rejects_future_date():
    """A verified touch is a past event — a future stamp would silently hide
    the deal from the radar (negative silence). Loud ValueError, no write."""
    writer = MagicMock()
    with pytest.raises(ValueError, match="future"):
        followup_state.stamp_verified_touch(writer, deal_id="d1", touch=date(9999, 1, 1))
    writer.apply.assert_not_called()


def test_stamp_verified_touch_allows_one_day_utc_skew():
    """The skill layer extracts dates from UTC timestamps, which read
    'tomorrow' during the operator's evening — one day of future skew must
    stamp, two must not."""
    from datetime import timedelta

    from models.business_calendar import operator_today

    writer = MagicMock()
    followup_state.stamp_verified_touch(
        writer, deal_id="d1", touch=operator_today() + timedelta(days=1)
    )
    writer.apply.assert_called_once()

    writer2 = MagicMock()
    with pytest.raises(ValueError, match="future"):
        followup_state.stamp_verified_touch(
            writer2, deal_id="d1", touch=operator_today() + timedelta(days=2)
        )
    writer2.apply.assert_not_called()


def test_stamp_verified_touch_rejects_ancient_date():
    """A stamp >400 days back is almost certainly a year typo — it would pin
    the deal as maximally stale and, being tier 1, no real data could ever
    contradict it. Loud ValueError, no write."""
    from datetime import timedelta

    from models.business_calendar import operator_today

    writer = MagicMock()
    with pytest.raises(ValueError, match="typo"):
        followup_state.stamp_verified_touch(
            writer, deal_id="d1", touch=operator_today() - timedelta(days=401)
        )
    writer.apply.assert_not_called()


def test_clear_verified_touch_builds_null_intent():
    """The authorized correction path: nulling drops the deal back to the
    interaction-join / created_at tiers."""
    writer = MagicMock()
    followup_state.clear_verified_touch(writer, deal_id="d1")
    intent = _intent(writer)
    assert intent.object == "deals"
    assert intent.is_list_entry is False
    assert intent.updates == {"last_verified_touch": None}
    assert intent.writer_module == "workflows.followup_state"


def test_stamp_verified_touch_is_deals_only_by_signature():
    """No `object` param — structurally deals-only, like stamp_referred_by."""
    import inspect

    params = inspect.signature(followup_state.stamp_verified_touch).parameters
    assert "object" not in params
    assert set(params) >= {"writer", "deal_id", "touch"}


def test_cli_followup_touch_ok(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    result = CliRunner().invoke(
        cli, ["followup-touch", "--id", "d1", "--date", "2026-03-20"]
    )
    assert result.exit_code == 0
    assert "ok: stamp_verified_touch deals:d1" in result.output


def test_cli_followup_touch_rejects_future_date(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    # Far-future → the writer's past-event guard refuses, cleanly.
    result = CliRunner().invoke(
        cli, ["followup-touch", "--id", "d1", "--date", "2099-01-01"]
    )
    assert result.exit_code == 1
    assert "future" in result.output


def test_cli_followup_touch_rejects_garbage_date(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    result = CliRunner().invoke(
        cli, ["followup-touch", "--id", "d1", "--date", "not-a-date"]
    )
    # Clean one-line ERROR + exit 1 — never a raw traceback.
    assert result.exit_code == 1
    assert "ERROR: followup-touch failed" in result.output


def test_cli_followup_touch_clear(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    result = CliRunner().invoke(cli, ["followup-touch", "--id", "d1", "--clear"])
    assert result.exit_code == 0
    assert "ok: clear_verified_touch deals:d1" in result.output


def test_cli_followup_touch_requires_exactly_one_of_date_or_clear(monkeypatch):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    for args in (
        ["followup-touch", "--id", "d1"],  # neither
        ["followup-touch", "--id", "d1", "--date", "2026-03-20", "--clear"],  # both
    ):
        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 1
        assert "exactly one" in result.output


# ── awaiting_reply_* writers (WAITING lane, v3) ─────────────────────────────


def test_set_awaiting_reply_builds_intent_and_co_stamps_deal_clock():
    writer = MagicMock()
    followup_state.set_awaiting_reply(
        writer, object="deals", record_id="d1",
        since=date(2026, 7, 8), thread_id="thr-1",
    )
    intent = _intent(writer)
    assert intent.object == "deals"
    assert intent.updates["awaiting_reply_since"] == "2026-07-08"
    assert intent.updates["awaiting_reply_thread_id"] == "thr-1"
    # Dual-clock coherence: a deal's send IS a verified touch — SAME intent.
    assert intent.updates["last_verified_touch"] == "2026-07-08"


def test_set_awaiting_reply_entry_does_not_co_stamp():
    writer = MagicMock()
    followup_state.set_awaiting_reply(
        writer, object="linkedin_outreach", record_id="ent-1",
        since=date(2026, 7, 8), thread_id="thr-1", list_id="L-9",
    )
    intent = _intent(writer)
    assert intent.is_list_entry is True
    assert "last_verified_touch" not in intent.updates


def test_set_awaiting_reply_overwrites_no_read_guard():
    # The stamp tracks the MOST RECENT unanswered send — re-stamping must be
    # a plain write (no read-before-write skip); two calls, two intents.
    writer = MagicMock()
    followup_state.set_awaiting_reply(
        writer, object="deals", record_id="d1", since=date(2026, 7, 1), thread_id="t1",
    )
    followup_state.set_awaiting_reply(
        writer, object="deals", record_id="d1", since=date(2026, 7, 8), thread_id="t1",
    )
    assert writer.apply.call_count == 2


def test_set_awaiting_reply_rejects_future_and_ancient_dates():
    writer = MagicMock()
    with pytest.raises(ValueError):
        followup_state.set_awaiting_reply(
            writer, object="deals", record_id="d1",
            since=date(9999, 1, 1), thread_id="t",
        )
    with pytest.raises(ValueError):
        followup_state.set_awaiting_reply(
            writer, object="deals", record_id="d1",
            since=date(2020, 1, 1), thread_id="t",
        )
    writer.apply.assert_not_called()


def test_set_awaiting_reply_requires_list_id_for_entries():
    writer = MagicMock()
    with pytest.raises(ValueError):
        followup_state.set_awaiting_reply(
            writer, object="linkedin_outreach", record_id="ent-1",
            since=date(2026, 7, 8), thread_id="t",
        )


def test_clear_awaiting_reply_keeps_note_id():
    writer = MagicMock()
    followup_state.clear_awaiting_reply(writer, object="deals", record_id="d1")
    updates = _intent(writer).updates
    assert updates == {
        "awaiting_reply_since": None,
        "awaiting_reply_thread_id": None,
        "awaiting_reply_nudge_count": None,
    }
    assert "awaiting_reply_note_id" not in updates  # canonical note survives


def test_increment_awaiting_nudge_counts_and_persists_note_id():
    writer = MagicMock()
    followup_state.increment_awaiting_nudge(
        writer, object="deals", record_id="d1", current_count=0, note_id="note-9",
    )
    updates = _intent(writer).updates
    assert updates["awaiting_reply_nudge_count"] == 1
    assert updates["awaiting_reply_note_id"] == "note-9"


def test_increment_awaiting_nudge_ceiling_is_unbypassable():
    writer = MagicMock()
    with pytest.raises(ValueError):
        followup_state.increment_awaiting_nudge(
            writer, object="deals", record_id="d1", current_count=2,
        )
    writer.apply.assert_not_called()


def test_set_awaiting_note_id_rejects_empty():
    writer = MagicMock()
    with pytest.raises(ValueError):
        followup_state.set_awaiting_note_id(
            writer, object="deals", record_id="d1", note_id="   ",
        )


# ── followup-await CLI ──────────────────────────────────────────────────────


def _invoke_await(monkeypatch, args):
    from click.testing import CliRunner

    from cli import cli

    monkeypatch.setattr("clients.attio_writer.AttioWriter", MagicMock)
    return CliRunner().invoke(cli, ["followup-await", *args])


def test_cli_await_since_ok(monkeypatch):
    result = _invoke_await(monkeypatch, [
        "--object", "deals", "--id", "d1",
        "--since", "2026-07-08", "--thread", "thr-1",
    ])
    assert result.exit_code == 0
    assert "ok: set_awaiting_reply" in result.output


def test_cli_await_since_requires_thread(monkeypatch):
    result = _invoke_await(monkeypatch, [
        "--object", "deals", "--id", "d1", "--since", "2026-07-08",
    ])
    assert result.exit_code == 1
    assert "--thread" in result.output


def test_cli_await_rejects_multiple_modes(monkeypatch):
    result = _invoke_await(monkeypatch, [
        "--object", "deals", "--id", "d1",
        "--since", "2026-07-08", "--thread", "t", "--clear",
    ])
    assert result.exit_code == 1
    assert "exactly one mode" in result.output


def test_cli_await_resolved_requires_clear(monkeypatch):
    result = _invoke_await(monkeypatch, [
        "--object", "deals", "--id", "d1",
        "--nudged", "--resolved", "2026-07-10",
    ])
    assert result.exit_code == 1
    assert "--resolved" in result.output


def test_cli_await_future_since_exits_1(monkeypatch):
    result = _invoke_await(monkeypatch, [
        "--object", "deals", "--id", "d1",
        "--since", "9999-01-01", "--thread", "t",
    ])
    assert result.exit_code == 1
    assert "future" in result.output


def test_cli_await_clear_plain_ok(monkeypatch):
    # Plain --clear (no --resolved): no Attio read, no event — just the clear.
    result = _invoke_await(monkeypatch, ["--object", "deals", "--id", "d1", "--clear"])
    assert result.exit_code == 0
    assert "ok: clear_awaiting_reply" in result.output


def test_cli_await_clear_resolved_emits_event_before_clear(monkeypatch):
    calls: list[str] = []

    def fake_read(object_, target_id):
        calls.append("read")
        return {"awaiting_reply_since": "2026-07-01", "awaiting_reply_nudge_count": 1}

    def fake_escalate(**kwargs):
        calls.append("escalate")
        fake_escalate.kwargs = kwargs
        return {}

    monkeypatch.setattr("cli._read_awaiting_state", fake_read)
    monkeypatch.setattr("workflows.escalation.escalate", fake_escalate)

    def spy_state_call(fn_name, *a, **k):
        calls.append(fn_name)
        # Don't hit Attio — the writer path is covered elsewhere.

    monkeypatch.setattr("cli._followup_state_call", spy_state_call)
    result = _invoke_await(monkeypatch, [
        "--object", "deals", "--id", "d1", "--clear", "--resolved", "2026-07-10",
    ])
    assert result.exit_code == 0
    assert calls == ["read", "escalate", "clear_awaiting_reply"]
    payload = fake_escalate.kwargs["payload"]
    assert payload["latency_days"] == 9
    assert payload["nudge_count"] == 1
    assert fake_escalate.kwargs["type"] == "awaiting_reply_resolved"


def test_cli_await_resolved_before_send_exits_1(monkeypatch):
    monkeypatch.setattr(
        "cli._read_awaiting_state",
        lambda o, t: {"awaiting_reply_since": "2026-07-08", "awaiting_reply_nudge_count": 0},
    )
    result = _invoke_await(monkeypatch, [
        "--object", "deals", "--id", "d1", "--clear", "--resolved", "2026-07-01",
    ])
    assert result.exit_code == 1
    assert "predates" in result.output


def test_cli_await_nudged_reads_count_and_increments(monkeypatch):
    monkeypatch.setattr(
        "cli._read_awaiting_state",
        lambda o, t: {"awaiting_reply_nudge_count": 1},
    )
    seen = {}

    def spy_state_call(fn_name, *a, **k):
        seen["fn"] = fn_name
        seen["kwargs"] = k

    monkeypatch.setattr("cli._followup_state_call", spy_state_call)
    result = _invoke_await(monkeypatch, ["--object", "deals", "--id", "d1", "--nudged"])
    assert result.exit_code == 0
    assert seen["fn"] == "increment_awaiting_nudge"
    assert seen["kwargs"]["current_count"] == 1


def test_cli_await_nudged_read_failure_refuses(monkeypatch):
    monkeypatch.setattr("cli._read_awaiting_state", lambda o, t: None)
    result = _invoke_await(monkeypatch, ["--object", "deals", "--id", "d1", "--nudged"])
    assert result.exit_code == 1
    assert "refusing" in result.output.lower()


def test_cli_await_note_id_alone(monkeypatch):
    result = _invoke_await(monkeypatch, [
        "--object", "deals", "--id", "d1", "--note-id", "note-7",
    ])
    assert result.exit_code == 0
    assert "ok: set_awaiting_note_id" in result.output


def test_cli_await_clear_resolved_read_failure_refuses(monkeypatch):
    # Review regression: a failed READ must never be treated as "no stamp" —
    # clearing blind would destroy awaiting_reply_since and the latency event.
    monkeypatch.setattr("cli._read_awaiting_state", lambda o, t: None)
    result = _invoke_await(monkeypatch, [
        "--object", "deals", "--id", "d1", "--clear", "--resolved", "2026-07-10",
    ])
    assert result.exit_code == 1
    assert "refusing to clear blind" in result.output


def test_cli_await_nudged_malformed_count_refuses(monkeypatch):
    # Review regression: a malformed count must not silently reset the ceiling.
    monkeypatch.setattr(
        "cli._read_awaiting_state",
        lambda o, t: {"awaiting_reply_nudge_count": "garbage"},
    )
    result = _invoke_await(monkeypatch, ["--object", "deals", "--id", "d1", "--nudged"])
    assert result.exit_code == 1
    assert "malformed" in result.output


def test_cli_await_resolved_absurd_future_refuses(monkeypatch):
    monkeypatch.setattr(
        "cli._read_awaiting_state",
        lambda o, t: {"awaiting_reply_since": "2026-07-01", "awaiting_reply_nudge_count": 0},
    )
    result = _invoke_await(monkeypatch, [
        "--object", "deals", "--id", "d1", "--clear", "--resolved", "2099-01-01",
    ])
    assert result.exit_code == 1


def test_stamp_verified_touch_uses_shared_guard():
    # DRY regression: the inline guard was replaced by _guard_touch_date —
    # both directions must still raise (same bounds as followup-await).
    writer = MagicMock()
    with pytest.raises(ValueError):
        followup_state.stamp_verified_touch(writer, deal_id="d1", touch=date(9999, 1, 1))
    with pytest.raises(ValueError):
        followup_state.stamp_verified_touch(writer, deal_id="d1", touch=date(2020, 1, 1))
    writer.apply.assert_not_called()
