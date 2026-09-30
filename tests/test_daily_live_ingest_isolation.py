"""Live CLI ingestion failures must not abort later phases; all I/O mocked."""
from contextlib import nullcontext
from unittest.mock import MagicMock, Mock

import pytest
from click.testing import CliRunner

from cli import cli


class ReachedInviteBoundary(Exception):
    """Stop fixture at the first send boundary, before any outbound operation."""


@pytest.mark.parametrize("enabled,fails,identity_hold,flags", [
    (True, True, False, []), (True, False, False, []), (False, True, False, []),
    (False, False, True, []), (False, False, True, ["--skip-dms"]),
    (False, False, True, ["--skip-dms", "--preview-dms-after-invites"]),
])
def test_live_ingestion_continues_to_invites(monkeypatch, enabled, fails, identity_hold, flags):
    env = {
        "ATTIO_LIST_ID": "offline-list", "PB_MESSAGE_SENDER_ID": "offline-sender" if identity_hold else "",
        "PB_PROFILE_SCRAPER_ID": "", "PB_SALES_NAV_PROFILE_SCRAPER_ID": "",
        "PB_INBOX_SCRAPER_ID": "offline-inbox" if identity_hold else "", "PB_NETWORK_BOOSTER_ID": "offline-inviter",
        "PRE_INVITE_DEGREE_CHECK_BACKEND": "regular",
        "BOTDOG_SEND_ENABLED": "1" if enabled else "false",
        "DISABLE_EMAIL_RESPONSE_DETECTION": "1",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr("cli._experiment_registry_preflight", lambda *a: None)
    monkeypatch.setattr("workflows.safety_limits.get_status", lambda: "offline")
    monkeypatch.setattr("workflows.run_provenance.assert_checkout_current", lambda **k: {})
    monkeypatch.setattr("workflows.run_lock.acquire_run_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr("workflows.audit.AuditLogger", lambda **k: nullcontext(MagicMock()))
    attio = MagicMock()
    attio.query_list_entries.return_value = []
    monkeypatch.setattr("clients.attio.AttioClient", lambda **_kwargs: attio)
    monkeypatch.setattr("clients.phantombuster.PhantomBusterClient", lambda: nullcontext(MagicMock()))
    from workflows.daily_run import DailyRun
    daily_run = MagicMock(spec=DailyRun) if identity_hold else MagicMock()
    daily_run.remaining.return_value = 25
    monkeypatch.setattr("workflows.daily_run.open_daily_run", lambda *a, **k: nullcontext(daily_run))
    monkeypatch.setattr("workflows.record_cache.preload_pipeline_persons", lambda *a, **k: 0)
    monkeypatch.setattr("scripts.validate_attio_schema_deltas.load_manifest", lambda *a: {})
    monkeypatch.setattr("scripts.validate_attio_schema_deltas.validate", lambda *a: [])
    monkeypatch.setattr("scripts.validate_attio_schema_deltas.check_attio_shipped", lambda *a, **k: [])
    monkeypatch.setattr("workflows.starvation.evaluate_pipeline_starvation", lambda *a: {})
    ingest = Mock(side_effect=RuntimeError("offline Botdog failure") if fails else None,
                  return_value={"polled": 0, "applied": 0, "failures": 0, "dry_run": False})
    monkeypatch.setattr("workflows.botdog_ingest.ingest_botdog_events", ingest)
    from workflows.detect_responses import IdentityResolutionHalt
    halt = IdentityResolutionHalt("eight profiles unavailable")
    monkeypatch.setattr("workflows.detect_responses.detect_responses", Mock(side_effect=halt))
    monkeypatch.setattr("workflows.pb_send_recovery.recover_unrecorded_dm_sends", lambda *a, **k: {})
    invite = Mock(return_value={"sent": 3}) if identity_hold else Mock(side_effect=ReachedInviteBoundary("later invite phase reached"))
    dm = Mock(side_effect=AssertionError("DM or rehearsal attempted with identity hold"))
    monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", dm)
    monkeypatch.setattr("workflows.daily_check.run_connection_requests", invite)
    if identity_hold:
        monkeypatch.setattr("workflows.daily_check.compute_due_dm_counts", lambda *a, **k: {
            "prospect_pool_size": 10, "due_dm1_count": 1, "due_dm2_count": 0, "due_dm3_count": 0,
        })
        monkeypatch.setattr("workflows.daily_check.run_end_summary", Mock(side_effect=RuntimeError("summary offline")))
    result = CliRunner().invoke(cli, ["daily", "--yes", "--skip-followups", *flags])
    if identity_hold:
        assert result.exception is halt, result.output + repr(result.exception)
        assert "Connections sent: 3" in result.output
        assert "DMs sent: 0" in result.output
        assert "Daily Check FAILED" in result.output
        assert "Skipping DMs and rehearsal" in result.output
        assert "Run-end summary failed" in result.output
        assert "summary offline" in result.output
        dm.assert_not_called()
    else:
        assert isinstance(result.exception, ReachedInviteBoundary), result.output + repr(result.exception)
    invite.assert_called_once()
    if enabled:
        ingest.assert_called_once()
        assert ingest.call_args.kwargs["dry_run"] is False
    else:
        ingest.assert_not_called()
        assert "Skipping (BOTDOG_SEND_ENABLED off)" in result.output
    if enabled and fails:
        assert "Phase 0.7 (Botdog event ingestion) FAILED" in result.output
        assert "offline Botdog failure" in result.output
    else:
        assert "Phase 0.7 (Botdog event ingestion) FAILED" not in result.output
