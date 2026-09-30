from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from clients.google_sheets import write_identity_batch
from clients.pb_envelope import PBLaunch, PBRunTimeout
from clients.phantombuster import PhantomBusterClient


def test_batch_uses_new_tab_raw_urls_and_verified_gid(monkeypatch):
    monkeypatch.setenv('GSHEET_AUTOCONNECT_ID', 'input-sheet')
    gc = MagicMock()
    sh = gc.open_by_key.return_value
    ws = sh.add_worksheet.return_value
    ws.id = 42
    urls = ['https://linkedin.com/sales/lead/AAA,name', 'https://linkedin.com/sales/people/BBB,name']
    ws.get_all_values.return_value = [[u] for u in urls]
    with patch('clients.google_sheets.get_client', return_value=gc):
        result = write_identity_batch(urls)
    sh.worksheet.assert_not_called()
    ws.clear.assert_not_called()
    ws.update.assert_called_once_with([[u] for u in urls], 'A1', value_input_option='RAW')
    assert result.endswith('/input-sheet/edit#gid=42')


def test_bad_batch_readback_never_returns_input(monkeypatch):
    monkeypatch.setenv('GSHEET_AUTOCONNECT_ID', 'input-sheet')
    gc = MagicMock()
    gc.open_by_key.return_value.add_worksheet.return_value.get_all_values.return_value = [['wrong']]
    with patch('clients.google_sheets.get_client', return_value=gc), pytest.raises(ValueError, match='readback'):
        write_identity_batch(['https://linkedin.com/sales/lead/AAA,name'])


def test_timeout_prevents_next_agent_launch_until_previous_container_finishes():
    pb = PhantomBusterClient(api_key='test')
    old = PBLaunch(container_id='old-container', agent_id='profiles', launched_at=datetime.now(UTC), arguments_sha256='test')
    with patch.object(pb, 'get_container_output', return_value={'containerId':'old-container','status':'running','isAgentRunning':True}), patch('clients.phantombuster.time.sleep'), pytest.raises(PBRunTimeout):
        pb.wait_for_completion(old, max_wait=1, poll_interval=1)
    with patch.object(pb, '_request') as req, patch.object(pb, 'get_container_output', return_value={'containerId':'old-container','status':'running','isAgentRunning':True}), patch('clients.phantombuster.time.sleep'), pytest.raises(PBRunTimeout):
        pb.launch_agent('inbox')
    req.assert_not_called()
    with patch.object(pb, '_request', return_value={'containerId':'new-container'}) as req, patch.object(pb, 'get_container_output', return_value={'containerId':'old-container','status':'finished','isAgentRunning':False}):
        assert pb.launch_agent('inbox').container_id == 'new-container'
    req.assert_called_once()
    assert not pb._timed_out_launches
    pb.close()


def test_fresh_client_reconciles_remote_run_and_blocks_until_terminal():
    pb = PhantomBusterClient(api_key='test')
    active = {'containerId':'orphan','isAgentRunning':True,'status':'running'}
    with patch.object(pb, 'list_agents', return_value=[{'id':'profiles'}]), patch.object(pb, 'get_output', return_value=active), patch.object(pb, 'get_container_output', return_value=active), patch('clients.phantombuster.time.sleep'), patch.object(pb, '_request') as req, pytest.raises(PBRunTimeout):
        pb.reconcile_workspace()
    req.assert_not_called()
    assert 'orphan' in pb._timed_out_launches
    pb.close()


def test_unknown_remote_state_blocks_fresh_run():
    pb = PhantomBusterClient(api_key='test')
    with patch.object(pb, 'list_agents', return_value=[{'id':'profiles'}]), patch.object(pb, 'get_output', return_value={'containerId':'orphan'}), pytest.raises(RuntimeError, match='unconfirmed'):
        pb.reconcile_workspace()
    pb.close()


def test_unattributed_terminal_poll_does_not_release_remote_container():
    pb = PhantomBusterClient(api_key="test")
    launch = PBLaunch(container_id="orphan", agent_id="profiles", launched_at=datetime.now(UTC), arguments_sha256="test")
    terminal = {"status": "finished", "isAgentRunning": False}
    with patch.object(pb, "get_container_output", return_value=terminal), patch("clients.phantombuster.time.sleep"), pytest.raises(PBRunTimeout):
        pb.wait_for_completion(launch, poll_interval=1, max_wait=1)
    assert "orphan" in pb._timed_out_launches
    with patch.object(pb, "_request") as request, patch.object(pb, "wait_for_completion", side_effect=PBRunTimeout(container_id="orphan", agent_id="profiles", elapsed_seconds=1, last_observed_status="finished", last_observed_output=terminal)), pytest.raises(PBRunTimeout):
        pb.launch_agent("another-agent")
    request.assert_not_called()
    pb.close()
@pytest.mark.parametrize("payload", [{}, {"data": None}, {"data": [{}]}, {"data": [{"id": ""}]}, ["invalid"]])
def test_malformed_workspace_inventory_blocks_reconciliation(payload):
    pb = PhantomBusterClient(api_key="test")
    with patch.object(pb, "_request", return_value=payload), pytest.raises(RuntimeError, match="unconfirmed"):
        pb.reconcile_workspace()
    pb.close()
