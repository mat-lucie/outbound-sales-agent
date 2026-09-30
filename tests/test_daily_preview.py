"""The public dry-run boundary must never enter execution infrastructure."""
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from click.testing import CliRunner

from cli import cli
from workflows.daily_preview import ReadOnlyPipelineClient


def test_cli_preview_never_enters_execution(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ATTIO_API_KEY", "offline")
    monkeypatch.setenv("ATTIO_LIST_ID", "pipeline")
    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "sales_nav")
    monkeypatch.setenv("OUTBOUND_USE_LLM_DISPATCH", "1")
    monkeypatch.setenv("BOTDOG_SEND_ENABLED", "1")
    monkeypatch.setenv("GSHEET_DRYRUN_ID", "sandbox")
    monkeypatch.setattr("workflows.daily_preview.operator_today", lambda: date(2026, 9, 16))
    calls = []
    def request(self, method, path, **kwargs):
        calls.append((method, path))
        assert method == "POST" and path == "/lists/pipeline/entries/query"
        return httpx.Response(200, json={"data": []}, request=httpx.Request(method, "https://api.attio.com" + path))
    monkeypatch.setattr(httpx.Client, "request", request)
    blocked = [
        "clients.phantombuster.PhantomBusterClient.__init__",
        "clients.gmail.GmailClient.from_credentials",
        "scripts.validate_sales_nav_health.quick_check",
        "workflows.run_lock.acquire_run_lock",
        "workflows.audit.AuditLogger.__init__",
        "workflows.daily_check.run_connection_requests",
        "workflows.botdog_ingest.ingest_botdog_events",
        "workflows.daily_check.run_dm_sequencing",
        "workflows.escalation.escalate",
        "workflows.llm_dispatch.LLMBudgetLedger.try_reserve",
    ]
    spies = []
    for target in blocked:
        spy = Mock(side_effect=AssertionError(target))
        monkeypatch.setattr(target, spy)
        spies.append(spy)
    result = CliRunner().invoke(cli, ["daily", "--dry-run", "--yes", "--force-weekend"])
    assert result.exit_code == 0, result.output + repr(result.exception)
    assert calls == [("POST", "/lists/pipeline/entries/query")]
    assert "not approved send batches" in result.output
    assert list(tmp_path.iterdir()) == []
    for spy in spies:
        spy.assert_not_called()


@pytest.mark.parametrize("method,path", [
    ("PATCH", "/lists/pipeline/entries/e"), ("DELETE", "/objects/people/records/x"),
    ("POST", "/lists/pipeline/entries"), ("POST", "/lists/other/entries/query"),
    ("POST", "https://example.com/lists/pipeline/entries/query"),
    ("GET", "/lists/pipeline/entries/query"),
])
def test_provider_rejects_non_query_before_transport(monkeypatch, method, path):
    monkeypatch.setenv("ATTIO_API_KEY", "offline")
    transport = Mock(side_effect=AssertionError("network"))
    monkeypatch.setattr(httpx.Client, "request", transport)
    with ReadOnlyPipelineClient("pipeline") as client, pytest.raises(RuntimeError, match="refused"):
        client._request(method, path)
    transport.assert_not_called()


def test_query_error_is_not_empty_success(monkeypatch):
    monkeypatch.setenv("ATTIO_API_KEY", "offline")
    monkeypatch.setenv("ATTIO_LIST_ID", "pipeline")
    monkeypatch.setattr(ReadOnlyPipelineClient, "query_list_entries", Mock(side_effect=RuntimeError("truncated")))
    result = CliRunner().invoke(cli, ["daily", "--dry-run"])
    assert result.exit_code != 0
    assert "Pipeline entries:" not in result.output


@pytest.mark.parametrize("pages", [
    [{}],
    [{"data": [{}] * 100}, {"error": "upstream page failed"}],
    [{"data": "not a list"}],
])
def test_malformed_attio_page_cannot_look_like_empty_inventory(monkeypatch, pages):
    monkeypatch.setenv("ATTIO_API_KEY", "offline")
    responses = iter(pages)
    monkeypatch.setattr("clients.attio.AttioClient._request", lambda *a, **k: next(responses))
    with ReadOnlyPipelineClient("pipeline") as client, pytest.raises(ValueError, match="no data list"):
        client.query_list_entries(list_id="pipeline", fail_if_truncated=True)


def test_inventory_reasons_and_unchecked_state(monkeypatch):
    monkeypatch.setenv("ATTIO_API_KEY", "offline")
    monkeypatch.setenv("ATTIO_LIST_ID", "pipeline")
    monkeypatch.setattr("workflows.daily_preview.operator_today", lambda: date(2026, 9, 16))
    rows = [
        {"stage": "Prospect", "quality_score": None},
        {"stage": "Prospect", "quality_score": "bad"},
                {"stage": "Prospect", "send_channel": "botdog"},
        {"stage": "Accepted", "last_contact_date": None},
    ]
    monkeypatch.setattr(ReadOnlyPipelineClient, "query_list_entries", lambda *a, **k: rows)
    from clients.crm.base import Entry, Stage
    monkeypatch.setattr("workflows.daily_preview.get_crm_provider", lambda **k: __import__("contextlib").nullcontext(
        SimpleNamespace(provider=SimpleNamespace(query_list_entries=lambda **kw: [
            Entry(entry_id=f"e{i}", record_id=f"r{i}", stage=Stage(row["stage"]), attributes=row)
            for i, row in enumerate(rows)]))))
    result = CliRunner().invoke(cli, ["daily", "--dry-run", "--skip-dms"])
    assert result.exit_code == 0, result.output
    import ast
    lines = result.output.splitlines()
    invite_counts = ast.literal_eval(next(line.split(": ", 1)[1] for line in lines if line.startswith("Invite inventory:")))
    dm_counts = ast.literal_eval(next(line.split(": ", 1)[1] for line in lines if line.startswith("DM cadence inventory:")))
    assert invite_counts == {"missing_quality_score": 1, "malformed_quality_score": 1,
                             "retired_botdog_channel": 1, "not_prospect": 1}
    assert dm_counts == {"missing_last_contact_date": 3,
                         "retired_botdog_channel": 1}
    for text in ("missing_quality_score", "malformed_quality_score",
                 "retired_botdog_channel", "missing_last_contact_date", "DM execution disabled",
                 "Unchecked:"):
        assert text in result.output


def test_live_cli_retains_execution_preflight(monkeypatch):
    preview = Mock(side_effect=AssertionError("preview should not run"))
    monkeypatch.setattr("workflows.daily_preview.preview_daily", preview)
    monkeypatch.setattr("workflows.safety_limits.get_status", lambda: "offline")
    preflight = Mock(side_effect=RuntimeError("live preflight reached"))
    monkeypatch.setattr("workflows.run_provenance.assert_checkout_current", preflight)
    result = CliRunner().invoke(cli, ["daily"])
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "live preflight reached"
    preflight.assert_called_once_with(dry_run=False, allow_stale=False)
    preview.assert_not_called()


@pytest.mark.parametrize("args", [
    ["daily", "--preview-dms-after-invites"],
    ["daily", "--skip-dms", "--preview-dms-after-invites", "--dry-run"],
])
def test_integrated_dm_rehearsal_requires_live_invite_only_run(args):
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 2
    assert "requires --skip-dms in a live daily run" in result.output
