"""Offline regressions for LUC-333/334/335; no provider calls."""
from unittest.mock import MagicMock

import httpx
import pytest
from click.testing import CliRunner

from cli import cli
from clients.attio import AttioClient, request_with_retry
from models.data_quality_report import P0_ALARM_SLUGS
from tests.test_canary_cli import _stub_client
from tests.test_dm_launch_cap_drain import _csv, _run, _url


@pytest.mark.parametrize("failure", [httpx.ReadTimeout("lost"), httpx.ReadError("lost"), 500, 502, 503])
def test_create_lost_response_never_reposts(monkeypatch, failure):
    monkeypatch.setattr("clients.attio.time.sleep", lambda _: None)
    calls = []
    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            if isinstance(failure, int):
                return httpx.Response(failure)
            raise failure
        return httpx.Response(200, json={"data": {"id": {"note_id": "duplicate"}}})
    with AttioClient(api_key="fake") as attio:
        attio._client.close()
        attio._client = httpx.Client(transport=httpx.MockTransport(handler), base_url=attio.BASE_URL)
        with pytest.raises(RuntimeError, match="ambiguous"):
            attio.create_note("record", "title", "content")
    assert len(calls) == 1


def test_outer_retry_cannot_repost_after_inconclusive_reconciliation(monkeypatch):
    monkeypatch.setattr("clients.attio.time.sleep", lambda _: None)
    attio = MagicMock()
    attio._request.side_effect = [httpx.ReadTimeout("lost"), {"data": {"id": "duplicate"}}]
    with pytest.raises(RuntimeError, match="ambiguous"):
        request_with_retry(attio, "POST", "/notes", recheck=lambda: None)
    assert attio._request.call_count == 1


def test_outer_retry_preserves_original_ambiguous_cause():
    from clients.attio import AmbiguousAttioWrite
    original = httpx.ReadTimeout("response lost after commit")
    with AttioClient(api_key="fake") as attio:
        attio._client.request = MagicMock(side_effect=original)
        with pytest.raises(AmbiguousAttioWrite) as caught:
            request_with_retry(attio, "POST", "/notes")
        assert caught.value.__cause__ is original
        attio._client.request.assert_called_once()


@pytest.mark.parametrize("slug", P0_ALARM_SLUGS)
def test_open_p0_blocks_actual_delivery(monkeypatch, slug):
    from tests import test_dm_launch_cap_drain as harness
    original = harness._attio_with_full_schema
    def client():
        attio = original()
        attio._request.side_effect = lambda *a, **kw: {"data": (
            [{"id": {"record_id": "alarm"}}]
            if kw.get("json", {}).get("filter", {}).get("type", {}).get("$eq") == slug else []
        )}
        return attio
    monkeypatch.setattr(harness, "_attio_with_full_schema", client)
    with pytest.raises(RuntimeError, match="data-quality"):
        _run(monkeypatch, 1, [_csv([_url(0)])])


@pytest.mark.parametrize("error,category", [
    (httpx.ConnectError("DNS unavailable"), "network_error"),
    (httpx.HTTPStatusError("denied", request=httpx.Request("POST", "https://example.test"), response=httpx.Response(403)), "authorization_error"),
])
def test_canary_failure_category(monkeypatch, error, category):
    def create(*args, **kwargs):
        raise error
    _stub_client(monkeypatch, create=create, delete=lambda _: pytest.fail("unexpected delete"))
    result = CliRunner().invoke(cli, ["canary"])
    assert result.exit_code == 1
    assert "mcp_scope_insufficient" in result.output  # legacy halt token
    assert f"reason={category}" in result.output


@pytest.mark.parametrize("method,path", [("GET", "/notes"), ("POST", "/objects/people/records/query"), ("POST", "/lists/outreach/entries/query")])
@pytest.mark.parametrize("failure", [httpx.ReadTimeout("lost"), 502, 503])
def test_safe_reads_still_retry(monkeypatch, method, path, failure):
    monkeypatch.setattr("clients.attio.time.sleep", lambda _: None)
    calls = []
    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            if isinstance(failure, int):
                return httpx.Response(failure)
            raise failure
        return httpx.Response(200, json={"data": []})
    with AttioClient(api_key="fake") as attio:
        attio._client.close()
        attio._client = httpx.Client(transport=httpx.MockTransport(handler), base_url=attio.BASE_URL)
        assert attio._request(method, path) == {"data": []}
    assert len(calls) == 2


@pytest.mark.parametrize("response", [None, {}, {"data": None}, {"data": {}}, {"data": ""}])
def test_malformed_queue_fails_closed(response):
    from workflows.dm_quality_gate import DataQualityUnavailable, require_clear_dm_quality_queue
    attio = MagicMock()
    attio._request.return_value = response
    with pytest.raises(DataQualityUnavailable):
        require_clear_dm_quality_queue(attio)


def test_unreadable_queue_fails_closed():
    from workflows.dm_quality_gate import DataQualityUnavailable, require_clear_dm_quality_queue
    attio = MagicMock()
    attio._request.side_effect = httpx.ConnectError("offline")
    with pytest.raises(DataQualityUnavailable):
        require_clear_dm_quality_queue(attio)


def test_alarm_between_batches_blocks_relaunch_and_releases_lease(monkeypatch):
    from models.data_quality_report import DataQualityHalt
    from tests import test_dm_launch_cap_drain as harness
    original = harness._attio_with_full_schema
    attio = original()
    queue_checks = []
    def query(method, path, **kw):
        if "type" not in kw.get("json", {}).get("filter", {}):
            return {"data": []}
        queue_checks.append(kw)
        return {"data": [] if len(queue_checks) <= 3 else [{"id": "alarm"}]}
    attio._request.side_effect = query
    monkeypatch.setattr(harness, "_attio_with_full_schema", lambda: attio)
    leases = harness._LeaseRecorder()
    with pytest.raises(DataQualityHalt):
        _run(monkeypatch, 2, [_csv([_url(0)]), _csv([_url(1)])], leases=leases)
    assert leases.ops == [("reserve", 2), ("confirm", 1), ("reserve", 1), ("release", "lease-2")]


@pytest.mark.parametrize("failure", [httpx.ConnectError("offline"), httpx.ReadTimeout("unknown")])
def test_delete_failure_keeps_cleanup_id_and_network_category(monkeypatch, failure):
    def delete(_):
        raise failure
    _stub_client(monkeypatch, create=lambda *a, **kw: {"id": {"note_id": "cleanup-note"}}, delete=delete)
    result = CliRunner().invoke(cli, ["canary"])
    assert result.exit_code == 1
    assert "reason=network_error" in result.output
    assert "cleanup-note" in result.output
    assert "verify whether it remains" in result.output
    assert "deletion could not be confirmed" in result.output


def test_ambiguous_canary_create_does_not_recommend_retry(monkeypatch):
    from clients.attio import AmbiguousAttioWrite
    def create(*a, **kw):
        raise AmbiguousAttioWrite("POST", "/notes")
    _stub_client(monkeypatch, create=create, delete=lambda _: pytest.fail("unexpected delete"))
    result = CliRunner().invoke(cli, ["canary"])
    assert result.exit_code == 1
    assert "reason=ambiguous_write" in result.output
    assert "reconcile before rerunning" in result.output
