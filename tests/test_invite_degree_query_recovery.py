"""LUC-339: match renamed output by echoed request without cross-assigning."""
from unittest.mock import MagicMock

import httpx
import pytest

from clients.phantombuster import PhantomBusterClient
from workflows.pre_invite_check import _launch_sales_nav_scrape


def scrape(monkeypatch, csv_text, urls):
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake")
    monkeypatch.setattr("workflows.daily_check.write_prospects_to_sheet", lambda *a, **k: "https://sheet.test/input")
    monkeypatch.setattr("workflows.daily_check._pb_session_args", lambda: {})
    pb = MagicMock()
    pb.get_agent.return_value = {"argument": '{"identities": [{}]}'}
    pb.launch_agent.return_value.container_id = "container-1"
    pb.download_result_csv.return_value = csv_text
    return _launch_sales_nav_scrape(pb, "scraper", urls)


def test_renamed_profile_recovers_echoed_query(monkeypatch):
    text = "linkedinProfileUrl,query,connectionDegree,hasPendingInvitation\nhttps://linkedin.com/in/new-handle,https://linkedin.com/in/original-handle,2nd,false\n"
    _, degrees, extras = scrape(monkeypatch, text, ["https://linkedin.com/in/original-handle"])
    assert degrees == {"https://linkedin.com/in/original-handle": "2nd"}
    assert extras["https://linkedin.com/in/original-handle"]["hasPendingInvitation"] == "false"


def test_conflicting_requested_identities_are_withheld(monkeypatch, capsys):
    text = "linkedinProfileUrl,query,connectionDegree,hasPendingInvitation\nhttps://linkedin.com/in/person-a,https://linkedin.com/in/person-b,2nd,false\n"
    _, degrees, extras = scrape(monkeypatch, text, ["https://linkedin.com/in/person-a", "https://linkedin.com/in/person-b"])
    assert degrees == {}
    assert extras == {}
    assert "conflicting" in capsys.readouterr().err.lower()


@pytest.mark.parametrize("conflict_first", [True, False])
def test_conflict_invalidates_other_rows_for_both_candidates(monkeypatch, conflict_first):
    header = "linkedinProfileUrl,query,connectionDegree,hasPendingInvitation\n"
    good = "https://linkedin.com/in/person-a,https://linkedin.com/in/person-a,2nd,false\n"
    conflict = "https://linkedin.com/in/person-a,https://linkedin.com/in/person-b,2nd,false\n"
    text = header + (conflict + good if conflict_first else good + conflict)
    _, degrees, extras = scrape(monkeypatch, text, ["https://linkedin.com/in/person-a", "https://linkedin.com/in/person-b"])
    assert degrees == {}
    assert extras == {}


def test_csv_download_failure_reports_status_without_url(monkeypatch, caplog):
    client = PhantomBusterClient(api_key="fake")
    url = "https://example.test/result.csv?secret=hidden"
    monkeypatch.setattr(client, "get_result_csv_url", lambda *a, **k: url)
    request = httpx.Request("GET", url)
    error = httpx.HTTPStatusError("hidden", request=request, response=httpx.Response(503, request=request))
    monkeypatch.setattr("clients.phantombuster.httpx.get", MagicMock(side_effect=error))
    launch = MagicMock(container_id="container-1")
    try:
        assert client.download_result_csv(launch, csv_name="degree-run") is None
    finally:
        client._client.close()
    assert "503" in caplog.text
    assert "container-1" in caplog.text
    assert "hidden" not in caplog.text


@pytest.mark.parametrize("retry_conflict", [False, True])
def test_retry_keeps_conflict_hold_and_original_identity_universe(monkeypatch, retry_conflict):
    from unittest.mock import patch

    from tests.test_pre_invite_sales_nav_path import _make_pb, _record_escalate, _row
    from workflows.pre_invite_check import _pre_invite_degree_check

    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "sales_nav")
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "scraper")
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake")
    monkeypatch.setattr("workflows.daily_check.write_prospects_to_sheet", lambda *a, **k: "https://sheet.test/input")
    monkeypatch.setattr("workflows.daily_check._pb_session_args", lambda: {})
    escalations = _record_escalate(monkeypatch)
    prefix = "https://linkedin.com/in/"
    header = "linkedinProfileUrl,query,connectionDegree,hasPendingInvitation\n"
    initial = header + f"{prefix}alice,{prefix}bob,2nd,false\n{prefix}carol,{prefix}carol,2nd,false\n"
    retry = header + (
        f"{prefix}carol,{prefix}alice,2nd,false\n" if retry_conflict
        else f"{prefix}alice,{prefix}alice,2nd,false\n{prefix}bob,{prefix}bob,2nd,false\n"
    )
    pb = _make_pb()
    pb.download_result_csv.side_effect = [initial, retry]
    with patch("workflows.recheck_cache.record_many") as record:
        still, already = _pre_invite_degree_check(
            [_row("A"), _row("B"), _row("C")], pb, None, MagicMock(), "list-id",
            sales_nav_profile_scraper_id="scraper",
        )
    assert pb.launch_agent.call_count == 2
    assert {r["entry_id"] for r in still} == (set() if retry_conflict else {"ent-C"})
    assert already == []
    held = {"rec-A", "rec-B", "rec-C"} if retry_conflict else {"rec-A", "rec-B"}
    assert held <= {c["payload"]["record_id"] for c in escalations}
    # No stale/conflicted degree can seed a later run's cache.
    if retry_conflict:
        assert not record.called or not record.call_args.args[0]
