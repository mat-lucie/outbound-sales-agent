"""Verified profile identity is mandatory for every inbox-derived mutation."""
import csv
import io
from unittest.mock import MagicMock, patch

import pytest

from workflows import detect_responses as dr


def _manifest(path):
    path.write_text(
        "sales_id,public_key,public_url,crm_alias_key,crm_alias_evidence,provenance\n"
        "OLD,https://linkedin.com/in/old,https://linkedin.com/in/old,,,provider-export\n"
    )


def _scrape_csv(query, public_url):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["query", "salesNavigatorUrl", "linkedinProfileUrl", "error"])
    writer.writeheader()
    writer.writerow({"query": query, "salesNavigatorUrl": query, "linkedinProfileUrl": public_url})
    return output.getvalue()


def test_unavailable_profile_is_deferred_until_next_operator_day(tmp_path, monkeypatch):
    import json
    from datetime import date

    path = tmp_path / "manifest.csv"
    _manifest(path)
    query = "https://www.linkedin.com/sales/people/NEW,NAME_SEARCH"
    pb = MagicMock()
    pb.launch_agent.return_value.container_id = "container-failed"
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["query", "error"])
    writer.writeheader()
    writer.writerow({"query": query, "error": "Profile couldn't be opened."})
    pb.download_result_csv.return_value = output.getvalue()
    monkeypatch.setattr("models.business_calendar.operator_today", lambda: date(2026, 9, 30))
    with patch.object(dr, "write_identity_batch", return_value="sheet"), patch.object(dr, "build_sales_nav_launch_args", return_value={}):
        with pytest.raises(ValueError, match="invalid provider evidence"):
            dr._enrich_missing_inbox_identities(pb, str(path), {"li-sales:NEW": query}, "agent")
        deferred = path.with_name(path.name + ".deferred.json")
        saved = json.loads(deferred.read_text())
        assert saved["li-sales:NEW"]["retry_on"] == "2026-10-01"
        assert saved["li-sales:NEW"]["container_id"] == "container-failed"
        assert deferred.stat().st_mode & 0o777 == 0o600
        with pytest.raises(ValueError, match="deferred until tomorrow"):
            dr._enrich_missing_inbox_identities(pb, str(path), {"li-sales:NEW": query}, "agent")
        assert pb.launch_agent.call_count == 1
        assert "li-sales:NEW" not in dr._load_verified_identity_bridge(str(path))
        monkeypatch.setattr("models.business_calendar.operator_today", lambda: date(2026, 10, 1))
        pb.download_result_csv.return_value = _scrape_csv(query, "https://linkedin.com/in/new-person")
        dr._enrich_missing_inbox_identities(pb, str(path), {"li-sales:NEW": query}, "agent")
        assert pb.launch_agent.call_count == 2
        assert dr._load_verified_identity_bridge(str(path))["li-sales:NEW"] == "https://linkedin.com/in/new-person"


@pytest.mark.parametrize("kind", ["people", "lead"])
def test_new_inbox_identity_uses_exact_pb_pair_and_persists_evidence(tmp_path, kind):
    path = tmp_path / "manifest.csv"
    _manifest(path)
    query = f"https://www.linkedin.com/sales/{kind}/NEW,NAME_SEARCH"
    pb = MagicMock()
    pb.launch_agent.return_value.container_id = "container-1"
    pb.download_result_csv.return_value = _scrape_csv(query, "https://www.linkedin.com/in/new-person")
    with (
        patch.object(dr, "write_identity_batch", return_value="https://docs.google.com/spreadsheets/d/test/edit#gid=7"),
        patch.object(dr, "get_client", side_effect=AssertionError("shared sheet used"), create=True),
        patch.object(dr, "write_prospects_to_sheet", side_effect=AssertionError("shared sheet used"), create=True),
        patch.object(dr, "build_sales_nav_launch_args", side_effect=lambda _pb, _id, *, spreadsheet_url, launch_count: {"spreadsheetUrl": spreadsheet_url, "numberOfProfilesPerLaunch": launch_count}, create=True),
    ):
        dr._enrich_missing_inbox_identities(pb, str(path), {"li-sales:NEW": query}, "agent-1")
    assert dr._load_verified_identity_bridge(str(path))["li-sales:NEW"] == "https://linkedin.com/in/new-person"
    assert "container-1" in path.read_text()
    assert pb.launch_agent.call_args.args[1]["spreadsheetUrl"].endswith("#gid=7")
    assert pb.launch_agent.call_args.args[1]["numberOfProfilesPerLaunch"] == 1


def test_new_inbox_identity_rejects_mismatched_pb_query_without_persisting(tmp_path):
    path = tmp_path / "manifest.csv"
    _manifest(path)
    before = path.read_text()
    query = "https://www.linkedin.com/sales/people/NEW,NAME_SEARCH"
    pb = MagicMock()
    pb.launch_agent.return_value.container_id = "container-2"
    pb.download_result_csv.return_value = _scrape_csv(
        "https://www.linkedin.com/sales/people/OTHER,NAME_SEARCH",
        "https://www.linkedin.com/in/new-person",
    )
    with (
        patch.object(dr, "write_identity_batch", return_value="https://docs.google.com/spreadsheets/d/test/edit#gid=7"),
        patch.object(dr, "get_client", side_effect=AssertionError("shared sheet used"), create=True),
        patch.object(dr, "write_prospects_to_sheet", side_effect=AssertionError("shared sheet used"), create=True),
        patch.object(dr, "build_sales_nav_launch_args", side_effect=lambda _pb, _id, *, spreadsheet_url, launch_count: {"spreadsheetUrl": spreadsheet_url, "numberOfProfilesPerLaunch": launch_count}, create=True),
        pytest.raises(ValueError, match="unexpected query") as error,
    ):
        dr._enrich_missing_inbox_identities(pb, str(path), {"li-sales:NEW": query}, "agent-1")
    assert path.read_text() == before
    assert "li-sales:NEW" in str(error.value)
    assert "container-2" in str(error.value)


def test_verified_identity_survives_later_profile_scrape_failure(tmp_path):
    path = tmp_path / "manifest.csv"
    _manifest(path)
    first = "https://www.linkedin.com/sales/people/FIRST,NAME_SEARCH"
    second = "https://www.linkedin.com/sales/people/SECOND,NAME_SEARCH"
    pb = MagicMock()
    pb.launch_agent.return_value.container_id = "container-batch"
    pb.download_result_csv.return_value = _scrape_csv(first, "https://www.linkedin.com/in/first-person") + _scrape_csv(second, "").split("\n", 1)[1]
    with (
        patch.object(dr, "write_identity_batch", return_value="https://docs.google.com/spreadsheets/d/test/edit#gid=7"),
        patch.object(dr, "build_sales_nav_launch_args", side_effect=lambda _pb, _id, *, spreadsheet_url, launch_count: {"spreadsheetUrl": spreadsheet_url, "numberOfProfilesPerLaunch": launch_count}),
        pytest.raises(ValueError, match="invalid provider evidence"),
    ):
        dr._enrich_missing_inbox_identities(pb, str(path), {
            "li-sales:FIRST": first, "li-sales:SECOND": second,
        }, "agent-1")
    bridge = dr._load_verified_identity_bridge(str(path))
    assert bridge["li-sales:FIRST"] == "https://linkedin.com/in/first-person"
    assert "li-sales:SECOND" not in bridge
    assert pb.launch_agent.call_count == 1
    assert pb.launch_agent.call_args.args[1]["numberOfProfilesPerLaunch"] == 2


def test_identity_scrape_timeout_reports_key_and_container(tmp_path):
    path = tmp_path / "manifest.csv"
    _manifest(path)
    query = "https://www.linkedin.com/sales/people/NEW,NAME_SEARCH"
    pb = MagicMock()
    pb.launch_agent.return_value.container_id = "container-timeout"
    pb.wait_for_completion.side_effect = TimeoutError("provider delayed")
    with (
        patch.object(dr, "write_identity_batch", return_value="https://docs.google.com/spreadsheets/d/test/edit#gid=7"),
        patch.object(dr, "build_sales_nav_launch_args", return_value={}),
        pytest.raises(RuntimeError) as error,
    ):
        dr._enrich_missing_inbox_identities(pb, str(path), {"li-sales:NEW": query}, "agent-1")
    assert "li-sales:NEW" in str(error.value)
    assert "container-timeout" in str(error.value)


def test_detect_responses_reloads_new_provider_identity_before_preflight(tmp_path, monkeypatch):
    path = tmp_path / "manifest.csv"
    _manifest(path)
    monkeypatch.setenv("OUTBOUND_INBOX_IDENTITY_MAP", str(path))
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "profile-agent")
    attio, pb, cache, daily = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    dm = person("dm", "https://linkedin.com/in/dm")
    dm.update(stage="DM1 Sent", dm_step=1)
    other = person("other", "https://linkedin.com/in/new-person")
    attio.query_list_entries.return_value = [dm, other]
    cache.get.side_effect = lambda rid: (
        "Alex Smith", "Co", f"https://linkedin.com/in/{'dm' if rid == 'dm' else 'new-person'}", "", "")
    row = thread("https://linkedin.com/sales/people/NEW,NAME_SEARCH")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
    pb.download_result_csv.return_value = output.getvalue()

    def enrich(_pb, manifest_path, missing, scraper_id):
        assert missing == {"li-sales:NEW": row["participantProfileUrl"]}
        assert scraper_id == "profile-agent"
        with open(manifest_path, "a") as handle:
            handle.write("NEW,https://linkedin.com/in/new-person,https://linkedin.com/in/new-person,,,provider-container-1\n")

    with (
        patch.object(dr.AttioClient, "parse_entry", side_effect=lambda entry: entry),
        patch.object(dr, "_pb_session_args", return_value={}),
        patch.object(dr, "_enrich_missing_inbox_identities", side_effect=enrich),
        patch.object(dr, "_detect_manual_touches"),
        patch.object(dr, "_detect_cadence_drift", return_value=[]),
    ):
        counts = dr.detect_responses(attio, pb, "scraper", cache=cache, daily_run=daily)
    assert counts.get("identity_holds", 0) == 0
    daily.set_reply_detection_status.assert_called_with("ok")


def person(entry, url, name="Alex Smith"):
    return {"entry_id": entry, "record_id": entry, "prospect_name": name,
            "linkedin_url": url, "stage": "Responded", "dm_step": 0}


def thread(url, name="Alex Smith"):
    return {"participantFullName": name, "participantProfileUrl": url,
            "isLastMessageFromMe": "true", "lastMessageBody": "Personal follow-up on Tuesday",
            "totalMessageCount": "4", "lastMessageDate": "2026-09-17T12:00:00Z"}


def resolve(row, people):
    index = {}
    for p in people:
        index.setdefault(dr._normalize_name(p["prospect_name"]), []).append(p)
    return dr._resolve_thread_entries(row, index)


def test_same_name_selects_only_verified_profile():
    a, b = person("a", "https://linkedin.com/in/alex-a"), person("b", "https://linkedin.com/in/alex-b")
    assert resolve(thread(b["linkedin_url"]), [a, b]) == [b]


@pytest.mark.parametrize("url", ["", "nonsense", "https://evil.test/in/alex-a", "https://linkedin.com/company/alex-a", "https://linkedin.com/sales/people/unknown", "https://linkedin.com/in/other"])
def test_unverified_identity_holds_even_unique_name(url, capsys):
    assert resolve(thread(url), [person("a", "https://linkedin.com/in/alex-a")]) == []
    assert "identity hold" in capsys.readouterr().err


def test_verified_duplicate_and_renamed_display_name():
    a = person("a", "https://linkedin.com/in/old-name-123456")
    b = person("b", "https://www.linkedin.com/in/new-name-123456/?x=1", "New Name")
    assert resolve(thread(b["linkedin_url"], "Renamed Display"), [a, b]) == [a, b]


def test_conflicting_crm_urls_hold():
    a = person("a", "https://linkedin.com/in/alex-a")
    a["canonical_linkedin_url"] = "https://linkedin.com/in/someone-else"
    assert resolve(thread(a["linkedin_url"]), [a]) == []


def test_cadence_only_repairs_verified_profile():
    a, b = person("a", "https://linkedin.com/in/alex-a"), person("b", "https://linkedin.com/in/alex-b")
    drifts = dr._detect_cadence_drift([thread(b["linkedin_url"])], {"alex smith": [a, b]})
    assert [d["entry_id"] for d in drifts] == ["b"]


def test_unverified_manual_touch_does_not_write(tmp_path):
    attio = MagicMock()
    counts = dr._empty_counts()
    with patch.object(dr, "_self_echo_templates", return_value=[]):
        dr._detect_manual_touches(attio=attio, list_id="list", scraped_threads=[thread("")],
            name_to_full_pipeline={"alex smith": [person("a", "https://linkedin.com/in/alex-a")]},
            today_iso="2026-09-17", counts=counts, state_path=tmp_path / "touch.json")
    attio.update_list_entry.assert_not_called()
    attio.create_note.assert_not_called()

@pytest.mark.parametrize("status_failure", [False, True])
def test_identity_hold_halts_run_without_ok_even_when_status_write_fails(status_failure):
    import csv
    import io

    import httpx
    attio, pb, cache, daily = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    entry = person("a", "https://linkedin.com/in/alex-a")
    entry.update(stage="DM1 Sent", dm_step=1)
    attio.query_list_entries.return_value = [entry]
    cache.get.return_value = ("Alex Smith", "Co", entry["linkedin_url"], "", "")
    row = thread("")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
    pb.download_result_csv.return_value = buf.getvalue()
    if status_failure:
        daily.set_reply_detection_status.side_effect = httpx.ConnectError("offline")
    with (
        patch.object(dr.AttioClient, "parse_entry", side_effect=lambda e: e),
        patch.object(dr, "_pb_session_args", return_value={}),
        pytest.raises(dr.IdentityResolutionHalt),
    ):
        dr.detect_responses(attio, pb, "scraper", cache=cache, daily_run=daily)
    daily.set_reply_detection_status.assert_called_once_with("failed")
    assert all(c.args != ("ok",) for c in daily.set_reply_detection_status.call_args_list)
    attio.update_list_entry.assert_not_called()
    attio.create_note.assert_not_called()


def test_manual_touch_selects_profile_and_all_its_verified_duplicates(tmp_path):
    attio = MagicMock()
    a = person("a", "https://linkedin.com/in/alex-a")
    b = person("b", "https://linkedin.com/in/alex-b")
    duplicate = person("dup", b["linkedin_url"])
    counts = dr._empty_counts()
    with patch.object(dr, "_self_echo_templates", return_value=[]):
        dr._detect_manual_touches(attio=attio, list_id="list",
            scraped_threads=[thread(b["linkedin_url"])],
            name_to_full_pipeline={"alex smith": [a, b, duplicate]},
            today_iso="2026-09-17", counts=counts, state_path=tmp_path / "touch.json")
    assert {call.kwargs["entry_id"] for call in attio.update_list_entry.call_args_list} == {"b", "dup"}


def test_unrelated_unknown_thread_does_not_hold_pipeline(capsys):
    counts = dr._empty_counts()
    assert dr._resolve_thread_entries(thread("", "Unrelated Person"),
        {"alex smith": [person("a", "https://linkedin.com/in/alex-a")]}, counts=counts) == []
    assert not counts.get("identity_holds")
    assert not capsys.readouterr().err


def test_explicit_sales_navigator_identity_is_case_sensitive():
    a = person("a", "https://linkedin.com/sales/people/ACw123,NAME_SEARCH,abc")
    assert resolve(thread("https://linkedin.com/sales/people/ACw123,OTHER,def"), [a]) == [a]
    assert resolve(thread("https://linkedin.com/sales/people/acw123"), [a]) == []


def test_conflicting_duplicate_cannot_hide_behind_clean_match():
    a = person("a", "https://linkedin.com/in/alex-a")
    duplicate = person("dup", a["linkedin_url"])
    duplicate["canonical_linkedin_url"] = "https://linkedin.com/in/other"
    assert resolve(thread(a["linkedin_url"]), [a, duplicate]) == []


def test_verified_bridge_selects_only_exact_sales_id_not_same_name():
    a = person("a", "https://linkedin.com/in/alex-a")
    b = person("b", "https://linkedin.com/in/alex-b")
    bridge = {"li-sales:ACwExact": dr._profile_identity(a["linkedin_url"])}
    index = {"alex smith": [a, b]}
    assert dr._resolve_thread_entries(
        thread("https://linkedin.com/sales/people/ACwExact,NAME_SEARCH,x"),
        index, identity_bridge=bridge,
    ) == [a]
    assert dr._resolve_thread_entries(
        thread("https://linkedin.com/sales/people/ACwOther,NAME_SEARCH,x"),
        index, identity_bridge=bridge,
    ) == []


def test_verified_redirect_alias_matches_only_same_profile():
    current = "https://linkedin.com/in/new-vanity"
    old = "https://linkedin.com/in/old-vanity"
    a = person("a", old)
    b = person("b", "https://linkedin.com/in/another-person")
    bridge = {
        "li-sales:ACwExact": dr._profile_identity(current),
        dr._profile_identity(old): dr._profile_identity(current),
    }
    assert dr._resolve_thread_entries(
        thread("https://linkedin.com/sales/people/ACwExact"),
        {"alex smith": [a, b]}, identity_bridge=bridge,
    ) == [a]


def test_bridge_does_not_override_conflicting_crm_fields():
    a = person("a", "https://linkedin.com/in/alex-a")
    a["canonical_linkedin_url"] = "https://linkedin.com/in/alex-b"
    bridge = {"li-sales:ACwExact": dr._profile_identity(a["linkedin_url"])}
    assert dr._resolve_thread_entries(
        thread("https://linkedin.com/sales/people/ACwExact"),
        {"alex smith": [a]}, identity_bridge=bridge,
    ) == []


def test_verified_bridge_loader_rejects_duplicate_or_unproven_identity(tmp_path):
    import csv

    path = tmp_path / "verified.csv"
    fields = ["sales_id", "public_key", "public_url", "crm_alias_key",
              "crm_alias_evidence", "provenance"]

    def write(rows):
        with path.open("w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    good = {"sales_id": "ACwExact", "public_key": dr._profile_identity("https://linkedin.com/in/alex-a"),
            "public_url": "https://linkedin.com/in/alex-a", "crm_alias_key": "",
            "crm_alias_evidence": "", "provenance": "provider-container-123"}
    write([good])
    assert dr._load_verified_identity_bridge(str(path))["li-sales:ACwExact"] == good["public_key"]

    write([good, {**good, "sales_id": "ACwOther"}])
    with pytest.raises(ValueError):
        dr._load_verified_identity_bridge(str(path))

    write([{**good, "provenance": ""}])
    with pytest.raises(ValueError):
        dr._load_verified_identity_bridge(str(path))

    write([{**good, "public_key": dr._profile_identity("https://linkedin.com/in/alex-b")}])
    with pytest.raises(ValueError):
        dr._load_verified_identity_bridge(str(path))

    write([{**good, "public_url": ""}])
    with pytest.raises(ValueError):
        dr._load_verified_identity_bridge(str(path))


@pytest.mark.parametrize("manifest_content", [
    None,
    "sales_id,public_key,public_url,crm_alias_key,crm_alias_evidence,provenance\n",
    ("sales_id,public_key,public_url,crm_alias_key,crm_alias_evidence,provenance\n"
     "ACwOther,https://linkedin.com/in/alex-a,https://linkedin.com/in/alex-a, , ,provider-export\n"),
])
@pytest.mark.parametrize("status_failure", [False, True])
def test_incomplete_identity_manifest_halts_before_inbox_writes(
    tmp_path, monkeypatch, capsys, manifest_content, status_failure,
):
    import csv
    import io

    attio, pb, cache, daily = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    entry = person("a", "https://linkedin.com/in/alex-a")
    entry.update(stage="DM1 Sent", dm_step=1)
    attio.query_list_entries.return_value = [entry]
    cache.get.return_value = ("Alex Smith", "Co", entry["linkedin_url"], "", "")
    row = thread("https://linkedin.com/sales/people/ACwExact,NAME_SEARCH,x")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
    pb.download_result_csv.return_value = buf.getvalue()
    path = tmp_path / "identity.csv"
    if manifest_content is not None:
        path.write_text(manifest_content)
    monkeypatch.setenv("OUTBOUND_INBOX_IDENTITY_MAP", str(path))
    if status_failure:
        daily.set_reply_detection_status.side_effect = RuntimeError("status offline")
    with (
        patch.object(dr.AttioClient, "parse_entry", side_effect=lambda e: e),
        patch.object(dr, "_pb_session_args", return_value={}),
        pytest.raises(dr.IdentityResolutionHalt),
    ):
        dr.detect_responses(attio, pb, "scraper", cache=cache, daily_run=daily)
    daily.set_reply_detection_status.assert_called_once_with("failed")
    if status_failure:
        stderr = capsys.readouterr().err
        assert "identity manifest validation failed" in stderr
        assert "Remote status may be stale" in stderr
        assert "DM sequencing must remain halted" in stderr
    attio.update_list_entry.assert_not_called()
    attio.create_note.assert_not_called()


@pytest.mark.parametrize("profile_url", [
    "", "https://linkedin.com/sales/lead/OtherIdentity", "https://evil.test/in/alex-a",
])
def test_changed_or_missing_scraper_identity_halts_even_if_name_changed(
    tmp_path, monkeypatch, profile_url,
):
    import csv
    import io

    attio, pb, cache, daily = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    entry = person("a", "https://linkedin.com/in/alex-a")
    entry.update(stage="DM1 Sent", dm_step=1)
    attio.query_list_entries.return_value = [entry]
    cache.get.return_value = ("Alex Smith", "Co", entry["linkedin_url"], "", "")
    row = thread(profile_url, "Renamed Person")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
    pb.download_result_csv.return_value = buf.getvalue()
    path = tmp_path / "identity.csv"
    path.write_text(
        "sales_id,public_key,public_url,crm_alias_key,crm_alias_evidence,provenance\n"
        "ACwExact,https://linkedin.com/in/alex-a,https://linkedin.com/in/alex-a,,,provider-export\n"
    )
    monkeypatch.setenv("OUTBOUND_INBOX_IDENTITY_MAP", str(path))
    with (
        patch.object(dr.AttioClient, "parse_entry", side_effect=lambda e: e),
        patch.object(dr, "_pb_session_args", return_value={}),
        pytest.raises(dr.IdentityResolutionHalt),
    ):
        dr.detect_responses(attio, pb, "scraper", cache=cache, daily_run=daily)
    daily.set_reply_detection_status.assert_called_once_with("failed")
    attio.update_list_entry.assert_not_called()
    attio.create_note.assert_not_called()


def test_header_only_inbox_csv_halts_with_manifest(tmp_path, monkeypatch):
    attio, pb, cache, daily = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    entry = person("a", "https://linkedin.com/in/alex-a")
    entry.update(stage="DM1 Sent", dm_step=1)
    attio.query_list_entries.return_value = [entry]
    cache.get.return_value = ("Alex Smith", "Co", entry["linkedin_url"], "", "")
    pb.download_result_csv.return_value = "participantProfileUrl,participantFullName\n"
    path = tmp_path / "identity.csv"
    path.write_text(
        "sales_id,public_key,public_url,crm_alias_key,crm_alias_evidence,provenance\n"
        "ACwExact,https://linkedin.com/in/alex-a,https://linkedin.com/in/alex-a,,,provider-export\n"
    )
    monkeypatch.setenv("OUTBOUND_INBOX_IDENTITY_MAP", str(path))
    with (
        patch.object(dr.AttioClient, "parse_entry", side_effect=lambda e: e),
        patch.object(dr, "_pb_session_args", return_value={}),
        pytest.raises(dr.IdentityResolutionHalt),
    ):
        dr.detect_responses(attio, pb, "scraper", cache=cache, daily_run=daily)
    daily.set_reply_detection_status.assert_called_once_with("failed")
    attio.update_list_entry.assert_not_called()


def test_full_pipeline_match_outside_dm_stages_does_not_create_later_hold(capsys):
    import csv
    import io

    attio, pb, cache, daily = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    dm = person("dm", "https://linkedin.com/in/alex-dm")
    dm.update(record_id="dm", stage="DM1 Sent", dm_step=1)
    responded = person("responded", "https://linkedin.com/in/alex-responded")
    responded.update(record_id="responded", stage="Responded")
    attio.query_list_entries.return_value = [dm, responded]
    cache.get.side_effect = lambda rid: (
        "Alex Smith", "Co", f"https://linkedin.com/in/alex-{rid}", "", "")
    row = thread(responded["linkedin_url"])
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
    pb.download_result_csv.return_value = buf.getvalue()
    with (
        patch.object(dr.AttioClient, "parse_entry", side_effect=lambda e: e),
        patch.object(dr, "_pb_session_args", return_value={}),
        patch.object(dr, "_detect_manual_touches"),
        patch.object(dr, "_detect_cadence_drift", return_value=[]),
    ):
        counts = dr.detect_responses(attio, pb, "scraper", cache=cache, daily_run=daily)
    assert counts.get("identity_holds", 0) == 0
    assert "Inbox identity hold" not in capsys.readouterr().err
    daily.set_reply_detection_status.assert_called_once_with("ok")
    attio.update_list_entry.assert_not_called()


@pytest.mark.parametrize("kind", ["people", "lead"])
def test_sales_nav_url_variants_preserve_exact_case_sensitive_identity(kind):
    url = f"https://www.linkedin.com/sales/{kind}/ACwExact,NAME_SEARCH,token"
    assert dr._profile_identity(url) == "li-sales:ACwExact"
    assert dr._profile_identity(url, identity_bridge={
        "li-sales:ACwExact": "https://linkedin.com/in/alex-a",
    }) == "https://linkedin.com/in/alex-a"
    assert dr._profile_identity(url.lower()) != "li-sales:ACwExact"


@pytest.mark.parametrize("tail", ["ACwExact%3Fbad", "ACwExact%2Fbad", ",token", "ACwExact%20bad"])
def test_sales_nav_identity_rejects_malformed_opaque_ids(tail):
    assert dr._profile_identity("https://linkedin.com/sales/lead/" + tail) == ""
