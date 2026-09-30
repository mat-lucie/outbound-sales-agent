"""2026-06-10 cap-trickle fix: backfill_companies repair launch per-launch cap.

Mirror of tests/test_dm_launch_cap_drain.py::
test_launch_args_carry_explicit_per_launch_count for the
``repair_bad_companies`` scraper launch: PB launch ``arguments`` REPLACE the
phantom's saved console argument wholesale, so the launch must pass
``numberOfProfilesPerLaunch=len(sheet_rows)`` explicitly or the phantom
falls back to its built-in default (10) and silently truncates bigger
repair batches.

Since 2026-06-12 the repair pipeline launches the Sales Navigator Profile
Scraper (the legacy Profile Scraper agent was deleted from the PB
workspace), so the launch must also carry the SN phantom's full-argument
contract: saved args preserved, fresh session injected into
``identities[0]`` (NOT top-level sessionCookie, which the SN phantom
silently ignores), and a fresh ``csvName`` to bust the processed-inputs
dedup DB — a repair re-scrape must re-visit profiles a prior daily-run
scrape already processed.
"""
from __future__ import annotations

import csv
from unittest.mock import MagicMock, patch


def test_repair_launch_args_carry_explicit_per_launch_count(tmp_path, monkeypatch):
    from workflows.backfill_companies import repair_bad_companies

    # Detection CSV with 3 affected records → sheet batch of 3.
    detect_csv_path = tmp_path / "detect.csv"
    with open(detect_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["linkedin_url", "attio_record_id"])
        writer.writeheader()
        for i in range(3):
            writer.writerow({
                "linkedin_url": f"https://www.linkedin.com/in/person{i}/",
                "attio_record_id": f"rec-{i}",
            })

    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake-sn-li-at")
    monkeypatch.setenv("PB_LI_USER_AGENT", "fake-ua")

    attio = MagicMock()
    attio.search_company_by_domain.return_value = None  # step 0: nothing poisoned

    pb = MagicMock()
    # Saved SN phantom argument — the launch must preserve unrelated saved
    # fields and inject the session into identities[0].
    pb.get_agent.return_value = {
        "argument": '{"identities": [{}], "savedField": "keep-me"}'
    }
    launch = MagicMock()
    launch.container_id = "ct-repair"
    pb.launch_agent.return_value = launch
    pb.download_result_csv.return_value = ""  # no CSV → early return after launch

    with patch(
        "clients.google_sheets.write_prospects_to_sheet",
        return_value="https://sheet-url.example",
    ) as mock_sheet:
        repair_bad_companies(
            attio, pb, sales_nav_profile_scraper_id="sn-scraper-id",
            detect_csv_path=str(detect_csv_path),
        )

    assert pb.launch_agent.called
    args, _ = pb.launch_agent.call_args
    assert args[0] == "sn-scraper-id"
    launch_args = args[1]
    assert launch_args["numberOfProfilesPerLaunch"] == 3, (
        "repair_bad_companies launch must pass numberOfProfilesPerLaunch == "
        "batch size (3 rows in a headerless scraper sheet); got "
        f"{launch_args.get('numberOfProfilesPerLaunch')!r}"
    )
    # SN full-argument contract: saved fields preserved, session injected
    # into identities[0], no top-level sessionCookie (the SN phantom
    # silently ignores it when identities is present).
    assert launch_args["savedField"] == "keep-me"
    assert launch_args["identities"][0]["sessionCookie"] == "fake-sn-li-at"
    assert launch_args["identities"][0]["userAgent"] == "fake-ua"
    assert "sessionCookie" not in launch_args
    # Fresh result-file name per launch: PB keys the phantom's
    # processed-inputs dedup DB on the file name; without it the repair
    # re-scrape silently no-ops on profiles a daily run already visited.
    assert launch_args.get("csvName"), "repair launch must set a fresh csvName"
    # The CSV download must be keyed to the same per-launch file name.
    assert pb.download_result_csv.call_args.kwargs.get("csv_name") == launch_args["csvName"]
    # The sheet write must request the profileUrl column — the writer's
    # default ["linkedInUrl", "message"] shape does not match the
    # {"profileUrl": ...} row keys and would write an empty sheet, turning
    # the whole repair into a silent no-op.
    assert mock_sheet.call_args.kwargs.get("columns") == ["profileUrl"], (
        "repair sheet write must pass columns=['profileUrl']; got "
        f"{mock_sheet.call_args.kwargs.get('columns')!r}"
    )
    assert mock_sheet.call_args.kwargs["include_header"] is False
