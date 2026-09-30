"""PB phantoms count Google-Sheet headers as processable CSV lines.

Send sheets retain their header and launch at batch + SHEET_HEADER_LINES.
Profile-scraper sheets omit the header and launch at the raw URL count.

Verified live 2026-06-12, twice:
1. Phase 0, 1 stale profile: PB log "Got 2 lines from csv → Processing 1
   profile" then "profileUrl is not a correct LinkedIn Profile URL" — the
   phantom processed the literal header and the run went BLIND (container
   1117150263943401). This was the behavior before headerless scraper sheets.
2. Surgical Phase 0 pass, 4 profiles: 3 scraped + flipped; the 4th got no
   result row and no recheck-cache stamp — the header ate one slot.

The send phantoms still receive a header, so their launch count includes
SHEET_HEADER_LINES. The scraper launch count now equals its URL row count.

Per-site launch-arg values are asserted in the existing site tests
(test_phase0_*, test_dm_launch_cap_drain, test_invite_launch_cap,
test_pre_invite_*, test_repair_launch_cap, test_backfill_companies_*).
This module locks the shared contract itself.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from clients.google_sheets import (
    SHEET_HEADER_LINES,
    profiles_per_launch,
    write_prospects_to_sheet,
)

# Verified 2026-06-12 via PB scripts/fetch (read-only) against the live
# workspace: SN Auto Connect (Network Booster, script id 29582) and SN
# Message Sender (script id 6318432035741982) both declare
# numberOfProfilesPerLaunch maximum=100; the SN Profile Scraper (script id
# 11108) declares 150, matching the 2026-06-11 verification. PB rejects
# the ENTIRE launch above the max, so every batch bound must leave
# SHEET_HEADER_LINES of headroom.
_SEND_PHANTOM_SCHEMA_MAX = 100


def test_send_batch_caps_leave_header_headroom_under_schema_max():
    """Invite batches are bounded by MAX_CONNECTIONS_PER_DAY and DM drain
    batches by MAX_MESSAGES_PER_DAY; both launch with batch +
    SHEET_HEADER_LINES, which must stay at or under the send phantoms'
    verified schema max or PB rejects the whole launch (a BLIND-run
    failure mode — see PHASE0_MAX_PROFILES_PER_LAUNCH's 2026-06-11
    incident for the scraper-side equivalent)."""
    from workflows.safety_limits import MAX_CONNECTIONS_PER_DAY, MAX_MESSAGES_PER_DAY

    assert MAX_CONNECTIONS_PER_DAY + SHEET_HEADER_LINES <= _SEND_PHANTOM_SCHEMA_MAX
    assert MAX_MESSAGES_PER_DAY + SHEET_HEADER_LINES <= _SEND_PHANTOM_SCHEMA_MAX


def test_profiles_per_launch_adds_exactly_the_header_lines():
    assert profiles_per_launch(1) == 1 + SHEET_HEADER_LINES
    assert profiles_per_launch(50) == 50 + SHEET_HEADER_LINES
    # The premise of the whole contract: exactly ONE header row today. If
    # this changes, write_prospects_to_sheet changed shape and every PB
    # launch count changes with it — re-verify against a live phantom log.
    assert SHEET_HEADER_LINES == 1


def test_sheet_writer_prepends_exactly_one_header_row():
    """The +1 exists because write_prospects_to_sheet always writes
    [columns] + data rows. Lock that premise."""
    ws = MagicMock()
    sh = MagicMock()
    sh.worksheet.return_value = ws
    gc = MagicMock()
    gc.open_by_key.return_value = sh

    rows = [{"profileUrl": f"https://www.linkedin.com/in/p{i}/"} for i in range(3)]
    with patch("clients.google_sheets.get_client", return_value=gc):
        write_prospects_to_sheet(rows, spreadsheet_id="sid", columns=["profileUrl"])

    written = ws.update.call_args.args[0]
    assert written[0] == ["profileUrl"], "first row must be the header"
    assert len(written) == len(rows) + SHEET_HEADER_LINES
    # ws.clear() before the write is LOAD-BEARING for the +1: it guarantees
    # the sheet holds exactly header + today's rows, so over-asking by the
    # header line can never reach a stale tail row from a previous, larger
    # batch (which on a send phantom would message a prospect not in
    # today's batch, unaccounted by the per-row advance loop).
    ws.clear.assert_called_once()


def test_profile_scraper_sheet_contains_only_url_rows():
    ws = MagicMock()
    sh = MagicMock()
    sh.worksheet.return_value = ws
    gc = MagicMock()
    gc.open_by_key.return_value = sh

    rows = [{"profileUrl": f"https://www.linkedin.com/in/p{i}/"} for i in range(3)]
    with patch("clients.google_sheets.get_client", return_value=gc):
        write_prospects_to_sheet(
            rows, spreadsheet_id="sid", columns=["profileUrl"],
            include_header=False,
        )

    assert ws.update.call_args.args[0] == [[row["profileUrl"]] for row in rows]
    ws.clear.assert_called_once()
