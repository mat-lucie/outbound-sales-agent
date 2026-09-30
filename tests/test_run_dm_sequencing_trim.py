"""Tests for the trim-to-cap behavior in run_dm_sequencing.

Locks in the DM1-first reservation policy: when the total queue exceeds the
daily messages cap, all DM1 (fresh-accept) entries must be sent before any
DM3 or DM2 entries are kept, since fresh-accept momentum decays fast and
the ACCEPTED-stage pool is naturally small.
"""
from __future__ import annotations

import os
from datetime import date
from unittest.mock import MagicMock, patch

from tests.test_consistency_sweep import _company as _sweep_company
from tests.test_consistency_sweep import _raw_entry as _sweep_raw_entry
from tests.test_integration import _attio_with_full_schema
from workflows.daily_run import DailyRun


def _entry(record_id: str, stage: str, last_date: str, dm_step: int = 0) -> dict:
    return {
        "record_id": record_id,
        "entry_id": f"ent-{record_id}",
        "stage": stage,
        "last_contact_date": last_date,
        "dm_step": dm_step,
        "persona": "operations_leaders",
        "language": "en",
    }


def _fake_daily_run(remaining: int = 30) -> MagicMock:
    """PR-17: dry_run trim tests still need a daily_run mock since the
    parameter is required and remaining_cap reads from daily_run.remaining()."""
    mock = MagicMock(spec=DailyRun)
    mock.remaining.return_value = remaining
    return mock


@patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
                          "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0",
                          "GSHEET_AUTOCONNECT_ID": "fake-sheet-id"})
@patch("workflows.daily_check._get_all_entries_with_raw")
@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet/x")
@patch("workflows.daily_check._pb_session_args", return_value={})
class TestDM1ReservationOnTrim:
    """Cap = 30. Queue: 4 DM1 + 30 DM2 + 6 DM3 = 40. Must send all 4 DM1."""

    def _setup_cache(self, *record_ids):
        cache = MagicMock()
        cache.get.side_effect = lambda rid: (
            f"Name-{rid}", f"Company-{rid}", f"https://www.linkedin.com/in/{rid}", "manufacturing", "Plant Director"
        )
        return cache

    def test_dm1_reserved_when_total_exceeds_cap(
        self, _pb_args, _sheet, _get_entries,
    ):
        from workflows.daily_check import run_dm_sequencing

        # 4 ACCEPTED + 30 DM1_SENT + 6 DM2_SENT, all cadence-eligible.
        today = date(2026, 5, 20)
        entries = (
            [_entry(f"a{i}", "Accepted", "2026-05-18", dm_step=0) for i in range(4)]
            + [_entry(f"b{i}", "DM1 Sent", "2026-05-10", dm_step=1) for i in range(30)]
            + [_entry(f"c{i}", "DM2 Sent", "2026-05-05", dm_step=2) for i in range(6)]
        )
        _get_entries.return_value = ([], entries)

        attio = _attio_with_full_schema(_person_to_company={})
        pb = MagicMock()
        pb.download_result_csv.return_value = ""
        cache = self._setup_cache()

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            result = run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
            )

        # All 4 DM1 reserved; remaining 26 split DM3 > DM2.
        # Dry-run reports queued-counts via `dry_run` sub-dict (see Task A.2).
        dr = result["dry_run"]
        assert dr["dm1"] == 4, f"expected 4 DM1 reserved, got {dr['dm1']}"
        assert dr["dm3"] == 6, "DM3 takes priority for remaining budget"
        assert dr["dm2"] == 20, "DM2 gets leftover budget (30 - 4 - 6 = 20)"
        assert dr["dm1"] + dr["dm2"] + dr["dm3"] == 30

    def test_dm1_alone_exceeds_cap_keeps_oldest(
        self, _pb_args, _sheet, _get_entries,
    ):
        """If DM1 queue alone exceeds cap (unusual — >30 accepts in a day),
        keep the oldest cap-worth and drop DM2/DM3 entirely."""
        from workflows.daily_check import run_dm_sequencing

        today = date(2026, 5, 20)
        # 35 ACCEPTED with varying ages (5 oldest at 2026-05-15, 30 at 2026-05-18)
        entries = (
            [_entry(f"a{i}", "Accepted", "2026-05-15", dm_step=0) for i in range(5)]
            + [_entry(f"a{5 + i}", "Accepted", "2026-05-18", dm_step=0) for i in range(30)]
            + [_entry("c0", "DM2 Sent", "2026-05-05", dm_step=2)]
        )
        _get_entries.return_value = ([], entries)

        attio = _attio_with_full_schema(_person_to_company={})
        pb = MagicMock()
        cache = self._setup_cache()

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            result = run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
            )

        assert result["dry_run"]["dm1"] == 30, "cap-worth of oldest DM1 kept"
        assert result["dry_run"]["dm2"] == 0
        assert result["dry_run"]["dm3"] == 0

    def test_no_trim_needed_below_cap(
        self, _pb_args, _sheet, _get_entries,
    ):
        """When total ≤ cap, trim block is skipped entirely — all queued sent."""
        from workflows.daily_check import run_dm_sequencing

        today = date(2026, 5, 20)
        entries = (
            [_entry("a0", "Accepted", "2026-05-18", dm_step=0)]
            + [_entry("b0", "DM1 Sent", "2026-05-10", dm_step=1)]
            + [_entry("c0", "DM2 Sent", "2026-05-05", dm_step=2)]
        )
        _get_entries.return_value = ([], entries)

        attio = _attio_with_full_schema(_person_to_company={})
        pb = MagicMock()
        cache = self._setup_cache()

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            result = run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
            )

        assert result["dry_run"] == {"dm1": 1, "dm2": 1, "dm3": 1}

    def test_no_dms_due_still_runs_consistency_sweep(
        self, _pb_args, _sheet, _get_entries,
    ):
        """Fix 1 wiring: when no DMs are due (early return), the
        consistency sweep epilogue still runs and its result is present
        in the return dict."""
        from workflows.daily_check import run_dm_sequencing

        today = date(2026, 5, 20)
        # No cadence-eligible entries → total_messages == 0 → early return
        _get_entries.return_value = ([], [])

        attio = _attio_with_full_schema()
        # search_companies already returns [] via _attio_with_full_schema
        pb = MagicMock()
        cache = self._setup_cache()

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            result = run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
            )

        assert "consistency_sweep" in result, (
            "Fix 1: consistency_sweep key must be present even on "
            "no-DMs-due early return"
        )
        assert result["consistency_sweep"]["companies_checked"] == 0

    def test_dry_run_final_epilogue_reuses_snapshot_no_refetch(
        self, _pb_args, _sheet, _get_entries,
    ):
        """Perf fix: dry runs advance nothing, so the final epilogue must
        reuse the run's snapshot instead of refetching via query_list_entries.

        Drive run_dm_sequencing(dry_run=True) with a non-empty DM queue so
        execution reaches the final epilogue (not an early-exit path).
        The raw snapshot passed back by _get_all_entries_with_raw contains a
        company's person entry so the sweep actually reaches its entries gate.
        Assert query_list_entries was NOT called (the sweep used the snapshot)
        and that results["consistency_sweep"] is present (epilogue ran).
        """
        from workflows.daily_check import run_dm_sequencing

        today = date(2026, 5, 20)
        # One DM1-due entry so there is a non-empty queue and the send loop
        # executes (dry branch echoes + continues, no writes).
        person_id = "rec_p_perf"
        entries = [_entry(person_id, "Accepted", "2026-05-18", dm_step=0)]
        # Non-empty raw snapshot: contains the person entry so the sweep's
        # entries_by_record index can be built without a refetch.
        raw_snapshot = [_sweep_raw_entry(person_id)]
        _get_entries.return_value = (raw_snapshot, entries)

        # Wire a company so the sweep reaches the entries gate (the
        # `if not companies: return summary` would short-circuit otherwise,
        # making query_list_entries unreachable even on the refetch path and
        # rendering this assertion vacuous).
        company = _sweep_company("co_perf", "2026-05-18T12:00:00Z", person_id, "DM1")
        attio = _attio_with_full_schema()
        attio.search_companies.return_value = [company]
        # Wire _filter_and_rank_entries_for_record so the sweep finds a
        # consistent entry (dm_step=1, stage DM1 Sent) → no repair needed,
        # epilogue completes cleanly.
        from tests.test_consistency_sweep import _entry as _sweep_entry
        attio._filter_and_rank_entries_for_record.return_value = [
            _sweep_entry("ent_perf", dm_step=1, stage="DM1 Sent")
        ]

        pb = MagicMock()
        cache = self._setup_cache(person_id)

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            result = run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
            )

        # Epilogue ran — key must be present.
        assert "consistency_sweep" in result, (
            "dry-run epilogue must run and populate consistency_sweep"
        )
        # The snapshot was reused — no refetch should have fired.
        attio.query_list_entries.assert_not_called()

    def test_epilogue_line_surfaces_unparseable_entries(
        self, _pb_args, _sheet, _get_entries, capsys,
    ):
        """entries_unparseable was audit-event-only — unparseable raw
        entries can hide divergences as false 'no_list_entry', so the
        operator-facing CLI summary line must show the count (and warn)
        when it is > 0."""
        from workflows.daily_check import run_dm_sequencing

        today = date(2026, 5, 20)
        person_id = "rec_p_unp"
        entries = [_entry(person_id, "Accepted", "2026-05-18", dm_step=0)]
        # Snapshot: one good raw entry + one garbage item parse_entry
        # cannot read (a str has no .get) → entries_unparseable == 1.
        raw_snapshot = [_sweep_raw_entry(person_id), "not-a-dict"]
        _get_entries.return_value = (raw_snapshot, entries)

        company = _sweep_company(
            "co_unp", "2026-05-18T12:00:00Z", person_id, "DM1"
        )
        attio = _attio_with_full_schema()
        attio.search_companies.return_value = [company]
        # Person entry is consistent → the ONLY warn trigger left is the
        # unparseable count, which pins that it warns on its own.
        from tests.test_consistency_sweep import _entry as _sweep_entry
        attio._filter_and_rank_entries_for_record.return_value = [
            _sweep_entry("ent_unp", dm_step=1, stage="DM1 Sent")
        ]

        pb = MagicMock()
        cache = self._setup_cache(person_id)

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            result = run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
            )

        assert result["consistency_sweep"]["entries_unparseable"] == 1
        captured = capsys.readouterr()
        # Warned line goes to stderr and carries the count.
        assert "1 entries unparseable" in captured.err, (
            f"CLI summary line must surface entries_unparseable; "
            f"stderr was: {captured.err!r}"
        )
        assert "1 entries unparseable" not in captured.out

    def test_epilogue_line_surfaces_company_cap_hit(
        self, _pb_args, _sheet, _get_entries, capsys,
    ):
        """company_cap_hit was audit-event-only: a capped search silently
        skips an unknown number of stamped companies, so the CLI summary
        line must carry the marker (and warn) — same class of gap as
        entries_unparseable."""
        from workflows.daily_check import run_dm_sequencing

        today = date(2026, 5, 20)
        # No DMs due → early exit path; the epilogue still sweeps.
        _get_entries.return_value = ([], [])

        attio = _attio_with_full_schema()
        # 500 returned companies == the search cap → company_cap_hit.
        # CONNECTION_SENT stamps carry no dm_step floor, so each is
        # skipped_no_floor (which does NOT warn on its own) — the cap
        # marker must trigger the warn by itself.
        attio.search_companies.return_value = [
            _sweep_company(
                f"co_cap{i}", "2026-05-18T12:00:00Z", f"p_cap{i}",
                "CONNECTION_SENT",
            )
            for i in range(500)
        ]

        pb = MagicMock()
        cache = self._setup_cache()

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            result = run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
            )

        assert result["consistency_sweep"]["company_cap_hit"] is True
        captured = capsys.readouterr()
        assert "COMPANY CAP HIT" in captured.err, (
            f"CLI summary line must surface the search-cap hit; "
            f"stderr was: {captured.err!r}"
        )

    def test_dm1_reservation_uses_oldest_when_dm1_queue_capped(
        self, _pb_args, _sheet, _get_entries, capsys,
    ):
        """When DM1 > cap, keep oldest by last_contact_date, defer newest."""
        from workflows.daily_check import run_dm_sequencing

        today = date(2026, 5, 20)
        # 32 DM1 entries: 30 old (2026-05-14) + 2 new (2026-05-18). Cap=30.
        entries = (
            [_entry(f"old{i}", "Accepted", "2026-05-14", dm_step=0) for i in range(30)]
            + [_entry(f"new{i}", "Accepted", "2026-05-18", dm_step=0) for i in range(2)]
        )
        _get_entries.return_value = ([], entries)

        attio = _attio_with_full_schema(_person_to_company={})
        pb = MagicMock()
        cache = self._setup_cache()

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            result = run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
            )

        assert result["dry_run"]["dm1"] == 30
        # Lock in WHICH 30 are kept by inspecting the [DRY RUN] click.echo
        # output for the new entries — they must NOT appear (deferred).
        # capsys captures everything click.echo writes to stdout.
        captured = capsys.readouterr().out
        assert "https://www.linkedin.com/in/new0" not in captured, \
            "newest DM1 new0 should be deferred, not dry-run-shown"
        assert "https://www.linkedin.com/in/new1" not in captured, \
            "newest DM1 new1 should be deferred, not dry-run-shown"


# ── Cancel-gate regression (dropped return restored) ────────────────────────


@patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
                          "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0",
                          "GSHEET_AUTOCONNECT_ID": "fake-sheet-id"})
@patch("workflows.daily_check._get_all_entries_with_raw")
@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet/x")
@patch("workflows.daily_check._pb_session_args", return_value={})
@patch("workflows.daily_check.click.confirm", return_value=False)
def test_cancel_at_confirm_gate_does_not_send(
    _confirm, _pb_args, _sheet, _get_entries,
):
    """Regression: the cancelled branch of run_dm_sequencing must `return`
    early, so answering "no" to the DM confirm prompt never falls through
    into the wet send pipeline.

    Invariants after a 'no' answer:
    - result["cancelled"] is True
    - pb.launch_agent is never called (no PB send)
    - attio.update_list_entry is never called (no Attio entry writes)
    - attio.update_company is never called (no company tally stamp —
      a tally without a send would poison the consistency sweep's
      source of truth)
    """
    from workflows.daily_check import run_dm_sequencing

    today = date(2026, 5, 20)
    # One DM1-due entry so the queue is non-empty and the confirm gate fires.
    entries = [_entry("a0", "Accepted", "2026-05-18", dm_step=0)]
    _get_entries.return_value = ([], entries)

    attio = _attio_with_full_schema()
    pb = MagicMock()
    cache = MagicMock()
    cache.get.return_value = (
        "Name-a0", "Company-a0", "https://www.linkedin.com/in/a0",
        "manufacturing", "Plant Director",
    )

    with patch("workflows.daily_check.date") as mock_date:
        mock_date.today.return_value = today
        mock_date.fromisoformat = date.fromisoformat
        result = run_dm_sequencing(
            attio, pb, "sender-id",
            daily_run=_fake_daily_run(remaining=30),
            dry_run=False,
            auto_confirm=False,
            cache=cache,
        )

    assert result.get("cancelled") is True, (
        "result must carry cancelled=True when the user answers 'no'"
    )
    pb.launch_agent.assert_not_called()
    attio.update_list_entry.assert_not_called()


# ── PR-237: per-run operator exclusions (--exclude) ─────────────────────────


@patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
                          "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0",
                          "GSHEET_AUTOCONNECT_ID": "fake-sheet-id"})
@patch("workflows.daily_check._get_all_entries_with_raw")
@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet/x")
@patch("workflows.daily_check._pb_session_args", return_value={})
class TestSendDmsExcludeFilter:
    """PR-237: `exclude_ids` drops matching entry_id/record_id rows from the
    DM-due selection for this run only, in BOTH the wet queue and the dry-run
    preview. This fork is machine-keyed (no ownership claim layer), so the
    filter simply prunes the parsed pool before selection."""

    def _setup_cache(self):
        cache = MagicMock()
        cache.get.side_effect = lambda rid: (
            f"Name-{rid}", f"Company-{rid}",
            f"https://www.linkedin.com/in/{rid}", "manufacturing",
            "Plant Director",
        )
        return cache

    def test_exclude_drops_matching_entry_and_record_id(
        self, _pb_args, _sheet, _get_entries, capsys,
    ):
        """exclude_ids matching an entry_id AND a record_id both drop from the
        queue; unmatched-but-present rows stay."""
        from workflows.daily_check import run_dm_sequencing

        today = date(2026, 5, 20)
        # 3 fresh-accept DM1-due rows: a0 (entry_id ent-a0), a1, a2.
        entries = [
            _entry("a0", "Accepted", "2026-05-18", dm_step=0),
            _entry("a1", "Accepted", "2026-05-18", dm_step=0),
            _entry("a2", "Accepted", "2026-05-18", dm_step=0),
        ]
        _get_entries.return_value = ([], entries)

        attio = _attio_with_full_schema()
        pb = MagicMock()
        cache = self._setup_cache()

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            result = run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
                # Exclude a0 by entry_id and a1 by record_id.
                exclude_ids={"ent-a0", "a1"},
            )

        # Only a2 survives to the DM1 queue.
        assert result["dry_run"]["dm1"] == 1, (
            f"only a2 should remain DM1-due, got {result['dry_run']}"
        )
        captured = capsys.readouterr().out
        assert "[excluded by operator]" in captured
        # The excluded rows must not be previewed.
        assert "https://www.linkedin.com/in/a0" not in captured
        assert "https://www.linkedin.com/in/a1" not in captured
        assert "https://www.linkedin.com/in/a2" in captured

    def test_exclude_dry_run_issues_zero_writes(
        self, _pb_args, _sheet, _get_entries,
    ):
        """The exclude filter is pure in-memory pruning: a dry run with an
        exclusion writes nothing to Attio and launches no PB agent."""
        from workflows.daily_check import run_dm_sequencing

        today = date(2026, 5, 20)
        entries = [
            _entry("a0", "Accepted", "2026-05-18", dm_step=0),
            _entry("a1", "Accepted", "2026-05-18", dm_step=0),
        ]
        _get_entries.return_value = ([], entries)

        attio = _attio_with_full_schema()
        pb = MagicMock()
        cache = self._setup_cache()

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
                exclude_ids={"ent-a0"},
            )

        pb.launch_agent.assert_not_called()
        attio.update_list_entry.assert_not_called()
        attio.update_company.assert_not_called()

    def test_exclude_unmatched_id_warns(
        self, _pb_args, _sheet, _get_entries, capsys,
    ):
        """A supplied id matching nothing prints a loud warning (typo /
        already-sent / not-due) and leaves the queue untouched."""
        from workflows.daily_check import run_dm_sequencing

        today = date(2026, 5, 20)
        entries = [_entry("a0", "Accepted", "2026-05-18", dm_step=0)]
        _get_entries.return_value = ([], entries)

        attio = _attio_with_full_schema()
        pb = MagicMock()
        cache = self._setup_cache()

        with patch("workflows.daily_check.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            result = run_dm_sequencing(
                attio, pb, "sender-id",
                daily_run=_fake_daily_run(remaining=30),
                dry_run=True, auto_confirm=True, cache=cache,
                exclude_ids={"does-not-exist"},
            )

        # Queue is unaffected — a0 still DM1-due.
        assert result["dry_run"]["dm1"] == 1
        captured = capsys.readouterr().out
        assert "does-not-exist" in captured
        assert "not in today's DM queue" in captured
