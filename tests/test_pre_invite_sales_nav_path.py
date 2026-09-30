"""PR-B.9 — end-to-end coverage of the Sales Nav backend branch of
:func:`workflows.pre_invite_check._pre_invite_degree_check`.

The Sales Nav branch ships in PR-B.5/B.10/B.13/B.16. Its §3.1-hardened
partition narrows the default arm to escalate-and-drop: ONLY explicit
"2nd" or "3rd" degrees route to invite. Blank cells, missing CSV rows,
unknown degree values, and `hasPendingInvitation="true"` all
escalate-and-drop. This file locks the partition decision-table so a
future refactor of either the Sales Nav phantom contract or the partition
default arm can't silently revert to the §3.1-violating pre-PR-B behavior.

Verified against PB phantom 602790655114603 on 2026-05-25:
- CSV columns: linkedinProfileUrl, query, fullName, connectionDegree,
  hasPendingInvitation, ...
- hasPendingInvitation is the string "true" or "false" (not bool/empty)
- query echoes the input URL exactly
- linkedinProfileUrl echoes the canonical /in/ URL
- connectionDegree shares the same column name as the legacy regular
  Profile Scraper (intentional schema overlap)
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from clients.pb_envelope import PBRunFailed, PBRunTimeout
from workflows.pre_invite_check import (
    REASON_SALES_NAV_CSV_MISS,
    REASON_SALES_NAV_DEGREE_UNKNOWN,
    _pre_invite_degree_check,
)

# ----------------------------------------------------------------------
# Fixtures + helpers
# ----------------------------------------------------------------------


SALES_NAV_SCRAPER_ID = "phantom-602790655114603"
LEGACY_SCRAPER_ID = "phantom-LEGACY-different-id"


@pytest.fixture(autouse=True)
def _sales_nav_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enable the sales_nav backend for every test in this file."""
    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "sales_nav")
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", SALES_NAV_SCRAPER_ID)
    monkeypatch.setenv("PB_PROFILE_SCRAPER_ID", LEGACY_SCRAPER_ID)
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake-li-at")
    monkeypatch.setenv("PB_LI_USER_AGENT", "Mozilla/5.0 (test)")


# Stable mapping suffix → full LinkedIn slug. Using single-letter slugs ("a")
# would silently collide with the CSV's "/in/alice" URLs and produce confusing
# test failures (URL mismatch != partition bug).
_SLUGS = {
    "A": "alice",
    "B": "bob",
    "C": "carol",
    "D": "dan",
    "E": "eve",
    "F": "frank",
}


def _row(suffix: str, url: str | None = None) -> dict:
    """Build a to_send_data row with all PR-21-required keys."""
    if url is None:
        slug = _SLUGS.get(suffix, suffix.lower())
        url = f"https://www.linkedin.com/in/{slug}"
    return {
        "linkedInUrl": url,
        "entry_id": f"ent-{suffix}",
        "record_id": f"rec-{suffix}",
        "current_stage": "Prospect",
        "experiment_id": "exp-test",
        "experiment_id_frozen_at": "prospect",
        "message": f"hi {suffix}",
    }


def _sales_nav_csv(rows: list[dict]) -> str:
    """Build a Sales Nav Profile Scraper CSV string.

    Each row dict must carry: url, degree, pending (optional, default "false"),
    name (optional, default ""). Header matches the columns we use; the live
    phantom emits ~25 columns but only these are read.
    """
    if not rows:
        return ""
    header = (
        "linkedinProfileUrl,connectionDegree,hasPendingInvitation,query,fullName\n"
    )
    body_lines = []
    for r in rows:
        pending = r.get("pending", "false")
        name = r.get("name", "")
        body_lines.append(
            f"{r['url']},{r.get('degree', '')},{pending},{r['url']},{name}"
        )
    return header + "\n".join(body_lines)


def _make_pb(csv_text: str | None = "", container_id: str = "container-SN-1") -> MagicMock:
    """Build a MagicMock PB client with sensible defaults for Sales Nav.

    The helper sets up:
    - `get_agent` returns a minimal saved-argument shape with an
      identities slot the launch helper will write into.
    - `launch_agent` returns a launch with the given container_id.
    - `wait_for_completion` is a no-op MagicMock (override for failure tests).
    - `download_result_csv` returns the given CSV (empty/None defaults).
    """
    pb = MagicMock()
    pb.get_agent.return_value = {
        "argument": '{"identities": [{}], "numberOfProfilesPerLaunch": 10}'
    }
    launch = MagicMock()
    launch.container_id = container_id
    pb.launch_agent.return_value = launch
    pb.download_result_csv.return_value = csv_text
    return pb


def _record_escalate(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """Replace the module-level escalate() with a recorder; return the call log."""
    calls: list[dict] = []

    def _fake(**kw):
        calls.append(kw)
        return {"id": "x"}

    monkeypatch.setattr(
        "workflows.pre_invite_check.escalate",
        _fake,
    )
    return calls


# ----------------------------------------------------------------------
# Happy path
# ----------------------------------------------------------------------


@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet.example/foo")
@patch("workflows.daily_check._pb_session_args", return_value={})
class TestSalesNavHappyPath:
    """Verified phantom contract: degree=2nd → invite, pending=true → drop."""

    def test_three_valid_two_pending_partitions_correctly(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """5 prospects: 3 with valid degree, 2 with hasPendingInvitation=true.
        Expect 3 in still_to_invite, 2 escalations, 0 already_connected."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()

        urls = [
            ("https://www.linkedin.com/in/alice", "2nd", "false"),
            ("https://www.linkedin.com/in/bob", "3rd", "false"),
            ("https://www.linkedin.com/in/carol", "2nd", "false"),
            ("https://www.linkedin.com/in/dan", "2nd", "true"),  # pending
            ("https://www.linkedin.com/in/eve", "3rd", "true"),  # pending
        ]
        csv = _sales_nav_csv([
            {"url": u, "degree": d, "pending": p}
            for u, d, p in urls
        ])
        pb = _make_pb(csv_text=csv)

        batch = [
            _row("A", urls[0][0]),
            _row("B", urls[1][0]),
            _row("C", urls[2][0]),
            _row("D", urls[3][0]),
            _row("E", urls[4][0]),
        ]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        assert len(still) == 3
        assert {r["entry_id"] for r in still} == {"ent-A", "ent-B", "ent-C"}
        assert already == []

        # The two pending rows are FLIPPED to CONNECTION_SENT (Pattern-A fix),
        # not escalate-and-dropped — so they leave the invite pool permanently
        # instead of re-queuing forever. §3.1: pending must NEVER reach
        # still_to_invite (asserted above).
        flips = {
            c.kwargs["entry_id"]: c.kwargs["entry_attributes"]
            for c in attio.update_list_entry.call_args_list
        }
        assert flips["ent-D"]["stage"] == "Connection Sent"
        assert flips["ent-E"]["stage"] == "Connection Sent"


# ----------------------------------------------------------------------
# Decision-table coverage — §3.1 SAFETY
# ----------------------------------------------------------------------


@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet.example/foo")
@patch("workflows.daily_check._pb_session_args", return_value={})
class TestSalesNavPartitionDecisionTable:
    """Each test pins one cell of the §3.1-hardened partition table.

    Decision table (verified against PR-B code, pre_invite_check.py lines
    ~819-852, against §3.1 violations F1/F2/F4):

      hasPendingInvitation=true → ALWAYS flip to CONNECTION_SENT
        (Pattern-A fix; never invite), regardless of degree.
      degree="1st"             → flip to ACCEPTED (Pattern-A path).
      degree in ("2nd", "3rd") → still_to_invite.
      degree="" (blank)        → drop+escalate (REASON_SALES_NAV_DEGREE_BLANK).
      degree=missing CSV row   → drop+escalate (REASON_SALES_NAV_CSV_MISS).
      degree=any other value   → drop+escalate (REASON_SALES_NAV_DEGREE_UNKNOWN).
    """

    def test_csv_missing_row_drops_and_escalates_csv_miss(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """Prospect not in CSV → drop + REASON_SALES_NAV_CSV_MISS.

        Note on double-escalation (KNOWN behavior, not a bug fixed by
        this test): a row missing from the CSV currently produces TWO
        operator-queue rows under sales_nav backend:

          1. Partial-CSV signal (scrape-time) → REASON_SALES_NAV_CSV_MISS
          2. Partition default arm (partition-time, sees degree="") →
             REASON_SALES_NAV_DEGREE_BLANK

        Both have distinct idempotency keys (failure_reason is part of
        the key) so the operator queue UI shows two rows for one prospect.
        Operationally noisy but not §3.1-violating — both decisions are
        "drop, do not invite." This test asserts the §3.1 safety property
        (Carol dropped + CSV_MISS emitted) and pins the double-emit shape
        so a future de-noise refactor (collapse to one) is intentional.

        The idempotency key MUST embed both record_id and the
        scrape_attempt_id so re-runs against the same scrape container
        don't duplicate the queue row."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()
        # Carol is intentionally missing from the CSV.
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/bob", "degree": "2nd"},
        ])
        pb = _make_pb(csv_text=csv, container_id="container-CSV-MISS")

        batch = [_row("A"), _row("B"), _row("C")]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        # A + B invited; Carol dropped.
        assert {r["entry_id"] for r in still} == {"ent-A", "ent-B"}
        assert already == []
        # Carol absent from still_to_invite (CRITICAL §3.1 assertion).
        assert all(r["entry_id"] != "ent-C" for r in still)

        carol_calls = [
            c for c in escalations
            if c["payload"]["record_id"] == "rec-C"
        ]
        # PR-B.9 fold-in (de-noise): missing-CSV-row emits CSV_MISS once
        # and SUPPRESSES the downstream BLANK-degree emit so the operator
        # queue shows exactly one row per missing prospect with the
        # richest payload (row counts). The pre-de-noise shape emitted
        # both — see migration plan reflective-singing-waterfall.md and
        # the unanimous (a) reviewer pick.
        carol_reasons = [c["payload"]["failure_reason"] for c in carol_calls]
        assert carol_reasons == [REASON_SALES_NAV_CSV_MISS], (
            f"missing row MUST emit CSV_MISS exactly once "
            f"(no DEGREE_BLANK follow-up); got {carol_reasons!r}"
        )

        csv_miss_call = carol_calls[0]
        # CSV_MISS payload carries observed/expected row counts so the
        # operator can spot phantom-side drift (e.g. 0/N → cookie expired).
        assert csv_miss_call["payload"]["csv_row_count_observed"] == 2
        assert csv_miss_call["payload"]["csv_row_count_expected"] == 3
        # Idempotency key embeds record_id, failure_reason, scrape container id.
        idem = csv_miss_call["idempotency_key"]
        assert "rec-C" in idem
        assert "container-CSV-MISS" in idem
        assert REASON_SALES_NAV_CSV_MISS in idem

    def test_blank_degree_cell_drops_and_escalates_via_csv_miss(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """Prospect in CSV but `connectionDegree` is empty string → drop
        + REASON_SALES_NAV_CSV_MISS.

        CRITICAL §3.1 assertion: pre-PR-B legacy code DID send blank-
        degree rows to still_to_invite. PR-B narrows the default arm —
        this test prevents regression.

        PR-B.9 fold-in (de-noise): the scrape parser drops blank-degree
        rows from degree_lookup, so blank-degree-in-CSV looks identical to
        missing-from-CSV downstream. CSV_MISS emits at scrape time with
        the richer payload (csv_row_count_observed/expected); the
        partition-time DEGREE_BLANK emit is SUPPRESSED for record_ids
        that already escalated CSV_MISS this run. One row per prospect."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()
        # Bob has a blank degree cell — explicit empty string.
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/bob", "degree": ""},
        ])
        pb = _make_pb(csv_text=csv)

        batch = [_row("A"), _row("B")]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        # Bob MUST NOT be in still_to_invite (§3.1 contract).
        assert all(r["entry_id"] != "ent-B" for r in still), (
            "§3.1 violation: blank-degree row reached invite queue"
        )
        assert {r["entry_id"] for r in still} == {"ent-A"}
        assert already == []

        bob_calls = [
            c for c in escalations
            if c["payload"]["record_id"] == "rec-B"
        ]
        reasons = [c["payload"]["failure_reason"] for c in bob_calls]
        # Exactly one escalation: CSV_MISS (DEGREE_BLANK suppressed).
        assert reasons == [REASON_SALES_NAV_CSV_MISS], (
            f"blank-degree should emit CSV_MISS exactly once; got {reasons!r}"
        )

    def test_unknown_degree_value_drops_and_escalates_degree_unknown(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """Both "You" (transient scrape noise) and "out_of_network" escalate
        REASON_SALES_NAV_DEGREE_UNKNOWN and are kept out of still_to_invite.

        Wave-2-A divergence: a genuine "out_of_network" target is ALSO parked
        at UNREACHABLE (it can never be invited from this account), so it
        leaves the invite pool for good instead of being re-scraped +
        re-escalated every run. "You" stays escalate-and-drop (a scrape
        glitch — the operator himself shows up as "You" — that should be retried)."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "You"},
            {"url": "https://www.linkedin.com/in/bob", "degree": "out_of_network"},
            {"url": "https://www.linkedin.com/in/carol", "degree": "2nd"},
        ])
        pb = _make_pb(csv_text=csv)

        batch = [_row("A"), _row("B"), _row("C")]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        # Only Carol invited.
        assert {r["entry_id"] for r in still} == {"ent-C"}
        assert already == []

        unknown_calls = [
            c for c in escalations
            if c["payload"].get("failure_reason")
            == REASON_SALES_NAV_DEGREE_UNKNOWN
        ]
        assert len(unknown_calls) == 2
        record_ids = {c["payload"]["record_id"] for c in unknown_calls}
        assert record_ids == {"rec-A", "rec-B"}
        # The unknown value is recorded in `last_known_degree` so operators
        # can spot phantom-contract drift.
        for c in unknown_calls:
            if c["payload"]["record_id"] == "rec-A":
                assert c["payload"]["last_known_degree"] == "You"
            elif c["payload"]["record_id"] == "rec-B":
                assert c["payload"]["last_known_degree"] == "out_of_network"

        # Wave-2-A: bob (genuine OON) is the ONLY stage write — parked at
        # UNREACHABLE via AttioWriter (keyword-arg update_list_entry). "You"
        # (alice) and "2nd" (carol) produce no stage write.
        attio.update_list_entry.assert_called_once()
        park_kwargs = attio.update_list_entry.call_args.kwargs
        assert park_kwargs["entry_id"] == "ent-B"
        assert park_kwargs["entry_attributes"] == {"stage": "Unreachable"}

    def test_pending_invitation_wins_over_second_degree(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """STRICT ordering: hasPendingInvitation="true" + degree="2nd" →
        flip to CONNECTION_SENT (Pattern-A fix), never invite.

        This is PR-B.16 (CSV wet-probe 2026-05-25) hardened by the Pattern-A
        re-prospecting fix: LinkedIn already received an invite from us, so we
        advance the prospect to CONNECTION_SENT (not escalate-and-drop) so it
        leaves the invite pool. The partition arm MUST check hasPendingInvitation
        FIRST before consulting degree."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()
        csv = _sales_nav_csv([
            # Alice: 2nd-degree BUT pending → must drop, not invite.
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd", "pending": "true"},
        ])
        pb = _make_pb(csv_text=csv)

        batch = [_row("A")]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        # CRITICAL: Alice MUST NOT be in still_to_invite even though her
        # degree is "2nd". The pending signal trumps degree.
        assert still == [], (
            "§3.1 violation: 2nd-degree+pending row reached invite queue. "
            "Re-inviting a profile that already has a pending invitation is "
            "the brand-event failure mode PR-B.16 closes."
        )
        assert already == []
        # Alice is FLIPPED to CONNECTION_SENT (Pattern-A fix) so she leaves the
        # invite pool, rather than being escalate-and-dropped (which left her at
        # PROSPECT to re-queue forever).
        flip_calls = [
            c for c in attio.update_list_entry.call_args_list
            if c.kwargs.get("entry_id") == "ent-A"
        ]
        assert len(flip_calls) == 1
        assert flip_calls[0].kwargs["entry_attributes"]["stage"] == "Connection Sent"

    def test_first_degree_flips_to_accepted_via_attio_writer(
        self, _pb_args, _sheet,
    ):
        """The Pattern-A flip path must work in sales_nav mode too —
        degree="1st" routes through the writer.apply(WriteIntent) call
        with stage + last_contact_date atomically."""
        attio = MagicMock()
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "1st"},
        ])
        pb = _make_pb(csv_text=csv)

        batch = [_row("A")]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        assert still == []
        assert len(already) == 1
        assert already[0]["entry_id"] == "ent-A"

        # Pattern-A flip went through AttioWriter. Wave-2-B: dispatch
        # routes through ``attio.update_list_entry``.
        ent_a_calls = [
            c for c in attio.update_list_entry.call_args_list
            if c.kwargs.get("entry_id") == "ent-A"
        ]
        assert len(ent_a_calls) == 1
        entry_values = ent_a_calls[0].kwargs["entry_attributes"]
        assert entry_values["stage"] == "Accepted"
        assert "last_contact_date" in entry_values

    def test_pending_invitation_flips_to_connection_sent(
        self, _pb_args, _sheet,
    ):
        """Pattern-A fix: hasPendingInvitation=true flips PROSPECT→CONNECTION_SENT
        (not escalate-and-drop) so the already-invited prospect leaves the invite
        pool and Phase 0 watches for acceptance. The flip stamps
        experiment_id_frozen_at='connection_sent' to preserve cohort attribution.
        """
        attio = MagicMock()
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd", "pending": "true"},
        ])
        pb = _make_pb(csv_text=csv)

        batch = [_row("A")]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        # §3.1: never invited; not lumped into already_connected (1st-degree).
        assert still == []
        assert already == []

        ent_a_calls = [
            c for c in attio.update_list_entry.call_args_list
            if c.kwargs.get("entry_id") == "ent-A"
        ]
        assert len(ent_a_calls) == 1
        entry_values = ent_a_calls[0].kwargs["entry_attributes"]
        assert entry_values["stage"] == "Connection Sent"
        assert entry_values["experiment_id_frozen_at"] == "connection_sent"
        assert "last_contact_date" in entry_values

    def test_pending_flip_failure_never_invites_and_opens_queue_row(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """§3.1 red line: if the pending→CONNECTION_SENT flip write FAILS, the
        prospect must NOT fall through to the invite queue (re-inviting an
        already-invited profile is the brand-event failure mode). It stays out
        of still_to_invite and a durable degree_unknown queue row is opened."""
        from clients.attio_writer import AttioError

        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd", "pending": "true"},
        ])
        pb = _make_pb(csv_text=csv)
        batch = [_row("A")]

        with patch(
            "clients.attio_writer.AttioWriter.apply",
            side_effect=AttioError("simulated flip failure"),
        ):
            still, already = _pre_invite_degree_check(
                batch, pb, None, attio, "list-id",
                sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
            )

        # CRITICAL: a failed flip must NEVER route the row to invite.
        assert still == []
        assert already == []
        # Durable triage row opened (not just an ephemeral stderr line).
        fail_rows = [
            c for c in escalations
            if str(c["payload"].get("scrape_attempt_id", "")).startswith(
                "pending-flip-fail"
            )
        ]
        assert len(fail_rows) == 1
        assert fail_rows[0]["payload"]["record_id"] == "rec-A"

    def test_mixed_mode_partition_per_row_decisions(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """6 prospects exercising every cell of the decision table at once.
        Validates per-row decisions (not all-or-nothing batching)."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()
        # Decision table per row:
        #   A: 2nd, pending=false       → invite
        #   B: 2nd, pending=false       → invite
        #   C: 3rd, pending=false       → invite
        #   D: missing from CSV         → escalate CSV_MISS
        #   E: 2nd, pending=true        → flip to CONNECTION_SENT (Pattern-A)
        #   F: blank degree, pending=false → escalate DEGREE_BLANK (+CSV_MISS)
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/bob", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/carol", "degree": "3rd"},
            # D missing
            {"url": "https://www.linkedin.com/in/eve", "degree": "2nd", "pending": "true"},
            {"url": "https://www.linkedin.com/in/frank", "degree": ""},
        ])
        pb = _make_pb(csv_text=csv, container_id="container-MIX")

        batch = [
            _row("A"),
            _row("B"),
            _row("C"),
            _row("D"),
            _row("E", "https://www.linkedin.com/in/eve"),
            _row("F", "https://www.linkedin.com/in/frank"),
        ]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        # Per-row assertions, not aggregate.
        assert {r["entry_id"] for r in still} == {"ent-A", "ent-B", "ent-C"}
        assert already == []

        # D escalated CSV_MISS exactly once (missing-from-CSV path).
        # PR-B.9 fold-in: post-de-noise, no DEGREE_BLANK follow-up.
        d_calls = [c for c in escalations if c["payload"]["record_id"] == "rec-D"]
        d_reasons = [c["payload"]["failure_reason"] for c in d_calls]
        assert d_reasons == [REASON_SALES_NAV_CSV_MISS], d_reasons

        # E flipped to CONNECTION_SENT (degree=2nd, pending=true) — Pattern-A fix
        # advances already-invited prospects instead of escalate-and-drop.
        e_flips = [
            c for c in attio.update_list_entry.call_args_list
            if c.kwargs.get("entry_id") == "ent-E"
        ]
        assert len(e_flips) == 1
        assert e_flips[0].kwargs["entry_attributes"]["stage"] == "Connection Sent"

        # F escalated CSV_MISS once (NOT DEGREE_BLANK after de-noise).
        # F is in the CSV with a blank `connectionDegree` cell. The scrape
        # parser drops blank-degree rows from `scraped_lookup`, so the
        # partial-CSV signal treats F as missing-from-scrape and fires
        # CSV_MISS. The partition-time BLANK arm sees F's record_id in
        # `csv_miss_emitted_for_record_ids` and suppresses its emit. Net:
        # one row per prospect with the richer CSV_MISS payload.
        f_calls = [c for c in escalations if c["payload"]["record_id"] == "rec-F"]
        f_reasons = [c["payload"]["failure_reason"] for c in f_calls]
        assert f_reasons == [REASON_SALES_NAV_CSV_MISS], f_reasons


# ----------------------------------------------------------------------
# PB failure-mode coverage
# ----------------------------------------------------------------------


@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet.example/foo")
@patch("workflows.daily_check._pb_session_args", return_value={})
class TestSalesNavPbFailureModes:
    """The Sales Nav launch helper retries ONCE on PBRunTimeout, drops the
    batch on second timeout, and drops the batch on PBRunFailed. These
    paths are fail-safe per §3.1 — better to lose a day's invites than to
    risk re-burning already-connected relationships."""

    def test_pb_run_failed_drops_entire_batch_no_escalations(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """PBRunFailed (PB reports status=error) → return ([], []),
        no escalations, no exceptions propagate."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()

        pb = _make_pb()
        pb.wait_for_completion.side_effect = PBRunFailed(
            container_id="container-FAIL",
            agent_id=SALES_NAV_SCRAPER_ID,
            log_tail="phantom error: cookie expired",
        )

        batch = [_row("A"), _row("B")]
        # No exception should escape.
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        assert still == []
        assert already == []
        # No escalations from the PB failure path — the partial-CSV signal
        # only fires when at least the launch succeeded.
        assert escalations == []

    def test_pb_timeout_retries_once_then_drops_batch(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """PBRunTimeout twice in a row → drop batch (return [], []).
        The launch helper retries once before giving up."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()

        pb = _make_pb()
        # wait_for_completion raises PBRunTimeout BOTH times.
        pb.wait_for_completion.side_effect = [
            PBRunTimeout(
                container_id="container-TO-1",
                agent_id=SALES_NAV_SCRAPER_ID,
                elapsed_seconds=300,
            ),
            PBRunTimeout(
                container_id="container-TO-2",
                agent_id=SALES_NAV_SCRAPER_ID,
                elapsed_seconds=300,
            ),
        ]

        batch = [_row("A")]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        assert still == []
        assert already == []
        # Helper retried — wait_for_completion called twice; launch_agent
        # called twice (re-launches on retry).
        assert pb.wait_for_completion.call_count == 2
        assert pb.launch_agent.call_count == 2
        assert escalations == []

    def test_pb_timeout_recovers_on_retry_and_parses_csv(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """PBRunTimeout once, success on retry → second call's CSV is
        parsed normally and the partition runs as if no timeout happened."""
        attio = MagicMock()
        pb = _make_pb(csv_text=_sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
        ]))
        # wait_for_completion raises once, then succeeds.
        pb.wait_for_completion.side_effect = [
            PBRunTimeout(
                container_id="container-TO-1",
                agent_id=SALES_NAV_SCRAPER_ID,
                elapsed_seconds=300,
            ),
            None,  # second call succeeds
        ]

        batch = [_row("A")]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        # Alice's CSV row was parsed on the retry — she lands in invite queue.
        assert len(still) == 1
        assert still[0]["entry_id"] == "ent-A"
        assert already == []
        assert pb.wait_for_completion.call_count == 2
        assert pb.launch_agent.call_count == 2
        # download_result_csv called on retry-success.
        assert pb.download_result_csv.called

    def test_empty_csv_after_successful_launch_drops_batch(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """Launch + wait succeed, but CSV is empty/None → drop batch.
        Mirrors the legacy path's fail-safe."""
        attio = MagicMock()
        pb = _make_pb(csv_text="")  # empty CSV

        batch = [_row("A"), _row("B")]
        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        # Empty CSV → helper returns (container_id, {}, {}) but the
        # outer function's `if not scraped_lookup and not container_id:`
        # check only fires on TOTAL helper failure. With container_id set
        # and empty CSV, the partition runs with degree_lookup={} → every
        # prospect routes to the §3.1 default arm. Per PR-B.13 the default
        # arm is escalate-and-drop. So still == [].
        assert still == []
        assert already == []

    def test_pb_get_agent_404_raises_config_error(
        self, _pb_args, _sheet,
    ):
        """If get_agent 404s (scraper-id doesn't exist — the deleted-agent
        event class that killed the legacy scraper), the error propagates as
        SalesNavConfigError naming the env var to fix — a hard config error,
        not a transient failure, and not silently dropped."""
        import httpx

        from workflows.daily_check_helpers import SalesNavConfigError

        attio = MagicMock()
        pb = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        pb.get_agent.side_effect = httpx.HTTPStatusError(
            "phantom not found",
            request=MagicMock(),
            response=mock_response,
        )

        batch = [_row("A")]
        with pytest.raises(
            SalesNavConfigError, match="PB_SALES_NAV_PROFILE_SCRAPER_ID"
        ):
            _pre_invite_degree_check(
                batch, pb, None, attio, "list-id",
                sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
            )


# ----------------------------------------------------------------------
# Launch arg shape — sanity check against the wet-probe verdict
# ----------------------------------------------------------------------


@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet.example/foo")
@patch("workflows.daily_check._pb_session_args", return_value={})
class TestSalesNavLaunchArgShape:
    """The phantom verified 2026-05-25 rejects partial argument objects.
    These tests pin the argument-shape contract."""

    def test_single_url_passes_url_directly_in_spreadsheet_url(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """The phantom auto-converts /in/ URLs internally — single-URL
        launches must pass the URL directly via spreadsheetUrl rather
        than wrapping it in a Google Sheets URL."""
        attio = MagicMock()
        pb = _make_pb(csv_text=_sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
        ]))

        batch = [_row("A")]
        _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        launch_args = pb.launch_agent.call_args.args[1]
        assert launch_args["spreadsheetUrl"] == "https://www.linkedin.com/in/alice"

    def test_session_cookie_injected_into_identities_zero(
        self, _pb_args, _sheet,
    ):
        """The phantom rejects top-level sessionCookie. The launch helper
        MUST inject the cookie into identities[0]."""
        attio = MagicMock()
        pb = _make_pb(csv_text=_sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
        ]))

        batch = [_row("A")]
        _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        launch_args = pb.launch_agent.call_args.args[1]
        # identities[0] must have sessionCookie set.
        assert "identities" in launch_args
        assert launch_args["identities"][0]["sessionCookie"] == "fake-li-at"
        # userAgent also injected (from PB_LI_USER_AGENT env).
        assert launch_args["identities"][0]["userAgent"] == "Mozilla/5.0 (test)"
        # Top-level sessionCookie MUST NOT appear.
        assert "sessionCookie" not in launch_args

    def test_number_of_profiles_per_launch_matches_batch_size(
        self, _pb_args, _sheet,
    ):
        """The phantom uses numberOfProfilesPerLaunch as a tight cap;
        the launch helper sets it to len(urls) for the headerless sheet,
        not the saved phantom default."""
        attio = MagicMock()
        pb = _make_pb(csv_text=_sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/bob", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/carol", "degree": "2nd"},
        ]))

        batch = [_row("A"), _row("B"), _row("C")]
        _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        launch_args = pb.launch_agent.call_args.args[1]
        assert launch_args["numberOfProfilesPerLaunch"] == 3
        assert _sheet.call_args.kwargs["include_header"] is False


# ----------------------------------------------------------------------
# A1/A2 — duplicate-entry healing + telemetry (Phase A)
# ----------------------------------------------------------------------


def _row_multi(suffix: str, entry_ids: list[str]) -> dict:
    """A to_send_data row carrying multiple list entries (dedup-merged
    duplicates) under `entry_ids`, mirroring _dedupe_by_linkedin_url output."""
    row = _row(suffix)
    row["entry_ids"] = entry_ids
    return row


@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet.example/foo")
@patch("workflows.daily_check._pb_session_args", return_value={})
class TestSalesNavDuplicateEntryHealing:
    """A1: a dedup-merged prospect carries every associated list entry in
    `entry_ids`. The Pattern-A flips must flip ALL of them — a duplicate left
    at PROSPECT is re-selected next run (the daily re-selection leak)."""

    def test_pending_flip_writes_all_entry_ids(self, _pb_args, _sheet):
        attio = MagicMock()
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd", "pending": "true"},
        ])
        pb = _make_pb(csv_text=csv)
        # Alice has two list entries (a duplicate that dedup merged).
        batch = [_row_multi("A", ["ent-A1", "ent-A2"])]

        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        assert still == []
        assert already == []
        flipped = {
            c.kwargs["entry_id"]: c.kwargs["entry_attributes"]
            for c in attio.update_list_entry.call_args_list
        }
        # BOTH entries flipped — the duplicate can't re-leak into tomorrow's pool.
        assert set(flipped) == {"ent-A1", "ent-A2"}
        assert flipped["ent-A1"]["stage"] == "Connection Sent"
        assert flipped["ent-A2"]["stage"] == "Connection Sent"

    def test_first_degree_flip_writes_all_entry_ids(self, _pb_args, _sheet):
        attio = MagicMock()
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "1st"},
        ])
        pb = _make_pb(csv_text=csv)
        batch = [_row_multi("A", ["ent-A1", "ent-A2"])]

        still, already = _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )

        assert still == []
        assert len(already) == 1
        flipped = {
            c.kwargs["entry_id"]: c.kwargs["entry_attributes"]
            for c in attio.update_list_entry.call_args_list
        }
        assert set(flipped) == {"ent-A1", "ent-A2"}
        assert flipped["ent-A1"]["stage"] == "Accepted"
        assert flipped["ent-A2"]["stage"] == "Accepted"

    def test_pending_flip_partial_entry_failure_never_invites(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """If one of a prospect's entry writes fails, the row must NOT fall
        through to the invite queue (§3.1) — it stays out and is queued for
        retry/triage."""
        from clients.attio_writer import AttioError

        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd", "pending": "true"},
        ])
        pb = _make_pb(csv_text=csv)
        batch = [_row_multi("A", ["ent-A1", "ent-A2"])]

        def _apply(intent):
            if intent.record_id == "ent-A2":
                raise AttioError("simulated second-entry failure")
            return None

        with patch("clients.attio_writer.AttioWriter.apply", side_effect=_apply):
            still, already = _pre_invite_degree_check(
                batch, pb, None, attio, "list-id",
                sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
            )

        assert still == []
        assert already == []
        assert any(
            str(c["payload"].get("scrape_attempt_id", "")).startswith("pending-flip-fail")
            for c in escalations
        )

    def test_pattern_a_pending_flips_audit_event_emitted(self, _pb_args, _sheet):
        """A2: the heal-path volume is emitted as an audit event so the daily
        re-selection leak is measurable day over day."""
        attio = MagicMock()
        audit_logger = MagicMock()
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd", "pending": "true"},
            {"url": "https://www.linkedin.com/in/bob", "degree": "2nd", "pending": "false"},
        ])
        pb = _make_pb(csv_text=csv)
        batch = [_row("A"), _row("B")]

        _pre_invite_degree_check(
            batch, pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
            audit_logger=audit_logger,
        )

        events = {
            c.args[0]: c.kwargs
            for c in audit_logger.event.call_args_list
            if c.args
        }
        assert "pattern_a_pending_flips" in events
        assert events["pattern_a_pending_flips"]["pending_flipped"] == 1
        assert events["pattern_a_pending_flips"]["failed"] == 0

    def test_audit_event_failure_never_aborts_the_run(self, _pb_args, _sheet):
        """The telemetry event fires after flips are durable; a sidecar failure
        must NOT propagate out and abort the wet invite send."""
        attio = MagicMock()
        audit_logger = MagicMock()
        audit_logger.event.side_effect = RuntimeError("audit sink down")
        csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd", "pending": "true"},
        ])
        pb = _make_pb(csv_text=csv)

        # Must not raise despite the audit sink failing.
        still, already = _pre_invite_degree_check(
            [_row("A")], pb, None, attio, "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
            audit_logger=audit_logger,
        )

        assert still == []
        # The flip itself still happened (durable before the audit call).
        assert any(
            c.kwargs.get("entry_id") == "ent-A"
            for c in attio.update_list_entry.call_args_list
        )


def test_explicit_unavailable_row_is_provenanced_without_degree():
    from workflows.pre_invite_check import _launch_sales_nav_scrape
    url = "https://linkedin.com/in/alice"
    pb = _make_pb(
        "query,linkedinProfileUrl,connectionDegree,error\n"
        + f"{url},{url},,profile unavailable\n"
    )
    cid, degrees, extras = _launch_sales_nav_scrape(pb, SALES_NAV_SCRAPER_ID, [url])
    assert degrees == {}
    assert extras[url]["profile_unavailable"] is True
    assert extras[url]["source_container_id"] == cid == "container-SN-1"


def test_unrequested_profile_identity_cannot_supply_requested_degree():
    from workflows.pre_invite_check import _launch_sales_nav_scrape
    requested = "https://linkedin.com/in/requested-123456"
    other = "https://linkedin.com/in/unrequested-654321"
    pb = _make_pb("query,linkedinProfileUrl,connectionDegree,hasPendingInvitation\n"
                  f"{requested},{other},2nd,false\n")
    conflicts = set()
    _, degrees, extras = _launch_sales_nav_scrape(
        pb, SALES_NAV_SCRAPER_ID, [requested], conflicting_urls=conflicts
    )
    assert requested not in degrees
    assert requested not in extras
    assert requested in conflicts


def test_later_clean_row_cannot_erase_identity_conflict():
    from workflows.pre_invite_check import _launch_sales_nav_scrape
    requested = "https://linkedin.com/in/requested-123456"
    other = "https://linkedin.com/in/unrequested-654321"
    pb = _make_pb("query,linkedinProfileUrl,connectionDegree,hasPendingInvitation\n"
                  f"{requested},{other},2nd,false\n"
                  f"{requested},{requested},2nd,false\n")
    conflicts = set()
    _, degrees, extras = _launch_sales_nav_scrape(
        pb, SALES_NAV_SCRAPER_ID, [requested], conflicting_urls=conflicts
    )
    assert not degrees
    assert not extras
    pb.download_result_csv.return_value = (
        "query,linkedinProfileUrl,connectionDegree,hasPendingInvitation\n"
        f"{requested},{requested},2nd,false\n"
    )
    _, degrees, extras = _launch_sales_nav_scrape(
        pb, SALES_NAV_SCRAPER_ID, [requested], conflicting_urls=conflicts
    )
    assert not degrees
    assert not extras


@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet.example/foo")
@patch("workflows.daily_check._pb_session_args", return_value={})
def test_unavailable_profile_is_held_without_retry(_args, _sheet, monkeypatch):
    _record_escalate(monkeypatch)
    pb = _make_pb(
        "query,linkedinProfileUrl,connectionDegree,error\n"
        "https://www.linkedin.com/in/alice,https://www.linkedin.com/in/alice,2nd,\n"
        "https://www.linkedin.com/in/bob,https://www.linkedin.com/in/bob,,profile unavailable\n"
    )
    with patch("workflows.recheck_cache.record_many"):
        still, already = _pre_invite_degree_check(
            [_row("A"), _row("B")], pb, None, MagicMock(), "list-id",
            sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
        )
    assert pb.launch_agent.call_count == 1
    assert [row["entry_id"] for row in still] == ["ent-A"]
    assert already == []


# ----------------------------------------------------------------------
# Bounded re-scrape of rows the SN phantom drops
# ----------------------------------------------------------------------


@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet.example/foo")
@patch("workflows.daily_check._pb_session_args", return_value={})
class TestSalesNavDroppedRowRetry:
    """The SN phantom intermittently omits rows from its result CSV. A wet
    run must re-scrape the missing rows ONCE and recover them rather than
    silently routing them to degree_unknown + a PROSPECT re-queue."""

    def test_initial_http_timeout_holds_entire_batch(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch, capsys,
    ):
        escalations = _record_escalate(monkeypatch)
        pb = _make_pb()
        pb.launch_agent.side_effect = httpx.ReadTimeout("timeout")
        with patch("workflows.recheck_cache.record_many"):
            still, already = _pre_invite_degree_check(
                [_row("A"), _row("B")], pb, None, MagicMock(), "list-id",
                sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
            )
        assert still == already == []
        assert pb.launch_agent.call_count == 1
        assert escalations == []  # whole-batch launch failure is held, not per-row CSV miss
        assert "no automatic relaunch" in capsys.readouterr().err

    def test_retry_http_timeout_holds_missing_profile(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch, capsys,
    ):
        escalations = _record_escalate(monkeypatch)
        pb = _make_pb(csv_text=_sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/bob", "degree": "2nd"},
        ]))
        pb.launch_agent.side_effect = [pb.launch_agent.return_value, httpx.ReadTimeout("timeout")]
        with patch("workflows.recheck_cache.record_many"):
            still, _ = _pre_invite_degree_check(
                [_row("A"), _row("B"), _row("C")], pb, None, MagicMock(), "list-id",
                sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
            )
        assert {r["entry_id"] for r in still} == {"ent-A", "ent-B"}
        assert pb.launch_agent.call_count == 2
        assert [c["payload"]["failure_reason"] for c in escalations
                if c["payload"].get("record_id") == "rec-C"] == [REASON_SALES_NAV_CSV_MISS]
        assert "launch outcome unknown" in capsys.readouterr().err

    def test_wet_rescrapes_dropped_row_and_recovers_it(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """First CSV drops carol; the retry CSV returns her as 2nd → she
        lands in still_to_invite, two launches fire, no escalation."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()
        batch = [_row("A"), _row("B"), _row("C")]

        first_csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/bob", "degree": "2nd"},
            # carol dropped from the first CSV
        ])
        retry_csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/carol", "degree": "2nd"},
        ])
        pb = _make_pb()
        pb.download_result_csv.side_effect = [first_csv, retry_csv]

        with patch("workflows.recheck_cache.record_many"):
            still, already = _pre_invite_degree_check(
                batch, pb, None, attio, "list-id",
                sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
            )

        assert pb.launch_agent.call_count == 2, "missing row must trigger one re-scrape"
        assert {r["entry_id"] for r in still} == {"ent-A", "ent-B", "ent-C"}
        assert escalations == [], "recovered row must not open a degree_unknown row"

    def test_wet_rescrape_still_missing_escalates_once(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """Retry also drops carol → exactly one CSV_MISS degree_unknown row,
        carol never reaches still_to_invite."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()
        batch = [_row("A"), _row("B"), _row("C")]

        first_csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/bob", "degree": "2nd"},
        ])
        pb = _make_pb()
        pb.download_result_csv.side_effect = [first_csv, ""]  # retry recovers nothing

        with patch("workflows.recheck_cache.record_many"):
            still, already = _pre_invite_degree_check(
                batch, pb, None, attio, "list-id",
                sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
            )

        assert pb.launch_agent.call_count == 2
        assert {r["entry_id"] for r in still} == {"ent-A", "ent-B"}
        carol = [c for c in escalations if c["payload"].get("record_id") == "rec-C"]
        carol_reasons = [c["payload"]["failure_reason"] for c in carol]
        assert carol_reasons == [REASON_SALES_NAV_CSV_MISS], (
            f"still-missing row must escalate CSV_MISS exactly once; got {carol_reasons!r}"
        )

    def test_retry_launch_failure_is_reported_distinctly(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch, capsys,
    ):
        """When the re-scrape LAUNCH itself fails (PBRunFailed → ('',{},{})),
        the operator sees a distinct 'launch did not complete' line rather than
        a 'recovered 0' line that reads like a persistent per-row drop. The
        still-missing row still escalates exactly once."""
        escalations = _record_escalate(monkeypatch)
        attio = MagicMock()
        batch = [_row("A"), _row("B"), _row("C")]
        first_csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/bob", "degree": "2nd"},
        ])
        pb = _make_pb(csv_text=first_csv)
        # First scrape completes; the retry launch fails outright.
        pb.wait_for_completion.side_effect = [
            MagicMock(),
            PBRunFailed(
                container_id="container-RETRY-FAIL",
                agent_id=SALES_NAV_SCRAPER_ID,
                log_tail="phantom error on retry",
            ),
        ]

        with patch("workflows.recheck_cache.record_many"):
            still, _ = _pre_invite_degree_check(
                batch, pb, None, attio, "list-id",
                sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
            )

        assert pb.launch_agent.call_count == 2
        assert {r["entry_id"] for r in still} == {"ent-A", "ent-B"}
        err = capsys.readouterr().err
        assert "re-scrape launch did not complete" in err
        carol = [c for c in escalations if c["payload"].get("record_id") == "rec-C"]
        assert [c["payload"]["failure_reason"] for c in carol] == [REASON_SALES_NAV_CSV_MISS]

    def test_dry_run_does_not_rescrape(
        self, _pb_args, _sheet, monkeypatch: pytest.MonkeyPatch,
    ):
        """Dry-run never burns a re-scrape.

        FORK DIVERGENCE from upstream #200: this fork's
        ``_pre_invite_degree_check`` fully short-circuits dry-run (Wave-1.6
        FIX-1 "dry-run honesty") and returns BEFORE any PB launch — so the
        whole scrape block (and therefore the bounded re-scrape) is wet-only
        by construction. The fork's guarantee is strictly stronger than
        upstream's (which launched a single preview scrape): zero launches in
        dry-run, not one."""
        _record_escalate(monkeypatch)
        attio = MagicMock()
        batch = [_row("A"), _row("B"), _row("C")]
        first_csv = _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/alice", "degree": "2nd"},
            {"url": "https://www.linkedin.com/in/bob", "degree": "2nd"},
        ])
        pb = _make_pb()
        pb.download_result_csv.side_effect = [first_csv, _sales_nav_csv([
            {"url": "https://www.linkedin.com/in/carol", "degree": "2nd"},
        ])]

        with (
            patch("workflows.recheck_cache.record_many"),
            patch("workflows.pre_invite_check.AttioWriter"),
        ):
            _pre_invite_degree_check(
                batch, pb, None, attio, "list-id",
                sales_nav_profile_scraper_id=SALES_NAV_SCRAPER_ID,
                dry_run=True,
            )

        assert pb.launch_agent.call_count == 0, (
            "dry-run short-circuits before any PB launch (fork divergence)"
        )
