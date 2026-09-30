"""Integration tests for the outbound-agent workflows."""

import os
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

from clients.crm.attio_provider import AttioProvider
from clients.pb_envelope import PBCompletion, PBLaunch, hash_arguments
from tests.fakes import fake_daily_run, stub_guard_reread
from workflows.daily_run import DailyRun
from workflows.schema_preflight import DM_COMPANY_WRITER_ATTRS, DM_ENTRY_WRITER_ATTRS


def _attio_with_full_schema(**extra_methods) -> MagicMock:
    """Return an attio MagicMock pre-configured to pass the DM-writer
    schema preflight (all entry + company attrs present).

    Also stubs search_companies to return [] so the consistency sweep
    epilogue that runs on every post-preflight exit no-ops cleanly in
    tests that don't exercise the sweep directly.
    """
    attio = MagicMock()
    # A bare MagicMock answers `_person_to_company.get()` with the SAME child
    # mock for every record, so every prospect looks like it shares one company
    # and the within-run same-company dedup silently drops rows. Start empty.
    attio._person_to_company = {}
    # Real str→str cache like AttioClient. A bare MagicMock attribute would
    # answer `.get(record_id)` with the SAME child mock for every record, so
    # every prospect in a batch looks like it shares one company and the
    # within-run `seen_company_ids` dedup silently drops all but the first
    # row before it is ever sent. Tests that want a company link set it
    # explicitly (`attio._person_to_company = {rec: company}`).
    attio._person_to_company = {}
    attio.is_person_company_corrupted.return_value = False
    attio.get_list_attributes.return_value = [{"api_slug": s} for s in DM_ENTRY_WRITER_ATTRS]
    attio.get_object_attributes.return_value = [{"api_slug": s} for s in DM_COMPANY_WRITER_ATTRS]
    attio.search_companies.return_value = []
    attio._request.return_value = {"data": []}  # explicit clean P0 queue
    for attr, val in extra_methods.items():
        setattr(attio, attr, val)
    return attio


def _crm(attio):
    """Wrap a raw-AttioClient integration mock in the reference provider.

    `run_weekly_prospecting` migrated to the vendor-neutral `CRMProvider`
    (P1c). The provider delegates every typed read/write to the same inner
    mock and normalizes its raw-dict returns into the contract dataclasses,
    so all existing `attio.upsert_person` / `attio.add_list_entry` /
    `attio.query_list_entries` assertions keep working unchanged. Phase 0's
    unmigrated `backfill_missing_industries` reaches the raw mock via the
    provider's `inner_client` escape hatch.
    """
    return AttioProvider(attio)


def _corruption_fake_daily_run(remaining: int = 30) -> MagicMock:
    """PR-17: integration tests need a DailyRun mock — required parameter."""
    mock = MagicMock(spec=DailyRun)
    mock.remaining.return_value = remaining
    mock.reserve_send.return_value = "token-int"
    return mock


def _typed_pb_mock(*, csv_text: str = "", container_id: str = "c_test", log: str = "") -> MagicMock:
    """F-PR-5: wire a PB MagicMock to return typed envelopes.

    Tests written before F-PR-5 used `pb.wait_for_completion.return_value =
    {"status": "finished", "output": ""}` — but after F-PR-5 the contract
    is `wait_for_completion(launch) -> PBCompletion`. This helper builds
    a MagicMock that returns a real PBLaunch from launch_agent and a
    real PBCompletion from wait_for_completion, so downstream
    `parse_send_outcome(launch, completion, csv_text, ...)` produces a
    SendOutcome the advance-gate can evaluate.

    Pass a CSV containing "Message sent" rows to make the gate pass;
    pass an empty CSV (or omit) for the dry-skip path.
    """
    pb = MagicMock()
    launch = PBLaunch(
        container_id=container_id,
        agent_id="agent-test",
        launched_at=datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
        arguments_sha256=hash_arguments(None),
    )
    completion = PBCompletion(
        container_id=container_id,
        status="finished",
        log_output=log,
        raw_output={"status": "finished", "output": log},
    )
    pb.launch_agent.return_value = launch
    pb.wait_for_completion.return_value = completion
    pb.download_result_csv.return_value = csv_text
    return pb

# ── Helpers ─────────────────────────────────────────────────────────────────

def _make_attio_entry(
    entry_id: str,
    record_id: str,
    stage: str,
    quality_score=75,
    persona="operations_leaders",
    language="en",
    last_contact_date: str | None = None,
    dm_step=0,
    created_at: str | None = None,
) -> dict:
    """Build an Attio list entry dict that parse_entry() can consume."""
    if last_contact_date is None:
        last_contact_date = (date.today() - timedelta(days=2)).isoformat()
    if created_at is None:
        # Default: entry created same day as last_contact (normal flow).
        created_at = f"{last_contact_date}T00:00:00.000000000Z"
    return {
        "entry_id": entry_id,
        "parent_record_id": record_id,
        "created_at": created_at,
        "entry_values": {
            "stage": [{"status": {"title": stage}}],
            "quality_score": [quality_score],
            "persona": [persona],
            "language": [language],
            "last_contact_date": [last_contact_date],
            "dm_step": [dm_step],
        },
    }


# Persona configs for tests that need the deterministic commit path: shipped
# configs sit at search_size_credit=15 (auto-pass deliberately unreachable —
# 2026-07-06 calibration), so these tests inject a credit above the line.
# Geometry: 30 (size credit) + 28 (decision-maker) + 20 (non-competitor) = 78
# > 75 → enterprise_pass without needing an industry label. (PR-227)
TEST_PERSONAS_DETERMINISTIC = {
    "operations_leaders": {
        "label": "Operations Leaders",
        "enterprise_mode": True,
        "search_headcount_filter": "501+ (test fixture)",
        "search_size_credit": 30,
        "search_queries": {"sn_search_urls": {}},
    },
}


def _make_sn_csv(rows: list[dict]) -> str:
    """Build a Sales Navigator-style CSV string from a list of row dicts."""
    import csv
    import io
    if not rows:
        return ""
    fieldnames = list(rows[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


# ── A. Weekly Prospect Integration ──────────────────────────────────────────

class TestWeeklyProspectIntegration:

    def _make_mocks(self):
        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        pb = MagicMock()
        return attio, pb

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_full_flow_sn_export_to_attio(self):
        """PB launches with full SN argument structure; Attio receives upsert + list entry at Prospect stage."""
        from workflows.weekly_prospect import run_weekly_prospecting

        csv_data = _make_sn_csv([{
            "firstName": "Ana",
            "lastName": "Ejemplo",
            "currentCompanyName": "Cementra",
            "linkedinProfileUrl": "https://linkedin.com/in/anaejemplo",
            "numberOfEmployees": "5001-10000",
            "currentPosition": "VP of Operations",
            "location": "Monterrey, Mexico",
        }])

        attio, pb = self._make_mocks()
        pb.download_result_csv.return_value = csv_data
        attio.search_person_by_linkedin.return_value = None  # not a duplicate
        attio.upsert_person.return_value = {"id": {"record_id": "rec-abc"}}

        with patch("workflows.weekly_prospect._get_all_searches") as mock_searches, \
             patch("workflows.weekly_prospect.load_personas", return_value=TEST_PERSONAS_DETERMINISTIC):
            mock_searches.return_value = [(
                "operations_leaders",
                "mx",
                "https://www.linkedin.com/sales/search/people?query=test",
            )]
            summary = run_weekly_prospecting(
                crm=_crm(attio),
                pb=pb,
                search_export_id="agent-search-001",
            )

        # PB should have been launched with the full argument structure
        pb.launch_agent.assert_called_once()
        launch_args = pb.launch_agent.call_args[0][1]
        assert launch_args["salesNavigatorSearchUrl"] == "https://www.linkedin.com/sales/search/people?query=test"
        assert launch_args["inputType"] == "salesNavigatorSearchUrl"
        assert launch_args["removeDuplicateProfiles"] is True
        assert launch_args["identities"][0]["sessionCookie"] == "fake-cookie"
        assert launch_args["identities"][0]["userAgent"] == "TestAgent/1.0"

        # Attio create_person called with the joined name. The provider
        # delegates `upsert_person(matching_attribute, attributes)`
        # POSITIONALLY to the inner client (was kwargs pre-migration), so the
        # attrs dict is the 2nd positional arg.
        attio.upsert_person.assert_called_once()
        create_call = attio.upsert_person.call_args
        attrs = (
            create_call.kwargs.get("attributes")
            if "attributes" in create_call.kwargs
            else create_call.args[1]
        )
        name_list = attrs.get("name", [{}])
        assert name_list[0]["full_name"] == "Ana Ejemplo"

        # add_list_entry called at Prospect stage
        attio.add_list_entry.assert_called_once()
        add_call = attio.add_list_entry.call_args
        stage_arg = add_call[1].get("stage_name", add_call[0][1] if len(add_call[0]) > 1 else None)
        assert stage_arg == "Prospect"

        assert summary["added"] == 1
        assert summary["duplicates"] == 0
        assert summary["rejected"] == 0

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "PB_LI_SESSION_COOKIE": "fake-cookie",
    })
    def test_sn_csv_column_mapping(self):
        """SN-specific CSV columns are normalised into expected prospect_data fields."""
        from workflows.quality_gate import score_prospect
        from workflows.weekly_prospect import run_weekly_prospecting

        csv_data = _make_sn_csv([{
            "firstName": "Pedro",
            "lastName": "Silva",
            "currentCompanyName": "Ambev",
            "linkedinProfileUrl": "https://linkedin.com/in/pedrosilva",
            "numberOfEmployees": "5001-10000",
            "currentPosition": "Director de Manufactura",
            "location": "São Paulo, Brazil",
        }])

        captured_prospect = {}

        def fake_score(prospect_data, **kw):
            captured_prospect.update(prospect_data)
            return score_prospect(prospect_data)

        attio, pb = self._make_mocks()
        pb.download_result_csv.return_value = csv_data
        attio.search_person_by_linkedin.return_value = None
        attio.upsert_person.return_value = {"id": {"record_id": "rec-xyz"}}

        with patch("workflows.weekly_prospect._get_all_searches") as mock_searches, \
             patch("workflows.weekly_prospect.score_prospect", side_effect=fake_score):
            mock_searches.return_value = [(
                "operations_leaders",
                "br",
                "https://www.linkedin.com/sales/search/people?query=test",
            )]
            run_weekly_prospecting(crm=_crm(attio), pb=pb, search_export_id="agent-search-001")

        # Verify the normalisation
        assert captured_prospect["name"] == "Pedro Silva"
        assert captured_prospect["company"] == "Ambev"
        # URL is canonicalized in-flight (no www, no trailing slash, lowercase)
        # to match Attio's stored form. See clients.attio._canonical_linkedin_url.
        assert captured_prospect["linkedin_url"] == "https://linkedin.com/in/pedrosilva"

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "PB_LI_SESSION_COOKIE": "fake-cookie",
    })
    def test_dedup_skips_existing(self):
        """When Attio already has the LinkedIn URL, the prospect is counted as duplicate and upsert is NOT called."""
        from workflows.weekly_prospect import run_weekly_prospecting

        csv_data = _make_sn_csv([{
            "firstName": "Maria",
            "lastName": "Lopez",
            "currentCompanyName": "CEMENTRA",
            "linkedinProfileUrl": "https://linkedin.com/in/marialopez",
            "numberOfEmployees": "5001-10000",
            "currentPosition": "VP Operaciones",
            "location": "Guadalajara, Mexico",
        }])

        attio, pb = self._make_mocks()
        pb.download_result_csv.return_value = csv_data
        # Simulate the record already existing in Attio AND already on the
        # pipeline list — the dedup-as-duplicate branch only fires when the
        # record is in BOTH (a record in Attio but not on the list gets
        # added to the list, not deduped).
        attio.search_person_by_linkedin.return_value = {"id": {"record_id": "existing-rec"}}
        attio.query_list_entries.return_value = [
            {"id": {"record_id": "existing-rec", "entry_id": "entry-X"}, "entry_values": {}},
        ]

        with patch("workflows.weekly_prospect._get_all_searches") as mock_searches, \
             patch("workflows.weekly_prospect.load_personas", return_value=TEST_PERSONAS_DETERMINISTIC):
            mock_searches.return_value = [(
                "operations_leaders",
                "mx",
                "https://www.linkedin.com/sales/search/people?query=test",
            )]
            summary = run_weekly_prospecting(crm=_crm(attio), pb=pb, search_export_id="agent-search-001")

        assert summary["duplicates"] == 1
        attio.upsert_person.assert_not_called()
        attio.add_list_entry.assert_not_called()

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_company_linked_after_create_person(self):
        """The person upsert payload includes the company record-reference.

        Production code links company at upsert-time (single call), not via
        a separate update_person call as an earlier code path did. Test
        verifies the link happens via upsert_person's `attributes` arg.
        """
        from unittest.mock import patch as _patch

        from workflows.weekly_prospect import run_weekly_prospecting

        csv_data = _make_sn_csv([{
            "firstName": "Ana",
            "lastName": "Ejemplo",
            "currentCompanyName": "Cementra",
            "linkedinProfileUrl": "https://linkedin.com/in/anaejemplo",
            "numberOfEmployees": "5001-10000",
            "currentPosition": "VP of Operations",
            "location": "Monterrey, Mexico",
        }])

        attio, pb = self._make_mocks()
        pb.download_result_csv.return_value = csv_data
        attio.search_person_by_linkedin.return_value = None
        attio.upsert_person.return_value = {"id": {"record_id": "rec-abc"}}

        with _patch("workflows.weekly_prospect._get_all_searches") as mock_searches, \
             _patch("workflows.weekly_prospect.load_personas", return_value=TEST_PERSONAS_DETERMINISTIC), \
             _patch("workflows.weekly_prospect.match_or_create_company", return_value="comp-test-123"):
            mock_searches.return_value = [(
                "operations_leaders",
                "mx",
                "https://www.linkedin.com/sales/search/people?query=test",
            )]
            run_weekly_prospecting(crm=_crm(attio), pb=pb, search_export_id="agent-search-001")

        attio.upsert_person.assert_called_once()
        # Provider delegates upsert_person POSITIONALLY (attrs = 2nd arg).
        _uc = attio.upsert_person.call_args
        upsert_attrs = (
            _uc.kwargs["attributes"] if "attributes" in _uc.kwargs else _uc.args[1]
        )
        assert upsert_attrs["company"] == [
            {"target_object": "companies", "target_record_id": "comp-test-123"}
        ]

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_company_linking_failure_doesnt_block_pipeline(self):
        """When match_or_create_company returns None, add_list_entry is still called but update_person is not."""
        from unittest.mock import patch as _patch

        from workflows.weekly_prospect import run_weekly_prospecting

        csv_data = _make_sn_csv([{
            "firstName": "Ana",
            "lastName": "Ejemplo",
            "currentCompanyName": "Cementra",
            "linkedinProfileUrl": "https://linkedin.com/in/anaejemplo",
            "numberOfEmployees": "5001-10000",
            "currentPosition": "VP of Operations",
            "location": "Monterrey, Mexico",
        }])

        attio, pb = self._make_mocks()
        pb.download_result_csv.return_value = csv_data
        attio.search_person_by_linkedin.return_value = None
        attio.upsert_person.return_value = {"id": {"record_id": "rec-abc"}}

        with _patch("workflows.weekly_prospect._get_all_searches") as mock_searches, \
             _patch("workflows.weekly_prospect.load_personas", return_value=TEST_PERSONAS_DETERMINISTIC), \
             _patch("workflows.weekly_prospect.match_or_create_company", return_value=None):
            mock_searches.return_value = [(
                "operations_leaders",
                "mx",
                "https://www.linkedin.com/sales/search/people?query=test",
            )]
            run_weekly_prospecting(crm=_crm(attio), pb=pb, search_export_id="agent-search-001")

        attio.add_list_entry.assert_called_once()
        attio.update_person.assert_not_called()

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "PB_LI_SESSION_COOKIE": "fake-cookie",
    })
    def test_quality_gate_rejects_low_score(self):
        """A junior analyst at a small company fails the quality gate; no Attio writes."""
        from workflows.weekly_prospect import run_weekly_prospecting

        csv_data = _make_sn_csv([{
            "firstName": "Bob",
            "lastName": "Jones",
            "currentCompanyName": "Tiny Startup",
            "linkedinProfileUrl": "https://linkedin.com/in/bobjones",
            "numberOfEmployees": "50",
            "currentPosition": "Junior Analyst",
            "location": "New York, USA",
        }])

        attio, pb = self._make_mocks()
        pb.download_result_csv.return_value = csv_data

        with patch("workflows.weekly_prospect._get_all_searches") as mock_searches:
            mock_searches.return_value = [(
                "operations_leaders",
                "us",
                "https://www.linkedin.com/sales/search/people?query=test",
            )]
            summary = run_weekly_prospecting(crm=_crm(attio), pb=pb, search_export_id="agent-search-001")

        assert summary["rejected"] >= 1
        attio.upsert_person.assert_not_called()
        attio.add_list_entry.assert_not_called()


# ── B. Daily Check — Connection Requests ────────────────────────────────────

class TestDailyConnectionsIntegration:

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_connection_requests_flow(self):
        """Qualified prospects at Prospect stage trigger Google Sheet write + PB launch + Attio update."""
        from workflows.daily_check import run_connection_requests

        entry = _make_attio_entry(
            entry_id="entry-001",
            record_id="rec-001",
            stage="Prospect",
            quality_score=75,
        )

        attio = _attio_with_full_schema()
        # F-PR-5: Network Booster now consumes SendOutcome + advance gate.
        # The CSV must mark this prospect's URL as "Message sent" for the
        # gate to pass and Attio update_list_entry to fire.
        pb = _typed_pb_mock(
            csv_text="query,status\nhttps://linkedin.com/in/carlosmendoza,Message sent\n",
            log="Invitation sent to carlosmendoza",
        )
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.record_connections"), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            mock_cache_get.return_value = ("Carlos Mendoza", "CEMEX", "https://linkedin.com/in/carlosmendoza", "", "")
            result = run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=fake_daily_run(),
            )

        # Agent should have been launched
        pb.launch_agent.assert_called_once()

        # Attio entry should be updated to Connection Sent
        stage_calls = [c for c in attio.update_list_entry.call_args_list
                       if "stage" in c.kwargs.get("entry_attributes", {})]
        assert len(stage_calls) == 1
        update_call = stage_calls[0]
        attrs = update_call[1].get("entry_attributes", update_call[0][1] if len(update_call[0]) > 1 else {})
        assert attrs["stage"] == "Connection Sent"

        assert result["sent"] == 1

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake",
        "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_already_pending_advances_stage_without_charging_today(self):
        from workflows.daily_check import run_connection_requests

        entry = _make_attio_entry(
            entry_id="entry-pending", record_id="rec-pending",
            stage="Prospect", quality_score=75,
        )
        attio = _attio_with_full_schema()
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])
        pb = _typed_pb_mock(
            csv_text="query,error\n",
            log=("Invitation for https://www.linkedin.com/in/pending-person "
                 "already sent, still pending."),
        )
        dr = fake_daily_run()
        with patch("workflows.daily_check.RecordCache.get", return_value=(
            "Pending Person", "Co", "https://linkedin.com/in/pending-person", "", "",
        )), patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet/x"), \
             patch("workflows.daily_check.record_connections") as record:
            result = run_connection_requests(
                attio=attio, pb=pb, network_booster_id="agent-nb",
                auto_confirm=True, daily_run=dr,
            )
        dr.confirm_lease.assert_called_once_with("fake-lease-token", confirmed_count=0)
        record.assert_called_once_with(0)
        assert result["sent"] == 0
        assert result["attio_updated"] == 1

    @patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake"})
    def test_respects_daily_limits(self):
        """When can_send_connections returns False, no PB calls are made."""
        from workflows.daily_check import run_connection_requests

        entry = _make_attio_entry(
            entry_id="entry-002",
            record_id="rec-002",
            stage="Prospect",
            quality_score=80,
        )

        attio = _attio_with_full_schema()
        pb = MagicMock()
        attio.query_list_entries.return_value = [entry]

        with patch("workflows.daily_check.can_send_connections", return_value=False):
            result = run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=fake_daily_run(),
            )

        pb.upload_csv.assert_not_called()
        pb.launch_agent.assert_not_called()
        assert result.get("reason") == "daily_limit"

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_connection_requests_takes_dry_skip_path_when_gate_fails(self):
        """Symmetric with `test_dm_sequencing_takes_dry_skip_path_when_csv_missing`.

        Invites advance OPTIMISTICALLY on a clean authenticated launch, so the
        dry-skip path now triggers on the one case where invites genuinely did
        NOT send: a LinkedIn AUTH FAILURE (dead cookie). Then
        `run_connection_requests` must NOT write Attio AND must NOT charge
        `record_connections` against tomorrow's invite cap. Instead, emit a
        `pb_silent_no_op` queue row.
        """
        from workflows.daily_check import run_connection_requests

        entry = _make_attio_entry(
            entry_id="entry-dryskip-001",
            record_id="rec-dryskip-001",
            stage="Prospect",
            quality_score=75,
        )
        attio = _attio_with_full_schema()
        pb = _typed_pb_mock(csv_text=None, log="❌ No valid credentials found")  # auth fail → invites NOT sent → gate fails
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.record_connections") as mock_record, \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.emit_pb_silent_no_op") as mock_emit:
            mock_cache_get.return_value = ("Dry Skip", "Co", "https://linkedin.com/in/dryskip", "", "")
            run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-dryskip",
                auto_confirm=True,
                daily_run=fake_daily_run(),
            )

        # The prelaunch hold remains, but no stage advance occurs.
        assert len(attio.update_list_entry.call_args_list) == 1
        assert attio.update_list_entry.call_args.kwargs["entry_attributes"] == {
            "invite_eligible_after": "2099-12-31"
        }
        # Dry-skip emission happens exactly once.
        mock_emit.assert_called_once()
        # Daily cap charged ZERO — gate-failed batches must not burn
        # tomorrow's invite budget.
        mock_record.assert_called_once_with(0)

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_connection_requests_clean_launch_advances_every_requested_row(self, capsys):
        """Phase B (2026-06-15, authoritative advance): on a clean,
        authenticated, uncapped Network Booster launch EVERY requested
        invite row advances to Connection Sent — including rows the
        per-launch CSV/log did not echo back — stamped with the
        `INVITE_OPTIMISTIC_ADVANCE` audit marker. The CSV/log list is a
        forensic diagnostic, not a per-row gate (see
        `clients.pb_envelope.compute_invite_outcome`).

        Supersedes the pre-Phase-B "ghost advance prevention" test, which
        asserted the opposite (CSV-unconfirmed row stays at Prospect). That
        test only kept passing because the fixture's `_person_to_company`
        was a bare MagicMock, so both rows resolved to the SAME company
        mock and the within-run same-company dedup dropped the second row
        before it was ever sent. With an honest cache the two rows are
        distinct companies and both go out.

        Setup: 2 Prospects at 2 different companies. PB CSV marks only ONE
        as sent. Expect: 2 Attio updates, both to Connection Sent.
        """
        from clients.pb_envelope import INVITE_OPTIMISTIC_ADVANCE
        from workflows.daily_check import run_connection_requests

        sent_entry = _make_attio_entry(
            entry_id="entry-sent-001",
            record_id="rec-sent-001",
            stage="Prospect",
            quality_score=75,
        )
        unechoed_entry = _make_attio_entry(
            entry_id="entry-unechoed-001",
            record_id="rec-unechoed-001",
            stage="Prospect",
            quality_score=75,
        )

        attio = _attio_with_full_schema()
        attio._person_to_company = {
            "rec-sent-001": "company-sent",
            "rec-unechoed-001": "company-unechoed",
        }
        # Network Booster explicitly confirms both URLs in its launch log.
        pb = _typed_pb_mock(
            csv_text=_make_sn_csv([{
                "query": "https://linkedin.com/in/sent-one",
                "linkedinProfileUrl": "https://linkedin.com/in/sent-one",
                "status": "Message sent",
            }]),
            log="Invitation sent to sent-one\nInvitation sent to unechoed-one",
        )
        attio.query_list_entries.return_value = [sent_entry, unechoed_entry]
        stub_guard_reread(attio, [sent_entry, unechoed_entry])

        def cache_side_effect(rid):
            return {
                "rec-sent-001": ("Sent One", "Co A", "https://linkedin.com/in/sent-one", "", ""),
                "rec-unechoed-001": ("Unechoed One", "Co B", "https://linkedin.com/in/unechoed-one", "", ""),
            }[rid]

        with patch("workflows.daily_check.RecordCache.get", side_effect=cache_side_effect), \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.record_connections") as mock_record, \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.emit_pb_silent_no_op") as mock_emit:
            result = run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=fake_daily_run(),
            )

        # Both requested rows advance — the CSV-unechoed one included.
        stage_calls = [c for c in attio.update_list_entry.call_args_list
                       if "stage" in c.kwargs.get("entry_attributes", {})]
        assert len(stage_calls) == 2
        advanced = {}
        for update_call in stage_calls:
            attrs = update_call[1].get("entry_attributes", update_call[0][1] if len(update_call[0]) > 1 else {})
            advanced[update_call[1]["entry_id"]] = attrs["stage"]
        assert advanced == {
            "entry-sent-001": "Connection Sent",
            "entry-unechoed-001": "Connection Sent",
        }
        assert result["sent"] == 2
        assert result["attio_updated"] == 2
        # The daily cap is charged for every requested row (charge parity
        # with what actually left the account), and no dry-skip row opens.
        mock_record.assert_called_once_with(2)
        mock_emit.assert_not_called()
        out = capsys.readouterr().out
        assert "OPTIMISTICALLY" not in out
        assert INVITE_OPTIMISTIC_ADVANCE not in out

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_connection_requests_cap_restriction_disables_optimistic_advance(self, capsys):
        """Hard block of the optimistic advance: a LinkedIn cap/restriction
        signal in the launch log means invites after the cap were silently
        DROPPED, so the authoritative-advance override is OFF and the batch
        falls back to per-row CSV confirmation. Only the row the CSV marks
        "Message sent" advances; the unechoed row stays at Prospect and
        re-queues tomorrow. No `INVITE_OPTIMISTIC_ADVANCE` marker is stamped
        or announced, and the cap is charged only for the confirmed send.
        (An empty CSV under auth failure takes the dry-skip path — see
        `test_connection_requests_takes_dry_skip_path_when_gate_fails`.)
        """
        from clients.pb_envelope import INVITE_OPTIMISTIC_ADVANCE
        from workflows.daily_check import run_connection_requests

        confirmed_entry = _make_attio_entry(
            entry_id="entry-cap-001",
            record_id="rec-cap-001",
            stage="Prospect",
            quality_score=75,
        )
        dropped_entry = _make_attio_entry(
            entry_id="entry-cap-002",
            record_id="rec-cap-002",
            stage="Prospect",
            quality_score=75,
        )
        attio = _attio_with_full_schema()
        attio._person_to_company = {
            "rec-cap-001": "company-cap-a",
            "rec-cap-002": "company-cap-b",
        }
        pb = _typed_pb_mock(
            csv_text=_make_sn_csv([{
                "query": "https://linkedin.com/in/cap-one",
                "linkedinProfileUrl": "https://linkedin.com/in/cap-one",
                "status": "Message sent",
            }]),
            log="Invitation sent to cap-one\n⚠️ You've reached the weekly invitation limit.",
        )
        attio.query_list_entries.return_value = [confirmed_entry, dropped_entry]
        stub_guard_reread(attio, [confirmed_entry, dropped_entry])

        def cache_side_effect(rid):
            return {
                "rec-cap-001": ("Cap One", "Co A", "https://linkedin.com/in/cap-one", "", ""),
                "rec-cap-002": ("Cap Two", "Co B", "https://linkedin.com/in/cap-two", "", ""),
            }[rid]

        with patch("workflows.daily_check.RecordCache.get", side_effect=cache_side_effect), \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.record_connections") as mock_record, \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.emit_pb_silent_no_op") as mock_emit:
            result = run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-cap",
                auto_confirm=True,
                daily_run=fake_daily_run(),
            )

        # Only the CSV-confirmed row advances; the dropped row stays put.
        stage_calls = [c for c in attio.update_list_entry.call_args_list
                       if "stage" in c.kwargs.get("entry_attributes", {})]
        assert len(stage_calls) == 1
        assert stage_calls[0].kwargs["entry_id"] == "entry-cap-001"
        # `sent` counts provider-confirmed new invites.
        assert result["sent"] == 1
        assert result["pb_queued"] == 2
        assert result["attio_updated"] == 1
        mock_record.assert_called_once_with(1)
        mock_emit.assert_not_called()
        out = capsys.readouterr().out
        assert "OPTIMISTICALLY" not in out
        assert INVITE_OPTIMISTIC_ADVANCE not in out

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_launch_sheet_contains_only_invite_rows(self):
        """2026-06-10 re-check removal: the Network Booster launch sheet must
        contain ONLY invite rows — CONNECTION_SENT profiles never ride along
        as empty-message "re-check" visit rows (Phase 0 owns acceptance
        detection)."""
        from workflows.daily_check import run_connection_requests

        prospect_entry = _make_attio_entry(
            entry_id="entry-p-001",
            record_id="rec-p-001",
            stage="Prospect",
            quality_score=75,
        )
        conn_sent_entry = _make_attio_entry(
            entry_id="entry-cs-010",
            record_id="rec-cs-010",
            stage="Connection Sent",
            quality_score=80,
        )

        attio = _attio_with_full_schema()
        pb = _typed_pb_mock(
            csv_text="query,status\nhttps://linkedin.com/in/carlosmendoza,Message sent\n",
            log="Invitation sent to carlosmendoza",
        )
        attio.query_list_entries.return_value = [prospect_entry, conn_sent_entry]
        stub_guard_reread(attio, [prospect_entry, conn_sent_entry])

        captured_sheet_rows = []

        def fake_write(rows, **kwargs):
            captured_sheet_rows.extend(rows)
            return "https://docs.google.com/spreadsheets/d/fake-sheet-id"

        with patch("workflows.daily_check.RecordCache.get") as mock_cache, \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.record_connections"), \
             patch("workflows.daily_check.get_remaining", return_value={"connections": 25, "messages": 30, "visits": 50}), \
             patch("workflows.daily_check.write_prospects_to_sheet", side_effect=fake_write):
            mock_cache.side_effect = lambda rid: {
                "rec-p-001": ("Carlos Mendoza", "CEMEX", "https://linkedin.com/in/carlosmendoza", "", ""),
                "rec-cs-010": ("Ana García", "Cemex", "https://linkedin.com/in/anagarcia", "", ""),
            }[rid]
            result = run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=fake_daily_run(),
            )

        # Exactly the invite row — the CONNECTION_SENT profile must NOT be
        # in the sheet, and no row may carry an empty (re-check) message.
        assert [r["linkedInUrl"] for r in captured_sheet_rows] == [
            "https://linkedin.com/in/carlosmendoza"
        ]
        assert all(len(r["message"]) > 0 for r in captured_sheet_rows)

        # Only the Prospect entry advances to Connection Sent.
        update_calls = [c for c in attio.update_list_entry.call_args_list
                        if "stage" in c.kwargs.get("entry_attributes", {})]
        assert len(update_calls) == 1
        updated_entry_id = update_calls[0][1].get("entry_id", update_calls[0][0][0] if update_calls[0][0] else None)
        assert updated_entry_id == "entry-p-001"

        assert result["sent"] == 1
        assert "rechecked" not in result


# ── C. Daily Check — DM Sequencing ──────────────────────────────────────────

class TestDailyDMsIntegration:

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_sends_dm1(self):
        """Accepted prospects with last_contact_date >= 1 day ago receive DM1; dm_step stored as int 1."""
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-dm1-001",
            record_id="rec-dm1-001",
            stage="Accepted",
            quality_score=70,
            last_contact_date=two_days_ago,
            dm_step=0,
        )

        attio = _attio_with_full_schema()
        # F-PR-5: CSV must mark "Message sent" for the advance gate to pass.
        pb = _typed_pb_mock(
            csv_text=_make_sn_csv([{
                "query": "https://linkedin.com/in/anagarcia",
                "linkedinProfileUrl": "https://linkedin.com/in/anagarcia",
                "status": "Message sent",
            }]),
        )
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            mock_cache_get.return_value = ("Ana García", "Cemex", "https://linkedin.com/in/anagarcia", "", "")
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-001",
                daily_run=_corruption_fake_daily_run(),
                auto_confirm=True,
            )

        # DM1 was sent via PB
        pb.launch_agent.assert_called_once()

        # Attio updated to DM1 Sent with dm_step as int 1
        attio.update_list_entry.assert_called_once()
        update_call = attio.update_list_entry.call_args
        attrs = update_call[1].get("entry_attributes", update_call[0][1] if len(update_call[0]) > 1 else {})
        assert attrs["stage"] == "DM1 Sent"
        assert attrs["dm_step"] == 1
        assert isinstance(attrs["dm_step"], int)

        assert result.get("dm1", 0) == 1

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_skips_when_sibling_entry_already_advanced(self):
        """Two Attio entries point at the same LinkedIn URL: one at DM1 Sent, one at Accepted.
        The Accepted entry must NOT re-queue DM1 because a sibling already advanced past DM1."""
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry_accepted = _make_attio_entry(
            entry_id="entry-dupe-accepted",
            record_id="rec-dupe-001",
            stage="Accepted",
            last_contact_date=two_days_ago,
        )
        entry_dm1_sent = _make_attio_entry(
            entry_id="entry-dupe-dm1",
            record_id="rec-dupe-002",
            stage="DM1 Sent",
            last_contact_date=two_days_ago,
        )

        attio = _attio_with_full_schema()
        pb = MagicMock()
        attio.query_list_entries.return_value = [entry_accepted, entry_dm1_sent]
        # Both records resolve to the same LinkedIn URL
        url = "https://linkedin.com/in/duplicated"

        def cache_side_effect(record_id):
            return ("Dup Person", "DupCo", url, "", "")

        with patch("workflows.daily_check.RecordCache.get", side_effect=cache_side_effect), \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-dupe",
                daily_run=_corruption_fake_daily_run(),
                auto_confirm=True,
            )

        # DM1 queue must be empty (sibling already at DM1 Sent). DM2 timing (5 days) not yet met.
        pb.launch_agent.assert_not_called()
        attio.update_list_entry.assert_not_called()
        assert result.get("dm1", 0) == 0
        assert result.get("dm2", 0) == 0

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_trusts_csv_over_log_parser(self):
        """PB delivered the message (CSV status='Message sent') but its log phrasing is missing
        the 'Sending message to' marker. Attio must still advance because CSV is authoritative."""
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-csv-001",
            record_id="rec-csv-001",
            stage="Accepted",
            last_contact_date=two_days_ago,
        )
        attio = _attio_with_full_schema()
        # F-PR-5: CSV is the sole source of truth (log parser is GONE).
        # Even ambiguous log text is irrelevant — only "Message sent" in
        # CSV passes the advance gate.
        pb = _typed_pb_mock(
            csv_text=_make_sn_csv([{
                "query": "https://linkedin.com/in/silentsend",
                "linkedinProfileUrl": "https://linkedin.com/in/silentsend",
                "status": "Message sent",
            }]),
        )
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            mock_cache_get.return_value = ("Silent Send", "SSCo", "https://linkedin.com/in/silentsend", "", "")
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-csv",
                daily_run=_corruption_fake_daily_run(),
                auto_confirm=True,
            )

        attio.update_list_entry.assert_called_once()
        update_call = attio.update_list_entry.call_args
        attrs = update_call[1].get("entry_attributes", update_call[0][1] if len(update_call[0]) > 1 else {})
        assert attrs["stage"] == "DM1 Sent"
        assert result.get("dm1", 0) == 1

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_reports_failed_batch_when_csv_missing(self):
        """F-PR-5: when PB emits no result CSV, the advance gate FAILS
        (csv_status='Empty', sent_count=0). The old log-fallback path
        was removed — log scraping is no longer trusted because PB
        silently drops sends from its log. Instead, daily_check emits
        a pb_silent_no_op queue row and leaves the stage unchanged so
        tomorrow's run retries the same DM."""
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-fb-001",
            record_id="rec-fb-001",
            stage="Accepted",
            last_contact_date=two_days_ago,
        )
        attio = _attio_with_full_schema()
        # CSV missing → advance gate fails → dry-skip path.
        pb = _typed_pb_mock(csv_text=None)
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.emit_pb_silent_no_op") as mock_emit:
            mock_cache_get.return_value = ("Fall Back", "FBCo", "https://linkedin.com/in/fallback", "", "")
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-fb",
                daily_run=_corruption_fake_daily_run(),
                auto_confirm=True,
            )

        # F-PR-5: advance gate failed. Attio NOT updated; queue
        # row opened so operator sees the silent-no-op.
        attio.update_list_entry.assert_not_called()
        mock_emit.assert_called_once()
        # Approved provider failure stays distinct from an intentional skip.
        assert result["failed_batches"][0]["requested"] == 1
        assert result["failed_batches"][0]["step"] == "dm1"
        assert "dm1" not in result.get("dry_skipped", {})

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_holds_stage_when_pb_csv_omits_prospect_url(self):
        """F-PR-5 §3.1 inversion: when PB's CSV reports sends for OTHER
        URLs (batch-level gate passes) but doesn't mention this specific
        prospect, the prospect's stage must NOT advance. Tomorrow's run
        retries the same DM. Prior behavior (`advance on PB completion
        regardless of log content`) was the §3.1 violation by omission
        that F-PR-5 fixes."""
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-ghost-001",
            record_id="rec-ghost-001",
            stage="Accepted",
            last_contact_date=two_days_ago,
            dm_step=0,
        )
        attio = _attio_with_full_schema()
        # PB CSV mentions a DIFFERENT URL than this prospect — batch-level
        # gate passes (csv_status=Message sent, sent_count=1), but the
        # per-URL match for THIS prospect is empty.
        pb = _typed_pb_mock(
            csv_text=_make_sn_csv([{
                "query": "https://linkedin.com/in/someone-else",
                "linkedinProfileUrl": "https://linkedin.com/in/someone-else",
                "status": "Message sent",
            }]),
        )
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            mock_cache_get.return_value = ("Ghost Sent", "GhostCo", "https://linkedin.com/in/ghost", "", "")
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-ghost",
                daily_run=_corruption_fake_daily_run(),
                auto_confirm=True,
            )

        # F-PR-5: Attio NOT updated. The prospect's URL isn't in
        # outcome.sent_urls, so per-URL advance skips it.
        attio.update_list_entry.assert_not_called()
        # L3-7: dm1 = Attio-confirmed advances (0 here — URL not in sent_urls).
        # dm1_queued = rows prepared for PB (1 row was queued).
        assert result.get("dm1", 0) == 0
        assert result.get("dm1_queued", 0) == 1

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_dm_step_failsafe_blocks_repeat(self):
        """Attio stage is stale (Accepted) but dm_step=1 proves DM1 went out.
        Must not re-queue — belt-and-suspenders against silent log-parse misses."""
        from workflows.daily_check import run_dm_sequencing

        three_days_ago = (date.today() - timedelta(days=3)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-stale-001",
            record_id="rec-stale-001",
            stage="Accepted",       # stage stale (old bug)
            last_contact_date=three_days_ago,
            dm_step=1,              # but dm_step already reflects DM1 sent
        )
        attio = _attio_with_full_schema()
        pb = MagicMock()
        attio.query_list_entries.return_value = [entry]

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            mock_cache_get.return_value = ("Stale Stage", "StaleCo", "https://linkedin.com/in/stale", "", "")
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-stale",
                daily_run=_corruption_fake_daily_run(),
                auto_confirm=True,
            )

        pb.launch_agent.assert_not_called()
        attio.update_list_entry.assert_not_called()
        assert result.get("dm1", 0) == 0

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_raises_on_pb_global_error(self):
        """F-PR-5: PB status=error is no longer silently swallowed. The
        typed `wait_for_completion` raises `PBRunFailed`, which bubbles
        out of run_dm_sequencing so the caller (the slash command)
        sees the failure. Attio writes never happen."""
        from clients.pb_envelope import PBLaunch, PBRunFailed, hash_arguments
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-err-001",
            record_id="rec-err-001",
            stage="Accepted",
            last_contact_date=two_days_ago,
        )
        attio = _attio_with_full_schema()
        pb = MagicMock()
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])
        # Typed contract: launch_agent returns PBLaunch;
        # wait_for_completion RAISES on error (no more silent dict return).
        launch = PBLaunch(
            container_id="c_err",
            agent_id="agent-msg-err",
            launched_at=datetime(2026, 5, 21, tzinfo=UTC),
            arguments_sha256=hash_arguments(None),
        )
        pb.launch_agent.return_value = launch
        pb.wait_for_completion.side_effect = PBRunFailed("c_err", "agent-msg-err", "boom")

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            mock_cache_get.return_value = ("Err Person", "ErrCo", "https://linkedin.com/in/errperson", "", "")
            import pytest
            with pytest.raises(PBRunFailed):
                run_dm_sequencing(
                    attio=attio,
                    pb=pb,
                    message_sender_id="agent-msg-err",
                    daily_run=_corruption_fake_daily_run(),
                    auto_confirm=True,
                )

        attio.update_list_entry.assert_not_called()

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_parks_pb_skipped_at_unreachable(self):
        """When PB's CSV explicitly marks a prospect's URL as skipped
        ("Can't send / InMail required"), the prospect was NOT messaged.
        Wave-2-A behavior:
        - `pb_inmail_dead_end` queue row opens per (URL, step) for operator
          visibility.
        - The prospect is parked at UNREACHABLE (stage-only write) so the
          sequencer STOPS re-queuing the same DM every run.
        - `dm_step` is NOT advanced and `last_contact_date` is NOT written —
          the DM was never delivered, so the `dm_response_rate` denominator
          must not be inflated and no contact should be implied.

        Exactly one Attio write happens: the stage → UNREACHABLE park.
        """
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-inmail-001",
            record_id="rec-inmail-001",
            stage="Accepted",
            last_contact_date=two_days_ago,
            dm_step=0,
        )
        attio = _attio_with_full_schema()
        # F-PR-5: CSV contains both an explicitly-skipped row (this
        # prospect, InMail-required) AND a separate "Message sent" row
        # — so the batch-level gate passes (sent_count=1) while the
        # per-URL match for this prospect goes through the
        # `is_pb_skipped` branch (dm_step + last_contact_date only,
        # NOT stage).
        pb = _typed_pb_mock(
            csv_text=_make_sn_csv([
                {
                    "query": "https://linkedin.com/in/inmailperson",
                    "linkedinProfileUrl": "https://linkedin.com/in/inmailperson",
                    "status": "Can't send message",
                },
                {
                    "query": "https://linkedin.com/in/sentok",
                    "linkedinProfileUrl": "https://linkedin.com/in/sentok",
                    "status": "Message sent",
                },
            ]),
        )
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.emit_pb_inmail_dead_end") as mock_inmail:
            mock_cache_get.return_value = ("InMail Person", "ImCo", "https://linkedin.com/in/inmailperson", "", "")
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-inmail",
                daily_run=_corruption_fake_daily_run(),
                auto_confirm=True,
            )

        # PB-flagged-skipped path opens the pb_inmail_dead_end queue row...
        mock_inmail.assert_called_once()
        call_kwargs = mock_inmail.call_args.kwargs
        assert call_kwargs["dm_step"] == "dm1"
        assert "inmailperson" in call_kwargs["linkedin_url"]
        # ...AND parks the prospect at UNREACHABLE via exactly one Attio
        # write (routed through AttioWriter → update_list_entry, keyword
        # args). Stage-only: no dm_step bump, no last_contact_date.
        attio.update_list_entry.assert_called_once()
        write_kwargs = attio.update_list_entry.call_args.kwargs
        assert write_kwargs["entry_attributes"] == {"stage": "Unreachable"}
        # L3-7: dm1 = Attio-confirmed advances (0 — URL was PB-flagged skipped,
        # not advanced to DM stage). dm1_queued = rows prepared for PB (1).
        assert result.get("dm1", 0) == 0
        assert result.get("dm1_queued", 0) == 1

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_inmail_park_write_failure_surfaces(self, capsys):
        """If the UNREACHABLE park write FAILS (AttioWriter exhausted retries
        → returns False), run_dm_sequencing must not crash, must not falsely
        record the row as advanced, and must SURFACE the failure (the row
        stays at its DM stage; PB re-blocks the InMail DM so nothing is
        re-sent, but the operator is told to reconcile attio_write_failed).
        Regression guard for the fire-and-forget bug the QA gate caught."""
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-pf-001", record_id="rec-pf-001",
            stage="Accepted", last_contact_date=two_days_ago, dm_step=0,
        )
        attio = _attio_with_full_schema()
        pb = _typed_pb_mock(csv_text=_make_sn_csv([
            {"query": "https://linkedin.com/in/pfperson",
             "linkedinProfileUrl": "https://linkedin.com/in/pfperson",
             "status": "Can't send message"},
            {"query": "https://linkedin.com/in/sentok",
             "linkedinProfileUrl": "https://linkedin.com/in/sentok",
             "status": "Message sent"},
        ]))
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.emit_pb_inmail_dead_end"), \
             patch("workflows.daily_check._attio_advance_with_escalation", return_value=False) as mock_park:
            mock_cache_get.return_value = ("PF Person", "PfCo", "https://linkedin.com/in/pfperson", "", "")
            result = run_dm_sequencing(
                attio=attio, pb=pb, message_sender_id="agent-pf",
                daily_run=_corruption_fake_daily_run(), auto_confirm=True,
            )

        # Park was attempted with a stage-only UNREACHABLE write...
        mock_park.assert_called_once()
        assert mock_park.call_args.kwargs["entry_attributes"] == {"stage": "Unreachable"}
        # ...returned False → run completes, queue still counted, no crash...
        # L3-7: dm1 = Attio-confirmed advances (0 — park failed, URL PB-skipped).
        assert result.get("dm1", 0) == 0
        assert result.get("dm1_queued", 0) == 1
        # ...and the failure is surfaced loudly (not fire-and-forget).
        assert "park write" in capsys.readouterr().err.lower()

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_inmail_park_covers_all_duplicate_entries(self):
        """A URL backed by multiple Attio list entries must park EVERY entry
        at UNREACHABLE — a partial park would leave a duplicate at an active
        DM stage and re-DM next run (the 2026-04-21 duplicate-row hazard)."""
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        e1 = _make_attio_entry("entry-dup-1", "rec-dup-1", "Accepted",
                               last_contact_date=two_days_ago, dm_step=0)
        e2 = _make_attio_entry("entry-dup-2", "rec-dup-2", "Accepted",
                               last_contact_date=two_days_ago, dm_step=0)
        attio = _attio_with_full_schema(_person_to_company={})
        pb = _typed_pb_mock(csv_text=_make_sn_csv([
            {"query": "https://linkedin.com/in/dupperson",
             "linkedinProfileUrl": "https://linkedin.com/in/dupperson",
             "status": "Can't send message"},
            {"query": "https://linkedin.com/in/sentok",
             "linkedinProfileUrl": "https://linkedin.com/in/sentok",
             "status": "Message sent"},
        ]))
        attio.query_list_entries.return_value = [e1, e2]
        stub_guard_reread(attio, [e1, e2])

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.emit_pb_inmail_dead_end"), \
             patch("workflows.daily_check._attio_advance_with_escalation", return_value=True) as mock_park:
            # Both record_ids resolve to the same URL → one deduped row whose
            # entry_ids carries BOTH list entries.
            mock_cache_get.return_value = ("Dup Person", "DupCo", "https://linkedin.com/in/dupperson", "", "")
            run_dm_sequencing(
                attio=attio, pb=pb, message_sender_id="agent-dup",
                daily_run=_corruption_fake_daily_run(), auto_confirm=True,
            )

        assert mock_park.call_count == 2
        parked = {c.kwargs["entry_id"] for c in mock_park.call_args_list}
        assert parked == {"entry-dup-1", "entry-dup-2"}
        for c in mock_park.call_args_list:
            assert c.kwargs["entry_attributes"] == {"stage": "Unreachable"}

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_trims_queue_when_over_cap(self):
        """When eligible DMs > daily cap, queue is trimmed instead of bailed.
        Prior bug: an over-cap queue caused script to abort entirely, leaving the
        backlog to grow indefinitely.
        Now: trim to remaining cap (DM3 > DM2 > DM1 priority, oldest-first), and
        defer the rest to next run."""
        from workflows.daily_check import run_dm_sequencing

        # Build 35 Accepted prospects, all eligible for DM1. Cap is 30.
        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entries = [
            _make_attio_entry(
                entry_id=f"entry-bulk-{i:02d}",
                record_id=f"rec-bulk-{i:02d}",
                stage="Accepted",
                last_contact_date=two_days_ago,
                dm_step=0,
            )
            for i in range(35)
        ]

        attio = _attio_with_full_schema(_person_to_company={})
        attio.query_list_entries.return_value = entries
        stub_guard_reread(attio, entries)
        # F-PR-5: 30 bulk URLs all marked "Message sent" so the
        # advance gate passes for every queued prospect.
        bulk_csv_rows = [
            {
                "query": f"https://linkedin.com/in/bulk{i:02d}",
                "linkedinProfileUrl": f"https://linkedin.com/in/bulk{i:02d}",
                "status": "Message sent",
            }
            for i in range(35)
        ]
        pb = _typed_pb_mock(csv_text=_make_sn_csv(bulk_csv_rows))

        def cache_side_effect(rid):
            n = rid.split("-")[-1]
            return (f"Bulk{n}", "BulkCo", f"https://linkedin.com/in/bulk{n}", "", "")

        with patch("workflows.daily_check.RecordCache.get", side_effect=cache_side_effect), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-bulk",
                daily_run=_corruption_fake_daily_run(remaining=30),
                auto_confirm=True,
            )

        # Cap is 30 → 30 DMs sent, not 0 (the old bug) and not all 35.
        assert result.get("dm1", 0) == 30, \
            f"Expected 30 DMs sent (trimmed to cap), got {result.get('dm1')}"
        # The other 5 deferred; not in `reason` because we sent something.
        assert "reason" not in result or result.get("reason") != "daily_limit"

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0", "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_sequencing_prioritizes_dm1_then_dm3_dm2_when_trimming(self):
        """Trim priority: DM1 (fresh-accept) reserved first; remainder splits DM3 > DM2.

        New policy (2026-05-18): DM1 momentum decays fast — a fresh-accept waiting
        2-3 days for DM1 kills the conversation. Reserve the whole DM1 queue first,
        then give remaining budget to DM3 > DM2 oldest-first.

        Setup: 2 DM3-eligible + 3 DM2-eligible + 5 DM1-eligible = 10 total. Cap=5.
        Expected: DM1=5 (all), DM2=0, DM3=0 (no budget left for backlog).
        """
        from workflows.daily_check import run_dm_sequencing

        def today_minus(d):
            return (date.today() - timedelta(days=d)).isoformat()
        entries = []
        # DM3 eligible: DM2_SENT, last_contact >= 10 days ago, dm_step=2
        for i in range(2):
            entries.append(_make_attio_entry(
                entry_id=f"entry-dm3-{i}", record_id=f"rec-dm3-{i}",
                stage="DM2 Sent", last_contact_date=today_minus(15), dm_step=2,
            ))
        # DM2 eligible: DM1_SENT, last_contact >= 5 days ago, dm_step=1
        for i in range(3):
            entries.append(_make_attio_entry(
                entry_id=f"entry-dm2-{i}", record_id=f"rec-dm2-{i}",
                stage="DM1 Sent", last_contact_date=today_minus(7), dm_step=1,
            ))
        # DM1 eligible (5 — exactly fills the cap)
        for i in range(5):
            entries.append(_make_attio_entry(
                entry_id=f"entry-dm1-{i}", record_id=f"rec-dm1-{i}",
                stage="Accepted", last_contact_date=today_minus(2), dm_step=0,
            ))

        attio = _attio_with_full_schema(_person_to_company={})
        attio.query_list_entries.return_value = entries
        stub_guard_reread(attio, entries)
        # F-PR-5: 5 DM1 URLs all marked "Message sent" to pass the gate.
        prio_csv_rows = [
            {
                "query": f"https://linkedin.com/in/rec-dm1-{i}",
                "linkedinProfileUrl": f"https://linkedin.com/in/rec-dm1-{i}",
                "status": "Message sent",
            }
            for i in range(5)
        ]
        pb = _typed_pb_mock(csv_text=_make_sn_csv(prio_csv_rows))

        def cache_side_effect(rid):
            return (f"P-{rid}", "Co", f"https://linkedin.com/in/{rid}", "", "")

        with patch("workflows.daily_check.RecordCache.get", side_effect=cache_side_effect), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-prio",
                daily_run=_corruption_fake_daily_run(remaining=5),
                auto_confirm=True,
            )

        # DM1 consumes the full cap (5); DM3 and DM2 get nothing.
        assert result.get("dm1", 0) == 5, f"DM1 expected 5, got {result.get('dm1')}"
        assert result.get("dm3", 0) == 0, f"DM3 expected 0 (no budget left), got {result.get('dm3')}"
        assert result.get("dm2", 0) == 0, f"DM2 expected 0 (no budget left), got {result.get('dm2')}"


# ── D. Daily Check — Auto-Detect Accepted Connections ─────────────────────

class TestDetectAcceptedConnections:

    def _setup_pb_scraper(self, pb, result_csv_data):
        """Configure PB mock for the Profile Scraper live-check flow.

        F-PR-5: launch_agent now returns PBLaunch; wait_for_completion
        returns PBCompletion. Helper hands back typed envelopes."""
        launch = PBLaunch(
            container_id="c_scraper",
            agent_id="agent-scraper",
            launched_at=datetime(2026, 5, 21, tzinfo=UTC),
            arguments_sha256=hash_arguments(None),
        )
        completion = PBCompletion(
            container_id="c_scraper",
            status="finished",
            log_output="",
            raw_output={"status": "finished"},
        )
        pb.launch_agent.return_value = launch
        pb.wait_for_completion.return_value = completion
        pb.download_result_csv.return_value = result_csv_data

    @patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake")
    @patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake"})
    def test_detects_accepted_connection(self, mock_sheet):
        """CONNECTION_SENT profile with connectionDegree '1st' transitions to Accepted."""
        from workflows.daily_check import detect_accepted_connections

        result_csv = _make_sn_csv([{
            "profileUrl": "https://linkedin.com/in/carlosejemplo",
            "connectionDegree": "1st",
        }])

        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        pb = MagicMock()
        self._setup_pb_scraper(pb, result_csv)

        entry = _make_attio_entry(
            entry_id="entry-cs-001",
            record_id="rec-cs-001",
            stage="Connection Sent",
            quality_score=75,
        )
        attio.query_list_entries.return_value = [entry]

        with patch("workflows.daily_check.RecordCache.get") as mock_cache:
            mock_cache.return_value = ("Carlos Ejemplo", "CEMENTRA", "https://linkedin.com/in/carlosejemplo", "", "")
            result = detect_accepted_connections(attio, pb, "agent-scraper-001")

        pb.launch_agent.assert_called_once()
        attio.update_list_entry.assert_called_once()
        update_call = attio.update_list_entry.call_args
        attrs = update_call[1].get("entry_attributes", update_call[0][1] if len(update_call[0]) > 1 else {})
        assert attrs["stage"] == "Accepted"
        assert result["accepted"] == 1

    @patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake")
    @patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake"})
    def test_pending_invitation_no_transition(self, mock_sheet):
        """CONNECTION_SENT profile with degree '3rd' stays unchanged."""
        from workflows.daily_check import detect_accepted_connections

        result_csv = _make_sn_csv([{
            "profileUrl": "https://linkedin.com/in/anaejemplo",
            "connectionDegree": "3rd",
        }])

        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        pb = MagicMock()
        self._setup_pb_scraper(pb, result_csv)

        entry = _make_attio_entry(
            entry_id="entry-cs-002",
            record_id="rec-cs-002",
            stage="Connection Sent",
            quality_score=80,
        )
        attio.query_list_entries.return_value = [entry]

        with patch("workflows.daily_check.RecordCache.get") as mock_cache:
            mock_cache.return_value = ("Ana Ejemplo", "Cementra", "https://linkedin.com/in/anaejemplo", "", "")
            result = detect_accepted_connections(attio, pb, "agent-scraper-001")

        attio.update_list_entry.assert_not_called()
        assert result["accepted"] == 0

    @patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake"})
    def test_no_connection_sent_profiles(self):
        """If no CONNECTION_SENT profiles exist, Phase 0 returns zero without launching PB."""
        from workflows.daily_check import detect_accepted_connections

        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        pb = MagicMock()
        attio.query_list_entries.return_value = []

        result = detect_accepted_connections(attio, pb, "agent-scraper-001")

        pb.launch_agent.assert_not_called()
        attio.update_list_entry.assert_not_called()
        assert result["accepted"] == 0
        assert result["checked"] == 0

    @patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake")
    @patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake"})
    def test_scraper_returns_no_csv_gracefully(self, mock_sheet):
        """If Profile Scraper returns no CSV, Phase 0 returns zero and does not crash."""
        from workflows.daily_check import detect_accepted_connections

        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        pb = MagicMock()
        # F-PR-5 typed shapes: launch_agent returns a PBLaunch (the no-CSV
        # escalation reads launch.container_id) and wait_for_completion a
        # PBCompletion (the silent-zero guard reads completion.log_output).
        pb.launch_agent.return_value = MagicMock(container_id="ct-int-003")
        pb.wait_for_completion.return_value = MagicMock(log_output="")
        pb.download_result_csv.return_value = None

        entry = _make_attio_entry(
            entry_id="entry-cs-003",
            record_id="rec-cs-003",
            stage="Connection Sent",
            quality_score=70,
        )
        attio.query_list_entries.return_value = [entry]

        with patch("workflows.daily_check.RecordCache.get") as mock_cache:
            mock_cache.return_value = ("Test User", "TestCo", "https://linkedin.com/in/testuser", "", "")
            result = detect_accepted_connections(attio, pb, "agent-scraper-001")

        assert result["accepted"] == 0
        assert result.get("error") == "no_csv"

    @patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake")
    @patch.dict(os.environ, {"ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake"})
    def test_www_normalization(self, mock_sheet):
        """Matches profiles when Attio has www. but PB result does not."""
        from workflows.daily_check import detect_accepted_connections

        result_csv = _make_sn_csv([{
            "profileUrl": "https://linkedin.com/in/carlosejemplo",
            "connectionDegree": "1st",
        }])

        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        pb = MagicMock()
        self._setup_pb_scraper(pb, result_csv)

        entry = _make_attio_entry(
            entry_id="entry-cs-004",
            record_id="rec-cs-004",
            stage="Connection Sent",
            quality_score=75,
        )
        attio.query_list_entries.return_value = [entry]

        with patch("workflows.daily_check.RecordCache.get") as mock_cache:
            # Attio stores with www., PB returns without
            mock_cache.return_value = ("Carlos Ejemplo", "CEMENTRA", "https://www.linkedin.com/in/carlosejemplo", "", "")
            result = detect_accepted_connections(attio, pb, "agent-scraper-001")

        assert result["accepted"] == 1
