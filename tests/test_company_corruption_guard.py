"""Guard against the LinkedIn-Clearbit corruption fingerprint.

A historical (pre-2026-04-11) ingestion path created Attio company records
where `domains=['linkedin.com']` and Clearbit enriched the record with
LinkedIn's own profile data, while PB's `companyName` field overwrote the
`name`. The result: a record named e.g. "Prolec GE" carrying LinkedIn's
description, Sunnyvale HQ, and `linkedin.com/company/linkedin` social link.

When daily_check.run_dm_sequencing personalises a DM with `[Company]`,
it pulls `company.name` — for a corrupted record this is a real-looking
employer name, so the message goes out with the wrong company.

These tests pin the guard: detect the fingerprint pre-render and skip
the row, fail loud rather than ship a poisoned DM.
"""

import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

from tests.test_integration import (
    _attio_with_full_schema,
    _make_attio_entry,
    _make_sn_csv,
    _typed_pb_mock,
)


class TestLinkedinClearbitFingerprint:
    """Direct unit tests for is_linkedin_clearbit_corrupted()."""

    def _company_record(self, *, name: str, domains: list[str] | None = None, linkedin: str | None = None) -> dict:
        values: dict = {"name": [{"value": name}]}
        if domains is not None:
            values["domains"] = [{"domain": d} for d in domains]
        if linkedin is not None:
            values["linkedin"] = [{"value": linkedin}]
        return {"values": values}

    def test_real_linkedin_record_is_not_corrupted(self):
        from clients.attio import is_linkedin_clearbit_corrupted
        record = self._company_record(
            name="LinkedIn",
            domains=["linkedin.com"],
            linkedin="https://linkedin.com/company/linkedin",
        )
        assert is_linkedin_clearbit_corrupted(record) is False

    def test_prolec_ge_with_linkedin_domain_is_corrupted(self):
        from clients.attio import is_linkedin_clearbit_corrupted
        record = self._company_record(
            name="Prolec GE",
            domains=["linkedin.com"],
            linkedin="https://linkedin.com/company/linkedin",
        )
        assert is_linkedin_clearbit_corrupted(record) is True

    def test_br_linkedin_subdomain_also_corrupted(self):
        from clients.attio import is_linkedin_clearbit_corrupted
        record = self._company_record(
            name="Em Transição de Carreira - In Career Transition",
            domains=["br.linkedin.com"],
            linkedin="https://linkedin.com/company/linkedin",
        )
        assert is_linkedin_clearbit_corrupted(record) is True

    def test_normal_company_with_own_linkedin_url_is_clean(self):
        from clients.attio import is_linkedin_clearbit_corrupted
        record = self._company_record(
            name="Cementra",
            domains=["cementra.com"],
            linkedin="https://linkedin.com/company/cementra",
        )
        assert is_linkedin_clearbit_corrupted(record) is False

    def test_corrupted_via_linkedin_self_url_only(self):
        """No linkedin.com domain, but `linkedin` field points at the LinkedIn-self URL → still corrupted."""
        from clients.attio import is_linkedin_clearbit_corrupted
        record = self._company_record(
            name="Acme Foods",
            domains=["acme.com"],
            linkedin="https://linkedin.com/company/linkedin",
        )
        assert is_linkedin_clearbit_corrupted(record) is True

    def test_empty_record_is_not_corrupted(self):
        from clients.attio import is_linkedin_clearbit_corrupted
        assert is_linkedin_clearbit_corrupted({"values": {}}) is False


class TestPhaseBSkipsCorruptedCompany:
    """Phase B refuses to queue any prospect whose linked company has the
    LinkedIn-Clearbit corruption fingerprint."""

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0",
    })
    def test_dm_sequencing_skips_corrupted_linkedin_clearbit_company(self):
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-corrupt-001",
            record_id="rec-corrupt-001",
            stage="Accepted",
            quality_score=70,
            last_contact_date=two_days_ago,
            dm_step=0,
        )

        attio = _attio_with_full_schema()
        pb = MagicMock()
        attio.query_list_entries.return_value = [entry]
        attio.is_person_company_corrupted.return_value = True

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            from workflows.daily_run import DailyRun as _DailyRun
            _fake_dr = MagicMock(spec=_DailyRun)
            _fake_dr.remaining.return_value = 30
            mock_cache_get.return_value = ("Oscar Orozco", "Prolec GE", "https://linkedin.com/in/oscar", "", "Plant Director")
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-corrupt",
                daily_run=_fake_dr,
                auto_confirm=True,
            )

        pb.launch_agent.assert_not_called()
        attio.update_list_entry.assert_not_called()
        assert result.get("dm1", 0) == 0
        assert result.get("skipped_corrupted_company", 0) == 1

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake", "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0",
    })
    def test_dm_sequencing_still_sends_when_company_is_clean(self):
        """Baseline: clean linkage still gets a DM (proves the guard isn't blocking everything)."""
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-clean-001",
            record_id="rec-clean-001",
            stage="Accepted",
            quality_score=70,
            last_contact_date=two_days_ago,
            dm_step=0,
        )

        attio = _attio_with_full_schema()
        attio.query_list_entries.return_value = [entry]
        attio.is_person_company_corrupted.return_value = False
        from tests.fakes import stub_guard_reread
        stub_guard_reread(attio, [entry])
        # F-PR-5 typed PB mock: CSV with "Message sent" → gate passes.
        pb = _typed_pb_mock(
            csv_text=_make_sn_csv([{
                "query": "https://linkedin.com/in/clean",
                "linkedinProfileUrl": "https://linkedin.com/in/clean",
                "status": "Message sent",
            }]),
        )

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://docs.google.com/spreadsheets/d/fake"):
            from workflows.daily_run import DailyRun as _DailyRun
            _fake_dr = MagicMock(spec=_DailyRun)
            _fake_dr.remaining.return_value = 30
            mock_cache_get.return_value = ("Ana Ejemplo", "Cementra", "https://linkedin.com/in/clean", "", "VP Operations")
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-clean",
                daily_run=_fake_dr,
                auto_confirm=True,
            )

        pb.launch_agent.assert_called_once()
        assert result.get("dm1", 0) == 1
        assert result.get("skipped_corrupted_company", 0) == 0
