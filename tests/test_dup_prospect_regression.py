"""Regression tests for the 2026-07-02 Álex-de-la-Torres duplicate cascade.

The incident chain:
  1. Weekly ingest re-created an existing prospect under a DIFFERENT LinkedIn
     vanity slug (same person, new URL). URL-keyed dedup missed it.
  2. The daily run found the "new" prospect already 1st-degree, Pattern-A
     flipped him to ACCEPTED, and sent a DM1 — 12 days after he'd completed a
     DM3 cadence.
  3. Reply detection counted our own duplicate DM1 as a "manual reply" and
     falsely flipped him to Responded.

The three fixes, each reproduced at unit level:
  * Fix 1 — name+company secondary dedup gate at ingest (weekly_prospect).
  * Fix 2 — Pattern-A quarantine for recently-created prospects
    (pre_invite_check).
  * Fix 3 — reply-detection self-echo guard (detect_responses).
"""

import csv
import io
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from models.pipeline import PipelineStage

# ─────────────────────────────────────────────────────────────────────────
# Fix 1 — name+company secondary dedup gate at ingest
# ─────────────────────────────────────────────────────────────────────────

class TestFix1NameCompanyDedupGate:
    """A candidate whose canonical URL is NOT in the pipeline (so the URL gates
    pass) but whose normalized name + company matches an existing Attio person
    must be staged for reprospect review, NOT committed.
    """

    # Álex under a NEW slug. score_prospect passes deterministically
    # (Director of Operations at a food company in Mexico → enterprise_pass).
    _CANDIDATE_RAW = {
        "fullName": "Álex de la Torres",
        "title": "Director of Operations",
        "companyName": "Example Alimentos",
        "location": "Monterrey, Mexico",
        "defaultProfileUrl": "https://www.linkedin.com/in/álex-de-la-torres-42a9a067",
    }

    def _summary(self):
        return {
            "exported": 0, "scored": 0, "qualified": 0, "duplicates": 0,
            "rejected": 0, "added": 0, "borderline_staged": 0,
            "reprospect_review": 0,
        }

    def _existing_person(self):
        # The person already in Attio under the OTHER slug (álexdelatorres).
        return {
            "id": {"record_id": "rec-alex-existing"},
            "values": {
                "name": [{"first_name": "Álex", "last_name": "de la Torres"}],
                "linkedin": [{"value": "https://linkedin.com/in/álexdelatorres"}],
                "company": [{"target_record_id": "co-example"}],
            },
        }

    def test_normalizer_is_pure_and_accent_folds(self):
        from workflows.weekly_prospect import _normalize_person_name

        assert _normalize_person_name("Álex de la Torres") == "alex de la torres"
        # Strip suffix after SPACE-ANCHORED separators only.
        assert _normalize_person_name("Álex de la Torres - Example") == "alex de la torres"
        assert _normalize_person_name("Álex de la Torres | CEO") == "alex de la torres"
        # A parenthetical nickname is NOT a separator — must not truncate
        # (finding 7: the old bare "(" cut collapsed this to "alex").
        assert (
            _normalize_person_name("Álex (Pat) de la Torres")
            == "alex (pat) de la torres"
        )
        # Collapse whitespace + lowercase.
        assert _normalize_person_name("  ÁLEX   DE  LA  TORRES ") == "alex de la torres"

    def _build_index_via_bulk_fetch(self, attio, existing_person):
        """Build the real name index from realistic person records — the
        production build path (finding 1: exercise index construction +
        lookup, do NOT mock a search return value)."""
        from workflows.weekly_prospect import _build_name_index

        attio.extract_record_info.return_value = (
            "Álex de la Torres",
            "Totally Different Foods" if existing_person["id"]["record_id"] == "rec-other-alex" else "Example Alimentos",
            "https://linkedin.com/in/álexdelatorres", None, "Director of Operations",
        )
        rid = existing_person["id"]["record_id"]
        attio.bulk_fetch_persons_by_record_ids.return_value = {rid: existing_person}
        # One list entry pointing at the existing record.
        entries = [{"parent_record_id": rid}]
        from clients.crm.attio_provider import AttioProvider
        from clients.crm.base import Entry, Stage
        return _build_name_index(AttioProvider(attio), [Entry(entry_id="e", record_id=rid, stage=Stage("Prospect"))])

    def test_url_variant_duplicate_staged_not_committed(self):
        from clients.crm.attio_provider import AttioProvider
        from workflows.weekly_prospect import _process_prospects

        attio = MagicMock()
        # URL gates PASS: the new slug is not found by linkedin search.
        attio.search_person_by_linkedin.return_value = None
        existing = self._existing_person()
        # Build the REAL run-start index from the existing person record.
        name_index = self._build_index_via_bulk_fetch(attio, existing)
        # Company isn't carried in the index → the gate confirms it by fetching
        # the suspected-dup's record. Wire get_person + extract_record_info.
        attio.get_person.return_value = existing
        attio.extract_record_info.return_value = (
            "Álex de la Torres", "Example Alimentos",
            "https://linkedin.com/in/álexdelatorres", None, "Director of Operations",
        )

        summary = self._summary()
        summary["dedup_gate_degraded"] = 0
        reprospect_review: list[dict] = []

        _process_prospects(
            [self._CANDIDATE_RAW],
            AttioProvider(attio),
            list_id="list-123",
            today="2026-07-02",
            dry_run=False,
            summary=summary,
            seen_urls=set(),
            in_list_record_ids=set(),
            persona_config={"key": "operations_leaders", "enterprise_mode": True, "search_size_credit": 30},
            borderline_stage=[],
            reprospect_review=reprospect_review,
            name_index=name_index,
        )

        # Never committed.
        attio.upsert_person.assert_not_called()
        attio.add_list_entry.assert_not_called()
        assert summary["added"] == 0

        # Staged for operator review with a name+company reason.
        assert summary["reprospect_review"] == 1
        assert len(reprospect_review) == 1
        entry = reprospect_review[0]
        assert entry["record_id"] == "rec-alex-existing"
        assert "name+company" in entry.get("reason", "").lower()

    def test_name_match_but_different_company_still_commits(self):
        """A same-name person at a DIFFERENT company is a distinct human — must
        NOT be blocked. Guards against over-eager staging on common names.
        """
        from clients.crm.attio_provider import AttioProvider
        from workflows.weekly_prospect import _process_prospects

        attio = MagicMock()
        attio.search_person_by_linkedin.return_value = None
        # The indexed same-name person is at a DIFFERENT company.
        other_alex = {
            "id": {"record_id": "rec-other-alex"},
            "values": {
                "name": [{"first_name": "Álex", "last_name": "de la Torres"}],
                "linkedin": [{"value": "https://linkedin.com/in/alex-otherco"}],
                "company": [{"target_record_id": "co-otherco"}],
            },
        }
        name_index = self._build_index_via_bulk_fetch(attio, other_alex)
        attio.get_person.return_value = other_alex
        attio.extract_record_info.return_value = (
            "Álex de la Torres", "Totally Different Foods",
            "https://linkedin.com/in/alex-otherco", None, "Director of Operations",
        )
        attio.upsert_person.return_value = {"id": {"record_id": "rec-new"}}

        summary = self._summary()
        summary["dedup_gate_degraded"] = 0
        reprospect_review: list[dict] = []

        _process_prospects(
            [self._CANDIDATE_RAW],
            AttioProvider(attio),
            list_id="list-123",
            today="2026-07-02",
            dry_run=False,
            summary=summary,
            seen_urls=set(),
            in_list_record_ids=set(),
            persona_config={"key": "operations_leaders", "enterprise_mode": True, "search_size_credit": 30},
            borderline_stage=[],
            reprospect_review=reprospect_review,
            name_index=name_index,
        )

        # Different company → not a URL-variant duplicate → committed as normal.
        assert summary["reprospect_review"] == 0
        assert reprospect_review == []
        attio.upsert_person.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────
# Fix 2 — Pattern-A quarantine for recently-created prospects
# ─────────────────────────────────────────────────────────────────────────

@patch("workflows.daily_check.write_prospects_to_sheet", return_value="https://sheet.example/foo")
@patch("workflows.daily_check._pb_session_args", return_value={})
class TestFix2PatternAQuarantine:
    """A 1st-degree row that only became a prospect within the quarantine
    window (14 days) must NOT be Pattern-A flipped to ACCEPTED — it is a
    suspected URL-variant duplicate. It is skipped for the day and escalated.
    Old records keep flipping (legitimate silent-acceptance Pattern-A).
    """

    def _csv(self, rows: list[dict]) -> str:
        header = "linkedinProfileUrl,connectionDegree\n"
        body = "\n".join(f"{r['url']},{r['degree']}" for r in rows)
        return header + body

    def _row(self, committed_at):
        return {
            "linkedInUrl": "https://www.linkedin.com/in/alex-new",
            "message": "hi",
            "entry_id": "ent-alex",
            "record_id": "rec-alex",
            "current_stage": "Prospect",
            "experiment_id": "exp-x",
            "experiment_id_frozen_at": "prospect",
            "name": "Álex de la Torres",
            "company": "Example Alimentos",
            "prospect_committed_at": committed_at,
        }

    def _run(self, committed_at):
        from workflows.daily_check import _pre_invite_degree_check

        attio = MagicMock()
        pb = MagicMock()
        pb.download_result_csv.return_value = self._csv([
            {"url": "https://www.linkedin.com/in/alex-new", "degree": "1st"},
        ])
        with patch("workflows.pre_invite_check.escalate") as mock_escalate:
            still, already = _pre_invite_degree_check(
                [self._row(committed_at)], pb, "scraper-id", attio, "list-id",
                today=date(2026, 7, 2),
            )
        return still, already, attio, mock_escalate

    def test_recent_prospect_quarantined_not_flipped(self, _pb_args, _sheet):
        # Committed 6 days ago (< 14) → quarantine.
        committed = (date(2026, 7, 2) - timedelta(days=6)).isoformat()
        still, already, attio, mock_escalate = self._run(committed)

        # Not flipped to ACCEPTED, not invited.
        assert already == []
        assert still == []
        attio.update_list_entry.assert_not_called()
        # Escalated with the new typed slug.
        assert mock_escalate.called
        assert mock_escalate.call_args.kwargs["type"] == "pattern_a_suspected_duplicate"

    def test_old_prospect_still_flips(self, _pb_args, _sheet):
        # Committed 30 days ago (> 14) → legitimate Pattern-A, flip as before.
        committed = (date(2026, 7, 2) - timedelta(days=30)).isoformat()
        still, already, attio, mock_escalate = self._run(committed)

        assert len(already) == 1
        assert already[0]["entry_id"] == "ent-alex"
        attio.update_list_entry.assert_called_once()
        entry_attrs = attio.update_list_entry.call_args.kwargs["entry_attributes"]
        assert entry_attrs["stage"] == PipelineStage.ACCEPTED.value
        # No quarantine escalation.
        quarantine_calls = [
            c for c in mock_escalate.call_args_list
            if c.kwargs.get("type") == "pattern_a_suspected_duplicate"
        ]
        assert quarantine_calls == []

    def test_missing_timestamp_still_flips(self, _pb_args, _sheet):
        # No prospect_committed_at → cannot prove recency → flip as before
        # (old records must keep the legitimate Pattern-A behavior).
        still, already, attio, mock_escalate = self._run(None)

        assert len(already) == 1
        attio.update_list_entry.assert_called_once()
        quarantine_calls = [
            c for c in mock_escalate.call_args_list
            if c.kwargs.get("type") == "pattern_a_suspected_duplicate"
        ]
        assert quarantine_calls == []


# ─────────────────────────────────────────────────────────────────────────
# Fix 3 — reply-detection self-echo guard
# ─────────────────────────────────────────────────────────────────────────

class TestFix3ReplySelfEchoGuard:
    """A thread whose last message is OUR own DM template (the dup-DM1 case)
    must NOT flip to Responded — it is a self-echo, not a manual reply. A
    genuine short reply must still flip.
    """

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        # detect_responses routes manual-reply writes through AttioWriter,
        # which requires ATTIO_LIST_ID; shrink the retry budget too.
        monkeypatch.setenv("ATTIO_LIST_ID", "test-list-id")
        monkeypatch.setattr("clients.attio_writer.RETRY_BUDGET_SECONDS", 0.0001)

    def _make_entry(self, entry_id, record_id, stage):
        return {
            "entry_id": entry_id,
            "record_id": record_id,
            "stage": stage,
            "persona": "operations_leaders",
            "language": "es",
            "dm_step": 1,
            "quality_score": 75,
            "last_contact_date": "2026-06-20",
            "experiment_id": None,
        }

    def _make_sn_csv(self, **row) -> str:
        fieldnames = [
            "participantProfileUrl", "participantFullName",
            "isLastMessageFromMe", "lastMessageBody", "lastMessageDate",
            "totalMessageCount",
        ]
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=fieldnames)
        w.writeheader()
        w.writerow({k: row.get(k, "") for k in fieldnames})
        return buf.getvalue()

    def _run(self, *, last_body: str):
        from workflows.detect_responses import detect_responses

        attio = MagicMock()
        pb = MagicMock()
        entry = self._make_entry("e-alex", "r-alex", PipelineStage.DM1_SENT.value)
        attio.query_list_entries.return_value = [entry]
        pb.download_result_csv.return_value = self._make_sn_csv(
            participantProfileUrl="https://linkedin.com/in/alex",
            participantFullName="Álex de la Torres",
            isLastMessageFromMe="true",
            lastMessageBody=last_body,
            lastMessageDate="2026-07-02T12:00:00Z",
            totalMessageCount="3",  # arithmetic says "manual reply"
        )
        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses._pb_session_args", return_value={}), \
             patch("workflows.detect_responses.RecordCache") as MockCache, \
             patch("workflows.detect_responses.escalate") as mock_escalate:
            mock_cache = MagicMock()
            mock_cache.get.return_value = (
                "Álex de la Torres", "Example", "https://linkedin.com/in/alex", "", "",
            )
            MockCache.return_value = mock_cache
            result = detect_responses(attio, pb, inbox_scraper_id="scraper-123")
        return result, attio, mock_escalate

    def test_similarity_fn_is_pure(self):
        # Our own DM1 template body (operations_leaders es) → high overlap.
        from models.campaign import load_messages
        from workflows.detect_responses import _looks_like_self_echo
        template = load_messages()["operations_leaders"]["dm1"]["es"]
        # Personalized copy of our own template still matches.
        echoed = template.replace("[Name]", "Álex").replace("[Company]", "Example")
        matched = _looks_like_self_echo(echoed)
        assert matched is not None

        # A genuine short reply does not match any template.
        assert _looks_like_self_echo("Gracias Álex, me interesa") is None

    def test_self_echo_suppressed_and_escalated(self):
        from models.campaign import load_messages
        template = load_messages()["operations_leaders"]["dm1"]["es"]
        echoed = template.replace("[Name]", "Álex").replace("[Company]", "Example")

        result, attio, mock_escalate = self._run(last_body=echoed)

        # NOT flipped to Responded (the self-echo was suppressed). Other
        # mechanisms (e.g. cadence-drift auto-repair) may still touch the
        # entry, so assert no write set stage=Responded rather than no write
        # at all.
        assert result.get("detected", 0) == 0
        responded_writes = [
            c for c in attio.update_list_entry.call_args_list
            if c.kwargs.get("entry_attributes", {}).get("stage")
            == PipelineStage.RESPONDED.value
        ]
        assert responded_writes == []
        # Escalated with the new typed slug.
        assert mock_escalate.called
        assert mock_escalate.call_args.kwargs["type"] == "manual_reply_suppressed_self_echo"

    def test_genuine_reply_still_flips(self):
        result, attio, mock_escalate = self._run(
            last_body="Gracias Álex, me interesa. Cuándo podemos hablar?",
        )

        # A real reply (last message from us after their reply) still flips.
        assert result.get("detected", 0) == 1
        attio.update_list_entry.assert_called()
        # No self-echo escalation.
        echo_calls = [
            c for c in mock_escalate.call_args_list
            if c.kwargs.get("type") == "manual_reply_suppressed_self_echo"
        ]
        assert echo_calls == []
