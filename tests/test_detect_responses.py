"""Tests for workflows/detect_responses.py (SN Inbox Scraper version)."""

import csv
import io
from unittest.mock import MagicMock, patch

import pytest

from models.pipeline import PipelineStage
from workflows.detect_responses import _normalize_name, detect_responses


@pytest.fixture(autouse=True)
def _detect_responses_test_env(monkeypatch):
    """Wave-2-B fix-up: detect_responses now routes through AttioWriter,
    which requires ATTIO_LIST_ID for list-entry writes. Also shrink the
    retry budget so transient-error tests don't burn ~30s on real sleep
    before AttioWriter exhausts retries.
    """
    monkeypatch.setenv("ATTIO_LIST_ID", "test-list-id")
    monkeypatch.setattr("clients.attio_writer.RETRY_BUDGET_SECONDS", 0.0001)

# ── Helpers ────────────────────────────────────────────────────────────

_STAGE_TO_DEFAULT_DM_STEP = {
    PipelineStage.DM1_SENT.value: 1,
    PipelineStage.DM2_SENT.value: 2,
    PipelineStage.DM3_SENT.value: 3,
}


def _make_entry(entry_id: str, record_id: str, stage: str, dm_step: int | None = None) -> dict:
    """Build a minimal Attio list entry dict as returned by parse_entry.

    `dm_step` defaults to the value consistent with `stage` (DM1_SENT → 1, etc.)
    so the Phase 0.5 auto-repair doesn't fire spurious corrections during tests.
    Override explicitly to simulate the drift cases.
    """
    if dm_step is None:
        dm_step = _STAGE_TO_DEFAULT_DM_STEP.get(stage)
    return {
        "entry_id": entry_id,
        "record_id": record_id,
        "stage": stage,
        "persona": "operations_leaders",
        "language": "en",
        "dm_step": dm_step,
        "quality_score": 75,
        "last_contact_date": "2026-04-01",
        "experiment_id": None,
    }


def _make_sn_csv(*rows: dict) -> str:
    """Build an SN Inbox Scraper CSV string."""
    if not rows:
        return ""
    fieldnames = [
        "participantProfileUrl", "participantFullName",
        "isLastMessageFromMe", "lastMessageBody", "lastMessageDate",
        "totalMessageCount",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k, "") for k in fieldnames})
    return buf.getvalue()


def _setup_mocks(attio, pb, entries, csv_content, name_map):
    """Common mock setup for detect_responses tests.

    name_map: dict of record_id → (name, company, linkedin_url)
    """
    attio.query_list_entries.return_value = entries
    pb.download_result_csv.return_value = csv_content

    mock_cache = MagicMock()
    mock_cache.get.side_effect = lambda rid: name_map.get(rid, ("", "", "", "", ""))

    return mock_cache


# ── Test: normalize_name ─────────────────────────────────────────────

class TestNormalizeName:
    def test_basic(self):
        assert _normalize_name("Carlos Lopez") == "carlos lopez"

    def test_extra_spaces(self):
        assert _normalize_name("  Carlos   Lopez  ") == "carlos lopez"

    def test_mixed_case(self):
        assert _normalize_name("VANESSA Solis") == "vanessa solis"


# ── Test: skip when no inbox_scraper_id ───────────────────────────────

class TestSkipWhenNoId:
    def test_returns_skipped_flag(self):
        result = detect_responses(MagicMock(), MagicMock(), inbox_scraper_id=None)
        assert result == {"skipped": True, "reason": "no_inbox_scraper_id"}

    def test_no_attio_calls_made(self):
        attio = MagicMock()
        pb = MagicMock()
        detect_responses(attio, pb, inbox_scraper_id=None)
        attio.query_list_entries.assert_not_called()
        pb.launch_agent.assert_not_called()

    def test_empty_string_id_also_skips(self):
        result = detect_responses(MagicMock(), MagicMock(), inbox_scraper_id="")
        assert result == {"skipped": True, "reason": "no_inbox_scraper_id"}


# ── Test: no prospects in DM stages ───────────────────────────────────

class TestNoDmProspects:
    def test_returns_zero_counts(self):
        attio = MagicMock()
        pb = MagicMock()

        prospect_entry = _make_entry("e1", "r1", PipelineStage.PROSPECT.value)
        attio.query_list_entries.return_value = [prospect_entry]

        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses.RecordCache") as MockCache:
            mock_cache_instance = MagicMock()
            mock_cache_instance.get.return_value = ("Test User", "TestCo", "https://linkedin.com/in/test", "", "")
            MockCache.return_value = mock_cache_instance

            result = detect_responses(attio, pb, inbox_scraper_id="scraper-123")

        pb.launch_agent.assert_not_called()
        assert result["detected"] == 0


# ── Test: idempotency ────────────────────────────────────────────────

class TestIdempotency:
    def test_skips_already_responded_entry(self):
        attio = MagicMock()
        pb = MagicMock()

        # Entry is already RESPONDED — should not be in dm_prospects
        responded_entry = _make_entry("e1", "r1", PipelineStage.RESPONDED.value)
        attio.query_list_entries.return_value = [responded_entry]

        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses.RecordCache") as MockCache:
            mock_cache_instance = MagicMock()
            mock_cache_instance.get.return_value = ("Carlos Lopez", "Acme", "https://linkedin.com/in/carlos", "", "")
            MockCache.return_value = mock_cache_instance

            result = detect_responses(attio, pb, inbox_scraper_id="scraper-123")

        # Not in DM stages → no prospects to check → no PB launch
        attio.update_list_entry.assert_not_called()
        assert result["detected"] == 0

    def test_skips_not_interested_entry(self):
        attio = MagicMock()
        pb = MagicMock()

        ni_entry = _make_entry("e2", "r2", PipelineStage.NOT_INTERESTED.value)
        attio.query_list_entries.return_value = [ni_entry]

        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses.RecordCache") as MockCache:
            mock_cache_instance = MagicMock()
            mock_cache_instance.get.return_value = ("Maria Gomez", "Corp", "https://linkedin.com/in/maria", "", "")
            MockCache.return_value = mock_cache_instance

            result = detect_responses(attio, pb, inbox_scraper_id="scraper-123")

        attio.update_list_entry.assert_not_called()
        assert result["detected"] == 0


# ── Test: classification routing ──────────────────────────────────────

class TestClassificationRouting:
    def _run_with_response(self, message_body, prospect_name="Test User", stage=PipelineStage.DM1_SENT.value):
        """Run detect_responses with one SN Inbox Scraper CSV row containing a reply."""
        attio = MagicMock()
        pb = MagicMock()

        entry = _make_entry("e10", "r10", stage)
        attio.query_list_entries.return_value = [entry]

        csv_content = _make_sn_csv({
            "participantProfileUrl": "https://linkedin.com/in/test",
            "participantFullName": prospect_name,
            "isLastMessageFromMe": "false",
            "lastMessageBody": message_body,
            "lastMessageDate": "2026-04-09T12:00:00Z",
        })
        pb.download_result_csv.return_value = csv_content

        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses._pb_session_args", return_value={}), \
             patch("workflows.detect_responses.RecordCache") as MockCache:
            mock_cache_instance = MagicMock()
            mock_cache_instance.get.return_value = (prospect_name, "TestCo", "https://linkedin.com/in/test", "", "")
            MockCache.return_value = mock_cache_instance

            result = detect_responses(attio, pb, inbox_scraper_id="scraper-123")

        return result, attio

    def test_negative_response_moves_to_not_interested(self):
        result, attio = self._run_with_response("No thanks, not interested.")
        assert result["detected"] == 1
        assert result["negative"] == 1
        call_kwargs = attio.update_list_entry.call_args
        assert call_kwargs[1]["entry_attributes"]["stage"] == PipelineStage.NOT_INTERESTED.value

    def test_positive_response_moves_to_responded(self):
        result, attio = self._run_with_response("Interested! Let's schedule a demo.")
        assert result["detected"] == 1
        assert result["positive"] == 1
        call_kwargs = attio.update_list_entry.call_args
        assert call_kwargs[1]["entry_attributes"]["stage"] == PipelineStage.RESPONDED.value

    def test_question_response_moves_to_responded(self):
        result, attio = self._run_with_response("How much does it cost?")
        assert result["detected"] == 1
        assert result["question"] == 1
        call_kwargs = attio.update_list_entry.call_args
        assert call_kwargs[1]["entry_attributes"]["stage"] == PipelineStage.RESPONDED.value

    def test_neutral_response_moves_to_responded(self):
        result, attio = self._run_with_response("Thanks for reaching out.")
        assert result["detected"] == 1
        assert result["neutral"] == 1
        call_kwargs = attio.update_list_entry.call_args
        assert call_kwargs[1]["entry_attributes"]["stage"] == PipelineStage.RESPONDED.value

    def test_note_is_created_on_response(self):
        _, attio = self._run_with_response("Interested! Let's talk.")
        attio.create_note.assert_called_once()
        note_call = attio.create_note.call_args
        assert "positive" in note_call[1]["title"]
        assert "Interested" in note_call[1]["content"]


# ── Test: isLastMessageFromMe / manual-reply detection ────────────────

class TestFromMeFiltering:
    """When isLastMessageFromMe is 'true', `totalMessageCount` disambiguates.

    Two scenarios share `is_from_me=true`:
      - Just our automated DMs in the thread, no reply yet → totalMessageCount
        equals the count of DMs sent at this stage → skip.
      - Prospect replied AND we replied back manually → totalMessageCount
        strictly exceeds expected → flag as RESPONDED for human triage,
        since the SN Inbox Scraper only exposes the last message body
        (which is ours, not the prospect's reply).
    """

    def _run(self, *, stage: str, total_message_count: str, last_message_from_me: str = "true"):
        attio = MagicMock()
        pb = MagicMock()

        entry = _make_entry("e20", "r20", stage)
        attio.query_list_entries.return_value = [entry]

        csv_content = _make_sn_csv({
            "participantProfileUrl": "https://linkedin.com/in/vanessa",
            "participantFullName": "Vanessa Solis",
            "isLastMessageFromMe": last_message_from_me,
            "lastMessageBody": "Gracias por tu respuesta, saludos!",
            "lastMessageDate": "2026-04-26T13:58:00Z",
            "totalMessageCount": total_message_count,
        })
        pb.download_result_csv.return_value = csv_content

        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses._pb_session_args", return_value={}), \
             patch("workflows.detect_responses.RecordCache") as MockCache:
            mock_cache_instance = MagicMock()
            mock_cache_instance.get.return_value = ("Vanessa Solis", "Acme", "https://linkedin.com/in/vanessa", "", "")
            MockCache.return_value = mock_cache_instance

            result = detect_responses(attio, pb, inbox_scraper_id="scraper-123")

        return result, attio

    def test_skip_when_only_our_dm1_in_thread(self):
        """DM1_SENT stage, total=1 → just our DM1, no reply yet."""
        result, attio = self._run(
            stage=PipelineStage.DM1_SENT.value,
            total_message_count="1",
        )
        attio.update_list_entry.assert_not_called()
        assert result["detected"] == 0

    def test_skip_when_only_our_dm1_dm2_in_thread(self):
        """DM2_SENT stage, total=2 → just our DM1+DM2, no reply yet."""
        result, attio = self._run(
            stage=PipelineStage.DM2_SENT.value,
            total_message_count="2",
        )
        attio.update_list_entry.assert_not_called()
        assert result["detected"] == 0

    def test_manual_reply_detected_at_dm1_stage(self):
        """The Sebastián case: DM1_SENT stage, total=3 (us, them, us-manual).

        Prospect replied and the operator replied back manually before the daily run
        could classify their reply. SN Scraper sees only the operator's last message.
        """
        result, attio = self._run(
            stage=PipelineStage.DM1_SENT.value,
            total_message_count="3",
        )
        attio.update_list_entry.assert_called_once()
        call_kwargs = attio.update_list_entry.call_args
        assert call_kwargs[1]["entry_attributes"]["stage"] == PipelineStage.RESPONDED.value
        attio.create_note.assert_called_once()
        note_call = attio.create_note.call_args
        assert "Manual reply" in note_call[1]["title"]
        assert result["detected"] == 1

    def test_manual_reply_detected_at_dm2_stage(self):
        """DM2_SENT stage, total=4 (us, them, us-DM2, them, us-manual)."""
        result, attio = self._run(
            stage=PipelineStage.DM2_SENT.value,
            total_message_count="4",
        )
        attio.update_list_entry.assert_called_once()
        call_kwargs = attio.update_list_entry.call_args
        assert call_kwargs[1]["entry_attributes"]["stage"] == PipelineStage.RESPONDED.value
        assert result["detected"] == 1

    def test_skip_when_total_message_count_missing(self):
        """Missing totalMessageCount → can't disambiguate → fall back to skip."""
        result, attio = self._run(
            stage=PipelineStage.DM1_SENT.value,
            total_message_count="",
        )
        attio.update_list_entry.assert_not_called()
        assert result["detected"] == 0

    def test_skip_when_total_message_count_garbage(self):
        """Non-numeric totalMessageCount → fall back to skip (treated as 0)."""
        result, attio = self._run(
            stage=PipelineStage.DM1_SENT.value,
            total_message_count="N/A",
        )
        attio.update_list_entry.assert_not_called()
        assert result["detected"] == 0


# ── Test: name matching ──────────────────────────────────────────────

class TestNameMatching:
    def test_unmatched_name_is_ignored(self):
        """CSV has a name that doesn't match any DM prospect."""
        attio = MagicMock()
        pb = MagicMock()

        entry = _make_entry("e40", "r40", PipelineStage.DM1_SENT.value)
        attio.query_list_entries.return_value = [entry]

        csv_content = _make_sn_csv({
            "participantProfileUrl": "https://linkedin.com/in/carlos",
            "participantFullName": "Unknown Person",
            "isLastMessageFromMe": "false",
            "lastMessageBody": "Interested!",
            "lastMessageDate": "2026-04-09T12:00:00Z",
        })
        pb.download_result_csv.return_value = csv_content

        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses._pb_session_args", return_value={}), \
             patch("workflows.detect_responses.RecordCache") as MockCache:
            mock_cache_instance = MagicMock()
            mock_cache_instance.get.return_value = ("Known User", "KnownCo", "https://linkedin.com/in/known", "", "")
            MockCache.return_value = mock_cache_instance

            result = detect_responses(attio, pb, inbox_scraper_id="scraper-123")

        attio.update_list_entry.assert_not_called()
        assert result["detected"] == 0

    def test_case_insensitive_name_match(self):
        """Name matching should be case-insensitive."""
        attio = MagicMock()
        pb = MagicMock()

        entry = _make_entry("e41", "r41", PipelineStage.DM1_SENT.value)
        attio.query_list_entries.return_value = [entry]

        csv_content = _make_sn_csv({
            "participantProfileUrl": "https://linkedin.com/in/carlos",
            "participantFullName": "CARLOS LOPEZ",  # uppercase in CSV
            "isLastMessageFromMe": "false",
            "lastMessageBody": "Interested! Let's talk.",
            "lastMessageDate": "2026-04-09T12:00:00Z",
        })
        pb.download_result_csv.return_value = csv_content

        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses._pb_session_args", return_value={}), \
             patch("workflows.detect_responses.RecordCache") as MockCache:
            mock_cache_instance = MagicMock()
            # Attio stores as mixed case
            mock_cache_instance.get.return_value = ("Carlos Lopez", "Acme", "https://linkedin.com/in/carlos", "", "")
            MockCache.return_value = mock_cache_instance

            result = detect_responses(attio, pb, inbox_scraper_id="scraper-123")

        assert result["detected"] == 1
        attio.update_list_entry.assert_called_once()


# ── Test: name collision (two distinct prospects share a normalized name) ──

class TestNameCollision:
    """In a LATAM list, common names (e.g. two 'Carlos Ejemplo' from different
    companies, or two duplicate Attio records for one person) all map to the
    same normalized name. A reply matched on that name must update *every*
    matched entry — the previous dict-overwrite implementation silently lost
    one of them and left them stuck in DM stage indefinitely.
    """

    def test_collision_updates_only_verified_profile(self):
        attio = MagicMock()
        pb = MagicMock()

        # Two distinct entries with the same normalized name
        entry_a = _make_entry("e-collide-a", "r-collide-a", PipelineStage.DM1_SENT.value)
        entry_b = _make_entry("e-collide-b", "r-collide-b", PipelineStage.DM2_SENT.value)
        attio.query_list_entries.return_value = [entry_a, entry_b]

        csv_content = _make_sn_csv({
            "participantProfileUrl": "https://linkedin.com/in/carlos-cemex",
            "participantFullName": "Carlos Ejemplo",
            "isLastMessageFromMe": "false",
            "lastMessageBody": "No me interesa, gracias.",
            "lastMessageDate": "2026-04-09T12:00:00Z",
        })
        pb.download_result_csv.return_value = csv_content

        def cache_side_effect(rid):
            if rid == "r-collide-a":
                return ("Carlos Ejemplo", "Cementra", "https://linkedin.com/in/carlos-cemex", "", "")
            return ("Carlos Ejemplo", "Bimbo", "https://linkedin.com/in/carlos-other-company", "", "")

        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses._pb_session_args", return_value={}), \
             patch("workflows.detect_responses.RecordCache") as MockCache:
            mock_cache_instance = MagicMock()
            mock_cache_instance.get.side_effect = cache_side_effect
            MockCache.return_value = mock_cache_instance

            result = detect_responses(attio, pb, inbox_scraper_id="scraper-collide")

        # Detected once (one inbox row matched).
        assert result["detected"] == 1
        # Only the verified profile is updated.
        assert attio.update_list_entry.call_count == 1
        updated_entry_ids = {
            call.kwargs.get("entry_id") for call in attio.update_list_entry.call_args_list
        }
        assert updated_entry_ids == {"e-collide-a"}

    def test_collision_with_one_terminal_entry_only_updates_actionable(self):
        """If one of the colliding entries is already in a terminal stage
        (RESPONDED/NOT_INTERESTED), it should be skipped — only actionable
        entries are touched.
        """
        attio = MagicMock()
        pb = MagicMock()

        entry_actionable = _make_entry("e-act", "r-act", PipelineStage.DM2_SENT.value)
        entry_terminal = _make_entry("e-term", "r-term", PipelineStage.RESPONDED.value)
        attio.query_list_entries.return_value = [entry_actionable, entry_terminal]

        csv_content = _make_sn_csv({
            "participantProfileUrl": "https://linkedin.com/in/r-act",
            "participantFullName": "Maria Gonzalez",
            "isLastMessageFromMe": "false",
            "lastMessageBody": "Sounds good, let's talk.",
            "lastMessageDate": "2026-04-09T12:00:00Z",
        })
        pb.download_result_csv.return_value = csv_content

        def cache_side_effect(rid):
            return ("Maria Gonzalez", "AcmeCo", f"https://linkedin.com/in/{rid}", "", "")

        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses._pb_session_args", return_value={}), \
             patch("workflows.detect_responses.RecordCache") as MockCache:
            mock_cache_instance = MagicMock()
            mock_cache_instance.get.side_effect = cache_side_effect
            MockCache.return_value = mock_cache_instance

            detect_responses(attio, pb, inbox_scraper_id="scraper-mixed")

        # Only the actionable entry was updated; terminal one was skipped.
        attio.update_list_entry.assert_called_once()
        assert attio.update_list_entry.call_args.kwargs.get("entry_id") == "e-act"


# ── Cadence drift detector ─────────────────────────────────────────────

class TestCadenceDriftDetector:
    """Unit tests for _detect_cadence_drift."""

    def test_terminal_stage_dm_step_low_flagged(self):
        """Responded prospect with dm_step=1 but thread shows 3 outbound from us → drift."""
        from workflows.detect_responses import _detect_cadence_drift
        threads = [{
            "participantProfileUrl": "https://linkedin.com/in/cadence-test",
            "participantFullName": "Alberto Cavia",
            "totalMessageCount": "4",
            "isLastMessageFromMe": "true",
        }]
        pipeline = {
            _normalize_name("Alberto Cavia"): [{
                "stage": PipelineStage.RESPONDED.value,
                "dm_step": 1,
                "company_name": "Conagra",
                "linkedin_url": "https://linkedin.com/in/cadence-test",
                "record_id": "r1",
            }],
        }
        drifts = _detect_cadence_drift(threads, pipeline)
        assert len(drifts) == 1
        assert drifts[0]["kind"] == "terminal_dm_step_low"
        assert drifts[0]["inferred_dm_step"] == 3

    def test_terminal_stage_with_correct_dm_step_no_drift(self):
        """Responded prospect with dm_step matching thread → no drift."""
        from workflows.detect_responses import _detect_cadence_drift
        threads = [{
            "participantProfileUrl": "https://linkedin.com/in/cadence-test",
            "participantFullName": "Bob Bob",
            "totalMessageCount": "2",
            "isLastMessageFromMe": "false",
        }]
        pipeline = {
            _normalize_name("Bob Bob"): [{
                "stage": PipelineStage.RESPONDED.value,
                "dm_step": 1,
                "company_name": "X",
                "linkedin_url": "https://linkedin.com/in/cadence-test",
                "record_id": "r2",
            }],
        }
        drifts = _detect_cadence_drift(threads, pipeline)
        assert drifts == []

    def test_terminal_stage_stage_never_flipped(self):
        """NOT_INTERESTED stays terminal even when thread shows full outbound cadence."""
        from workflows.detect_responses import _detect_cadence_drift
        threads = [{
            "participantProfileUrl": "https://linkedin.com/in/cadence-test",
            "participantFullName": "Patricia Ejemplo",
            "totalMessageCount": "3",
            "isLastMessageFromMe": "true",
        }]
        pipeline = {
            _normalize_name("Patricia Ejemplo"): [{
                "stage": PipelineStage.NOT_INTERESTED.value,
                "dm_step": 0,
                "company_name": "Novo Nordisk",
                "linkedin_url": "https://linkedin.com/in/cadence-test",
                "record_id": "r3",
            }],
        }
        drifts = _detect_cadence_drift(threads, pipeline)
        # dm_step drift flagged but stage NOT one of the non-terminal kinds.
        assert len(drifts) == 1
        assert drifts[0]["kind"] == "terminal_dm_step_low"
        assert drifts[0]["current_stage"] == PipelineStage.NOT_INTERESTED.value

    def test_unreachable_stage_not_unparked_by_drift(self):
        """Wave-2-A: UNREACHABLE is terminal — a matching thread must NOT
        un-park the row back toward an active stage. Because UNREACHABLE is
        in _TERMINAL_STAGES_FOR_DRIFT, it takes the terminal branch (kind
        `terminal_dm_step_low`, same as NOT_INTERESTED) whose inferred_stage
        stays UNREACHABLE — so an auto-repair is a stage no-op, never a flip
        to a DM/RESPONDED stage. (Regression guard: drop UNREACHABLE from
        that set and this row would infer an active stage and get un-parked.)"""
        from workflows.detect_responses import _detect_cadence_drift
        threads = [{
            "participantProfileUrl": "https://linkedin.com/in/cadence-test",
            "participantFullName": "Daniel Mayorga",
            "totalMessageCount": "3",
            "isLastMessageFromMe": "true",
        }]
        pipeline = {
            _normalize_name("Daniel Mayorga"): [{
                "stage": PipelineStage.UNREACHABLE.value,
                "dm_step": 0,
                "company_name": "Nissan",
                "linkedin_url": "https://linkedin.com/in/cadence-test",
                "record_id": "r-unreach",
            }],
        }
        drifts = _detect_cadence_drift(threads, pipeline)
        assert len(drifts) == 1
        assert drifts[0]["current_stage"] == PipelineStage.UNREACHABLE.value
        assert drifts[0]["kind"] == "terminal_dm_step_low"
        # The crux: the row is NOT inferred onto an active stage — it stays
        # UNREACHABLE, so no repair can un-park it.
        assert drifts[0]["inferred_stage"] == PipelineStage.UNREACHABLE.value

    def test_non_terminal_attio_underestimates(self):
        """Accepted prospect with dm_step=0 but thread shows 2 outbound from us → drift up."""
        from workflows.detect_responses import _detect_cadence_drift
        threads = [{
            "participantProfileUrl": "https://linkedin.com/in/cadence-test",
            "participantFullName": "Rafael Ejemplo",
            "totalMessageCount": "2",
            "isLastMessageFromMe": "true",
        }]
        pipeline = {
            _normalize_name("Rafael Ejemplo"): [{
                "stage": PipelineStage.ACCEPTED.value,
                "dm_step": 0,
                "company_name": "FarmaSur",
                "linkedin_url": "https://linkedin.com/in/cadence-test",
                "record_id": "r4",
            }],
        }
        drifts = _detect_cadence_drift(threads, pipeline)
        assert len(drifts) == 1
        assert drifts[0]["kind"] == "attio_dm_step_lower_than_thread"
        assert drifts[0]["inferred_dm_step"] == 2

    def test_attio_overstates_empty_thread(self):
        """DM1 Sent in Attio but no outbound messages exist → drift down."""
        from workflows.detect_responses import _detect_cadence_drift
        threads = [{
            "participantProfileUrl": "https://linkedin.com/in/cadence-test",
            "participantFullName": "Phantom Send",
            "totalMessageCount": "0",
            "isLastMessageFromMe": "false",
        }]
        pipeline = {
            _normalize_name("Phantom Send"): [{
                "stage": PipelineStage.DM1_SENT.value,
                "dm_step": 1,
                "company_name": "PhantomCo",
                "linkedin_url": "https://linkedin.com/in/cadence-test",
                "record_id": "r5",
            }],
        }
        drifts = _detect_cadence_drift(threads, pipeline)
        assert len(drifts) == 1
        assert drifts[0]["kind"] == "attio_overstates_thread_empty"

    def test_no_matching_pipeline_entry_no_drift(self):
        """Inbox thread for a non-pipeline contact → no false positive."""
        from workflows.detect_responses import _detect_cadence_drift
        threads = [{
            "participantProfileUrl": "https://linkedin.com/in/cadence-test",
            "participantFullName": "Random Person",
            "totalMessageCount": "5",
            "isLastMessageFromMe": "true",
        }]
        drifts = _detect_cadence_drift(threads, {})
        assert drifts == []

    def test_drift_records_carry_entry_id_and_inferred_stage(self):
        """Drift dicts must include entry_id and inferred_stage so the auto-
        repair can write to Attio without re-deriving them."""
        from workflows.detect_responses import _detect_cadence_drift
        threads = [{
            "participantProfileUrl": "https://linkedin.com/in/cadence-test",
            "participantFullName": "Arianne Sales",
            "totalMessageCount": "5",
            "isLastMessageFromMe": "true",
        }]
        pipeline = {
            _normalize_name("Arianne Sales"): [{
                "stage": PipelineStage.DM1_SENT.value,
                "dm_step": 1,
                "company_name": "Stellantis",
                "linkedin_url": "https://linkedin.com/in/cadence-test",
                "record_id": "r-arianne",
                "entry_id": "e-arianne",
            }],
        }
        drifts = _detect_cadence_drift(threads, pipeline)
        assert len(drifts) == 1
        d = drifts[0]
        assert d["entry_id"] == "e-arianne"
        assert d["inferred_stage"] == PipelineStage.DM3_SENT.value
        assert d["inferred_dm_step"] == 3


# ── Cadence auto-repair ────────────────────────────────────────────────

class TestCadenceAutoRepair:
    """Unit tests for _apply_cadence_repairs (Phase 0.5 auto-repair)."""

    def _drift(self, **overrides) -> dict:
        base = {
            "kind": "attio_dm_step_lower_than_thread",
            "name": "Arianne Sales",
            "company": "Stellantis",
            "current_stage": PipelineStage.DM1_SENT.value,
            "current_dm_step": 1,
            "thread_total": 5,
            "thread_last_from_me": True,
            "inferred_stage": PipelineStage.DM3_SENT.value,
            "inferred_dm_step": 3,
            "linkedin_url": "https://linkedin.com/in/cadence-test",
                "record_id": "r-arianne",
            "entry_id": "e-arianne",
        }
        base.update(overrides)
        return base

    def test_applies_regressed_drift(self):
        from workflows.detect_responses import _apply_cadence_repairs
        attio = MagicMock()
        applied, failed = _apply_cadence_repairs(
            attio, list_id="L1", drifts=[self._drift()], today_iso="2026-05-20",
        )
        assert (applied, failed) == (1, 0)
        attio.update_list_entry.assert_called_once()
        call = attio.update_list_entry.call_args
        assert call.kwargs["entry_attributes"] == {
            "stage": PipelineStage.DM3_SENT.value,
            "dm_step": 3,
            "last_contact_date": "2026-05-20",
        }
        attio.create_note.assert_called_once()
        assert "Cadence auto-repair" in attio.create_note.call_args.kwargs["title"]

    def test_skips_advisory_kind_overstated(self):
        """attio_dm_step_higher_than_thread is never auto-repaired."""
        from workflows.detect_responses import _apply_cadence_repairs
        attio = MagicMock()
        applied, failed = _apply_cadence_repairs(
            attio, list_id="L1",
            drifts=[self._drift(kind="attio_dm_step_higher_than_thread")],
            today_iso="2026-05-20",
        )
        assert (applied, failed) == (0, 0)
        attio.update_list_entry.assert_not_called()

    def test_skips_advisory_kind_empty_thread(self):
        """attio_overstates_thread_empty is never auto-repaired."""
        from workflows.detect_responses import _apply_cadence_repairs
        attio = MagicMock()
        applied, failed = _apply_cadence_repairs(
            attio, list_id="L1",
            drifts=[self._drift(kind="attio_overstates_thread_empty")],
            today_iso="2026-05-20",
        )
        assert (applied, failed) == (0, 0)
        attio.update_list_entry.assert_not_called()

    def test_applies_terminal_dm_step_low(self):
        """terminal_dm_step_low IS auto-applied (dm_step monotonic-forward, stage stays)."""
        from workflows.detect_responses import _apply_cadence_repairs
        attio = MagicMock()
        d = self._drift(
            kind="terminal_dm_step_low",
            current_stage=PipelineStage.RESPONDED.value,
            inferred_stage=PipelineStage.RESPONDED.value,  # sticky
        )
        applied, failed = _apply_cadence_repairs(
            attio, list_id="L1", drifts=[d], today_iso="2026-05-20",
        )
        assert (applied, failed) == (1, 0)
        call = attio.update_list_entry.call_args
        assert call.kwargs["entry_attributes"]["stage"] == PipelineStage.RESPONDED.value

    def test_missing_entry_id_skipped(self):
        """Defensively skip drift records without entry_id (shouldn't happen in prod)."""
        from workflows.detect_responses import _apply_cadence_repairs
        attio = MagicMock()
        applied, failed = _apply_cadence_repairs(
            attio, list_id="L1",
            drifts=[self._drift(entry_id=None)],
            today_iso="2026-05-20",
        )
        assert (applied, failed) == (0, 0)
        attio.update_list_entry.assert_not_called()

    def test_failure_counted_does_not_crash(self):
        """One failed write doesn't abort the rest."""
        import httpx

        from workflows.detect_responses import _apply_cadence_repairs
        attio = MagicMock()
        # Only httpx-class exceptions are caught now (post-QA fold-in:
        # broad `except Exception` was a §3 #9 silent-failure risk).
        attio.update_list_entry.side_effect = [
            httpx.RequestError("attio down"),
            None,
        ]
        applied, failed = _apply_cadence_repairs(
            attio, list_id="L1",
            drifts=[
                self._drift(entry_id="e1", record_id="r1"),
                self._drift(entry_id="e2", record_id="r2"),
            ],
            today_iso="2026-05-20",
        )
        assert (applied, failed) == (1, 1)

    def test_update_succeeds_note_fails_counts_as_applied(self):
        """Partial-success path: state-write OK, audit-note fails.

        Pre-QA-fold-in the entry was counted as 'failed' even though
        Attio state was already modified — destroyed audit symmetry
        because the entry's stage/dm_step had advanced but the
        forensics note was missing. Post-fold-in: applied=1, failed=0;
        the audit-note gap is surfaced to stderr but does NOT flip
        the counter. Mirrors the silent-failure-hunter BLOCKER and
        the code-reviewer BLOCKER from QA round 1.
        """
        import httpx

        from workflows.detect_responses import _apply_cadence_repairs
        attio = MagicMock()
        attio.update_list_entry.return_value = None  # state-write OK
        attio.create_note.side_effect = httpx.RequestError("note quota")
        applied, failed = _apply_cadence_repairs(
            attio, list_id="L1", drifts=[self._drift()], today_iso="2026-05-20",
        )
        # State was correctly repaired; don't flip applied → failed.
        # A re-run next day would re-attempt the audit note but the
        # state-write would now be a no-op (idempotent by current_stage
        # already matching inferred_stage post-repair).
        assert (applied, failed) == (1, 0)
        attio.update_list_entry.assert_called_once()
        attio.create_note.assert_called_once()

    def test_non_httpx_exception_propagates(self):
        """Non-httpx exceptions are NOT silently counted as failed.

        Per QA-fold-in §3 #9 + silent-failure-hunter BLOCKER #2: the
        previous bare `except Exception` would swallow KeyError /
        AttributeError / future code-bug exceptions and mask them
        as 'failed repair'. Narrowed catch only includes httpx errors.
        """
        from workflows.detect_responses import _apply_cadence_repairs
        attio = MagicMock()
        attio.update_list_entry.side_effect = AttributeError("bug")
        try:
            _apply_cadence_repairs(
                attio, list_id="L1", drifts=[self._drift()],
                today_iso="2026-05-20",
            )
        except AttributeError as e:
            assert "bug" in str(e)
        else:
            raise AssertionError("AttributeError should have propagated")

    def test_malformed_drift_dict_counted_failed_not_raised(self):
        """Missing required keys → counted as failed with explicit message.

        Per QA-fold-in: drift dicts missing required keys (e.g. from
        test scaffolding or future _detect_cadence_drift refactor
        regression) used to raise KeyError inside the try block and
        be silently caught as a generic 'failed' repair. Post-fold-in:
        explicit guard fails fast with the missing-keys list logged.
        """
        from workflows.detect_responses import _apply_cadence_repairs
        attio = MagicMock()
        malformed = self._drift()
        del malformed["inferred_stage"]
        del malformed["thread_total"]
        applied, failed = _apply_cadence_repairs(
            attio, list_id="L1", drifts=[malformed], today_iso="2026-05-20",
        )
        assert (applied, failed) == (0, 1)
        # The state-write should NEVER have been attempted on a
        # malformed dict — that's the whole point of the guard.
        attio.update_list_entry.assert_not_called()


# ── End-to-end: auto-repair integration in detect_responses ───────────

class TestAutoRepairIntegration:
    """Integration tests: auto-repair triggers correctly from detect_responses."""

    def test_classifier_touched_prospects_skip_auto_repair(self):
        """When the response classifier just moved a prospect to RESPONDED,
        the cadence auto-repair must NOT overwrite that decision with the
        DM-cadence-inferred stage."""
        attio = MagicMock()
        pb = MagicMock()
        attio.query_list_entries.return_value = [
            _make_entry("e1", "r1", PipelineStage.DM1_SENT.value),
        ]
        # Manual-reply signature: stage=DM1, total=3, last from us → classifier
        # moves to RESPONDED. Drift detector would also see this as drift
        # (inferred_stage=DM3_SENT) but should defer to classifier.
        pb.download_result_csv.return_value = _make_sn_csv({
            "participantProfileUrl": "https://linkedin.com/in/test",
            "participantFullName": "Manual Reply Case",
            "isLastMessageFromMe": "true",
            "lastMessageBody": "Hola, gracias por escribir.",
            "lastMessageDate": "2026-04-09T12:00:00Z",
            "totalMessageCount": "3",
        })
        mock_cache = MagicMock()
        mock_cache.get.side_effect = (
            lambda rid: ("Manual Reply Case", "Co", "https://linkedin.com/in/test", "", "Title")
            if rid == "r1" else ("", "", "", "", "")
        )
        # parse_entry pass-through: test fixture entries are already in
        # the parsed-attrs shape.
        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses._pb_session_args", return_value={}):
            result = detect_responses(attio, pb, "inbox_id", cache=mock_cache)
        # Classifier should have moved to RESPONDED (one call).
        # Auto-repair must NOT have run a SECOND call with stage=DM3_SENT.
        assert attio.update_list_entry.call_count == 1
        call = attio.update_list_entry.call_args
        assert call.kwargs["entry_attributes"]["stage"] == PipelineStage.RESPONDED.value
        assert result["drift_auto_repaired"] == 0
        assert result["detected"] == 1


class TestSelfEchoGuard:
    """`_looks_like_self_echo` distinguishes our own DM echoed back (the
    PR-241 dup-DM1 case) from a genuine reply. Pure, module-level."""

    def test_own_template_matches(self):
        from models.campaign import load_messages
        from workflows.detect_responses import _looks_like_self_echo
        template = load_messages()["operations_leaders"]["dm1"]["es"]
        echoed = template.replace("[Name]", "René").replace("[Company]", "Acme Foods")
        assert _looks_like_self_echo(echoed) is not None

    def test_genuine_short_reply_does_not_match(self):
        from workflows.detect_responses import _looks_like_self_echo
        assert _looks_like_self_echo("Gracias René, me interesa") is None

    def test_genuine_longer_reply_does_not_match(self):
        from workflows.detect_responses import _looks_like_self_echo
        assert _looks_like_self_echo(
            "Hola, gracias por escribir. Me interesa saber más, "
            "cuándo podemos agendar una llamada?"
        ) is None

    def test_empty_body_returns_none(self):
        from workflows.detect_responses import _looks_like_self_echo
        assert _looks_like_self_echo("") is None

    def test_template_load_failure_fails_open(self, monkeypatch, capsys):
        # A missing/corrupt messages.json must NOT crash all of
        # detect_responses. The guard hoists + caches the template token sets;
        # on load failure it warns loudly ONCE and fails OPEN (returns None) so
        # manual-reply flips proceed without the self-echo check this run
        # rather than the whole run crashing.
        import workflows.detect_responses as dr

        def _boom():
            raise FileNotFoundError("messages.json missing")

        dr._reset_self_echo_template_cache()
        monkeypatch.setattr(dr, "load_messages", _boom, raising=False)
        monkeypatch.setattr("models.campaign.load_messages", _boom)
        try:
            # A body that WOULD match a template if templates loaded — but the
            # load fails, so the guard fails open and returns None.
            assert dr._looks_like_self_echo("hola rene me interesa mucho") is None
            out = capsys.readouterr()
            combined = out.out + out.err
            assert "self-echo guard offline" in combined
            # Second call must NOT re-warn (warn-once per run).
            assert dr._looks_like_self_echo("otra vez") is None
            out2 = capsys.readouterr()
            assert "self-echo guard offline" not in (out2.out + out2.err)
        finally:
            dr._reset_self_echo_template_cache()


# ── Test: manual-DM touch detection (Phase 0.5) ───────────────────────

@pytest.fixture(autouse=True)
def _isolate_manual_touch_state(tmp_path, monkeypatch):
    """Never let a test write the real exports/manual_touch_state.json."""
    monkeypatch.setattr(
        "workflows.detect_responses.MANUAL_TOUCH_STATE_PATH",
        tmp_path / "manual_touch_state.json",
    )


class TestManualTouchDetection:
    """`_detect_manual_touches` stamps `last_contact_date` (to the row's
    lastMessageDate) + a "DM manual" note when the inbox scrape shows OUR
    last message on a RESPONDED entry, the body is not one of our
    templates, and the (count|body) fingerprint is new for that entry.
    Local JSON state keeps the same touch from re-recording on later runs.
    """

    NAME = "Vanessa Solis"
    BODY = "Hola Vanessa, te dejo la propuesta que comentamos, cualquier duda me dices."
    ROW_DATE = "2026-09-01T13:58:00Z"

    def _row(self, *, body=None, from_me="true", name=None, count="5", row_date=None) -> dict:
        return {
            "participantProfileUrl": "https://linkedin.com/in/vanessa",
            "participantFullName": self.NAME if name is None else name,
            "isLastMessageFromMe": from_me,
            "lastMessageBody": self.BODY if body is None else body,
            "lastMessageDate": self.ROW_DATE if row_date is None else row_date,
            "totalMessageCount": count,
        }

    def _entry(self, stage=PipelineStage.RESPONDED.value, entry_id="e-resp",
               record_id="r-resp", last_contact="2026-04-01", merged_into=None) -> dict:
        e = _make_entry(entry_id, record_id, stage, dm_step=3)
        e["prospect_name"] = self.NAME
        e["linkedin_url"] = "https://linkedin.com/in/vanessa"
        e["company_name"] = "Acme"
        e["last_contact_date"] = last_contact
        e["merged_into"] = merged_into
        return e

    def _run(self, tmp_path, *, rows, entries, state=None, attio=None):
        import json

        from workflows.detect_responses import (
            _detect_manual_touches,
            _empty_counts,
            _normalize_name,
        )

        state_path = tmp_path / "manual_touch_state.json"
        if state is not None:
            state_path.write_text(json.dumps(state) if not isinstance(state, str) else state)
        attio = attio or MagicMock()
        counts = _empty_counts()
        index: dict[str, list[dict]] = {}
        for e in entries:
            index.setdefault(_normalize_name(e["prospect_name"]), []).append(e)
        _detect_manual_touches(
            attio=attio,
            list_id="test-list-id",
            scraped_threads=rows,
            name_to_full_pipeline=index,
            today_iso="2026-09-02",
            counts=counts,
            state_path=state_path,
        )
        return counts, attio, state_path

    @staticmethod
    def _state(state_path):
        import json
        return json.loads(state_path.read_text())

    def test_counts_contract_has_manual_touch_keys(self):
        from workflows.detect_responses import _empty_counts
        c = _empty_counts()
        for key in (
            "manual_touches_detected", "manual_touch_failed", "manual_touch_note_failed",
            "manual_touch_state_unreadable", "manual_touch_state_write_failed",
            "manual_touch_pass_crashed", "manual_touch_guard_offline",
            "manual_touch_unknown_direction", "manual_touch_missing_name",
            "manual_touch_empty_body",
            "manual_touch_ambiguous_name", "manual_touch_date_fallback",
            "manual_touch_prospect_replied",
        ):
            assert c[key] == 0, key

    def test_registry_authorizes_manual_touch_writer(self):
        from clients.attio_writer_registry import is_authorized_writer
        assert is_authorized_writer(
            "linkedin_outreach", "last_contact_date",
            "workflows.detect_responses._detect_manual_touches",
        )

    @pytest.mark.parametrize("value", ["", "unknown", None, True, {}])
    def test_invalid_sender_direction_is_counted_without_writes(self, tmp_path, value):
        counts, attio, state_path = self._run(
            tmp_path, rows=[self._row(from_me=value)], entries=[self._entry()],
        )
        assert counts["manual_touch_unknown_direction"] == 1
        assert counts["manual_touches_detected"] == 0
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_not_called()
        assert not state_path.exists()

    @pytest.mark.parametrize("field,counter", [
        ("participantFullName", "manual_touch_missing_name"),
        ("lastMessageBody", "manual_touch_empty_body"),
    ])
    @pytest.mark.parametrize("value", ["", "  ", None, {}])
    def test_incomplete_row_is_counted_without_writes(self, tmp_path, field, counter, value):
        row = {**self._row(), field: value}
        counts, attio, state_path = self._run(tmp_path, rows=[row], entries=[self._entry()])
        assert counts[counter] == 1
        assert counts["manual_touches_detected"] == 0
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_not_called()
        assert not state_path.exists()

    def test_note_failure_log_reports_actual_date_write(self, tmp_path, capsys):
        import httpx

        attio = MagicMock()
        attio.create_note.side_effect = httpx.RequestError("note service unavailable")
        counts, _, _ = self._run(
            tmp_path, rows=[self._row()], entries=[self._entry()], attio=attio,
        )
        output = capsys.readouterr().out
        assert counts["manual_touch_note_failed"] == 1
        assert "last_contact_date updated to 2026-09-01" in output
        assert "note write failed; will retry next run" in output
        assert "created" not in output

    def test_note_only_log_preserves_existing_contact_date(self, tmp_path, capsys):
        _, attio, _ = self._run(
            tmp_path, rows=[self._row()], entries=[self._entry(last_contact="2026-09-02")],
        )
        output = capsys.readouterr().out
        attio.update_list_entry.assert_not_called()
        assert "last_contact_date unchanged (2026-09-02)" in output
        assert "note 'DM manual — 2026-09-01' created" in output
        assert "updated to" not in output

    def test_stamps_note_and_state_on_new_manual_touch(self, tmp_path):
        counts, attio, state_path = self._run(
            tmp_path, rows=[self._row()], entries=[self._entry()],
        )
        assert counts["manual_touches_detected"] == 1
        assert counts["manual_touch_failed"] == 0
        attio.update_list_entry.assert_called_once()
        kw = attio.update_list_entry.call_args[1]
        # Stamped to the ROW's message date, not to today (2026-09-02).
        assert kw["entry_attributes"] == {"last_contact_date": "2026-09-01"}
        attio.create_note.assert_called_once()
        note = attio.create_note.call_args[1]
        assert note["record_id"] == "r-resp"
        assert note["title"] == "DM manual — 2026-09-01"
        assert note["content"] == self.BODY
        saved = self._state(state_path)["e-resp"]
        assert saved["touch_date"] == "2026-09-01"
        assert saved["stamped_date"] == "2026-09-02"
        assert saved["note_written"] is True
        assert saved["fingerprint"]

    def test_old_message_is_stamped_on_its_own_date(self, tmp_path):
        """First run after deploy: a July manual DM must not become 'today'."""
        counts, attio, _ = self._run(
            tmp_path, rows=[self._row(row_date="2026-07-02T09:00:00Z")],
            entries=[self._entry()],
        )
        assert counts["manual_touches_detected"] == 1
        assert attio.update_list_entry.call_args[1]["entry_attributes"] == {
            "last_contact_date": "2026-07-02"
        }
        assert attio.create_note.call_args[1]["title"] == "DM manual — 2026-07-02"

    def test_missing_row_date_falls_back_to_today_and_counts(self, tmp_path):
        counts, attio, _ = self._run(
            tmp_path, rows=[self._row(row_date="")], entries=[self._entry()],
        )
        assert counts["manual_touches_detected"] == 1
        assert counts["manual_touch_date_fallback"] == 1
        assert attio.update_list_entry.call_args[1]["entry_attributes"] == {
            "last_contact_date": "2026-09-02"
        }

    def test_never_moves_last_contact_date_backwards(self, tmp_path):
        """The CRM already has a later touch → note only, no PATCH."""
        counts, attio, state_path = self._run(
            tmp_path, rows=[self._row(row_date="2026-08-20T09:00:00Z")],
            entries=[self._entry(last_contact="2026-08-25")],
        )
        assert counts["manual_touches_detected"] == 1
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_called_once()
        assert "e-resp" in self._state(state_path)

    def test_skips_when_last_message_is_from_prospect(self, tmp_path):
        counts, attio, _ = self._run(
            tmp_path, rows=[self._row(from_me="false")], entries=[self._entry()],
        )
        assert counts["manual_touches_detected"] == 0
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_not_called()

    def test_skips_non_responded_stage(self, tmp_path):
        counts, attio, _ = self._run(
            tmp_path, rows=[self._row()],
            entries=[self._entry(stage=PipelineStage.DM1_SENT.value)],
        )
        assert counts["manual_touches_detected"] == 0
        attio.update_list_entry.assert_not_called()

    def test_skips_soft_deleted_duplicate(self, tmp_path):
        counts, attio, _ = self._run(
            tmp_path, rows=[self._row()],
            entries=[self._entry(merged_into="e-winner")],
        )
        assert counts["manual_touches_detected"] == 0
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_not_called()

    def test_same_fingerprint_does_not_rerecord(self, tmp_path):
        counts1, _, state_path = self._run(
            tmp_path, rows=[self._row()], entries=[self._entry()],
        )
        assert counts1["manual_touches_detected"] == 1
        saved = self._state(state_path)
        counts2, attio2, _ = self._run(
            tmp_path, rows=[self._row()], entries=[self._entry()], state=saved,
        )
        assert counts2["manual_touches_detected"] == 0
        attio2.update_list_entry.assert_not_called()
        attio2.create_note.assert_not_called()

    def test_new_body_is_a_new_touch(self, tmp_path):
        _, _, state_path = self._run(tmp_path, rows=[self._row()], entries=[self._entry()])
        saved = self._state(state_path)
        counts, attio, _ = self._run(
            tmp_path, rows=[self._row(body="Segundo mensaje manual, distinto.", count="6",
                                      row_date="2026-09-02T10:00:00Z")],
            entries=[self._entry()], state=saved,
        )
        assert counts["manual_touches_detected"] == 1
        attio.update_list_entry.assert_called_once()
        assert self._state(state_path)["e-resp"]["fingerprint"] != saved["e-resp"]["fingerprint"]

    def test_identical_nudge_sent_again_is_a_new_touch(self, tmp_path):
        """Same text, more messages in the thread → count is in the key."""
        _, _, state_path = self._run(tmp_path, rows=[self._row(count="5")], entries=[self._entry()])
        saved = self._state(state_path)
        counts, attio, _ = self._run(
            tmp_path, rows=[self._row(count="7", row_date="2026-09-02T10:00:00Z")],
            entries=[self._entry()], state=saved,
        )
        assert counts["manual_touches_detected"] == 1
        attio.update_list_entry.assert_called_once()

    def test_own_template_body_is_not_a_manual_touch(self, tmp_path):
        from models.campaign import load_messages
        template = load_messages()["operations_leaders"]["dm1"]["es"]
        echoed = template.replace("[Name]", "Vanessa").replace("[Company]", "Acme")
        counts, attio, _ = self._run(
            tmp_path, rows=[self._row(body=echoed)], entries=[self._entry()],
        )
        assert counts["manual_touches_detected"] == 0
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_not_called()

    def test_template_guard_offline_skips_pass(self, tmp_path, capsys):
        with patch("workflows.detect_responses._self_echo_templates", return_value=None):
            counts, attio, _ = self._run(
                tmp_path, rows=[self._row()], entries=[self._entry()],
            )
        assert counts["manual_touch_guard_offline"] == 1
        assert counts["manual_touches_detected"] == 0
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_not_called()
        assert "guard offline" in capsys.readouterr().err

    def test_ambiguous_name_is_skipped_loudly(self, tmp_path, capsys):
        two_people = [
            self._entry(entry_id="e-a", record_id="r-a"),
            self._entry(entry_id="e-b", record_id="r-b"),
        ]
        counts, attio, _ = self._run(tmp_path, rows=[{**self._row(), "participantProfileUrl": ""}], entries=two_people)
        assert counts["identity_holds"] == 1
        assert counts["manual_touches_detected"] == 0
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_not_called()
        assert "identity hold" in capsys.readouterr().err

    def test_same_person_two_entries_both_recorded(self, tmp_path):
        """Two list entries for ONE record (same record_id) is not ambiguous."""
        same_person = [
            self._entry(entry_id="e-a", record_id="r-x"),
            self._entry(entry_id="e-b", record_id="r-x"),
        ]
        counts, attio, _ = self._run(tmp_path, rows=[self._row()], entries=same_person)
        assert counts["manual_touches_detected"] == 2
        assert attio.update_list_entry.call_count == 2

    def test_attio_write_failure_counts_and_leaves_no_state(self, tmp_path):
        import httpx
        attio = MagicMock()
        attio.update_list_entry.side_effect = httpx.ConnectError("boom")
        with patch("clients.attio_writer.AttioWriter._sleep_with_jitter", lambda *a, **k: None):
            counts, attio, state_path = self._run(
                tmp_path, rows=[self._row()], entries=[self._entry()], attio=attio,
            )
        assert counts["manual_touches_detected"] == 0
        assert counts["manual_touch_failed"] == 1
        attio.create_note.assert_not_called()
        assert not state_path.exists() or "e-resp" not in self._state(state_path)

    def test_note_failure_keeps_stamp_and_marks_note_pending(self, tmp_path, capsys):
        import httpx
        attio = MagicMock()
        resp = MagicMock(status_code=500, text="server error")
        attio.create_note.side_effect = httpx.HTTPStatusError(
            "500", request=MagicMock(), response=resp,
        )
        counts, attio, state_path = self._run(
            tmp_path, rows=[self._row()], entries=[self._entry()], attio=attio,
        )
        assert counts["manual_touches_detected"] == 1
        assert counts["manual_touch_note_failed"] == 1
        assert self._state(state_path)["e-resp"]["note_written"] is False
        assert "note failed" in capsys.readouterr().err

    def test_pending_note_is_retried_without_restamp(self, tmp_path):
        import httpx
        failing = MagicMock()
        failing.create_note.side_effect = httpx.ConnectError("down")
        _, _, state_path = self._run(
            tmp_path, rows=[self._row()], entries=[self._entry()], attio=failing,
        )
        saved = self._state(state_path)
        counts, attio, _ = self._run(
            tmp_path, rows=[self._row()], entries=[self._entry()], state=saved,
        )
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_called_once()
        assert attio.create_note.call_args[1]["title"] == "DM manual — 2026-09-01"
        assert counts["manual_touches_detected"] == 0
        assert self._state(state_path)["e-resp"]["note_written"] is True

    def test_mid_loop_crash_still_persists_landed_stamps(self, tmp_path):
        attio = MagicMock()
        attio.create_note.side_effect = [None, RuntimeError("unexpected")]
        entries = [
            self._entry(entry_id="e-1", record_id="r-1"),
            self._entry(entry_id="e-2", record_id="r-2"),
        ]
        rows = [self._row(), self._row(name="Other Person")]
        entries[1]["prospect_name"] = "Other Person"
        with pytest.raises(RuntimeError):
            self._run(tmp_path, rows=rows, entries=entries, attio=attio)
        saved = self._state(tmp_path / "manual_touch_state.json")
        assert "e-1" in saved and "e-2" not in saved

    def test_corrupt_state_file_skips_pass_loudly(self, tmp_path, capsys):
        counts, attio, _ = self._run(
            tmp_path, rows=[self._row()], entries=[self._entry()], state="{not json",
        )
        assert counts["manual_touches_detected"] == 0
        assert counts["manual_touch_state_unreadable"] == 1
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_not_called()
        assert "manual_touch_state.json" in capsys.readouterr().err

    def test_state_write_failure_is_loud_not_fatal(self, tmp_path, capsys):
        import workflows.detect_responses as dr

        def _boom(path, state):
            raise OSError("disk full")

        with patch.object(dr, "_save_manual_touch_state", side_effect=_boom):
            counts, attio, _ = self._run(
                tmp_path, rows=[self._row()], entries=[self._entry()],
            )
        assert counts["manual_touches_detected"] == 1
        assert counts["manual_touch_state_write_failed"] == 1
        assert "re-stamp" in capsys.readouterr().err

    def _integration(self, pass_patch=None, rows_extra=()):
        attio = MagicMock()
        pb = MagicMock()
        dm_entry = _make_entry("e-dm", "r-dm", PipelineStage.DM1_SENT.value)
        responded = _make_entry("e-resp", "r-resp", PipelineStage.RESPONDED.value, dm_step=3)
        attio.query_list_entries.return_value = [dm_entry, responded]
        csv_content = _make_sn_csv(
            self._row(),
            {
                "participantProfileUrl": "https://linkedin.com/in/carlos",
                "participantFullName": "Carlos Lopez",
                "isLastMessageFromMe": "true",
                "lastMessageBody": "our dm1",
                "lastMessageDate": "2026-09-01T10:00:00Z",
                "totalMessageCount": "1",
            },
            *rows_extra,
        )
        pb.download_result_csv.return_value = csv_content
        name_map = {
            "r-dm": ("Carlos Lopez", "Beta", "https://linkedin.com/in/carlos", "", ""),
            "r-resp": (self.NAME, "Acme", "https://linkedin.com/in/vanessa", "", ""),
        }
        with patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e), \
             patch("workflows.detect_responses._pb_session_args", return_value={}), \
             patch("workflows.detect_responses.RecordCache") as MockCache:
            MockCache.return_value = _setup_mocks(
                attio, pb, [dm_entry, responded], csv_content, name_map,
            )
            if pass_patch is not None:
                with pass_patch:
                    result = detect_responses(attio, pb, inbox_scraper_id="scraper-123")
            else:
                result = detect_responses(attio, pb, inbox_scraper_id="scraper-123")
        return result, attio

    def test_detect_responses_runs_the_pass_after_reply_loop(self):
        """Integration: a RESPONDED entry is invisible to the reply loop
        (skip_stages) but must still be recorded by the manual-touch pass."""
        result, attio = self._integration()
        assert result["manual_touches_detected"] == 1
        stamped = [
            c for c in attio.update_list_entry.call_args_list
            if c[1]["entry_attributes"] == {"last_contact_date": "2026-09-01"}
        ]
        assert len(stamped) == 1
        titles = [c[1]["title"] for c in attio.create_note.call_args_list]
        assert "DM manual — 2026-09-01" in titles

    def test_pass_crash_is_counted_and_phase_completes(self, capsys):
        """Fail-open: a crash inside the pass must not break Phase 0.5."""
        result, _ = self._integration(
            pass_patch=patch(
                "workflows.detect_responses._detect_manual_touches",
                side_effect=TypeError("bug"),
            ),
        )
        assert result["manual_touch_pass_crashed"] == 1
        assert result["manual_touch_failed"] == 0
        assert "manual-touch pass crashed" in capsys.readouterr().err

    def test_malformed_row_counts_are_reported_at_end_of_phase(self, capsys):
        result, _ = self._integration(rows_extra=[self._row(from_me="unknown"), self._row(name="")])
        output = capsys.readouterr().out
        assert result["manual_touch_unknown_direction"] == 1
        assert result["manual_touch_missing_name"] == 1
        assert "manual_touch_unknown_direction=1" in output
        assert "manual_touch_missing_name=1" in output

    def test_crash_report_does_not_claim_failed_fingerprints_were_saved(self, capsys):
        def crash(**kwargs):
            kwargs["counts"]["manual_touches_detected"] = 1
            kwargs["counts"]["manual_touch_state_write_failed"] = 1
            raise RuntimeError("later row failed")

        result, _ = self._integration(pass_patch=patch(
            "workflows.detect_responses._detect_manual_touches", side_effect=crash,
        ))
        output = capsys.readouterr().err
        assert result["manual_touch_pass_crashed"] == 1
        assert "fingerprint save failures: 1" in output
        assert "fingerprints saved" not in output

    def test_registry_violation_halts_phase(self):
        from clients.attio_writer import UnauthorizedAttioWriteError
        with pytest.raises(UnauthorizedAttioWriteError):
            self._integration(
                pass_patch=patch(
                    "workflows.detect_responses._detect_manual_touches",
                    side_effect=UnauthorizedAttioWriteError("bad writer"),
                ),
            )


class TestManualTouchBallTracking:
    """The state file also tracks WHO spoke last per entry so the follow-up
    radar's cold-responder lane never nudges a prospect who answered a manual
    DM (a reply the CRM cannot see)."""

    NAME = TestManualTouchDetection.NAME
    BODY = TestManualTouchDetection.BODY
    ROW_DATE = TestManualTouchDetection.ROW_DATE
    _row = TestManualTouchDetection._row
    _entry = TestManualTouchDetection._entry
    _run = TestManualTouchDetection._run
    _state = staticmethod(TestManualTouchDetection._state)

    def _ours_state(self, **over):
        base = {
            "fingerprint": "old-fp", "stamped_date": "2026-08-20",
            "touch_date": "2026-08-20", "note_written": True,
            "ball": "ours", "last_body": "Te dejo la propuesta.",
        }
        base.update(over)
        return {"e-resp": base}

    def test_new_state_entry_carries_ball_and_body(self, tmp_path):
        counts, _, state_path = self._run(
            tmp_path, rows=[self._row()], entries=[self._entry()],
        )
        assert counts["manual_touches_detected"] == 1
        saved = self._state(state_path)["e-resp"]
        assert saved["ball"] == "ours"
        assert saved["last_body"] == self.BODY

    def test_body_kept_in_state_is_truncated(self, tmp_path):
        long_body = "x" * 900
        counts, _, state_path = self._run(
            tmp_path, rows=[self._row(body=long_body)], entries=[self._entry()],
        )
        assert counts["manual_touches_detected"] == 1
        assert len(self._state(state_path)["e-resp"]["last_body"]) == 500

    def test_prospect_reply_flips_ball_to_theirs_without_attio_write(self, tmp_path):
        counts, attio, state_path = self._run(
            tmp_path,
            rows=[self._row(from_me="false", body="Gracias, lo reviso con mi equipo",
                            row_date="2026-08-28T10:00:00Z", count="6")],
            entries=[self._entry()],
            state=self._ours_state(),
        )
        assert counts["manual_touch_prospect_replied"] == 1
        assert counts["manual_touches_detected"] == 0
        attio.update_list_entry.assert_not_called()
        attio.create_note.assert_not_called()
        saved = self._state(state_path)["e-resp"]
        assert saved["ball"] == "theirs"
        assert saved["ball_observed"] == "2026-08-28"
        # The original touch record is preserved (idempotency intact).
        assert saved["fingerprint"] == "old-fp"
        assert saved["touch_date"] == "2026-08-20"

    def test_prospect_reply_without_state_entry_is_ignored(self, tmp_path):
        # Nothing recorded as ours → nothing to flip, no file created.
        counts, attio, state_path = self._run(
            tmp_path,
            rows=[self._row(from_me="false", body="Hola, quién eres?")],
            entries=[self._entry()],
        )
        assert counts["manual_touch_prospect_replied"] == 0
        assert not state_path.exists()
        attio.update_list_entry.assert_not_called()

    def test_theirs_flip_is_idempotent(self, tmp_path):
        state = self._ours_state(ball="theirs", ball_observed="2026-08-25")
        counts, _, state_path = self._run(
            tmp_path,
            rows=[self._row(from_me="false", body="ok", row_date="2026-08-28T10:00:00Z")],
            entries=[self._entry()],
            state=state,
        )
        assert counts["manual_touch_prospect_replied"] == 0
        assert self._state(state_path)["e-resp"]["ball_observed"] == "2026-08-25"

    def test_new_manual_dm_after_their_reply_resets_ball_to_ours(self, tmp_path):
        state = self._ours_state(ball="theirs", ball_observed="2026-08-25")
        counts, _, state_path = self._run(
            tmp_path,
            rows=[self._row(count="7", body="Perfecto, te mando tres horarios.")],
            entries=[self._entry()],
            state=state,
        )
        assert counts["manual_touches_detected"] == 1
        saved = self._state(state_path)["e-resp"]
        assert saved["ball"] == "ours"
        assert "ball_observed" not in saved
        assert saved["last_body"] == "Perfecto, te mando tres horarios."

    def test_unknown_from_me_value_is_inert(self, tmp_path):
        # A renamed/missing column must never read as "prospect wrote last".
        for raw in ("", "N/A", "yes"):
            counts, attio, state_path = self._run(
                tmp_path,
                rows=[self._row(from_me=raw)],
                entries=[self._entry()],
                state=self._ours_state(),
            )
            assert counts["manual_touch_prospect_replied"] == 0, raw
            assert counts["manual_touches_detected"] == 0, raw
            assert self._state(state_path)["e-resp"]["ball"] == "ours", raw

    def test_ambiguous_name_is_not_flipped(self, tmp_path):
        state = {**self._ours_state(), "e-other": {**self._ours_state()["e-resp"]}}
        counts, _, state_path = self._run(
            tmp_path,
            rows=[{**self._row(from_me="false", body="ok"), "participantProfileUrl": ""}],
            entries=[self._entry(), self._entry(entry_id="e-other", record_id="r-other")],
            state=state,
        )
        assert counts["manual_touch_prospect_replied"] == 0
        assert counts["identity_holds"] == 1
        saved = self._state(state_path)
        assert saved["e-resp"]["ball"] == "ours" and saved["e-other"]["ball"] == "ours"

    def test_flip_with_unparsable_date_falls_back_and_counts(self, tmp_path):
        counts, _, state_path = self._run(
            tmp_path,
            rows=[self._row(from_me="false", body="ok", row_date="garbage")],
            entries=[self._entry()],
            state=self._ours_state(),
        )
        assert counts["manual_touch_prospect_replied"] == 1
        assert counts["manual_touch_date_fallback"] == 1
        assert self._state(state_path)["e-resp"]["ball_observed"] == "2026-09-02"
