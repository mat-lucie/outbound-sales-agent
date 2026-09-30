"""Tests for workflows/email_campaign.py — import, anti-collision, and daily run."""

import csv
import os
import tempfile
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from models.email_campaign import EmailStep, detect_language
from workflows.daily_check import UnresolvedPlaceholderError
from workflows.daily_check_helpers import BlankMessageError
from workflows.email_campaign import (
    _domain_from_email,
    _is_collision,
    import_contacts,
    run_email_daily,
)


@pytest.fixture(autouse=True)
def _stub_email_suppression():
    """run_email_daily now builds a cross-channel suppression set (which needs
    ATTIO_LIST_ID + a live-shaped attio). These tests exercise the send / cap /
    collision / placeholder paths, not suppression — that's covered by
    test_wave2_blast_suppression.py and test_email_compliance.py. Stub it to an
    empty set so the send-path behavior is unchanged from pre-suppression."""
    with patch("workflows.email_campaign.build_suppression_set", return_value=set()):
        yield


class TestDomainFromEmail:
    def test_normal_email(self):
        assert _domain_from_email("john@example.com") == "example.com"

    def test_no_at_sign(self):
        assert _domain_from_email("invalid") == ""

    def test_empty_string(self):
        assert _domain_from_email("") == ""

    def test_uppercase_normalized(self):
        assert _domain_from_email("John@Example.COM") == "example.com"


class TestDetectLanguage:
    def test_country_code_br(self):
        assert detect_language("danone.com", "BR") == "pt"

    def test_country_code_mx(self):
        assert detect_language("heineken.com", "MX") == "es"

    def test_country_code_us(self):
        assert detect_language("adm.com", "US") == "en"

    def test_country_code_overrides_domain(self):
        # Person at a .com domain but located in Brazil
        assert detect_language("effem.com", "BR") == "pt"

    def test_fallback_to_domain_br(self):
        assert detect_language("seara.com.br") == "pt"

    def test_fallback_to_domain_mx(self):
        assert detect_language("jumex.com.mx") == "es"

    def test_fallback_to_domain_cl(self):
        assert detect_language("embonor.cl") == "es"

    def test_no_country_com_defaults_en(self):
        assert detect_language("adm.com") == "en"

    def test_empty_defaults_to_en(self):
        assert detect_language("") == "en"

    def test_country_code_case_insensitive(self):
        assert detect_language("example.com", "br") == "pt"


class TestIsCollision:
    def test_exact_match(self):
        collision_set = {("john", "doe", "example.com")}
        assert _is_collision("John", "Doe", "example.com", collision_set)

    def test_no_match(self):
        collision_set = {("john", "doe", "example.com")}
        assert not _is_collision("Jane", "Smith", "other.com", collision_set)

    def test_case_insensitive(self):
        collision_set = {("john", "doe", "example.com")}
        assert _is_collision("JOHN", "DOE", "EXAMPLE.COM", collision_set)


class TestImportContacts:
    def _write_csv(self, rows: list[dict]) -> str:
        """Helper to write a temp CSV and return its path."""
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="")
        fieldnames = ["name", "first_name", "last_name", "email", "domain", "title", "linkedin"]
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
        tmp.close()
        return tmp.name

    def test_imports_valid_contact(self):
        csv_path = self._write_csv([
            {"name": "John Doe", "first_name": "John", "last_name": "Doe",
             "email": "john@example.com", "domain": "example.com"},
        ])
        attio = MagicMock()
        result = import_contacts(attio, csv_path, dry_run=True)
        assert result["queued"] == 1
        assert result["skipped"] == 0
        os.unlink(csv_path)

    def test_skips_unknown_name(self):
        csv_path = self._write_csv([
            {"name": "Unknown", "first_name": "", "last_name": "",
             "email": "unknown@example.com", "domain": "example.com"},
        ])
        attio = MagicMock()
        result = import_contacts(attio, csv_path, dry_run=True)
        assert result["queued"] == 0
        assert result["skipped"] == 1
        os.unlink(csv_path)

    def test_skips_no_email(self):
        csv_path = self._write_csv([
            {"name": "John Doe", "first_name": "John", "last_name": "Doe",
             "email": "", "domain": ""},
        ])
        attio = MagicMock()
        result = import_contacts(attio, csv_path, dry_run=True)
        assert result["queued"] == 0
        assert result["skipped"] == 1
        os.unlink(csv_path)

    def test_mixed_valid_and_invalid(self):
        csv_path = self._write_csv([
            {"name": "John Doe", "first_name": "John", "last_name": "Doe",
             "email": "john@example.com", "domain": "example.com"},
            {"name": "Unknown", "first_name": "", "last_name": "",
             "email": "anon@example.com", "domain": "example.com"},
            {"name": "Jane Smith", "first_name": "Jane", "last_name": "Smith",
             "email": "jane@example.com", "domain": "example.com"},
        ])
        attio = MagicMock()
        result = import_contacts(attio, csv_path, dry_run=True)
        assert result["queued"] == 2
        assert result["skipped"] == 1
        os.unlink(csv_path)


def _make_attio_person(record_id, first_name, last_name, email, stage, last_sent=""):
    """Helper to build a mock Attio person record."""
    record = {
        "id": {"record_id": record_id},
        "values": {
            "name": [{"first_name": first_name, "last_name": last_name}],
            "email_addresses": [{"email_address": email}],
            "email_campaign_stage": [{"value": stage}],
            "email_campaign_last_sent": [{"value": last_sent}] if last_sent else [],
            "company": [{"value": "Test Corp"}],
        },
    }
    return record


class TestRunEmailDaily:
    """Integration tests for run_email_daily with mocked Attio + Resend."""

    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_sends_email1_to_queued_contacts(self, mock_collision, mock_date):
        mock_date.today.return_value = date(2026, 4, 7)  # Tuesday
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()

        attio = MagicMock()
        attio.search_people.side_effect = [
            # QUEUED stage query
            [_make_attio_person("r1", "John", "Doe", "john@acme.com", "queued")],
            # EMAIL1_SENT stage query
            [],
            # EMAIL2_SENT stage query
            [],
        ]

        resend = MagicMock()
        resend.send_email.return_value = {"id": "email-123"}

        result = run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert result["sent"] == 1
        resend.send_email.assert_called_once()
        call_kwargs = resend.send_email.call_args[1]
        assert call_kwargs["to"] == "john@acme.com"
        assert call_kwargs["reply_to"] == "outbound@example.com"

        # Verify Attio update
        attio.update_person.assert_called_once()
        update_args = attio.update_person.call_args[0]
        assert update_args[0] == "r1"
        assert update_args[1]["email_campaign_stage"] == "email1_sent"

    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_resend_id_captured_on_send(self, mock_collision, mock_date):
        """PR-243: the Resend message id is stamped onto the person for
        future thread-based reply matching."""
        mock_date.today.return_value = date(2026, 4, 7)  # Tuesday
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()

        attio = MagicMock()
        attio.search_people.side_effect = [
            [_make_attio_person("r1", "John", "Doe", "john@acme.com", "queued")],
            [],
            [],
        ]

        resend = MagicMock()
        resend.send_email.return_value = {"id": "resend-abc-123"}

        run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        update_args = attio.update_person.call_args[0]
        assert update_args[1]["email_last_resend_id"] == "resend-abc-123"

    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_resend_id_400_falls_back_without_key(self, mock_collision, mock_date):
        """PR-243: until the Attio attribute exists, a 400 on the write
        including email_last_resend_id must retry without it — the stage
        advance MUST land or the prospect gets a duplicate email next run."""
        import httpx

        mock_date.today.return_value = date(2026, 4, 7)  # Tuesday
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()

        attio = MagicMock()
        attio.search_people.side_effect = [
            [_make_attio_person("r1", "John", "Doe", "john@acme.com", "queued")],
            [],
            [],
        ]

        resp_400 = httpx.Response(400, request=httpx.Request("PATCH", "https://api.example.com/x"))
        attio.update_person.side_effect = [
            httpx.HTTPStatusError("400", request=resp_400.request, response=resp_400),
            {},
        ]

        resend = MagicMock()
        resend.send_email.return_value = {"id": "resend-abc-123"}

        result = run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert result["sent"] == 1
        assert attio.update_person.call_count == 2
        retry_attrs = attio.update_person.call_args_list[1][0][1]
        assert "email_last_resend_id" not in retry_attrs
        assert retry_attrs["email_campaign_stage"] == "email1_sent"

    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_skips_collision_contacts(self, mock_collision, mock_date):
        mock_date.today.return_value = date(2026, 4, 7)  # Tuesday
        mock_date.fromisoformat = date.fromisoformat
        # John Doe at acme.com is in LinkedIn pipeline
        mock_collision.return_value = {("john", "doe", "acme.com")}

        attio = MagicMock()
        attio.search_people.side_effect = [
            [_make_attio_person("r1", "John", "Doe", "john@acme.com", "queued")],
            [],
            [],
        ]

        resend = MagicMock()

        result = run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert result["collisions"] == 1
        assert result["sent"] == 0
        resend.send_email.assert_not_called()

    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_sequences_email2_after_3_business_days(self, mock_collision, mock_date):
        mock_date.today.return_value = date(2026, 4, 10)  # Friday, 3 bdays after Tue
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()

        attio = MagicMock()
        attio.search_people.side_effect = [
            # QUEUED
            [],
            # EMAIL1_SENT — sent on Tuesday Apr 7
            [_make_attio_person("r2", "Jane", "Smith", "jane@corp.com", "email1_sent", "2026-04-07")],
            # EMAIL2_SENT
            [],
        ]

        resend = MagicMock()
        resend.send_email.return_value = {"id": "email-456"}

        result = run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert result["sent"] == 1
        update_args = attio.update_person.call_args[0]
        assert update_args[1]["email_campaign_stage"] == "email2_sent"

    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_email3_marks_completed(self, mock_collision, mock_date):
        mock_date.today.return_value = date(2026, 4, 15)  # Wed, 4 bdays after Thu Apr 9
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()

        attio = MagicMock()
        attio.search_people.side_effect = [
            [],
            [],
            # EMAIL2_SENT — sent on Thu Apr 9
            [_make_attio_person("r3", "Bob", "Lee", "bob@inc.com", "email2_sent", "2026-04-09")],
        ]

        resend = MagicMock()
        resend.send_email.return_value = {"id": "email-789"}

        result = run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert result["sent"] == 1
        update_args = attio.update_person.call_args[0]
        assert update_args[1]["email_campaign_stage"] == "completed"

    @patch("workflows.email_campaign.is_weekday")
    def test_skips_on_weekend(self, mock_weekday):
        mock_weekday.return_value = False

        attio = MagicMock()
        result = run_email_daily(attio, None, dry_run=True)

        assert result["reason"] == "weekend"
        assert result["sent"] == 0

    @patch("workflows.email_campaign._build_linkedin_collision_set")
    @patch("workflows.email_campaign.is_weekday")
    def test_force_weekend_overrides_skip(self, mock_weekday, mock_collision):
        # Saturday + force_weekend=True → workflow proceeds past the gate.
        mock_weekday.return_value = False
        mock_collision.return_value = set()

        attio = MagicMock()
        attio.list_records.return_value = []  # no contacts → exits cleanly past gate

        result = run_email_daily(attio, None, dry_run=True, force_weekend=True)

        assert result.get("reason") != "weekend"

    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_respects_daily_cap(self, mock_collision, mock_date):
        mock_date.today.return_value = date(2026, 4, 7)
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()

        # Create 25 queued contacts (cap is 20)
        queued_contacts = [
            _make_attio_person(f"r{i}", f"Person{i}", "Test", f"p{i}@test.com", "queued")
            for i in range(25)
        ]

        attio = MagicMock()
        attio.search_people.side_effect = [queued_contacts, [], []]

        resend = MagicMock()
        resend.send_email.return_value = {"id": "ok"}

        result = run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert result["sent"] == 20
        assert resend.send_email.call_count == 20


class TestEmailPlaceholderGuard:
    """The email send paths must run the same _assert_no_unresolved_placeholders
    guard the DM and connection-note paths use, so a [bracket] token left in a
    template never ships to a prospect.
    """

    @patch("workflows.email_campaign.get_email_template")
    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_email1_raises_on_unresolved_bracket_token(
        self, mock_collision, mock_date, mock_template,
    ):
        mock_date.today.return_value = date(2026, 4, 7)  # Tuesday
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()
        # Subject is fine; body has an unresolved [industry] token. Must abort.
        mock_template.return_value = {
            "subject": "Hi {{name}}",
            "body_html": "Hi {{name}}, plants in [industry] need scheduling.",
        }

        attio = MagicMock()
        attio.search_people.side_effect = [
            [_make_attio_person("r1", "John", "Doe", "john@acme.com", "queued")],
            [],
            [],
        ]
        resend = MagicMock()

        with pytest.raises(UnresolvedPlaceholderError) as exc:
            run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert "[industry]" in str(exc.value)
        # Resend must not have been called — guard fires before send.
        resend.send_email.assert_not_called()

    @patch("workflows.email_campaign.get_email_template")
    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_dry_run_also_catches_placeholder_leak(
        self, mock_collision, mock_date, mock_template,
    ):
        # Dry-run must fail too, so CI catches template bugs without needing a real send.
        mock_date.today.return_value = date(2026, 4, 7)
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()
        mock_template.return_value = {
            "subject": "Hi {{name}}",
            "body_html": "Hi {{name}}, [persona]-specific opener.",
        }

        attio = MagicMock()
        attio.search_people.side_effect = [
            [_make_attio_person("r1", "John", "Doe", "john@acme.com", "queued")],
            [],
            [],
        ]

        with pytest.raises(UnresolvedPlaceholderError):
            run_email_daily(attio, resend=None, dry_run=True)


class TestEmailBlankTemplateGuard:
    """A template edit that empties one language's subject or body must halt
    the run loudly at ZERO sends — not ship a subject-less email, record it
    in the already_sent ledger, and advance the stage. Blankness is checked
    pre-flight per distinct template (batch-level, like the DM guard) and
    again per send as a backstop; the join the placeholder guard checks
    ("{subject}\\n{body}") masks a blank half, so subject and body are
    validated separately via _assert_email_not_blank.
    """

    def _attio_with_queued(self):
        attio = MagicMock()
        attio.search_people.side_effect = [
            [_make_attio_person("r1", "John", "Doe", "john@acme.com", "queued")],
            [],
            [],
        ]
        return attio

    @patch("workflows.email_campaign.get_email_template")
    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_email1_blank_subject_halts_batch(
        self, mock_collision, mock_date, mock_template,
    ):
        mock_date.today.return_value = date(2026, 4, 7)  # Tuesday
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()
        mock_template.return_value = {"subject": "   ", "body_html": "<p>Hola John</p>"}

        attio = self._attio_with_queued()
        resend = MagicMock()

        with pytest.raises(BlankMessageError) as exc:
            run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert "subject" in str(exc.value)
        resend.send_email.assert_not_called()
        attio.update_person.assert_not_called()

    @patch("workflows.email_campaign.get_email_template")
    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_email1_blank_body_halts_batch(
        self, mock_collision, mock_date, mock_template,
    ):
        mock_date.today.return_value = date(2026, 4, 7)
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()
        mock_template.return_value = {"subject": "Asunto claro", "body_html": ""}

        attio = self._attio_with_queued()
        resend = MagicMock()

        with pytest.raises(BlankMessageError) as exc:
            run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert "body" in str(exc.value)
        resend.send_email.assert_not_called()
        attio.update_person.assert_not_called()

    def _attio_with_sequenced(self):
        attio = MagicMock()
        attio.search_people.side_effect = [
            [],
            [_make_attio_person("r2", "Jane", "Smith", "jane@corp.com", "email1_sent", "2026-04-07")],
            [],
        ]
        return attio

    @patch("workflows.email_campaign.get_email_template")
    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_sequence_blank_subject_halts_batch(
        self, mock_collision, mock_date, mock_template,
    ):
        mock_date.today.return_value = date(2026, 4, 10)  # Friday, 3 bdays after Tue
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()
        mock_template.return_value = {"subject": "", "body_html": "<p>Hola Jane</p>"}

        attio = self._attio_with_sequenced()
        resend = MagicMock()

        with pytest.raises(BlankMessageError) as exc:
            run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert "subject" in str(exc.value)
        assert "email2" in str(exc.value)
        resend.send_email.assert_not_called()
        attio.update_person.assert_not_called()

    @patch("workflows.email_campaign.get_email_template")
    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_sequence_blank_body_halts_batch(
        self, mock_collision, mock_date, mock_template,
    ):
        mock_date.today.return_value = date(2026, 4, 10)
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()
        mock_template.return_value = {"subject": "Asunto claro", "body_html": " \n "}

        attio = self._attio_with_sequenced()
        resend = MagicMock()

        with pytest.raises(BlankMessageError) as exc:
            run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert "body" in str(exc.value)
        resend.send_email.assert_not_called()
        attio.update_person.assert_not_called()

    @patch("workflows.email_campaign.get_email_template")
    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_dry_run_also_catches_blank_template(
        self, mock_collision, mock_date, mock_template,
    ):
        # Dry-run must fail too, so CI catches template bugs without needing a real send.
        mock_date.today.return_value = date(2026, 4, 7)
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()
        mock_template.return_value = {"subject": "", "body_html": "<p>Hola John</p>"}

        attio = self._attio_with_queued()

        with pytest.raises(BlankMessageError):
            run_email_daily(attio, resend=None, dry_run=True)

    @patch("workflows.email_campaign.get_email_template")
    @patch("workflows.email_campaign.date")
    @patch("workflows.email_campaign._build_linkedin_collision_set")
    def test_blank_template_halts_at_zero_sends_across_the_whole_run(
        self, mock_collision, mock_date, mock_template,
    ):
        """Pin the pre-flight contract: a blank email2 template halts the run
        BEFORE the healthy email1 contact sends — zero sends, not a partial
        batch whose blast radius depends on contact ordering."""
        mock_date.today.return_value = date(2026, 4, 10)  # Friday, 3 bdays after Tue
        mock_date.fromisoformat = date.fromisoformat
        mock_collision.return_value = set()
        mock_template.side_effect = lambda step, language: (
            {"subject": "", "body_html": "<p>Hola Jane</p>"}
            if step == EmailStep.EMAIL2
            else {"subject": "Asunto claro", "body_html": "<p>Hola John</p>"}
        )

        attio = MagicMock()
        attio.search_people.side_effect = [
            # QUEUED — healthy email1 template, would send if the loop ran
            [_make_attio_person("r1", "John", "Doe", "john@acme.com", "queued")],
            # EMAIL1_SENT — due email2, whose template is blank
            [_make_attio_person("r2", "Jane", "Smith", "jane@corp.com", "email1_sent", "2026-04-07")],
            [],
        ]
        resend = MagicMock()

        with pytest.raises(BlankMessageError) as exc:
            run_email_daily(attio, resend, dry_run=False, auto_confirm=True)

        assert "email2" in str(exc.value)
        resend.send_email.assert_not_called()
        attio.update_person.assert_not_called()


class TestReplyScanMaps:
    """Phase 0.6 (PR-243) stage-selection + classification maps."""

    def test_reply_scan_stages_exclude_queued_and_terminals(self):
        from models.email_campaign import REPLY_SCAN_STAGES, EmailStage

        # Nothing sent yet -> not scannable.
        assert EmailStage.QUEUED not in REPLY_SCAN_STAGES
        # Terminal reply stages are excluded (the idempotency marker guards
        # them anyway); scanning them would be wasted work.
        assert EmailStage.RESPONDED not in REPLY_SCAN_STAGES
        assert EmailStage.NOT_INTERESTED not in REPLY_SCAN_STAGES
        assert EmailStage.UNSUBSCRIBED not in REPLY_SCAN_STAGES
        # Everyone who received >= 1 email is scannable.
        assert EmailStage.EMAIL1_SENT in REPLY_SCAN_STAGES
        assert EmailStage.COMPLETED in REPLY_SCAN_STAGES

    def test_classification_map_routes_negative_to_hard_stop(self):
        from models.email_campaign import EMAIL_CLASS_TO_STAGE, EmailStage

        assert EMAIL_CLASS_TO_STAGE["negative"] == EmailStage.NOT_INTERESTED
        for cls in ("positive", "question", "neutral", "defensive"):
            assert EMAIL_CLASS_TO_STAGE[cls] == EmailStage.RESPONDED


@pytest.fixture(autouse=True)
def _email_guard_unit_boundary():
    # This suite covers rendering/caps/collision; real guard behavior is pinned
    # independently in test_public_send_preconditions.py.
    from workflows.email_send_guard import GuardResult
    with patch("workflows.email_campaign.verify_email_send_preconditions", return_value=GuardResult(True)):
        yield
