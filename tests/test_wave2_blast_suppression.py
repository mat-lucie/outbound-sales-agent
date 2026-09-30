"""PR-37: wave2_blast must consult cross-channel suppression before sending.

Belt-and-suspenders defense for §3.1 (no re-sends). The runtime gate catches
any row the backfill missed (e.g. defensive manual replies the auto-classifier
never scored — §3.16)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from tests.fakes import make_wave2_person as _make_attio_person
from workflows.wave2_blast import WAVE2_STAGE, run_wave2_blast


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_suppressed_record_is_skipped(mock_suppression, mock_collision, mock_date):
    mock_date.today.return_value = date(2026, 5, 5)  # Tuesday
    mock_collision.return_value = set()
    mock_suppression.return_value = {"rec-suppress"}

    attio = MagicMock()
    attio.search_people.side_effect = [
        [_make_attio_person("rec-suppress", "Defen", "Sive", "d@corp.com", "email1_sent")],
        [],
    ]
    resend = MagicMock()

    result = run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    assert result == {"sent": 0, "collisions": 0, "suppressed": 1, "send_guard_skipped": 0,
        "already_sent_repaired": 0, "repair_failed": 0}
    resend.send_email.assert_not_called()
    attio.update_person.assert_not_called()


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_non_suppressed_record_is_sent(mock_suppression, mock_collision, mock_date):
    mock_date.today.return_value = date(2026, 5, 5)
    mock_collision.return_value = set()
    mock_suppression.return_value = set()

    attio = MagicMock()
    attio.search_people.side_effect = [
        [_make_attio_person("rec-clean", "Clean", "Sweep", "c@corp.com", "email1_sent")],
        [],
    ]
    resend = MagicMock()
    resend.send_email.return_value = {"id": "msg-001"}

    # Phase 3 guard: re-read must confirm stage still matches.
    attio._request.return_value = {
        "data": {"values": {"email_campaign_stage": [{"value": "email1_sent"}]}}
    }

    result = run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    assert result == {"sent": 1, "collisions": 0, "suppressed": 0, "send_guard_skipped": 0,
        "already_sent_repaired": 0, "repair_failed": 0}
    resend.send_email.assert_called_once()
    attio.update_person.assert_called_once()
    update_args = attio.update_person.call_args[0]
    assert update_args[0] == "rec-clean"
    assert update_args[1]["email_campaign_stage"] == WAVE2_STAGE


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_collision_takes_precedence_over_suppression(
    mock_suppression, mock_collision, mock_date,
):
    """A LinkedIn-active contact who is ALSO suppressed: should count as a
    collision (skipped by the earlier branch); never reach the suppression
    counter. Keeps the two metrics non-overlapping."""
    mock_date.today.return_value = date(2026, 5, 5)
    mock_collision.return_value = {("collide", "person", "corp.com")}
    mock_suppression.return_value = {"rec-collide"}

    attio = MagicMock()
    attio.search_people.side_effect = [
        [_make_attio_person("rec-collide", "Collide", "Person", "c@corp.com", "email1_sent")],
        [],
    ]
    resend = MagicMock()

    result = run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    assert result == {"sent": 0, "collisions": 1, "suppressed": 0, "send_guard_skipped": 0,
        "already_sent_repaired": 0, "repair_failed": 0}
    resend.send_email.assert_not_called()


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_mixed_outcomes_counted_independently(
    mock_suppression, mock_collision, mock_date,
):
    """Three contacts: one collides, one suppressed, one sent."""
    mock_date.today.return_value = date(2026, 5, 5)
    mock_collision.return_value = {("collide", "person", "corp.com")}
    mock_suppression.return_value = {"rec-suppress"}

    attio = MagicMock()
    attio.search_people.side_effect = [
        [
            _make_attio_person("rec-collide", "Collide", "Person", "c@corp.com", "email1_sent"),
            _make_attio_person("rec-suppress", "Defen", "Sive", "d@corp.com", "email1_sent"),
            _make_attio_person("rec-clean", "Clean", "Sweep", "x@corp.com", "email1_sent"),
        ],
        [],
    ]
    resend = MagicMock()
    resend.send_email.return_value = {"id": "msg-x"}

    # Phase 3 guard: re-read must confirm stage still matches for rec-clean.
    attio._request.return_value = {
        "data": {"values": {"email_campaign_stage": [{"value": "email1_sent"}]}}
    }

    result = run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    assert result == {"sent": 1, "collisions": 1, "suppressed": 1, "send_guard_skipped": 0,
        "already_sent_repaired": 0, "repair_failed": 0}
    resend.send_email.assert_called_once()
    # Only the clean contact gets advanced.
    attio.update_person.assert_called_once()
    assert attio.update_person.call_args[0][0] == "rec-clean"


@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_dry_run_does_not_send_or_update(
    mock_suppression, mock_collision, mock_date,
):
    mock_date.today.return_value = date(2026, 5, 5)
    mock_collision.return_value = set()
    mock_suppression.return_value = {"rec-suppress"}

    attio = MagicMock()
    attio.search_people.side_effect = [
        [
            _make_attio_person("rec-suppress", "Defen", "Sive", "d@corp.com", "email1_sent"),
            _make_attio_person("rec-clean", "Clean", "Sweep", "x@corp.com", "email1_sent"),
        ],
        [],
    ]

    result = run_wave2_blast(attio, None, dry_run=True, auto_confirm=True)

    # Suppressed counted, clean is the only "would-send".
    assert result == {"sent": 1, "collisions": 0, "suppressed": 1, "send_guard_skipped": 0,
        "already_sent_repaired": 0, "repair_failed": 0}
    attio.update_person.assert_not_called()


# ────────────────────────────────────────────────────────────────────────
# W2A-3: company-fetch exception discipline (mirrors FU-3 / PR #111)
# ────────────────────────────────────────────────────────────────────────


def _make_attio_person_with_company(record_id, first_name, last_name, email, stage, company_id):
    """Person record with a company reference (drives the company-fetch path)."""
    return {
        "id": {"record_id": record_id},
        "values": {
            "name": [{"first_name": first_name, "last_name": last_name}],
            "email_addresses": [{"email_address": email}],
            "email_campaign_stage": [{"value": stage}],
            "primary_location": [{"country_code": "US"}],
            "company": [{"target_record_id": company_id}],
        },
    }


class TestCompanyFetchExceptionDiscipline:
    """W2A-3: the company-fetch ``except Exception: pass`` originally
    swallowed every error — including programmer errors (ImportError,
    TypeError, AttributeError) that should crash loudly. Mirrors the
    FU-3 (PR #111) tiered-except pattern applied to the analogous
    ``_emit_industry_low_confidence`` helper.
    """

    @patch("workflows.wave2_blast.date")
    @patch("workflows.wave2_blast._build_linkedin_collision_set")
    @patch("workflows.wave2_blast.build_suppression_set")
    def test_attribute_error_during_company_fetch_propagates(
        self, mock_suppression, mock_collision, mock_date,
    ):
        """AttributeError signals a refactor that renamed an attribute on
        AttioClient (e.g. ``_request`` rename). Must surface — silently
        skipping every record with a non-cached company would mask the bug
        and ship blast emails with blank company names."""
        mock_date.today.return_value = date(2026, 5, 5)
        mock_collision.return_value = set()
        mock_suppression.return_value = set()

        attio = MagicMock()
        attio.search_people.side_effect = [
            [_make_attio_person_with_company("rec-1", "First", "Last", "f@corp.com", "email1_sent", "co-1")],
            [],
        ]
        attio._request.side_effect = AttributeError("'AttioClient' has no attribute '_request'")

        with pytest.raises(AttributeError, match="has no attribute"):
            run_wave2_blast(attio, MagicMock(), dry_run=True, auto_confirm=True)

    @patch("workflows.wave2_blast.date")
    @patch("workflows.wave2_blast._build_linkedin_collision_set")
    @patch("workflows.wave2_blast.build_suppression_set")
    def test_type_error_during_company_fetch_propagates(
        self, mock_suppression, mock_collision, mock_date,
    ):
        """TypeError signals a call-shape regression (caller passing wrong
        type, _request signature changing). Must surface."""
        mock_date.today.return_value = date(2026, 5, 5)
        mock_collision.return_value = set()
        mock_suppression.return_value = set()

        attio = MagicMock()
        attio.search_people.side_effect = [
            [_make_attio_person_with_company("rec-1", "First", "Last", "f@corp.com", "email1_sent", "co-1")],
            [],
        ]
        attio._request.side_effect = TypeError("_request() got unexpected keyword argument 'foo'")

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            run_wave2_blast(attio, MagicMock(), dry_run=True, auto_confirm=True)

    @patch("workflows.wave2_blast.date")
    @patch("workflows.wave2_blast._build_linkedin_collision_set")
    @patch("workflows.wave2_blast.build_suppression_set")
    def test_import_error_during_company_fetch_propagates(
        self, mock_suppression, mock_collision, mock_date,
    ):
        """ImportError signals a packaging or refactor regression. Must
        surface — was silently swallowed by the pre-PR ``except Exception:
        pass`` shape."""
        mock_date.today.return_value = date(2026, 5, 5)
        mock_collision.return_value = set()
        mock_suppression.return_value = set()

        attio = MagicMock()
        attio.search_people.side_effect = [
            [_make_attio_person_with_company("rec-1", "First", "Last", "f@corp.com", "email1_sent", "co-1")],
            [],
        ]
        attio._request.side_effect = ImportError("simulated packaging regression")

        with pytest.raises(ImportError, match="simulated packaging regression"):
            run_wave2_blast(attio, MagicMock(), dry_run=True, auto_confirm=True)

    @patch("workflows.wave2_blast.date")
    @patch("workflows.wave2_blast._build_linkedin_collision_set")
    @patch("workflows.wave2_blast.build_suppression_set")
    def test_runtime_error_during_company_fetch_logs_and_continues(
        self, mock_suppression, mock_collision, mock_date, caplog,
    ):
        """Transient infra failures (Attio 5xx, network blip) must NOT abort
        the blast — the blast can still proceed with a blank company name.
        But the failure must log loudly with the failing company record_id
        so an operator can audit blank-company rows in post-blast review."""
        mock_date.today.return_value = date(2026, 5, 5)
        mock_collision.return_value = set()
        mock_suppression.return_value = set()

        attio = MagicMock()
        attio.search_people.side_effect = [
            [_make_attio_person_with_company("rec-1", "First", "Last", "f@corp.com", "email1_sent", "co-failing")],
            [],
        ]
        attio._request.side_effect = RuntimeError("attio 502 bad gateway")

        with caplog.at_level("WARNING"):
            result = run_wave2_blast(attio, None, dry_run=True, auto_confirm=True)

        # Blast did NOT abort — the contact was still processed with a
        # blank company name.
        assert result["sent"] == 1
        # Warning carries the failing company record_id, the person
        # record_id, and the underlying error message — enough context for
        # an operator to audit the blank-company row.
        matching = [
            rec for rec in caplog.records
            if rec.levelname == "WARNING"
            and "company-fetch failed" in rec.getMessage()
            and "co-failing" in rec.getMessage()
            and "rec-1" in rec.getMessage()
            and "attio 502" in rec.getMessage()
        ]
        assert matching, (
            f"expected one WARNING with company record_id, person record_id, "
            f"and underlying error; got "
            f"{[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )

    @patch("workflows.wave2_blast.date")
    @patch("workflows.wave2_blast._build_linkedin_collision_set")
    @patch("workflows.wave2_blast.build_suppression_set")
    def test_runtime_error_does_not_poison_company_cache(
        self, mock_suppression, mock_collision, mock_date, caplog,
    ):
        """QA fold-in (convergent silent-failure-hunter CRITICAL +
        pr-test-analyzer IMPORTANT): a transient Attio failure for one
        person must NOT cache the empty-string result and silently ship
        blank-company emails for every subsequent person referencing the
        same company with only ONE warning log.

        Two persons share ``co-failing``. The fix moves the cache write
        into an ``else:`` clause so failed fetches don't memoize. Each
        affected person triggers an independent ``attio._request`` and an
        independent warning log — matching the true blast radius.
        """
        mock_date.today.return_value = date(2026, 5, 5)
        mock_collision.return_value = set()
        mock_suppression.return_value = set()

        attio = MagicMock()
        attio.search_people.side_effect = [
            [
                _make_attio_person_with_company(
                    "rec-A", "Alice", "One", "a@corp.com", "email1_sent", "co-failing"
                ),
                _make_attio_person_with_company(
                    "rec-B", "Bob", "Two", "b@corp.com", "email1_sent", "co-failing"
                ),
            ],
            [],
        ]
        attio._request.side_effect = RuntimeError("attio 502 bad gateway")

        with caplog.at_level("WARNING"):
            result = run_wave2_blast(attio, None, dry_run=True, auto_confirm=True)

        # Both contacts processed (blast didn't abort).
        assert result["sent"] == 2
        # Cache was NOT poisoned: the Attio company-fetch fired
        # independently for each person (no silent cache short-circuit on
        # the second).
        assert attio._request.call_count == 2
        # Two independent warnings — one per affected row — so oncall
        # sees the true blast radius, not one warning masking N silent
        # blank-company emails.
        person_warnings = [
            rec for rec in caplog.records
            if rec.levelname == "WARNING"
            and "company-fetch failed" in rec.getMessage()
        ]
        assert len(person_warnings) == 2, (
            f"expected 2 independent WARNING records (one per affected "
            f"person); got {len(person_warnings)}: "
            f"{[r.getMessage() for r in person_warnings]}"
        )
        # Each warning carries the right person record_id.
        warning_messages = " | ".join(r.getMessage() for r in person_warnings)
        assert "rec-A" in warning_messages
        assert "rec-B" in warning_messages
