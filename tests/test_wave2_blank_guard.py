"""Blank-template guard on the wave2 blast path.

Companion to TestEmailBlankTemplateGuard in test_email_campaign.py: wave2
renders subject and body from content/emails.json and previously ran only the
placeholder guard over their "{subject}\n{body}" join, which masks a blank
half. A wave2 template edit that empties subject or body must halt the blast
loudly before any Resend call or stage advance.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from workflows.daily_check_helpers import BlankMessageError
from workflows.wave2_blast import run_wave2_blast


def _make_attio_person(record_id, first_name, last_name, email, stage, country="US"):
    """Mock Attio person record (mirrors the wave2 suppression test fixture)."""
    return {
        "id": {"record_id": record_id},
        "values": {
            "name": [{"first_name": first_name, "last_name": last_name}],
            "email_addresses": [{"email_address": email}],
            "email_campaign_stage": [{"value": stage}],
            "primary_location": [{"country_code": country}],
            "company": [],
        },
    }


def _attio_with_stalled_contact():
    attio = MagicMock()
    attio.search_people.side_effect = [
        [_make_attio_person("rec-1", "John", "Doe", "john@acme.com", "email1_sent")],
        [],
    ]
    return attio


@patch("workflows.wave2_blast._load_wave2_template")
@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_blank_subject_halts_batch(
    mock_suppression, mock_collision, mock_date, mock_template,
):
    mock_date.today.return_value = date(2026, 5, 5)  # Tuesday
    mock_collision.return_value = set()
    mock_suppression.return_value = set()
    mock_template.return_value = {"subject": "   ", "body_html": "<p>Hola John</p>"}

    attio = _attio_with_stalled_contact()
    resend = MagicMock()

    with pytest.raises(BlankMessageError) as exc:
        run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    assert "subject" in str(exc.value)
    assert "wave2" in str(exc.value)
    resend.send_email.assert_not_called()
    attio.update_person.assert_not_called()


@patch("workflows.wave2_blast._load_wave2_template")
@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_blank_body_halts_batch(
    mock_suppression, mock_collision, mock_date, mock_template,
):
    mock_date.today.return_value = date(2026, 5, 5)
    mock_collision.return_value = set()
    mock_suppression.return_value = set()
    mock_template.return_value = {"subject": "Asunto claro", "body_html": ""}

    attio = _attio_with_stalled_contact()
    resend = MagicMock()

    with pytest.raises(BlankMessageError) as exc:
        run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    assert "body" in str(exc.value)
    resend.send_email.assert_not_called()
    attio.update_person.assert_not_called()


@patch("workflows.wave2_blast._load_wave2_template")
@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_dry_run_also_catches_blank_template(
    mock_suppression, mock_collision, mock_date, mock_template,
):
    # Dry-run must fail too, so CI catches template bugs without needing a real send.
    mock_date.today.return_value = date(2026, 5, 5)
    mock_collision.return_value = set()
    mock_suppression.return_value = set()
    mock_template.return_value = {"subject": "", "body_html": ""}

    attio = _attio_with_stalled_contact()

    with pytest.raises(BlankMessageError):
        run_wave2_blast(attio, None, dry_run=True)


@patch("workflows.wave2_blast._load_wave2_template")
@patch("workflows.wave2_blast.date")
@patch("workflows.wave2_blast._build_linkedin_collision_set")
@patch("workflows.wave2_blast.build_suppression_set")
def test_wave2_blank_template_halts_at_zero_sends(
    mock_suppression, mock_collision, mock_date, mock_template,
):
    """Pin the pre-flight contract: a blank ES template halts the blast
    BEFORE the healthy EN contact (ordered first) sends — zero sends, not a
    partial batch whose blast radius depends on contact ordering."""
    mock_date.today.return_value = date(2026, 5, 5)
    mock_collision.return_value = set()
    mock_suppression.return_value = set()
    mock_template.side_effect = lambda language: (
        {"subject": "", "body_html": "<p>Hola</p>"}
        if language == "es"
        else {"subject": "Quick check-in", "body_html": "<p>Hi John</p>"}
    )

    attio = MagicMock()
    attio.search_people.side_effect = [
        [
            _make_attio_person("rec-en", "John", "Doe", "john@acme.com", "email1_sent", country="US"),
            _make_attio_person("rec-es", "Maria", "Lopez", "maria@corp.mx", "email1_sent", country="MX"),
        ],
        [],
    ]
    resend = MagicMock()

    with pytest.raises(BlankMessageError) as exc:
        run_wave2_blast(attio, resend, dry_run=False, auto_confirm=True)

    assert "wave2" in str(exc.value)
    resend.send_email.assert_not_called()
    attio.update_person.assert_not_called()
