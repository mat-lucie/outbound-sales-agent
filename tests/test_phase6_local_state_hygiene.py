"""Phase 6 — local state hygiene.

Tests for the Attio-authority check added to ``workflows/association_outreach``:

- When AttioClient is provided, ``get_pending_association_emails`` consults
  ``outreach_channel`` on each Person record as the AUTHORITY check.
- Local file is a fast-path negative cache only.
- On divergence (local file missing contact, Attio has ``association_outreach``
  in ``outreach_channel``), Attio wins: contact excluded, local file repaired,
  ``local_ledger_divergence`` audit event emitted.
- When no AttioClient is provided, local-file-only path is unchanged.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from workflows.association_outreach import (
    _attio_already_sent,
    get_pending_association_emails,
    run_association_outreach,
)
from workflows.cross_channel_suppression import OUTREACH_CHANNEL_ASSOCIATION

# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────

def _contacts(ids: list[str]) -> list[dict]:
    """Build minimal contact dicts with unique ids."""
    return [
        {
            "id": cid,
            "email": f"{cid}@example.com",
            "name": cid.title(),
            "organization": "TestOrg",
            "subject": f"Subj {cid}",
            "body_html": "<p>hi</p>",
        }
        for cid in ids
    ]


def _attio_with_outreach_channels(
    email_to_record_id: dict[str, str],
    outreach_channel_by_record_id: dict[str, list[str]],
) -> MagicMock:
    """Build an AttioClient mock.

    ``email_to_record_id`` maps contact email → Attio record_id.
    ``outreach_channel_by_record_id`` maps record_id → list of outreach channels.
    """
    attio = MagicMock()

    def _search_people(filter_=None, limit=50, *, fail_if_truncated=False):
        email = (filter_ or {}).get("email_addresses", "")
        rid = email_to_record_id.get(email)
        if rid is None:
            return []
        return [{"id": {"record_id": rid}}]

    def _get_person(record_id):
        channels = outreach_channel_by_record_id.get(record_id, [])
        raw_channels = [{"option": {"title": ch}} for ch in channels]
        return {"id": {"record_id": record_id}, "values": {"outreach_channel": raw_channels}}

    attio.search_people.side_effect = _search_people
    attio.get_person.side_effect = _get_person
    return attio


# ────────────────────────────────────────────────────────────────────────────
# Unit tests for _attio_already_sent
# ────────────────────────────────────────────────────────────────────────────


class TestAttioAlreadySent:
    """Unit-test the internal helper that checks Attio outreach_channel."""

    def test_returns_true_when_channel_present(self):
        attio = _attio_with_outreach_channels(
            {"alice@example.com": "rid-1"},
            {"rid-1": [OUTREACH_CHANNEL_ASSOCIATION]},
        )
        contact = _contacts(["alice"])[0]
        assert _attio_already_sent(attio, contact) is True

    def test_returns_false_when_channel_absent(self):
        attio = _attio_with_outreach_channels(
            {"alice@example.com": "rid-1"},
            {"rid-1": ["linkedin"]},
        )
        contact = _contacts(["alice"])[0]
        assert _attio_already_sent(attio, contact) is False

    def test_returns_none_when_person_not_found(self):
        attio = _attio_with_outreach_channels({}, {})
        contact = _contacts(["nobody"])[0]
        assert _attio_already_sent(attio, contact) is None

    def test_returns_false_when_outreach_channel_empty(self):
        attio = _attio_with_outreach_channels(
            {"alice@example.com": "rid-1"},
            {"rid-1": []},
        )
        contact = _contacts(["alice"])[0]
        assert _attio_already_sent(attio, contact) is False

    def test_returns_none_on_attio_error(self):
        """Three-state contract: if Attio is unreachable the helper returns None
        (skip-this-run-only).  Returning True (old fail-closed) would write the
        contact into the local ledger, permanently skipping it after any transient
        outage — the critical bug fixed in Phase-6 review.
        """
        attio = MagicMock()
        attio.search_people.side_effect = RuntimeError("network error")
        contact = _contacts(["alice"])[0]
        # should not raise; should return None (skip this run, no local write)
        result = _attio_already_sent(attio, contact)
        assert result is None

    def test_contact_with_no_email_returns_none(self):
        attio = MagicMock()
        contact = {"id": "x", "name": "No Email", "email": ""}
        assert _attio_already_sent(attio, contact) is None
        attio.search_people.assert_not_called()


# ────────────────────────────────────────────────────────────────────────────
# get_pending_association_emails — Attio authority
# ────────────────────────────────────────────────────────────────────────────


class TestGetPendingAttioAuthority:
    """get_pending_association_emails with Attio client provided."""

    def test_attio_authority_excludes_already_sent(self, tmp_path):
        contacts = _contacts(["alice", "bob"])
        attio = _attio_with_outreach_channels(
            {"alice@example.com": "rid-alice", "bob@example.com": "rid-bob"},
            # alice already sent via Attio
            {"rid-alice": [OUTREACH_CHANNEL_ASSOCIATION], "rid-bob": []},
        )
        with (
            patch("workflows.association_outreach.SENT_STATE_FILE", tmp_path / "sent.json"),
            patch("workflows.association_outreach._load_contacts", return_value=contacts),
        ):
            pending = get_pending_association_emails(attio=attio)

        ids = [c["id"] for c in pending]
        assert "alice" not in ids
        assert "bob" in ids

    def test_local_file_fast_path_skips_attio_read(self, tmp_path):
        """Contact already in local file: Attio should NOT be consulted."""
        contacts = _contacts(["alice"])
        sent_file = tmp_path / "sent.json"
        sent_file.write_text(
            json.dumps({"alice": {"sent_at": "2026-01-01T00:00:00Z", "email": "alice@example.com"}})
        )
        attio = MagicMock()

        with (
            patch("workflows.association_outreach.SENT_STATE_FILE", sent_file),
            patch("workflows.association_outreach._load_contacts", return_value=contacts),
        ):
            pending = get_pending_association_emails(attio=attio)

        assert pending == []
        # Attio was never called — local file was the fast-path negative cache
        attio.search_people.assert_not_called()

    def test_divergence_repairs_local_file(self, tmp_path):
        """Attio says sent, local file does not. Local file must be repaired."""
        contacts = _contacts(["alice"])
        sent_file = tmp_path / "sent.json"
        # Local file starts empty
        sent_file.write_text("{}")
        attio = _attio_with_outreach_channels(
            {"alice@example.com": "rid-alice"},
            {"rid-alice": [OUTREACH_CHANNEL_ASSOCIATION]},
        )

        with (
            patch("workflows.association_outreach.SENT_STATE_FILE", sent_file),
            patch("workflows.association_outreach._load_contacts", return_value=contacts),
        ):
            pending = get_pending_association_emails(attio=attio)

        # alice is excluded (Attio wins)
        assert pending == []
        # Local file is repaired
        repaired = json.loads(sent_file.read_text())
        assert "alice" in repaired
        assert repaired["alice"]["repaired_from_attio"] is True

    def test_divergence_emits_audit_event(self, tmp_path):
        """local_ledger_divergence audit event emitted on divergence."""
        contacts = _contacts(["alice"])
        sent_file = tmp_path / "sent.json"
        sent_file.write_text("{}")
        attio = _attio_with_outreach_channels(
            {"alice@example.com": "rid-alice"},
            {"rid-alice": [OUTREACH_CHANNEL_ASSOCIATION]},
        )
        audit_logger = MagicMock()

        with (
            patch("workflows.association_outreach.SENT_STATE_FILE", sent_file),
            patch("workflows.association_outreach._load_contacts", return_value=contacts),
        ):
            get_pending_association_emails(attio=attio, audit_logger=audit_logger)

        audit_logger.event.assert_called_once()
        args, kwargs = audit_logger.event.call_args
        assert args[0] == "local_ledger_divergence"
        assert kwargs.get("contact_id") == "alice"
        assert kwargs.get("ledger") == "association_outreach_sent.json"
        assert kwargs.get("resolution") == "attio_wins"

    def test_no_attio_uses_local_file_only(self, tmp_path):
        """Legacy path: no attio client → local file is sole guard."""
        contacts = _contacts(["alice", "bob"])
        sent_file = tmp_path / "sent.json"
        sent_file.write_text(
            json.dumps({"alice": {"sent_at": "2026-01-01T00:00:00Z", "email": "alice@example.com"}})
        )

        with (
            patch("workflows.association_outreach.SENT_STATE_FILE", sent_file),
            patch("workflows.association_outreach._load_contacts", return_value=contacts),
        ):
            pending = get_pending_association_emails(attio=None)

        ids = [c["id"] for c in pending]
        assert "alice" not in ids
        assert "bob" in ids

    def test_multiple_divergences_in_one_run(self, tmp_path):
        """Multiple divergences are each repaired and emit their own audit event."""
        contacts = _contacts(["alice", "bob", "carol"])
        sent_file = tmp_path / "sent.json"
        sent_file.write_text("{}")
        attio = _attio_with_outreach_channels(
            {
                "alice@example.com": "rid-alice",
                "bob@example.com": "rid-bob",
                "carol@example.com": "rid-carol",
            },
            {
                "rid-alice": [OUTREACH_CHANNEL_ASSOCIATION],
                "rid-bob": [],
                "rid-carol": [OUTREACH_CHANNEL_ASSOCIATION],
            },
        )
        audit_logger = MagicMock()

        with (
            patch("workflows.association_outreach.SENT_STATE_FILE", sent_file),
            patch("workflows.association_outreach._load_contacts", return_value=contacts),
        ):
            pending = get_pending_association_emails(attio=attio, audit_logger=audit_logger)

        # only bob is pending
        assert [c["id"] for c in pending] == ["bob"]
        # two divergence events emitted
        assert audit_logger.event.call_count == 2
        event_names = [c.args[0] for c in audit_logger.event.call_args_list]
        assert all(e == "local_ledger_divergence" for e in event_names)
        # both alice and carol are repaired
        repaired = json.loads(sent_file.read_text())
        assert "alice" in repaired
        assert "carol" in repaired


# ────────────────────────────────────────────────────────────────────────────
# run_association_outreach — attio parameter threaded through
# ────────────────────────────────────────────────────────────────────────────


class TestRunAssociationOutreachAttioParam:
    """Smoke-test that run_association_outreach accepts and threads through attio."""

    @patch("workflows.association_outreach.is_send_day", return_value=True)
    @patch("workflows.association_outreach.get_pending_association_emails", return_value=[])
    def test_passes_attio_to_get_pending(self, mock_pending, mock_is_send_day):
        attio = MagicMock()
        run_association_outreach(resend=None, dry_run=True, attio=attio)
        mock_pending.assert_called_once()
        _, kwargs = mock_pending.call_args
        assert kwargs.get("attio") is attio

    @patch("workflows.association_outreach.is_send_day", return_value=True)
    @patch("workflows.association_outreach.get_pending_association_emails", return_value=[])
    def test_no_attio_still_works(self, mock_pending, mock_is_send_day):
        run_association_outreach(resend=None, dry_run=True)
        mock_pending.assert_called_once()
        _, kwargs = mock_pending.call_args
        assert kwargs.get("attio") is None
