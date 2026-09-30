"""Tests for workflows/detect_email_responses.py (Phase 0.6, PR-243).

Mirrors tests/test_detect_responses.py: skip contract, idempotency,
classification -> stage routing, auto-reply filtering, note creation,
Gmail-error resilience, and the load-bearing write ordering.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from models.email_campaign import ACTIVE_STAGES, EmailStage
from workflows.detect_email_responses import (
    MAX_RESPONSE_TEXT_LEN,
    _reconstruct_opener,
    detect_email_responses,
)

# ── Helpers ─────────────────────────────────────────────────────────


def _person(
    record_id: str = "rec-1",
    email: str = "prospect@acme.com",
    stage: str = "email1_sent",
    started: str = "2026-07-01",
    response_received_at: str = "",
) -> dict:
    values = {
        "email_addresses": [{"email_address": email}],
        "email_campaign_stage": [{"value": stage}],
        "email_campaign_started": [{"value": started}],
    }
    if response_received_at:
        values["email_response_received_at"] = [{"value": response_received_at}]
    return {"id": {"record_id": record_id}, "values": values}


def _attio_with(people: list[dict], stage: str = "email1_sent") -> MagicMock:
    """Attio mock returning `people` for one scan stage, [] for the rest."""
    attio = MagicMock()

    def search(filter_=None, **kwargs):
        if (filter_ or {}).get("email_campaign_stage") == stage:
            return people
        return []

    attio.search_people.side_effect = search
    return attio


def _gmail_with(body: str, headers: dict | None = None, internal: int = 1000) -> MagicMock:
    gmail = MagicMock()
    gmail.search_inbound.return_value = [{"message_id": "m1", "thread_id": "t1"}]
    gmail.get_message.return_value = (body, headers or {"from": "prospect@acme.com"}, internal)
    return gmail


def _classifier(classification: str, source: str = "llm") -> dict:
    return {
        "classification": classification,
        "source": source,
        "reasoning": "r",
        "suggested_action": "a",
        "summary": "s",
    }


def _run(attio, gmail, classification="negative", source="llm"):
    with (
        patch(
            "workflows.detect_email_responses.classify_reply_llm",
            return_value=_classifier(classification, source),
        ) as mock_classify,
        patch("workflows.detect_email_responses.AttioWriter") as mock_writer_cls,
        patch("workflows.detect_email_responses.escalate") as mock_escalate,
    ):
        counts = detect_email_responses(attio, gmail)
    return counts, mock_classify, mock_writer_cls, mock_escalate


# ── Skip contract ───────────────────────────────────────────────────


def test_skips_when_no_gmail_client():
    result = detect_email_responses(MagicMock(), None)
    assert result["skipped"] is True
    assert result["reason"] == "no_gmail_client"
    assert result["detected"] == 0


def test_zero_counts_when_no_prospects():
    attio = _attio_with([])
    counts, *_ = _run(attio, MagicMock())
    assert counts["detected"] == 0
    assert counts["scanned"] == 0


# ── Idempotency ─────────────────────────────────────────────────────


def test_skips_already_processed_person():
    attio = _attio_with([_person(response_received_at="2026-07-10T12:00:00+00:00")])
    gmail = _gmail_with("no thanks")
    counts, *_ = _run(attio, gmail)
    assert counts["already_processed"] == 1
    assert counts["detected"] == 0
    gmail.search_inbound.assert_not_called()


def test_missing_start_timestamp_counted_not_dropped_silently():
    """A prospect in a scan stage with no email_campaign_started/last_sent is
    unscannable (can't be date-bounded). It must be COUNTED in the rollup, not
    dropped invisibly."""
    person = _person()
    person["values"]["email_campaign_started"] = []  # no start timestamp at all
    attio = _attio_with([person])
    gmail = _gmail_with("no thanks")
    counts, *_ = _run(attio, gmail)
    assert counts["no_start_timestamp"] == 1
    assert counts["detected"] == 0
    gmail.search_inbound.assert_not_called()


# ── Stage routing ───────────────────────────────────────────────────


def test_negative_reply_routes_to_not_interested():
    attio = _attio_with([_person()])
    gmail = _gmail_with("We'll build it in-house, thanks.")
    counts, _, _, _ = _run(attio, gmail, classification="negative")
    assert counts["detected"] == 1
    assert counts["negative"] == 1
    attio.update_person.assert_called_once_with(
        "rec-1", {"email_campaign_stage": EmailStage.NOT_INTERESTED.value}
    )


@pytest.mark.parametrize("classification", ["positive", "question", "neutral", "defensive"])
def test_non_negative_replies_route_to_responded(classification):
    attio = _attio_with([_person()])
    gmail = _gmail_with("tell me more")
    counts, *_ = _run(attio, gmail, classification=classification)
    assert counts["detected"] == 1
    attio.update_person.assert_called_once_with(
        "rec-1", {"email_campaign_stage": EmailStage.RESPONDED.value}
    )


def test_terminal_stages_not_in_active_stages():
    """Load-bearing sequencer-stop invariant: terminal reply stages must
    never be selected for email2/3."""
    assert EmailStage.RESPONDED not in ACTIVE_STAGES
    assert EmailStage.NOT_INTERESTED not in ACTIVE_STAGES


# ── Response attrs write via AttioWriter ────────────────────────────


def test_response_attrs_written_via_attio_writer():
    attio = _attio_with([_person()])
    from datetime import UTC, datetime

    reply_ms = 1752345600000
    expected_prefix = datetime.fromtimestamp(reply_ms / 1000, tz=UTC).isoformat()[:10]
    gmail = _gmail_with("x" * 2000, internal=reply_ms)
    _, _, mock_writer_cls, _ = _run(attio, gmail, classification="negative")
    intent = mock_writer_cls.return_value.apply.call_args.args[0]
    assert intent.object == "people"
    assert intent.record_id == "rec-1"
    assert intent.updates["email_response_classification"] == "negative"
    assert intent.updates["email_response_received_at"].startswith(expected_prefix)
    assert len(intent.updates["last_email_response_text"]) == MAX_RESPONSE_TEXT_LEN
    assert intent.writer_module == "workflows.detect_email_responses.detect_email_responses"


def test_stage_flip_failure_writes_nothing_else():
    """Write ordering is load-bearing: the stage flip lands FIRST. If it
    fails, the idempotency marker must NOT be written (attrs skipped), so
    the prospect retries cleanly next run instead of being stranded in an
    active stage while marked processed."""
    import httpx

    attio = _attio_with([_person()])
    attio.update_person.side_effect = httpx.RequestError("attio down")
    gmail = _gmail_with("no thanks")
    counts, _, mock_writer_cls, mock_escalate = _run(attio, gmail, classification="negative")
    assert counts["attio_update_failures"] == 1
    assert counts["detected"] == 0
    mock_writer_cls.return_value.apply.assert_not_called()
    attio.create_note.assert_not_called()
    mock_escalate.assert_not_called()


def test_attr_write_failure_after_stage_flip_still_counts_detected():
    """An attr-write failure after the stage flip only loses enrichment:
    the sequencer stop already landed, detection is counted, and
    AttioWriter has DLQ'd + escalated."""
    from clients.attio_writer import AttioError

    attio = _attio_with([_person()])
    gmail = _gmail_with("no thanks")
    with (
        patch(
            "workflows.detect_email_responses.classify_reply_llm",
            return_value=_classifier("negative"),
        ),
        patch("workflows.detect_email_responses.AttioWriter") as mock_writer_cls,
        patch("workflows.detect_email_responses.escalate"),
    ):
        mock_writer_cls.return_value.apply.side_effect = AttioError("boom")
        counts = detect_email_responses(attio, gmail)
    assert counts["attio_update_failures"] == 1
    assert counts["detected"] == 1
    attio.update_person.assert_called_once_with(
        "rec-1", {"email_campaign_stage": EmailStage.NOT_INTERESTED.value}
    )


# ── Auto-generated filtering ────────────────────────────────────────


def test_auto_reply_does_not_flip_stage():
    attio = _attio_with([_person()])
    gmail = _gmail_with(
        "I am out of office until Aug 1",
        headers={"auto-submitted": "auto-replied"},
    )
    counts, mock_classify, _, _ = _run(attio, gmail)
    assert counts["auto_generated_skipped"] == 1
    assert counts["detected"] == 0
    mock_classify.assert_not_called()
    attio.update_person.assert_not_called()


def test_newest_real_message_wins():
    attio = _attio_with([_person()])
    gmail = MagicMock()
    gmail.search_inbound.return_value = [
        {"message_id": "m1", "thread_id": "t"},
        {"message_id": "m2", "thread_id": "t"},
    ]
    gmail.get_message.side_effect = [
        ("older reply", {"from": "f@f.com"}, 100),
        ("newer reply", {"from": "f@f.com"}, 200),
    ]
    with (
        patch(
            "workflows.detect_email_responses.classify_reply_llm",
            return_value=_classifier("neutral"),
        ) as mock_classify,
        patch("workflows.detect_email_responses.AttioWriter"),
        patch("workflows.detect_email_responses.escalate"),
    ):
        counts = detect_email_responses(attio, gmail)
    assert counts["detected"] == 1
    assert mock_classify.call_args.args[1] == "newer reply"


# ── Note + escalation ───────────────────────────────────────────────


def test_note_and_queue_row_created():
    attio = _attio_with([_person()])
    gmail = _gmail_with("no thanks, we build internally")
    _, _, _, mock_escalate = _run(attio, gmail, classification="negative")
    note_kwargs = attio.create_note.call_args.kwargs
    assert "negative" in note_kwargs["title"]
    assert "we build internally" in note_kwargs["content"]
    esc_kwargs = mock_escalate.call_args.kwargs
    assert esc_kwargs["type"] == "email_response_detected"
    assert esc_kwargs["idempotency_key"].startswith("email-reply|rec-1|")
    assert esc_kwargs["payload"]["classification"] == "negative"


def test_note_failure_does_not_fail_detection():
    import httpx

    attio = _attio_with([_person()])
    attio.create_note.side_effect = httpx.RequestError("nope")
    gmail = _gmail_with("no")
    counts, *_ = _run(attio, gmail, classification="negative")
    assert counts["detected"] == 1
    attio.update_person.assert_called_once()


# ── Gmail error resilience ──────────────────────────────────────────


def test_gmail_search_error_skips_prospect_not_run():
    attio = _attio_with([_person(record_id="rec-1"), _person(record_id="rec-2", email="b@b.com")])
    gmail = MagicMock()
    gmail.search_inbound.side_effect = [
        RuntimeError("429 rate limited"),
        [{"message_id": "m1", "thread_id": "t"}],
    ]
    gmail.get_message.return_value = ("reply", {"from": "b@b.com"}, 100)
    with (
        patch(
            "workflows.detect_email_responses.classify_reply_llm",
            return_value=_classifier("neutral"),
        ),
        patch("workflows.detect_email_responses.AttioWriter"),
        patch("workflows.detect_email_responses.escalate"),
    ):
        counts = detect_email_responses(attio, gmail)
    assert counts["gmail_errors"] == 1
    assert counts["detected"] == 1


# ── Blank opener guard ──────────────────────────────────────────────


@pytest.fixture()
def _fresh_opener_cache():
    """Isolate _reconstruct_opener's lru_cache from other tests."""
    _reconstruct_opener.cache_clear()
    yield
    _reconstruct_opener.cache_clear()


def test_blank_rendered_opener_warns_on_stderr(_fresh_opener_cache, capsys):
    with patch(
        "workflows.detect_email_responses.get_email_template",
        return_value={"subject": "", "body_html": "<p>   </p>"},
    ):
        _reconstruct_opener("en")
    err = capsys.readouterr().err
    assert "blank" in err
    assert "'en'" in err
    assert "content/emails.json" in err


def test_template_missing_keys_warns_on_stderr(_fresh_opener_cache, capsys):
    with patch(
        "workflows.detect_email_responses.get_email_template",
        return_value={},
    ):
        _reconstruct_opener("es")
    err = capsys.readouterr().err
    assert "blank" in err
    assert "'es'" in err


def test_healthy_opener_no_warning(_fresh_opener_cache, capsys):
    opener = _reconstruct_opener("en")
    assert "Subject:" in opener
    assert len(opener) > 50
    assert capsys.readouterr().err == ""
