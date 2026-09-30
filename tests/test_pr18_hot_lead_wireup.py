"""PR-18 fold-in integration tests for emit_hot_lead wiring.

Cross-agent QA convergence flagged: the unit tests in
test_pr18_hot_lead_alert.py prove ``emit_hot_lead`` works in isolation
but nothing proves ``detect_responses()`` actually invokes it on the
right code path. This file fills GAP-1 + GAP-3 + GAP-4:

1. Positive reply → emit_hot_lead called once with the right record_id
2. Defensive reply → emit_hot_lead NOT called (routing to DEFENSIVE_HOLD)
3. ``HotLeadEmitFailed`` re-raises from detect_responses (loop halts)
4. Double-emit for the same record_id is idempotent (queue refresh, not duplicate)
5. ``hot_lead|`` namespace prefix locked on secondary ``resend_delivery_failed``
"""
from __future__ import annotations

import csv
import io
from unittest.mock import MagicMock, patch

import httpx
import pytest

from models.pipeline import PipelineStage
from workflows.hot_lead_alert import HotLeadEmitFailed, emit_hot_lead


def _make_sn_csv(**row: str) -> str:
    """Build a single-row SN inbox scraper CSV."""
    row.setdefault("participantProfileUrl", "https://linkedin.com/in/test-prospect")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
    return output.getvalue()


def _attio_with_one_dm_prospect(
    name: str, stage: str = "DM1 Sent", quality_score: int = 80,
) -> tuple[MagicMock, dict]:
    """Wire an AttioClient mock that returns a single DM-stage prospect.

    Entry shape matches what ``AttioClient.parse_entry`` reads: flat
    ``parent_record_id`` + ``entry_id`` keys (or nested ``id: {entry_id,
    record_id}``). Uses the flat form to keep tests compact.
    """
    entry = {
        "entry_id": "ent-1",
        "parent_record_id": "rec-1",
        "entry_values": {
            "stage": [{"status": {"title": stage}}],
            "quality_score": [{"value": quality_score}],
            "persona": [{"option": {"title": "operations_leaders"}}],
            "language": [{"option": {"title": "es"}}],
            "last_contact_date": [{"value": "2026-05-19"}],
            "dm_step": [{"value": 1}],
        },
    }
    attio = MagicMock()
    attio.query_list_entries.return_value = [entry]
    attio.update_list_entry.return_value = None
    return attio, entry


def _run_detect(attio, pb, *, csv: str, prospect_name: str, resend=None):
    """Helper: bind common detect_responses call with a configured PB mock."""
    from workflows.detect_responses import detect_responses

    pb.launch_agent.return_value = MagicMock(container_id="c_test")
    pb.wait_for_completion.return_value = MagicMock(status="finished")
    pb.download_result_csv.return_value = csv

    with patch("workflows.detect_responses.RecordCache.get") as cache_get, \
         patch.dict("os.environ", {"ATTIO_LIST_ID": "list-1"}, clear=False):
        cache_get.return_value = (prospect_name, "ACME", "https://linkedin.com/in/test-prospect", "", "")
        return detect_responses(attio, pb, "agent-inbox", resend=resend)


# ── GAP-1 — emit_hot_lead is called on positive replies ─────────────────


def test_detect_responses_calls_emit_hot_lead_on_positive_reply():
    """The whole point of PR-18: a positive reply in the inbox scrape
    must invoke emit_hot_lead with the matching record_id, classification,
    and message excerpt. Unit tests on the module prove emit_hot_lead
    works in isolation; this proves detect_responses wires it correctly."""
    attio, _ = _attio_with_one_dm_prospect("Carlos Perez")
    pb = MagicMock()
    csv = _make_sn_csv(
        participantFullName="Carlos Perez",
        isLastMessageFromMe="false",
        lastMessageBody="Yes please demo us next week",
        totalMessageCount="2",
    )

    with patch("workflows.detect_responses.emit_hot_lead") as mock_emit:
        _run_detect(attio, pb, csv=csv, prospect_name="Carlos Perez")

    assert mock_emit.called, "emit_hot_lead was not invoked for a positive reply"
    call_kwargs = mock_emit.call_args.kwargs
    assert call_kwargs["record_id"] == "rec-1"
    assert call_kwargs["response_classification"] == "positive"
    assert "demo" in call_kwargs["message_excerpt"].lower()


def test_detect_responses_does_not_call_emit_hot_lead_on_defensive_reply():
    """Defensive replies route to DEFENSIVE_HOLD; the spec is explicit
    that emit_hot_lead MUST NOT fire. Locks in the routing decision at
    the call site, not just in the gate function."""
    attio, _ = _attio_with_one_dm_prospect("Patricia Ejemplo")
    pb = MagicMock()
    # "estoy segura que no" is a canonical defensive-keyword phrase per
    # workflows/quality_gate.py:1181. Comma-free + at the start so the
    # keyword matcher finds it cleanly.
    csv = _make_sn_csv(
        participantFullName="Patricia Ejemplo",
        isLastMessageFromMe="false",
        lastMessageBody="estoy segura que no aplica a nuestra operación",
        totalMessageCount="2",
    )

    with patch("workflows.detect_responses.emit_hot_lead") as mock_emit:
        _run_detect(attio, pb, csv=csv, prospect_name="Patricia Ejemplo")

    assert not mock_emit.called, (
        "emit_hot_lead fired for a defensive reply — defensive routes to "
        "DEFENSIVE_HOLD, not hot-lead. §3.6 violation."
    )
    # Verify the stage write went to DEFENSIVE_HOLD.
    update_call = attio.update_list_entry.call_args
    assert update_call is not None
    new_attrs = update_call[1]["entry_attributes"]
    assert new_attrs["stage"] == PipelineStage.DEFENSIVE_HOLD.value


def test_detect_responses_does_not_call_emit_hot_lead_on_negative_reply():
    """Negative routes to NOT_INTERESTED; hot-lead must not fire."""
    attio, _ = _attio_with_one_dm_prospect("José Martínez")
    pb = MagicMock()
    csv = _make_sn_csv(
        participantFullName="José Martínez",
        isLastMessageFromMe="false",
        lastMessageBody="No me interesa, gracias.",
        totalMessageCount="2",
    )

    with patch("workflows.detect_responses.emit_hot_lead") as mock_emit:
        _run_detect(attio, pb, csv=csv, prospect_name="José Martínez")

    assert not mock_emit.called


# ── GAP-1 (HotLeadEmitFailed propagation) ───────────────────────────────


def test_detect_responses_reraises_HotLeadEmitFailed():
    """If emit_hot_lead raises ``HotLeadEmitFailed`` (the queue write
    failed → durability guarantee broken), detect_responses must
    propagate the exception so the daily run halts. The loop must NOT
    swallow this error and continue processing more replies — that
    would leave the operator blind to multiple hot leads."""
    attio, _ = _attio_with_one_dm_prospect("Carlos Perez")
    pb = MagicMock()
    csv = _make_sn_csv(
        participantFullName="Carlos Perez",
        isLastMessageFromMe="false",
        lastMessageBody="Yes please demo us next week",
        totalMessageCount="2",
    )

    with patch("workflows.detect_responses.emit_hot_lead") as mock_emit:
        mock_emit.side_effect = HotLeadEmitFailed(
            "queue write failed for rec-1"
        )
        with pytest.raises(HotLeadEmitFailed, match="rec-1"):
            _run_detect(attio, pb, csv=csv, prospect_name="Carlos Perez")


# ── GAP-3 — Idempotency double-invocation ───────────────────────────────


def test_emit_hot_lead_idempotency_same_record_id_refreshes_not_duplicates():
    """Two consecutive emits for the same record_id must result in ONE
    queue row (refresh), not two. The contract is documented in the
    module docstring; this test pins it via the escalate() idempotency
    behavior on (type='hot_lead_positive_reply', idempotency_key=record_id).
    """
    call_count = {"create": 0, "query": 0}

    def _attio_request(method, path, **kwargs):
        if "/query" in path:
            call_count["query"] += 1
            # Second emit sees the row created by the first emit.
            if call_count["create"] == 0:
                return {"data": []}
            return {"data": [
                {"id": {"record_id": "rec_q1"}, "values": {}}
            ]}
        call_count["create"] += 1
        return {"data": {"id": {"record_id": "rec_q1"}, "values": {}}}

    attio = MagicMock()
    attio._request.side_effect = _attio_request

    for _ in range(2):
        emit_hot_lead(
            record_id="rec_lead_double",
            response_classification="positive",
            prospect_score=90,
            message_excerpt="Yes",
            thread_url="https://x",
            attio=attio,
            resend=None,
        )

    assert call_count["create"] == 1, (
        f"expected ONE create for double-emit on same record_id, got "
        f"{call_count['create']}; idempotency contract broken"
    )
    assert call_count["query"] == 2, (
        "expected TWO queries (one per emit), the second hitting cache"
    )


# ── GAP-4 — Namespace prefix lock on secondary resend escalation ────────


def test_resend_delivery_failed_uses_hot_lead_prefix():
    """When Resend fails, the secondary ``resend_delivery_failed``
    escalation must use ``f"hot_lead|{record_id}"`` as its
    idempotency_key. The ``hot_lead|`` prefix namespace-separates from
    PR-30's weekly-report Resend failures (which use
    ``f"weekly_report|{week_starting}"``). A future cleanup that drops
    the prefix would risk collision; locking the prefix here prevents
    that regression."""
    captured: list[dict] = []

    def _attio_request(method, path, **kwargs):
        body = kwargs.get("json") or {}
        if "/query" in path:
            return {"data": []}
        # Capture create calls — both the hot_lead and resend_delivery_failed
        # rows land via this path.
        captured.append(body.get("data", {}).get("values", {}))
        return {"data": {"id": {"record_id": "rec_q"}, "values": {}}}

    attio = MagicMock()
    attio._request.side_effect = _attio_request

    resend = MagicMock()
    req = httpx.Request("POST", "https://api.resend.com/emails")
    resend.send_email.side_effect = httpx.RequestError("down", request=req)

    emit_hot_lead(
        record_id="rec_namespace_test",
        response_classification="positive",
        prospect_score=85,
        message_excerpt="Yes",
        thread_url="https://x",
        attio=attio,
        resend=resend,
    )

    # Find the resend_delivery_failed creation.
    resend_failed_rows = [
        v for v in captured
        if v.get("type") == "resend_delivery_failed"
    ]
    assert resend_failed_rows, (
        f"no resend_delivery_failed escalation fired despite Resend failure; "
        f"captured rows: {captured}"
    )
    key = resend_failed_rows[0].get("idempotency_key", "")
    assert key.startswith("hot_lead|"), (
        f"resend_delivery_failed idempotency_key must start with "
        f"'hot_lead|' to namespace-separate from PR-30's weekly-report "
        f"failures; got {key!r}"
    )
    assert key == "hot_lead|rec_namespace_test"
