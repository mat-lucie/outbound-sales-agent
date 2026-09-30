"""Task 5 wire-up tests: LLM classifier integration in detect_responses.

Covers:
- LLM classifier is called with reconstructed opener context
- Keyword fallback path (no ANTHROPIC_API_KEY) still correctly classifies
  Patricia's canonical reply as defensive end-to-end
- Defensive replies route to RESPONDED (not NOT_INTERESTED)
- response_classification and last_response_text are written to Attio
- last_response_text truncation at 1000 chars
- Attio update failures are counted and logged, not silent
- source field ("llm" vs "keyword") tracked in the return counts
"""

from __future__ import annotations

import csv
import io
from unittest.mock import MagicMock, patch

from models.pipeline import PipelineStage
from workflows.detect_responses import (
    MAX_RESPONSE_TEXT_LEN,
    _reconstruct_opener,
    detect_responses,
)

# ── Helpers ─────────────────────────────────────────────────────────


def _make_entry(
    entry_id: str,
    record_id: str,
    stage: str,
    persona: str = "operations_leaders",
    language: str = "es",
) -> dict:
    return {
        "entry_id": entry_id,
        "record_id": record_id,
        "stage": stage,
        "persona": persona,
        "language": language,
        "dm_step": None,
        "quality_score": 75,
        "last_contact_date": "2026-04-01",
        "experiment_id": "exp-042",
        "response_classification": None,
        "last_response_text": None,
    }


def _make_sn_csv(**fields) -> str:
    fieldnames = [
        "participantProfileUrl",
        "participantFullName",
        "isLastMessageFromMe",
        "lastMessageBody",
        "lastMessageDate",
        # Wave-1.6-ext: needed to exercise the manual-reply branch
        # (_handle_manual_reply at detect_responses.py:444) which compares
        # totalMessageCount against EXPECTED_OUR_MESSAGES per stage.
        "totalMessageCount",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    fields.setdefault("participantProfileUrl", "https://linkedin.com/in/p")
    writer.writerow({k: fields.get(k, "") for k in fieldnames})
    return buf.getvalue()


def _run_detect(
    attio,
    pb,
    entry,
    csv_content,
    prospect_name="Patricia Ejemplo",
    company="Novo Nordisk",
):
    attio.query_list_entries.return_value = [entry]
    pb.download_result_csv.return_value = csv_content

    # Wave-2-B: AttioWriter requires a non-empty list_id when
    # is_list_entry=True. detect_responses reads ATTIO_LIST_ID from
    # env; seed it so the writer doesn't fail-loud on an unrelated
    # configuration miss in tests. Also reduce the retry budget so
    # transient-5xx tests don't spend ~30s burning AttioWriter
    # retries before giving up.
    with (
        patch.dict("os.environ", {"ATTIO_LIST_ID": "test-list-id"}, clear=False),
        patch("clients.attio_writer.RETRY_BUDGET_SECONDS", 0.0001),
        patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e),
        patch("workflows.detect_responses._pb_session_args", return_value={}),
        patch("workflows.detect_responses.RecordCache") as MockCache,
    ):
        mock_cache_instance = MagicMock()
        mock_cache_instance.get.return_value = (prospect_name, company, "https://linkedin.com/in/p", "", "")
        MockCache.return_value = mock_cache_instance
        return detect_responses(attio, pb, inbox_scraper_id="scraper-x")


# ── _reconstruct_opener unit ────────────────────────────────────────


class TestReconstructOpener:
    def test_rebuilds_dm1_for_persona_language(self):
        opener = _reconstruct_opener(
            persona_value="digitalization_champions",
            language_value="es",
            prospect_name="Patricia Ejemplo",
            company="Novo Nordisk",
        )
        # dm1 template references the company and uses the first name
        assert "Novo Nordisk" in opener
        assert "Patricia" in opener
        # Genericization: the opener is reconstructed from whatever DM1 copy the
        # active content dir ships, so we no longer assert any operator-specific
        # brand string. Intent preserved: personalization produced a substantive
        # opener (the company + name made it in, copy is non-trivial).
        assert len(opener) > 20
        # No literal placeholders should leak through personalization
        assert "[Name]" not in opener and "[Company]" not in opener

    def test_falls_back_to_ops_leaders_es_for_missing_persona(self):
        opener = _reconstruct_opener(
            persona_value=None,
            language_value=None,
            prospect_name="Test User",
            company="Acme",
        )
        # OPERATIONS_LEADERS + ES — still produces a real template
        assert opener
        assert "Acme" in opener or "your company" in opener or len(opener) > 20

    def test_gracefully_handles_unknown_persona(self):
        """A corrupt persona value mustn't crash the whole detect run."""
        opener = _reconstruct_opener(
            persona_value="does_not_exist",
            language_value="es",
            prospect_name="Test",
            company="Corp",
        )
        assert isinstance(opener, str)
        assert len(opener) > 0

    def test_anonymous_name_coerces_to_placeholder(self):
        opener = _reconstruct_opener(
            persona_value="operations_leaders",
            language_value="en",
            prospect_name="",
            company="Corp",
        )
        assert isinstance(opener, str)


# ── Defensive routing (end-to-end, keyword fallback path) ───────────


class TestDefensiveRoutingKeywordFallback:
    """The ANTHROPIC_API_KEY is unset by conftest autouse fixture, so the
    LLM path falls back to keyword classification inside classify_reply_llm.
    These tests verify the end-to-end wire-up works without a real API key.
    """

    def test_patricia_reply_classified_as_defensive(self):
        attio = MagicMock()
        pb = MagicMock()

        entry = _make_entry("e1", "r1", PipelineStage.DM1_SENT.value)
        csv_content = _make_sn_csv(
            participantFullName="Patricia Ejemplo",
            isLastMessageFromMe="false",
            lastMessageBody=(
                "Hola Andrés no tenemos plantas de producción en Mexico. "
                "Pero estoy segura que no viven en Excel con toda certeza. Saludos"
            ),
        )

        result = _run_detect(attio, pb, entry, csv_content, prospect_name="Patricia Ejemplo")

        assert result["detected"] == 1
        assert result["defensive"] == 1
        assert result["negative"] == 0
        assert result["classifier_keyword"] == 1  # fallback fired
        assert result["classifier_llm"] == 0

        update_call = attio.update_list_entry.call_args
        new_attrs = update_call[1]["entry_attributes"]
        # PR-18 B-SD-003: defensive replies route to DEFENSIVE_HOLD,
        # not RESPONDED. The prospect signaled reactance, not interest.
        assert new_attrs["stage"] == PipelineStage.DEFENSIVE_HOLD.value
        assert new_attrs["response_classification"] == "defensive"
        assert "estoy segura que no" in new_attrs["last_response_text"]

    def test_positive_reply_writes_to_attio(self):
        attio = MagicMock()
        pb = MagicMock()

        entry = _make_entry("e2", "r2", PipelineStage.DM2_SENT.value)
        reply_body = "Interesado, agendemos una demo"
        csv_content = _make_sn_csv(
            participantFullName="Carlos Perez",
            isLastMessageFromMe="false",
            lastMessageBody=reply_body,
        )

        _run_detect(attio, pb, entry, csv_content, prospect_name="Carlos Perez")

        update_call = attio.update_list_entry.call_args
        new_attrs = update_call[1]["entry_attributes"]
        assert new_attrs["response_classification"] == "positive"
        assert new_attrs["last_response_text"] == reply_body
        assert new_attrs["stage"] == PipelineStage.RESPONDED.value


# ── Truncation ──────────────────────────────────────────────────────


class TestTruncation:
    def test_last_response_text_is_truncated(self):
        attio = MagicMock()
        pb = MagicMock()

        entry = _make_entry("e3", "r3", PipelineStage.DM1_SENT.value)
        very_long_body = "Interesante. " + ("x" * 2000)
        csv_content = _make_sn_csv(
            participantFullName="Luisa Gomez",
            isLastMessageFromMe="false",
            lastMessageBody=very_long_body,
        )

        _run_detect(attio, pb, entry, csv_content, prospect_name="Luisa Gomez")

        update_call = attio.update_list_entry.call_args
        new_attrs = update_call[1]["entry_attributes"]
        assert len(new_attrs["last_response_text"]) == MAX_RESPONSE_TEXT_LEN
        assert new_attrs["last_response_text"].startswith("Interesante.")


# ── LLM path when API key present ───────────────────────────────────


class TestLlmPathWithKey:
    def test_llm_path_used_when_key_available(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

        attio = MagicMock()
        pb = MagicMock()

        entry = _make_entry("e4", "r4", PipelineStage.DM1_SENT.value)
        csv_content = _make_sn_csv(
            participantFullName="Ana Ramirez",
            isLastMessageFromMe="false",
            lastMessageBody="Una pregunta específica: cuánto cuesta?",
        )

        with patch("workflows.detect_responses.classify_reply_llm") as mock_classify:
            mock_classify.return_value = {
                "classification": "question",
                "reasoning": "pricing question",
                "defensive_score": 0.0,
                "engagement_score": 0.7,
                "source": "llm",
                # Wave-1.6 FIX-3b: the create_note path reads these fields;
                # pre-1.6 they could be omitted because the broad-except at
                # line 1055 swallowed the resulting KeyError. Now KeyError
                # propagates as a code bug (correctly) — supply real values.
                "suggested_action": "Reply with pricing details",
                "summary": "Pricing question — provide quote",
            }
            result = _run_detect(
                attio, pb, entry, csv_content, prospect_name="Ana Ramirez"
            )

        assert result["question"] == 1
        assert result["classifier_llm"] == 1
        assert result["classifier_keyword"] == 0
        update_call = attio.update_list_entry.call_args
        assert update_call[1]["entry_attributes"]["response_classification"] == "question"


# ── Attio failure handling ──────────────────────────────────────────


class TestAttioFailureHandling:
    def test_attio_update_4xx_counted_and_escalated(self):
        """Wave-1.6 FIX-3b contract: Attio 4xx on response update is
        counted, the batch loop continues, AND an attio_write_failed
        Operator Review Queue row is opened. Pre-1.6 the broad-except
        swallowed the failure with only a counter increment — no
        operator-visible signal."""
        import httpx

        attio = MagicMock()
        pb = MagicMock()
        # Simulate Attio rejecting stage='Defensive Hold' or similar with 400.
        response = MagicMock(spec=httpx.Response)
        response.status_code = 400
        response.text = '{"error": "stage value not in enum"}'
        attio.update_list_entry.side_effect = httpx.HTTPStatusError(
            "400 Bad Request", request=MagicMock(), response=response
        )

        entry = _make_entry("e5", "r5", PipelineStage.DM1_SENT.value)
        csv_content = _make_sn_csv(
            participantFullName="Error User",
            isLastMessageFromMe="false",
            lastMessageBody="Interesado, agendemos",
        )

        # Wave-2-B: AttioWriter routes the PATCH and emits
        # ``attio_write_failed`` via its built-in _dlq_and_escalate
        # using the canonical ``workflows.escalation.escalate``
        # module. Patch at the source so the assertion catches it.
        with patch("workflows.escalation.escalate") as mock_escalate:
            result = _run_detect(
                attio, pb, entry, csv_content, prospect_name="Error User"
            )

        assert result["detected"] == 1
        assert result["positive"] == 1
        # 4xx counted as before.
        assert result["attio_update_failed"] == 1
        # AttioWriter opened the operator-visible queue row(s).
        # Wave-2-B note: the Phase-1 fallback (`defensive_score` /
        # `engagement_score` drop on 400) issues a second AttioWriter
        # PATCH which also 400s in this test fixture, so AttioWriter
        # ends up escalating twice (once per write attempt). The
        # operator surface is still the same — both rows carry the
        # same idempotency_key and Attio's escalate() dedupes them
        # in production.
        attio_write_calls = [
            c for c in mock_escalate.call_args_list
            if c.kwargs.get("type") == "attio_write_failed"
        ]
        assert len(attio_write_calls) >= 1
        payload = attio_write_calls[0].kwargs["payload"]
        assert payload["error_class"] == "HTTPStatusError"

    def test_attio_update_5xx_propagates(self):
        """Wave-1.6 FIX-3b: 5xx is infra failure, not a row-level issue.
        The function must NOT silently absorb it (the §0 #9 contract);
        the caller (cli daily) is the right place to see the halt.

        Wave-2-B: AttioWriter wraps the exhausted-retry 5xx as
        ``AttioRateLimitExhausted`` (subclass of ``AttioError``); the
        helper re-raises so the run still halts loudly."""
        import httpx
        import pytest

        from clients.attio_writer import AttioRateLimitExhausted

        attio = MagicMock()
        pb = MagicMock()
        response = MagicMock(spec=httpx.Response)
        response.status_code = 503
        response.text = '{"error": "service unavailable"}'
        attio.update_list_entry.side_effect = httpx.HTTPStatusError(
            "503 Service Unavailable", request=MagicMock(), response=response
        )

        entry = _make_entry("e5x", "r5x", PipelineStage.DM1_SENT.value)
        csv_content = _make_sn_csv(
            participantFullName="Infra Outage",
            isLastMessageFromMe="false",
            lastMessageBody="Interesado, agendemos",
        )

        with pytest.raises(AttioRateLimitExhausted):
            _run_detect(
                attio, pb, entry, csv_content, prospect_name="Infra Outage"
            )

    def test_manual_reply_attio_4xx_counted_and_escalated(self):
        """Wave-1.6-ext FIX-3b' contract: the manual_unclassified branch
        at detect_responses.py:513 had the SAME silent-swallow risk
        class as the auto-classifier branch at :1055. Attio writes
        `response_classification='manual_unclassified'` (value missing
        from production select enum — see done-qa-adversarial F5);
        every such write 400s. Pre-Wave-1.6-ext the broad-except only
        incremented attio_update_failed — defensive responders stayed
        at DM3_SENT eligible for re-engagement (§3.1 risk).

        After FIX-3b' a 4xx is counted, queue row opens via
        `attio_write_failed`, and the loop continues."""
        import httpx

        attio = MagicMock()
        pb = MagicMock()
        # Simulate Attio rejecting response_classification=
        # 'manual_unclassified' (value not in enum) with 400.
        response = MagicMock(spec=httpx.Response)
        response.status_code = 400
        response.text = (
            '{"error": "response_classification value not in select enum"}'
        )
        attio.update_list_entry.side_effect = httpx.HTTPStatusError(
            "400 Bad Request", request=MagicMock(), response=response
        )

        # Build a manual-reply scenario: dm_step=1 (we sent DM1) and the
        # thread has 3 messages with isLastMessageFromMe=true — meaning
        # the prospect replied AND we replied manually after.
        entry = _make_entry("e-manual", "r-manual", PipelineStage.DM1_SENT.value)
        entry["dm_step"] = 1
        csv_content = _make_sn_csv(
            participantFullName="Manual Reply User",
            isLastMessageFromMe="true",
            lastMessageBody="Gracias por su respuesta, agendemos.",
            totalMessageCount="3",
        )

        # Wave-2-B: AttioWriter handles the 4xx and routes
        # attio_write_failed via the canonical
        # workflows.escalation.escalate module. Patch at the source.
        with patch("workflows.escalation.escalate") as mock_escalate:
            result = _run_detect(
                attio,
                pb,
                entry,
                csv_content,
                prospect_name="Manual Reply User",
            )

        # 4xx counted, NOT silently swallowed.
        assert result["attio_update_failed"] == 1
        attio_failed_calls = [
            c
            for c in mock_escalate.call_args_list
            if c.kwargs.get("type") == "attio_write_failed"
        ]
        assert len(attio_failed_calls) >= 1, (
            f"expected at least 1 attio_write_failed escalation; got "
            f"{[c.kwargs.get('type') for c in mock_escalate.call_args_list]}"
        )
        payload = attio_failed_calls[0].kwargs["payload"]
        assert payload["error_class"] == "HTTPStatusError"

    def test_manual_reply_attio_5xx_propagates(self):
        """Wave-1.6-ext FIX-3b': 5xx on manual-reply path is infra
        failure — must propagate so the run halts loudly.

        Wave-2-B: AttioWriter wraps the exhausted-retry 5xx as
        ``AttioRateLimitExhausted``; the manual-reply branch re-raises
        so the run still halts."""
        import httpx
        import pytest

        from clients.attio_writer import AttioRateLimitExhausted

        attio = MagicMock()
        pb = MagicMock()
        response = MagicMock(spec=httpx.Response)
        response.status_code = 503
        response.text = '{"error": "service unavailable"}'
        attio.update_list_entry.side_effect = httpx.HTTPStatusError(
            "503 Service Unavailable", request=MagicMock(), response=response
        )

        entry = _make_entry("e-manual-5xx", "r-manual-5xx", PipelineStage.DM1_SENT.value)
        entry["dm_step"] = 1
        csv_content = _make_sn_csv(
            participantFullName="Manual Reply Infra Outage",
            isLastMessageFromMe="true",
            lastMessageBody="Gracias por su respuesta, agendemos.",
            totalMessageCount="3",
        )

        with pytest.raises(AttioRateLimitExhausted):
            _run_detect(
                attio,
                pb,
                entry,
                csv_content,
                prospect_name="Manual Reply Infra Outage",
            )

    def test_classification_branch_transient_request_error_continues(self):
        """Wave-1.6.2 FIX-C (adversarial EXT-SB-4 IMPORTANT): symmetric
        error handling between FIX-3b (classification, L1116) and
        FIX-3b' (manual_unclassified, L513). Pre-FIX-C the classification
        branch caught HTTPStatusError ONLY; a transient
        `httpx.RequestError` / `httpx.TimeoutException` (DNS blip, conn
        reset, 30s Attio timeout) propagated out of the per-row inner
        loop and halted the entire batch. The manual_unclassified branch
        at L576 already had counter+continue for the same exceptions.

        After FIX-C both branches behave symmetrically: transient
        network errors counted + skipped + batch continues. Schema/code
        bugs (KeyError, TypeError) still propagate.
        """
        import httpx

        attio = MagicMock()
        pb = MagicMock()
        attio.update_list_entry.side_effect = httpx.ConnectError(
            "DNS resolution failed", request=MagicMock()
        )

        entry = _make_entry("e-trans", "r-trans", PipelineStage.DM1_SENT.value)
        csv_content = _make_sn_csv(
            participantFullName="Transient Network",
            isLastMessageFromMe="false",
            lastMessageBody="Interesado, agendemos",
        )

        # MUST NOT raise — the transient error must be caught + counted.
        result = _run_detect(
            attio, pb, entry, csv_content, prospect_name="Transient Network"
        )

        # Failure counted, batch survived.
        assert result["attio_update_failed"] == 1, (
            "FIX-C regression: transient ConnectError was not counted as "
            "attio_update_failed (or the function raised). The "
            "classification branch must mirror manual_unclassified's "
            "counter+continue handling at L576."
        )

    def test_classification_branch_code_bug_still_propagates(self):
        """FIX-C narrowness check: a KeyError (programmer bug) must
        STILL propagate even after we widen the catch to RequestError /
        TimeoutException. Narrow catches keep code bugs visible."""
        import pytest

        attio = MagicMock()
        pb = MagicMock()
        attio.update_list_entry.side_effect = KeyError("missing_attribute")

        entry = _make_entry("e-bug", "r-bug", PipelineStage.DM1_SENT.value)
        csv_content = _make_sn_csv(
            participantFullName="Code Bug",
            isLastMessageFromMe="false",
            lastMessageBody="Interesado, agendemos",
        )

        with pytest.raises(KeyError):
            _run_detect(
                attio, pb, entry, csv_content, prospect_name="Code Bug"
            )

    def test_classification_4xx_escalate_raise_does_not_orphan_subsequent_row(self):
        """Wave-1.6.3 (adversarial follow-up NIT — symmetric with FIX-A
        test_regular_invite_path_escalate_raise_does_not_orphan_other_rows).

        FIX-C symmetric: when classification's Attio update returns 4xx,
        the inner branch opens an `attio_write_failed` operator-review
        queue row via escalate(). Pre-Wave-1.6.3 that escalate() was
        NAKED — if it ALSO raised (transient Attio 5xx burst, network
        blip during the queue write, payload schema drift), the
        propagation killed the outer for-row classification loop and
        subsequent rows orphaned. NOT §3.1 (stage stayed at DM_SENT for
        those rows), but §3 #9 silent batch-truncation.

        Wave-1.6.3 wraps the escalate() call in a try/except Exception
        so the per-row loop survives. This test pins that guarantee with
        a 2-row batch where the first row's escalate raises.
        """
        import httpx

        attio = MagicMock()
        pb = MagicMock()
        # Attio responds 400 (in the FIX-3b "schema-gap" range) — drives
        # the 4xx branch where the escalate() call lives.
        response_4xx = MagicMock(spec=httpx.Response)
        response_4xx.status_code = 400
        response_4xx.text = '{"error": "stage value not in enum"}'
        attio.update_list_entry.side_effect = httpx.HTTPStatusError(
            "400 Bad Request", request=MagicMock(), response=response_4xx
        )

        # Two distinct entries with distinct participant names. The CSV
        # below has 2 rows, each matching one entry.
        entry_1 = _make_entry("e-esc-1", "r-esc-1", PipelineStage.DM1_SENT.value)
        entry_2 = _make_entry("e-esc-2", "r-esc-2", PipelineStage.DM1_SENT.value)
        attio.query_list_entries.return_value = [entry_1, entry_2]

        # 2-row CSV. Build inline because _make_sn_csv only writes one
        # row.
        fieldnames = [
            "participantProfileUrl",
            "participantFullName",
            "isLastMessageFromMe",
            "lastMessageBody",
            "lastMessageDate",
            "totalMessageCount",
        ]
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({
            "participantProfileUrl": "https://linkedin.com/in/orphan-1",
            "participantFullName": "Orphan One",
            "isLastMessageFromMe": "false",
            "lastMessageBody": "Interesado, agendemos",
            "lastMessageDate": "",
            "totalMessageCount": "2",
        })
        writer.writerow({
            "participantProfileUrl": "https://linkedin.com/in/orphan-2",
            "participantFullName": "Orphan Two",
            "isLastMessageFromMe": "false",
            "lastMessageBody": "Tambien interesado, agendemos",
            "lastMessageDate": "",
            "totalMessageCount": "2",
        })
        pb.download_result_csv.return_value = buf.getvalue()

        # Per-record cache.get → distinct prospect names so name-based
        # matching maps each CSV row to a unique entry.
        cache_lookup = {
            "r-esc-1": ("Orphan One", "Acme", "https://linkedin.com/in/orphan-1", "", ""),
            "r-esc-2": ("Orphan Two", "Beta", "https://linkedin.com/in/orphan-2", "", ""),
        }

        # Patch escalate to ITSELF raise. The FIX-C wrap in
        # detect_responses must swallow + tally + continue so row 2 is
        # still attempted (its Attio update will ALSO 4xx, count as
        # attio_update_failed=2, and would ALSO try to escalate → which
        # also raises → tally=2). The function must NOT raise out.
        response_503 = MagicMock(spec=httpx.Response)
        response_503.status_code = 503
        response_503.text = '{"error": "queue write 503"}'

        with (
            patch.dict("os.environ", {"ATTIO_LIST_ID": "test-list-id"}, clear=False),
            patch("clients.attio_writer.RETRY_BUDGET_SECONDS", 0.0001),
            patch(
                "workflows.detect_responses.AttioClient.parse_entry",
                side_effect=lambda e: e,
            ),
            patch("workflows.detect_responses._pb_session_args", return_value={}),
            patch("workflows.detect_responses.RecordCache") as MockCache,
            patch(
                "workflows.escalation.escalate",
                side_effect=httpx.HTTPStatusError(
                    "503 Service Unavailable",
                    request=MagicMock(),
                    response=response_503,
                ),
            ) as mock_escalate,
        ):
            mock_cache_instance = MagicMock()
            mock_cache_instance.get.side_effect = lambda rid: cache_lookup.get(rid)
            MockCache.return_value = mock_cache_instance

            # MUST NOT raise — the FIX-C wrap absorbs the escalate raise
            # and continues the per-row loop.
            result = detect_responses(attio, pb, inbox_scraper_id="scraper-x")

        # Both rows fired AttioWriter's _dlq_and_escalate (and both
        # raised — AttioWriter's escalate carve-out swallows that
        # error and proceeds to raise AttioPermanentError; my outer
        # except catches it + continues). Filter by type so
        # downstream cadence-drift escalates don't confound the
        # count.
        # Wave-2-B: AttioWriter's idempotency key is
        # ``f"{object}-{record_id}-{kind}"`` (e.g.
        # ``linkedin_outreach-e-esc-1-permanent_http_400``) rather
        # than the legacy ``detect-responses-attio-*`` prefix.
        attio_failed_calls = [
            c
            for c in mock_escalate.call_args_list
            if c.kwargs.get("type") == "attio_write_failed"
            and "linkedin_outreach-e-esc-" in c.kwargs.get("idempotency_key", "")
        ]
        assert len(attio_failed_calls) >= 2, (
            f"Wave-1.6.3 regression: expected at least 2 attio_write_failed "
            f"escalate() calls from AttioWriter (one per row); got "
            f"{len(attio_failed_calls)}. §3 #9 silent batch-truncation."
        )
        # Both rows attempted the write → both raised → both tallied.
        # Belt + suspenders verification.
        assert result["attio_update_failed"] == 2, (
            f"Wave-1.6.3 regression: expected attio_update_failed=2 "
            f"(both rows hit 4xx); got {result['attio_update_failed']}. "
            f"The loop likely terminated after row 1."
        )
        # Confirm the two escalates targeted the two distinct rows
        # (not the same row twice — that would prove single-row retry,
        # not loop continuation). Wave-2-B: AttioWriter passes the
        # ``intent.record_id`` (the entry_id for list-entry writes)
        # in the payload, so the assertion targets ``e-esc-{1,2}``
        # rather than the person record_ids.
        targeted_entry_ids = {
            c.kwargs["payload"].get("record_id")
            for c in attio_failed_calls
        }
        assert {"e-esc-1", "e-esc-2"}.issubset(targeted_entry_ids), (
            f"Wave-1.6.3 regression: expected escalates for both "
            f"distinct entries; got {targeted_entry_ids}. The loop "
            f"may have retried row 1 instead of advancing to row 2."
        )


# ── Defensive count in return dict ──────────────────────────────────


class TestDefensiveCountInReturnDict:
    def test_defensive_counter_always_present(self):
        attio = MagicMock()
        pb = MagicMock()

        entry = _make_entry("e6", "r6", PipelineStage.DM1_SENT.value)
        csv_content = _make_sn_csv(
            participantFullName="Neutral User",
            isLastMessageFromMe="false",
            lastMessageBody="Gracias por el mensaje.",
        )

        result = _run_detect(attio, pb, entry, csv_content, prospect_name="Neutral User")
        assert "defensive" in result
        assert result["defensive"] == 0
        assert result["neutral"] == 1

    def test_counter_keys_stable_across_runs(self):
        """The counters dict shape must be stable for downstream consumers."""
        attio = MagicMock()
        pb = MagicMock()
        # No entries — still returns proper envelope
        attio.query_list_entries.return_value = []
        with (
            patch("workflows.detect_responses.AttioClient.parse_entry", side_effect=lambda e: e),
            patch("workflows.detect_responses.RecordCache"),
        ):
            result = detect_responses(attio, pb, inbox_scraper_id="scraper-x")
        for key in ("detected", "positive", "negative", "question", "neutral", "defensive"):
            assert key in result
