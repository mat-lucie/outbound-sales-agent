"""Tests for workflows/escalation.py and escalation_schemas.py (F-PR-3)."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock

import pytest

from workflows import escalation
from workflows.escalation_schemas import (
    CONFIGURATION_DECISION_KEYS,
    CONFIGURATION_DECISION_KEYS_SET,
    ESCALATION_SCHEMAS,
    ESCALATION_TYPES,
    ESCALATION_TYPES_SET,
    EscalationSchemaError,
    MissingAttioCredentials,
    MissingDecisionKey,
    UnknownEscalationType,
)


@pytest.fixture
def mock_attio():
    """Return a MagicMock AttioClient with a `_request` method whose
    default behavior is `query returns empty` + `create returns the
    body we sent` (so the test can introspect the create call)."""
    client = MagicMock()

    # Default: lookup queries return no existing row.
    def _request(method, path, json=None, **kwargs):
        if path.endswith("/records/query"):
            return {"data": []}
        if path.endswith("/records") and method == "POST":
            # Echo back the create body so we can verify attributes set.
            return {"data": {"values": (json or {}).get("data", {}).get("values", {}),
                             "id": {"record_id": "rec_fake_uuid"}}}
        return {"data": {}}

    client._request.side_effect = _request
    return client


class TestSlugInventory:
    """The 79-slug enum is the canonical contract — pin its shape."""

    def test_total_slug_count_is_99(self):
        # Per refactor-QA Rec #3: 89 original − 11 collapsed + 1 meta-slug = 79.
        # Wave-1.6 FIX-3 added 2 narrowed-fallback slugs:
        #   - pipeline_starvation_check_failed (cli.py starvation eval failure)
        #   - attio_schema_missing (weekly_report 404 on missing object/list)
        # Wave-1.6-ext FIX-2' added 1 more for adversarial SB-4:
        #   - experiment_id_immutability_violation (daily_check.py:943
        #     escalate-and-continue replacing the raise-mid-batch that
        #     orphaned PB-sent invites).
        # Wave-2 #21 added 1 more:
        #   - phase0_scrape_timeout (daily_check.py Phase 0 PBRunTimeout degrade
        #     opens a queue row so a silently-degraded run is visible Attio-side).
        # PR #180 added 1 more:
        #   - phase0_scrape_failed (Phase 0 PBRunFailed degrade — PB
        #     status="error" no longer crashes the daily run before Parts A/B)
        # fix/dm-desync-invariant (2026-06-09 design §2) added 1 more:
        #   - dm_person_advance_desync (consistency sweep repair-failure escalation)
        # fix/audit-silent-skip-escalations (#173) added 4 more (L1-2/4/5/6):
        #   - missing_linkedin_url
        #   - missing_quality_score
        #   - accepted_missing_last_contact_date
        #   - stale_connection_sent
        # feat/port-phase0-rescrape (PR #179 port) added 1 more:
        #   - phase0_stale_scrape (silent-zero guard for stale/dedup-refused scrapes)
        # fix/weekly-restamp-phase0-sweep (port of upstream #206) added 2 more:
        #   - recent_outreach_map_empty (weekly_prospect._load_recent_outreach_map
        #     surfaces the silent zero-map no-op of the 14-day re-prospect guard).
        #   - prospect_first_degree_with_depth (detect_accepted_connections
        #     PROSPECT sweep flags a Pattern-A regression — a 1st-degree
        #     PROSPECT that already carries DM depth — instead of flipping it
        #     to ACCEPTED and wiping its cadence depth).
        # feat/port-phase0-scrape (port of upstream #208) added 1 more:
        #   - phase0_suspected_stale_degree (detect_accepted_connections
        #     reconcile alarm — a CONNECTION_SENT row the SN scrape reports as
        #     invite-resolved (hasPendingInvitation=false) but still not
        #     1st-degree; the operator cross-references against LinkedIn).
        # feat/port-reliability-primitives (port of upstream #241) added 2 more:
        #   - pattern_a_suspected_duplicate (pre_invite_check quarantines a
        #     1st-degree row committed <14d — suspected URL-variant duplicate).
        #   - manual_reply_suppressed_self_echo (detect_responses suppresses a
        #     manual-reply flip whose last message is our own DM echoed back).
        # feat/port-qualifier-family (PR-240 port) added 1 more:
        #   - language_mismatch (fail-closed language guard — stored `language`
        #     disagrees with its HQ/lane-derived expected language at DM /
        #     connection render time; skip + queue row instead of a
        #     wrong-language send).
        # feat/port-followup-radar (PR-247 WAITING lane) added 1 more:
        #   - awaiting_reply_resolved (a WAITING-lane cycle ended: the prospect
        #     replied to a tracked send; emitted by followup-await
        #     --clear --resolved, carrying the reply-latency learning signal).
        # feat/port-phase06-email (PR-243) added 1 more:
        #   - email_response_detected (Phase 0.6 auto-classified an inbound
        #     email reply and flipped email_campaign_stage; operator cue to
        #     read the note and take over the thread).
        # New count: 89 + 1 + 2 + 1 + 2 + 1 + 1 + 1 = 98.
        assert len(ESCALATION_TYPES) == 99

    def test_no_duplicate_slugs(self):
        assert len(ESCALATION_TYPES) == len(ESCALATION_TYPES_SET)

    def test_configuration_decision_meta_slug_present(self):
        assert "configuration_decision" in ESCALATION_TYPES_SET

    def test_11_collapsed_slugs_no_longer_present(self):
        """These 11 slugs were collapsed into configuration_decision per
        refactor-QA Rec #3. If any reappear, the enum drifted."""
        collapsed = {
            "icp_phasing_decision",
            "throttle_ttl_policy_decision",
            "cohort_archaeology_threshold_decision",
            "deal_creation_threshold_decision",
            "unit_economics_ceiling_decision",
            "per_cell_n_threshold_decision",
            "industry_threshold_calibration",  # was math, also a config decision
            "llm_cost_ceiling_calibration",
            "association_contact_source_decision",
            "cadence_policy_decision",
            "strategy_vs_implementation_decision",
        }
        survivors = collapsed & ESCALATION_TYPES_SET
        assert not survivors, f"these slugs should be collapsed: {survivors}"

    def test_decision_keys_count_is_11(self):
        assert len(CONFIGURATION_DECISION_KEYS) == 11

    def test_decision_keys_unique(self):
        assert len(CONFIGURATION_DECISION_KEYS) == len(CONFIGURATION_DECISION_KEYS_SET)


class TestValidateType:
    def test_unknown_type_raises(self):
        with pytest.raises(UnknownEscalationType):
            escalation._validate_type("not_a_real_type")

    def test_known_type_ok(self):
        escalation._validate_type("dedup_review")  # should not raise


class TestPayloadValidation:
    def test_registered_typeddict_missing_field_raises(self):
        # AttioWriteFailedPayload requires `object`, `record_id`, etc.
        with pytest.raises(EscalationSchemaError, match="missing required fields"):
            escalation._validate_payload_against_typeddict(
                "attio_write_failed", {"object": "linkedin_outreach"}
            )

    def test_registered_typeddict_complete_payload_ok(self):
        escalation._validate_payload_against_typeddict(
            "attio_write_failed",
            {
                "object": "linkedin_outreach",
                "record_id": "rec_abc",
                "attribute_writes": {"dm_step": 2},
                "error_class": "AttioPermanentError",
                "error_msg": "...",
                "retry_count": 5,
            },
        )

    def test_pattern_a_suspected_duplicate_schema(self):
        # Registered → missing fields raise; complete payload validates.
        with pytest.raises(EscalationSchemaError, match="missing required fields"):
            escalation._validate_payload_against_typeddict(
                "pattern_a_suspected_duplicate", {"record_id": "rec"}
            )
        escalation._validate_payload_against_typeddict(
            "pattern_a_suspected_duplicate",
            {
                "record_id": "rec",
                "entry_id": "ent",
                "linkedin_url": "https://linkedin.com/in/x",
                "name": "René",
                "company": "Acme Foods",
                "prospect_committed_at": "2026-06-28",
                "degree": "1st",
                # Slug-variant cadence-leak fix: prior-cadence context is
                # required (empty when no profile-id sibling exists).
                "prior_cadence_entries": [],
            },
        )

    def test_manual_reply_suppressed_self_echo_schema(self):
        with pytest.raises(EscalationSchemaError, match="missing required fields"):
            escalation._validate_payload_against_typeddict(
                "manual_reply_suppressed_self_echo", {"record_id": "rec"}
            )
        escalation._validate_payload_against_typeddict(
            "manual_reply_suppressed_self_echo",
            {
                "record_id": "rec",
                "entry_id": "ent",
                "name": "René",
                "stage": "DM1 Sent",
                "total_messages": 3,
                "expected": 1,
                "matched_template_id": "operations_leaders/dm1/es",
            },
        )

    def test_unregistered_type_accepts_any_dict(self):
        # Unregistered types skip strict validation at this layer; the
        # CI guard tracks coverage.
        # PR-19 fold-in (code-reviewer NIT #7): use a meta circuit-
        # breaker slug that is structurally unlikely to ever gain a
        # TypedDict (vs ``unstamped_send_blocked`` which is a normal
        # lens-owned slug and likely to be typed soon).
        escalation._validate_payload_against_typeddict("meta_plan_qa_diverging", {})


class TestEscalateConfigurationDecision:
    def test_configuration_decision_requires_decision_key(self, mock_attio):
        with pytest.raises(MissingDecisionKey):
            escalation.escalate(
                type="configuration_decision",
                idempotency_key="x",
                payload={
                    "question": "?",
                    "options": [],
                    "recommended_option": "x",
                    "default_on_expiry": "x",
                    "rationale": "x",
                },
                attio=mock_attio,
            )

    def test_unknown_decision_key_rejected(self, mock_attio):
        with pytest.raises(MissingDecisionKey):
            escalation.escalate(
                type="configuration_decision",
                idempotency_key="x",
                decision_key="not_a_real_decision_key",
                payload={
                    "decision_key": "not_a_real_decision_key",
                    "question": "?",
                    "options": [],
                    "recommended_option": "x",
                    "default_on_expiry": "x",
                    "rationale": "x",
                },
                attio=mock_attio,
            )

    def test_valid_configuration_decision_writes_row(self, mock_attio):
        result = escalation.escalate(
            type="configuration_decision",
            idempotency_key="icp-2026-05-21",
            decision_key="icp_phasing",
            payload={
                "decision_key": "icp_phasing",  # caller can omit too; injected
                "question": "Keep ICP2 active or sunset?",
                "options": [
                    {"key": "keep", "label": "Keep both", "description": "..."},
                    {"key": "sunset", "label": "Sunset ICP2", "description": "..."},
                ],
                "recommended_option": "keep",
                "default_on_expiry": "keep",
                "rationale": "Preserves in-flight ICP2 prospects per §3.1.",
            },
            attio=mock_attio,
        )
        # Verify the queue row payload landed with decision_key.
        values = result["values"]
        assert values["type"] == "configuration_decision"
        assert values["decision_key"] == "icp_phasing"
        # Uniqueness key includes decision_key so two different
        # decision_keys with the same idempotency_key don't collide.
        assert "icp_phasing" in values["uniqueness_key"]
        assert "icp-2026-05-21" in values["uniqueness_key"]

    def test_decision_key_injected_into_payload_if_missing(self, mock_attio):
        result = escalation.escalate(
            type="configuration_decision",
            idempotency_key="x",
            decision_key="throttle_ttl_policy",
            payload={
                # Caller forgot to include decision_key in payload.
                "question": "?",
                "options": [],
                "recommended_option": "x",
                "default_on_expiry": "x",
                "rationale": "x",
            },
            attio=mock_attio,
        )
        payload = json.loads(result["values"]["payload_json"])
        # escalate() injected it from the decision_key arg.
        assert payload["decision_key"] == "throttle_ttl_policy"


class TestEscalateIdempotency:
    def test_existing_row_returns_existing_no_create(self, mock_attio):
        existing_row = {"id": {"record_id": "rec_existing"}, "values": {"type": "dedup_review"}}

        def _request(method, path, json=None, **kwargs):
            if path.endswith("/records/query"):
                return {"data": [existing_row]}
            # If a POST /records call happens, the test should fail.
            raise AssertionError(f"unexpected call: {method} {path}")

        mock_attio._request.side_effect = _request
        result = escalation.escalate(
            type="dedup_review",
            idempotency_key="dedup-group-1",
            payload={
                "canonical_linkedin_url": "url",
                "record_ids": ["a", "b"],
                "conflict_shape": "x",
                "auto_mergeable": False,
            },
            attio=mock_attio,
        )
        assert result is existing_row

    def test_new_row_creates_when_query_empty(self, mock_attio):
        result = escalation.escalate(
            type="dedup_review",
            idempotency_key="dedup-group-2",
            payload={
                "canonical_linkedin_url": "url",
                "record_ids": ["a", "b"],
                "conflict_shape": "x",
                "auto_mergeable": False,
            },
            attio=mock_attio,
        )
        # The mock _request returned the create body — verify it shipped.
        assert result["values"]["type"] == "dedup_review"
        # idempotency_key + uniqueness_key were both stamped.
        assert result["values"]["idempotency_key"] == "dedup-group-2"
        assert result["values"]["uniqueness_key"] == "dedup_review|dedup-group-2"

    def test_configuration_decision_uniqueness_key_includes_decision_key(self, mock_attio):
        result = escalation.escalate(
            type="configuration_decision",
            idempotency_key="batch-2026-05-21",
            decision_key="cohort_archaeology_threshold",
            payload={
                "decision_key": "cohort_archaeology_threshold",
                "question": "?",
                "options": [],
                "recommended_option": "x",
                "default_on_expiry": "x",
                "rationale": "x",
            },
            attio=mock_attio,
        )
        assert result["values"]["uniqueness_key"] == \
            "configuration_decision|cohort_archaeology_threshold|batch-2026-05-21"


class TestEscalateMisc:
    def test_unknown_type_raises_before_attio_call(self, mock_attio):
        # If type is rejected upfront, no Attio call should fire.
        mock_attio._request.side_effect = AssertionError("attio should not be called")
        with pytest.raises(UnknownEscalationType):
            escalation.escalate(
                type="not_a_real_type",
                idempotency_key="x",
                payload={},
                attio=mock_attio,
            )

    def test_deadline_written_when_provided(self, mock_attio):
        result = escalation.escalate(
            type="dedup_review",
            idempotency_key="dr-1",
            payload={
                "canonical_linkedin_url": "url",
                "record_ids": ["a"],
                "conflict_shape": "x",
                "auto_mergeable": False,
            },
            deadline=date(2026, 6, 1),
            attio=mock_attio,
        )
        assert result["values"]["deadline"] == "2026-06-01"

    def test_initial_status_is_open(self, mock_attio):
        result = escalation.escalate(
            type="dedup_review",
            idempotency_key="dr-2",
            payload={
                "canonical_linkedin_url": "url",
                "record_ids": ["a"],
                "conflict_shape": "x",
                "auto_mergeable": False,
            },
            attio=mock_attio,
        )
        assert result["values"]["status"] == "open"
        assert result["values"]["decision_source"] == "agent_opened"


class TestRegisteredSchemas:
    def test_every_registered_schema_slug_is_a_valid_type(self):
        for slug in ESCALATION_SCHEMAS:
            assert slug in ESCALATION_TYPES_SET, (
                f"ESCALATION_SCHEMAS has {slug!r} but it's not in "
                f"ESCALATION_TYPES — drift"
            )

    def test_configuration_decision_has_a_schema(self):
        # The meta-slug MUST have a typed schema since callers need
        # decision_key validation; downstream PRs add other TypedDicts.
        assert "configuration_decision" in ESCALATION_SCHEMAS

    def test_high_stakes_salesman_weekly_slugs_have_schemas(self):
        """salesman-weekly-QA-build3 #1 — the operator's Monday morning queue
        rows must not arrive as opaque payloads for the highest-stakes
        types. weekly_brain_proposal and cost_ceiling_breached both ship
        with TypedDicts so payload_json carries the fields the operator needs to
        decide without digging."""
        assert "weekly_brain_proposal" in ESCALATION_SCHEMAS
        assert "cost_ceiling_breached" in ESCALATION_SCHEMAS


class TestPayloadTypeChecking:
    """engineer-QA-build3 #2 — TypedDict validation now type-checks
    primitives, not just presence."""

    def test_wrong_type_for_str_field_raises(self):
        # AttioWriteFailedPayload.record_id is `str`. Passing an int
        # would silently slip through presence-only validation; now it raises.
        with pytest.raises(EscalationSchemaError, match="record_id"):
            escalation._validate_payload_against_typeddict(
                "attio_write_failed",
                {
                    "object": "linkedin_outreach",
                    "record_id": 12345,  # wrong: should be str
                    "attribute_writes": {},
                    "error_class": "x",
                    "error_msg": "x",
                    "retry_count": 1,
                },
            )

    def test_wrong_type_for_int_field_raises(self):
        with pytest.raises(EscalationSchemaError, match="retry_count"):
            escalation._validate_payload_against_typeddict(
                "attio_write_failed",
                {
                    "object": "linkedin_outreach",
                    "record_id": "rec_x",
                    "attribute_writes": {},
                    "error_class": "x",
                    "error_msg": "x",
                    "retry_count": "five",  # wrong: should be int
                },
            )

    def test_wrong_type_for_list_field_raises(self):
        with pytest.raises(EscalationSchemaError, match="record_ids"):
            escalation._validate_payload_against_typeddict(
                "dedup_review",
                {
                    "canonical_linkedin_url": "url",
                    "record_ids": "rec_a,rec_b",  # wrong: should be list
                    "conflict_shape": "x",
                    "auto_mergeable": True,
                },
            )

    def test_unrecognized_complex_field_type_passes_silently(self):
        """Generic-aliased types (TypedDict-of-TypedDict, etc.) can't be
        runtime-checked deeply. Container-shape only — keeps the contract
        honest about its limits."""
        escalation._validate_payload_against_typeddict(
            "configuration_decision",
            {
                "decision_key": "icp_phasing",
                "question": "?",
                "options": [{"key": "a", "label": "b", "description": "c"}],
                "recommended_option": "a",
                "default_on_expiry": "a",
                "rationale": "r",
            },
        )  # should not raise


class TestMissingAttioCredentials:
    """engineer-QA-build3 #3 — bare KeyError from AttioClient construction
    is now wrapped in a typed exception."""

    def test_raises_typed_error_when_atio_key_missing(self, monkeypatch):
        monkeypatch.delenv("ATTIO_API_KEY", raising=False)
        # escalate() with attio=None constructs AttioClient internally,
        # which raises KeyError. We re-raise as MissingAttioCredentials.
        with pytest.raises(MissingAttioCredentials, match="ATTIO_API_KEY"):
            escalation.escalate(
                type="dedup_review",
                idempotency_key="x",
                payload={
                    "canonical_linkedin_url": "url",
                    "record_ids": ["a"],
                    "conflict_shape": "x",
                    "auto_mergeable": False,
                },
            )


def _transient_500():
    import httpx
    request = httpx.Request("POST", "https://api.attio.com/v2/x")
    response = httpx.Response(500, request=request)
    return httpx.HTTPStatusError("500", request=request, response=response)


class TestEscalateTransient500Retry:
    """Escalation queue-row writes survive transient Attio 5xx (PR-259).

    Regression for the daily-run crashes: a single transient 500 on the
    operator_review_queue query or create killed the run mid-Part-A before any
    invites were sent, forcing a full re-run (~50 profile-scrape visits). Both
    `_find_existing_row` and `_create_row` now flow through
    `clients.attio.request_with_retry`.
    """

    @pytest.fixture(autouse=True)
    def _no_sleep(self, monkeypatch):
        import clients.attio as attio_mod
        monkeypatch.setattr(attio_mod.time, "sleep", lambda _s: None)

    def test_company_throttled_survives_transient_500_on_create(self, mock_attio):
        """The crash class, replayed: query ok, create 500s twice."""
        create_failures = [_transient_500(), _transient_500()]

        def _request(method, path, json=None, **kwargs):
            if path.endswith("/records/query"):
                return {"data": []}
            if create_failures:
                raise create_failures.pop(0)
            return {"data": {"values": json["data"]["values"],
                             "id": {"record_id": "rec_created"}}}

        mock_attio._request.side_effect = _request
        from clients.attio import AmbiguousAttioWrite
        with pytest.raises(AmbiguousAttioWrite):
            escalation.escalate(
                type="company_throttled",
                idempotency_key="company-throttled|rec_p|2026-07-20",
                payload={
                    "record_id": "rec_p",
                    "company_id": "rec_c",
                    "throttle_date": "2026-07-20",
                    "window_days": 30,
                },
                attio=mock_attio,
            )

        assert sum(c.args[1].endswith("/records") for c in mock_attio._request.call_args_list) == 1

    def test_survives_transient_500_on_idempotency_query(self, mock_attio):
        query_failures = [_transient_500()]
        existing_row = {"id": {"record_id": "rec_existing"}}

        def _request(method, path, json=None, **kwargs):
            if path.endswith("/records/query"):
                if query_failures:
                    raise query_failures.pop(0)
                return {"data": [existing_row]}
            raise AssertionError(f"unexpected call: {method} {path}")

        mock_attio._request.side_effect = _request
        result = escalation.escalate(
            type="dedup_review",
            idempotency_key="dedup-retry-1",
            payload={
                "canonical_linkedin_url": "url",
                "record_ids": ["a", "b"],
                "conflict_shape": "x",
                "auto_mergeable": False,
            },
            attio=mock_attio,
        )
        assert result is existing_row

    def test_persistent_500_still_raises_after_bounded_attempts(self, mock_attio):
        import httpx

        calls = {"n": 0}

        def _request(method, path, json=None, **kwargs):
            calls["n"] += 1
            raise _transient_500()

        mock_attio._request.side_effect = _request
        with pytest.raises(httpx.HTTPStatusError):
            escalation.escalate(
                type="dedup_review",
                idempotency_key="dedup-retry-2",
                payload={
                    "canonical_linkedin_url": "url",
                    "record_ids": ["a", "b"],
                    "conflict_shape": "x",
                    "auto_mergeable": False,
                },
                attio=mock_attio,
            )
        assert calls["n"] == 5  # bounded — not infinite

    def test_400_on_create_raises_immediately(self, mock_attio):
        import httpx

        calls = {"n": 0}

        def _request(method, path, json=None, **kwargs):
            if path.endswith("/records/query"):
                return {"data": []}
            calls["n"] += 1
            request = httpx.Request("POST", "https://api.attio.com/v2/x")
            response = httpx.Response(400, request=request)
            raise httpx.HTTPStatusError("400", request=request, response=response)

        mock_attio._request.side_effect = _request
        with pytest.raises(httpx.HTTPStatusError):
            escalation.escalate(
                type="dedup_review",
                idempotency_key="dedup-retry-3",
                payload={
                    "canonical_linkedin_url": "url",
                    "record_ids": ["a", "b"],
                    "conflict_shape": "x",
                    "auto_mergeable": False,
                },
                attio=mock_attio,
            )
        assert calls["n"] == 1

    def test_lost_response_create_that_landed_returns_row_without_duplicate(
        self, mock_attio
    ):
        """The ambiguous-500 case: Attio commits the row, the response is lost.
        The retry must find the landed row via uniqueness_key and NOT re-POST
        (uniqueness_key has no server-side unique constraint, so a blind
        re-POST would duplicate the queue row)."""
        landed_row = {"id": {"record_id": "rec_landed"}}
        state = {"created": 0, "queries": 0}

        def _request(method, path, json=None, **kwargs):
            if path.endswith("/records/query"):
                state["queries"] += 1
                # First query: escalate()'s idempotency check — row absent.
                # Later queries: the post-failure probe — row landed.
                return {"data": []} if state["queries"] == 1 else {"data": [landed_row]}
            state["created"] += 1
            raise _transient_500()  # response lost; write committed server-side

        mock_attio._request.side_effect = _request
        result = escalation.escalate(
            type="dedup_review",
            idempotency_key="dedup-landed-1",
            payload={
                "canonical_linkedin_url": "url",
                "record_ids": ["a", "b"],
                "conflict_shape": "x",
                "auto_mergeable": False,
            },
            attio=mock_attio,
        )
        assert result is landed_row
        assert state["created"] == 1  # never re-POSTed

    def test_probe_failure_does_not_block_create_retry(self, mock_attio):
        """Under a rough patch the probe itself may 500 — that must not crash
        escalate(); the create retry proceeds."""
        state = {"queries": 0, "creates": 0}

        def _request(method, path, json=None, **kwargs):
            if path.endswith("/records/query"):
                state["queries"] += 1
                if state["queries"] == 1:
                    return {"data": []}  # idempotency check: absent
                raise _transient_500()  # probe fails
            state["creates"] += 1
            if state["creates"] == 1:
                raise _transient_500()
            return {"data": {"values": json["data"]["values"],
                             "id": {"record_id": "rec_created"}}}

        mock_attio._request.side_effect = _request
        from clients.attio import AmbiguousAttioWrite
        with pytest.raises(AmbiguousAttioWrite):
            escalation.escalate(
                type="dedup_review",
                idempotency_key="dedup-probe-fail-1",
                payload={
                    "canonical_linkedin_url": "url",
                    "record_ids": ["a", "b"],
                    "conflict_shape": "x",
                    "auto_mergeable": False,
                },
                attio=mock_attio,
            )

        assert sum(c.args[1].endswith("/records") for c in mock_attio._request.call_args_list) == 1
        assert state["creates"] == 1
