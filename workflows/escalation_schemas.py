"""Typed schemas for every operator escalation (F-PR-3).

This module is the canonical contract for what the agent can ask the
operator to decide. Every value in `ESCALATION_TYPES` is one valid
`type` field on an `Operator Review Queue` row.

# Refactor-QA Rec #3 — `configuration_decision` meta-slug

The plan v1.7 originally enumerated 89 slugs. 11 of those were one-time
configuration decisions: `icp_phasing_decision`,
`throttle_ttl_policy_decision`, `cohort_archaeology_threshold_decision`,
`deal_creation_threshold_decision`, `unit_economics_ceiling_decision`,
`per_cell_n_threshold_decision`, `industry_threshold_calibration`,
`llm_cost_ceiling_calibration`, `association_contact_source_decision`,
`cadence_policy_decision`, `strategy_vs_implementation_decision`.

Each was a unique TypedDict for the same shape: a one-time question, a
recommended option, a deadline, a default-on-expiry rule. The refactor-QA
pass proposed collapsing them into a single `configuration_decision`
slug with a required `decision_key` field — same Attio row shape, same
idempotency, same default-on-expiry resolver, smaller surface.

The 11 original slug *names* now live in `CONFIGURATION_DECISION_KEYS`.
Operators searching the Attio queue UI for, say, "icp_phasing_decision"
filter on `decision_key="icp_phasing"` instead of `type="..."`.

Total slug count: 89 − 11 + 1 = **79**.

# Adding a new escalation type

1. Add the slug to `ESCALATION_TYPES`.
2. Define a `TypedDict` for the payload shape.
3. Register it in `ESCALATION_SCHEMAS`.

`escalate()` enforces type membership at write time. Payload TypedDict
registration is enforced by the CI guard
`tests/test_escalation_schemas.py::test_every_type_has_a_typeddict`
which is currently warn-only — F-PR-3 lands with a starter set of
TypedDicts and each downstream PR contributes its own.
"""

from typing import Literal, NotRequired, TypedDict

# Intentionally NOT `from __future__ import annotations` — the runtime
# validator in workflows.escalation reads `TypedDict.__required_keys__`
# to decide which payload fields are mandatory. Under PEP 563 (deferred
# annotations) `NotRequired[T]` is stored as a string `"NotRequired[T]"`,
# the TypedDict metaclass can't see the marker at class-creation time,
# and every field collapses into `__required_keys__` — which makes
# `NotRequired` a silent no-op. Keep annotations eager so the markers
# work as documented.

# ==================================================================
# ESCALATION_TYPES — the 84 valid `type` values for queue rows.
# ==================================================================
#
# Grouping reflects the lens that owns each slug (per plan §4 lens
# decomposition). Order is stable; do not reorder casually because
# the Attio select option order is generated from this list.

ESCALATION_TYPES: tuple[str, ...] = (
    # ---- Engineer / SRE (§4.4) ----
    "pb_csv_empty",
    "attio_write_failed",
    "attio_query_truncated",
    "drift_detector_finding",
    "mcp_scope_insufficient",
    "daily_run_collision",
    "attio_schema_extension_request",
    "attio_partial_write_rollback",
    "mcp_canary_orphan_note",
    "phase_e_github_mcp_unavailable",
    "mcp_scope_unknown_error",
    "phase0_scrape_timeout",
    "phase0_scrape_failed",

    # ---- Salesman-daily (§4.2) ----
    "pb_inmail_dead_end",
    "pb_silent_no_op",
    "pb_invite_unconfirmed",
    "hot_lead_positive_reply",
    "manual_reply_unclassified",
    "ambiguous_reply_match",
    "degree_unknown",
    "config_missing",
    "unstamped_send_blocked",
    "pending_pb_verification",
    "resend_delivery_failed",
    "dm_sequencing_blocked_on_reply_failure",
    "email_response_detected",

    # ---- Advertiser (§4.3 + §4.7) ----
    "defensive_classification_review",
    "experiment_baseline_repair",
    "experiment_unknown_cohort_growth",
    "qualifier_diagnostic_verdict",
    "diagnostic_override",
    "verdict_revised_on_dm3_settle",
    "holdout_seed_required",
    "variant_proposal_pending",
    "synth_prescreen_call_failed",

    # ---- Math (§4.8) ----
    "baseline_invariant_violation",
    "coerce_range_violation",
    "industry_low_confidence",
    "posterior_inconclusive_long_running",
    "threshold_recommendation",

    # ---- Prospect-weekly (§4.5) ----
    "icp2_geo_violation",
    "recent_outreach_map_empty",
    "target_list_stale",
    "weekly_borderline_orphan",
    "icp_geo_unresolved",
    "hq_classification_low_confidence",
    "missing_industry_for_personalization",
    "quality_gate_borderline_llm_outage",
    "disqualifier_match",

    # ---- Salesman-weekly (§4.6) ----
    "pipeline_starvation",
    "pipeline_starvation_check_failed",
    "weekly_brain_proposal",
    "variant_paused_defensive",
    "weekly_finalize_stale",
    "llm_budget_exhausted",
    "cost_ceiling_breached",
    "deal_pipeline_stale",
    "defensive_sample_gap",
    "attio_schema_missing",
    "experiment_id_immutability_violation",

    # ---- GTM (§4.9) ----
    "deal_creation_candidate",
    "deal_creation_confirm",
    "post_dm3_routing_decision",
    "unit_economics_alarm",
    "unit_economics_alarm_warning",
    "abm_account_review",
    "weekly_strategy_review",

    # ---- Data Steward (§4.10) ----
    "dedup_review",
    "dedup_experiment_id_conflict",
    "data_preservation_risk",
    "classifier_refresh_cadence",
    "cohort_archaeology_ambiguity",
    "cohort_tagging_regression",
    "write_owner_invariant_violated",
    "migration_idempotency_regression",
    "manual_reply_classification_gap",
    "irreversible_migration_approved",

    # ---- Prospect-daily (§4.1) ----
    "company_throttled",
    "missing_language",
    "language_mismatch",
    "missing_copy",
    "corrupted_company_invite_skipped",
    "missing_language_for_classification",
    "copy_claim_violation",
    "inmail_required_dead_end",
    # audit-silent-skip-escalations fixes
    "missing_linkedin_url",
    "missing_quality_score",
    "accepted_missing_last_contact_date",
    "stale_connection_sent",

    # ---- Meta-slug for one-time configuration decisions (refactor-QA Rec #3) ----
    "configuration_decision",

    # ---- Meta circuit breakers (§10) ----
    "meta_plan_qa_diverging",
    "meta_build_qa_diverging",

    # ---- DM desync invariant (2026-06-09 design §2) ----
    # Emitted by workflows.consistency_sweep when a divergent entry
    # cannot be auto-repaired (no list entry, unsafe stage, advance
    # write failed, or the repair circuit breaker is open). Operator
    # reconciles manually.
    "dm_person_advance_desync",

    # ---- Phase 0 re-scrape blindness fix (PR #179) ----
    # Emitted when the acceptance-detection scrape returns zero fresh rows
    # or the container log carries a dedup-refusal marker (PB keyed its
    # processed-inputs DB on the CSV filename; repeating the name silently
    # re-joins stale rows). Per-launch csvName bust prevents future
    # occurrences; this escalation surfaces any residual blind runs.
    "phase0_stale_scrape",
    "phase0_suspected_stale_degree",

    # ---- Phase 0 PROSPECT sweep (Defect 2) ----
    # Emitted by workflows.daily_check.detect_accepted_connections when a
    # PROSPECT-stage record is confirmed 1st-degree by the degree check BUT
    # already carries DM engagement (dm_step>0 or any *_sent_at /
    # response_received_at). Flipping such a row to ACCEPTED would wipe its
    # cadence depth (Pattern-A regression). The sweep leaves it at PROSPECT
    # and surfaces it for the repair tooling instead of guessing a DM stage.
    "prospect_first_degree_with_depth",

    # ---- Dup-prospect ingest cascade (PR-241 René RCA) ----
    # pre_invite_check quarantines a 1st-degree row that only became a prospect
    # within the last PATTERN_A_QUARANTINE_DAYS — a likely URL-variant
    # duplicate of a person who already ran a full cadence. Skip today's flip,
    # escalate for operator triage.
    "pattern_a_suspected_duplicate",
    # detect_responses suppressed a "manual reply" that was actually our own
    # duplicate DM echoed back (dup-DM1 case) — do NOT flip to Responded.
    "manual_reply_suppressed_self_echo",

    # ---- Follow-up Radar: WAITING lane (PR-247) ----
    # A WAITING-lane cycle ended — the prospect replied to a send the radar was
    # tracking. Emitted by `followup-await --clear --resolved` before the attrs
    # are cleared, carrying the reply-latency learning signal.
    "awaiting_reply_resolved",
)

ESCALATION_TYPES_SET: frozenset[str] = frozenset(ESCALATION_TYPES)


# ==================================================================
# CONFIGURATION_DECISION_KEYS — the 11 valid `decision_key` values
# for `type="configuration_decision"` rows.
# ==================================================================

CONFIGURATION_DECISION_KEYS: tuple[str, ...] = (
    "icp_phasing",                    # was: icp_phasing_decision
    "throttle_ttl_policy",            # was: throttle_ttl_policy_decision
    "cohort_archaeology_threshold",   # was: cohort_archaeology_threshold_decision
    "deal_creation_threshold",        # was: deal_creation_threshold_decision
    "unit_economics_ceiling",         # was: unit_economics_ceiling_decision
    "per_cell_n_threshold",           # was: per_cell_n_threshold_decision
    "industry_threshold_calibration", # was: industry_threshold_calibration
    "llm_cost_ceiling_calibration",   # was: llm_cost_ceiling_calibration
    "association_contact_source",     # was: association_contact_source_decision
    "cadence_policy",                 # was: cadence_policy_decision
    "strategy_vs_implementation",     # was: strategy_vs_implementation_decision
)

CONFIGURATION_DECISION_KEYS_SET: frozenset[str] = frozenset(CONFIGURATION_DECISION_KEYS)


# ==================================================================
# Per-type TypedDict registry
# ==================================================================
#
# F-PR-3 lands TypedDicts for the meta-slug (`configuration_decision`)
# and a small starter set of universally-needed types. Each downstream
# PR adds the TypedDict for the slug(s) it emits in the same PR diff.
# `escalate()` validates payloads against the registered TypedDict
# at call time when one exists; unregistered types accept any dict.


class ConfigurationDecisionOption(TypedDict):
    """One choice in a `configuration_decision` payload."""
    key: str
    label: str
    description: str


class ConfigurationDecisionPayload(TypedDict):
    """Payload for `type="configuration_decision"`.

    `decision_key` MUST be in `CONFIGURATION_DECISION_KEYS_SET` — this is
    the field that disambiguates between the 11 one-time decisions
    formerly each having their own slug.
    """
    decision_key: str
    question: str
    options: list[ConfigurationDecisionOption]
    recommended_option: str
    default_on_expiry: str
    rationale: str


# ---- Engineer starter TypedDicts ----

class AttioWriteFailedPayload(TypedDict):
    """`attio_write_failed` — the DLQ-equivalent escalation from
    F-PR-4's AttioWriter.

    Wave-2-B fix-up (multi-agent I-4): the underlying call sites also
    pass optional ``is_list_entry`` + ``list_id`` keys so operators
    reading the queue row can tell whether ``record_id`` is a list
    entry_id (and which list it belongs to) vs a record record_id.
    Those keys aren't declared as `NotRequired` here because the
    escalate() validator only checks for required fields — extra keys
    pass through. Documenting them in the docstring keeps the shape
    discoverable.
    """
    object: str
    record_id: str
    attribute_writes: dict
    error_class: str
    error_msg: str
    retry_count: int


class McpScopeInsufficientPayload(TypedDict):
    """`mcp_scope_insufficient` — emitted by the §3.20 canary if
    read+list+write+delete doesn't all pass."""
    step_failed: int
    error_text: str
    mcp_tool: str
    sentinel_record_id: str


# ---- Data Steward starter TypedDict ----

class DedupReviewPayload(TypedDict):
    """`dedup_review` — opened per conflict group during PR-9.5 union
    merge (§3.11). 23 expected on first run per the triage doc."""
    canonical_linkedin_url: str
    record_ids: list[str]
    conflict_shape: str
    auto_mergeable: bool


class DedupExperimentIdConflictPayload(TypedDict):
    """`dedup_experiment_id_conflict` — opened by PR-9.5 union-merge
    (§3.11) when multiple duplicates in a single conflict group carry
    different non-null `experiment_id` values. The group is NOT
    auto-merged; operator decides which experiment_id is canonical
    (or whether to split the group into two records).

    Auto-merging across distinct cohort IDs would silently corrupt
    `learn.py`'s per-step rate denominators — by the time
    `evaluate_experiments` reads the merged winner, the cohort identity
    is fabricated. The escalation forces operator-in-the-loop instead.

    `winner_id` + `loser_ids` identify the group. `experiment_ids` is
    the list of distinct non-null values observed (ordered by record);
    runtime validation enforces ≥2 distinct ids (that IS the conflict).
    `conflict_shape` mirrors `DedupReviewPayload` so operators reading
    either queue type see the same group-shape vocabulary.
    """
    canonical_linkedin_url: str
    winner_id: str
    loser_ids: list[str]
    experiment_ids: list[str]
    conflict_shape: str


# ---- Salesman-weekly starter TypedDicts (salesman-weekly-QA #1) ----

class WeeklyBrainProposalPayload(TypedDict):
    """`weekly_brain_proposal` — a strategic change proposed by the
    weekly_brain critique. The operator reviews and approves/rejects via the
    sales-approve CLI (lands in PR-32).

    `proposal_text` is the human-readable description the operator reads.
    `diff_against_prior_week` lets the operator see what shifted week-over-week.
    `recommended_action` is the agent's explicit ask (approve / reject /
    revise). PR-32 wires the CLI; until then operator resolves via Attio UI.
    """
    proposal_text: str
    diff_against_prior_week: str
    recommended_action: str
    rationale: str
    cohort_evidence: list[dict]


class CostCeilingBreachedPayload(TypedDict):
    """`cost_ceiling_breached` — §3.7 fail-loud rule.

    `step` is required so the operator can tell at a glance WHICH of the 7 LLM
    steps exhausted its weekly cap. `cap_usd` and `consumed_usd` show
    the breach magnitude. The pipeline halts for `step` only — other
    steps continue if they have budget — so the operator can prioritize.
    """
    step: str
    week_starting: str
    cap_usd: float
    consumed_usd: float
    halt_scope: str


# ---- Math: ROC threshold calibration ----

class ThresholdRecommendationPayload(TypedDict):
    """`threshold_recommendation` — emitted by
    `workflows.threshold_calibration.calibrate_score_threshold` when
    the labeled cohort is large enough for an ROC sweep.

    The payload carries the full ROC table + both selection variants
    (max-Youden and cost-weighted). Operator confirms via the queue
    row UI before any change to the live `quality_gate.score_prospect`
    pass threshold.
    """
    today: str
    n_labeled: int
    cost_per_send: float
    value_per_positive: float
    roc_table: list[dict]
    max_youden_threshold: int
    max_youden_score: float
    cost_weighted_threshold: int
    cost_weighted_tpr: float
    cost_weighted_tnr: float


# ---- Salesman-weekly: pipeline starvation ----

class PipelineStarvationPayload(TypedDict):
    """`pipeline_starvation` — emitted by
    `workflows.starvation.evaluate_pipeline_starvation` when one of three
    triggers fires (low_prospects / stale_weekly / short_runway).

    `trigger` is the discriminator; only the fields relevant to that
    trigger should be supplied by the caller (the rest are `NotRequired`
    so the runtime presence check skips them).
    """
    trigger: Literal["low_prospects", "stale_weekly", "short_runway"]
    today: str
    invite_eligible_pool: int
    low_floor: NotRequired[int]
    quarantined_pool: NotRequired[int]
    most_recent_commit: NotRequired[str | None]
    bdays_since_commit: NotRequired[int]
    stale_floor: NotRequired[int]
    daily_rate: NotRequired[int]
    runway_bdays_remaining: NotRequired[float]
    runway_floor: NotRequired[int]


# ---- Wave-1.6 FIX-3 payloads (narrowed silent fallbacks) ----

class PipelineStarvationCheckFailedPayload(TypedDict):
    """`pipeline_starvation_check_failed` — emitted by
    `cli.py` daily when `evaluate_pipeline_starvation` itself raised a
    transient Attio failure (timeout / 5xx) BEFORE producing a verdict.

    Distinct from `pipeline_starvation` (which carries a real trigger
    verdict). This slug surfaces evaluator-side failures so the operator
    knows the daily-run pool measurement was lost — defeats the
    Wave-1.5 silent-fallback breach at cli.py:307.
    """
    today: str
    error_class: str
    error_msg: str
    context: str


class AttioSchemaMissingPayload(TypedDict):
    """`attio_schema_missing` — emitted by weekly_report and other
    consumers that hit a 404 because the Attio object/list referenced
    by the manifest doesn't exist in production.

    Wave-2 will deploy the missing schema; Wave-1.6 makes the deploy
    gap visible by surfacing the 404 instead of silently swallowing it
    (the broken `_upsert_kpi_snapshot` broad-except has been masking
    this gap since day one — see done-qa-salesman-weekly-pass1-round2
    BLOCKER-1).
    """
    object_slug: str
    operation: str
    error_msg: str
    context: str


class Phase0ScrapeTimeoutPayload(TypedDict):
    """`phase0_scrape_timeout` — emitted by
    `workflows.daily_check.detect_accepted_connections` when the Phase 0
    Sales Nav profile scrape times out and the run degrades gracefully
    (skips live acceptance detection, continues to Parts A/B). Makes the
    otherwise stderr-only degrade visible Attio-side (#21). A registered
    schema means a payload-key typo raises EscalationSchemaError (caught in
    tests/CI) instead of slipping silently into Attio.
    """
    run_date: str
    backend: str
    profiles_pending: int
    # 2026-06-11 oversized-launch fix: stale rows beyond the per-run scrape
    # cap (PHASE0_MAX_PROFILES_PER_LAUNCH) that were never submitted this
    # run. Lets the operator see the backlog behind a degraded run.
    profiles_deferred: int
    wait_max_seconds: int
    error: str


class Phase0ScrapeFailedPayload(TypedDict):
    """`phase0_scrape_failed` — emitted by
    `workflows.daily_check.detect_accepted_connections` when the Phase 0
    profile scrape returns PB status="error" (PBRunFailed) and the run
    degrades gracefully (skips live acceptance detection, continues to
    Parts A/B). Makes the otherwise stderr-only degrade visible Attio-side.
    A registered schema means a payload-key typo raises EscalationSchemaError
    (caught in tests/CI) instead of slipping silently into Attio. (PR #180.)
    """
    run_date: str
    backend: str
    profiles_pending: int
    # Same backlog-visibility field as Phase0ScrapeTimeoutPayload.
    profiles_deferred: int
    container_id: str
    error: str


class Phase0StaleScrapePayload(TypedDict):
    """`phase0_stale_scrape` — emitted by
    `workflows.daily_check.detect_accepted_connections` when the scrape
    returns ZERO fresh rows for the submitted batch (PB dedup refusal or
    CSV match-back failure) or the container log carries a dedup marker.
    Pre-fix, this state joined stale result.csv rows and reported a
    confident "0 accepted" while acceptances went undetected.
    """
    run_date: str
    backend: str
    profiles_submitted: int
    # 2026-06-11 oversized-launch fix: BLIND/no_csv runs stamp nothing into
    # the recheck cache, so the SAME capped head batch is re-picked every
    # run while this flavor keeps firing — without this field the deferred
    # tail queued behind a poison head batch is invisible to the operator.
    profiles_deferred: int
    rows_matched: int
    dedup_marker_present: bool
    container_id: str
    log_excerpt: str


class Phase0SuspectedStaleDegreePayload(TypedDict):
    """`phase0_suspected_stale_degree` — emitted by
    `workflows.daily_check.detect_accepted_connections` (PR-208) when the Sales
    Nav scrape reports a CONNECTION_SENT row as invite-resolved
    (hasPendingInvitation="false") yet still NOT 1st-degree. That set is
    {declined/withdrawn invites} ∪ {accepted invites whose degree the scraper is
    mis-reading}; the SN scraper's connectionDegree lags the live graph, so an
    accepted connection can read "2nd" for days. Visibility only — the operator
    cross-references against LinkedIn "My Network"; no stage change is made
    (pending="false" alone cannot distinguish accepted from declined). Aggregated
    to one row/day, mirroring `stale_connection_sent`.
    """
    run_date: str
    backend: str
    count: int
    record_ids: list


class ExperimentIdImmutabilityViolationPayload(TypedDict):
    """`experiment_id_immutability_violation` — emitted by
    `workflows.daily_check.run_connection_requests` when the regular
    invite-success path detects a row whose prior `experiment_id` (set
    at PROSPECT commit time) differs from the experiment running NOW.

    Wave-1.6 originally `raise`d ExperimentIdImmutableError here
    (commit 5910149); the adversarial lens-QA round-2 audit (SB-4)
    found this orphans rows already PB-sent earlier in the same batch
    when the raise fires mid-loop. Wave-1.6-ext converts to
    escalate-and-continue:

    - prior experiment_id is preserved (cohort tag honored — same
      semantic the raise was protecting).
    - The row's stage still advances to CONNECTION_SENT so tomorrow's
      run does NOT re-invite the same prospect (§3.1 hard red line).
    - The escalation row gives the operator visibility into the
      experiment_id mismatch without halting the batch.

    See done-qa-adversarial-pass1-round2.json finding SB-4.
    """
    record_id: str
    entry_id: NotRequired[str]
    prior_experiment_id: str
    current_experiment_id: str
    effective_experiment_id: str
    context: str


# ---- PB send-phantom payloads (advance gate + dead-end emissions) ----

class PBSilentNoOpPayload(TypedDict):
    """`pb_silent_no_op` — batch-level advance-gate failure.

    Opened by `workflows.pb_advance_gate.emit_pb_silent_no_op` when
    the §3.1 advance gate rejects a PB send-phantom outcome
    (csv_status not "Message sent" OR container mismatch OR
    sent_count == 0).

    Required fields trace the launch (container_id, agent_id,
    launched_at, arguments_sha256), the batch counters
    (requested_count, sent_count), and the gate signal (csv_status,
    drift_skipped_reason, next_day_drift_key). Optional fields
    (`experiment_id`, `skipped_urls`) let the next-day drift detector
    attribute the no-op to a specific experiment cohort without a
    secondary Attio lookup; both use `NotRequired[...]` rather than
    `total=False` so the 9 required keys still get runtime presence
    validation via `escalation._validate_payload_against_typeddict`.
    """
    container_id: str
    agent_id: str
    launched_at: str
    arguments_sha256: str
    requested_count: int
    sent_count: int
    csv_status: str
    drift_skipped_reason: str | None
    next_day_drift_key: str
    experiment_id: NotRequired[str | None]
    skipped_urls: NotRequired[list[str]]


class PBInviteUnconfirmedPayload(TypedDict):
    """Invite candidate held until its exact provider state is verified."""
    container_id: str
    profile_url: str
    record_id: str
    entry_id: str
    hold_until: str


class PbInmailDeadEndPayload(TypedDict):
    """`pb_inmail_dead_end` — per-URL send dead end.

    Opened by `daily_check.run_dm_sequencing` and
    `daily_check.run_connection_requests` when PB's CSV explicitly
    marks a prospect's URL as skipped ("Can't send message", "InMail
    required", "Already a 1st-degree connection", "Invite limit
    reached", etc.). The prospect was NOT messaged. The advance gate
    inverts the prior policy of "bump dm_step + last_contact_date" —
    bumping dm_step would (a) suppress tomorrow's retry AND (b)
    inflate `dm_response_rate` denominators in
    `learn.py::_per_step_rates`.

    Idempotency: `(URL, dm_step)` — one row per (prospect, attempted
    step). `dm_step` values: "dm1" | "dm2" | "dm3" for the DM path;
    "invite" for the connection-request path. Operator decides
    whether to flag the prospect out manually (set stage to
    NOT_INTERESTED or similar) or wait for the underlying LinkedIn
    constraint to lift.
    """
    linkedin_url: str
    dm_step: str
    container_id: str
    pb_status: str
    experiment_id: NotRequired[str | None]


# ---- Prospect-weekly PR-26: disqualifier_match payload ----


class DisqualifierMatchPayload(TypedDict):
    """`disqualifier_match` — keyword-deterministic disqualifier hit at
    PROSPECT-commit time (PR-26).

    Emitted by `workflows.weekly_prospect._process_prospects` when
    `score_prospect` returns `verdict_path` in
    `DISQUALIFIER_VERDICT_PATHS`. Original 6 (PR-26): `disqualifier_hr`,
    `disqualifier_finance`, `disqualifier_innovation`,
    `disqualifier_pe`, `disqualifier_state_owned`,
    `disqualifier_consulting`. PR-222 Rec E adds 5 deterministic
    pre-LLM company/employer families: `disqualifier_academic`,
    `disqualifier_government`, `disqualifier_healthcare`,
    `disqualifier_competitor`, `disqualifier_freelance`. PR-238 adds the
    medical/regulatory/clinical-affairs title family:
    `disqualifier_medical_regulatory`. PR-298 adds
    `disqualifier_integrator` — a service-provider company DESCRIPTION at a
    company the industry classifier labelled off-ICP ("sells to our buyers",
    not "is one"); `matched_keyword` there is the description phrase, not a
    title or company-name token.

    `matched_keyword` is the specific keyword that fired — operators
    audit keyword false-positives without re-running the matcher.
    Idempotency key is `f"{linkedin_url}|{verdict_path}"`. `score` is
    the standard-signal score BEFORE the disqualifier short-circuit;
    informational only.
    """
    linkedin_url: str
    title: str
    company: str
    verdict_path: str
    matched_keyword: str
    score: int


# ---- Prospect-weekly PR-28: icp2_geo_violation payload ----


class Icp2GeoViolationPayload(TypedDict):
    """`icp2_geo_violation` — ICP-2 (mid-market family-owned mfg) lane
    requires LATAM geography per the original operator's ICP (MX/CL/CO
    primary; broader LATAM acceptable for ICP-2 phasing-out coverage).
    A prospect scored as ICP-2 but with a non-LATAM location is a
    likely PB CSV drift or persona-config mis-tag.

    Emitted by `workflows.weekly_prospect.enforce_icp_lane_geo` at
    PROSPECT-commit time. The PROSPECT entry is NOT committed; the
    queue row lets the operator triage. Idempotency key is
    `linkedin_url` so the same prospect doesn't open multiple rows
    across re-runs of the weekly batch.

    `icp_lane` is `Literal[1, 2]` — type-design QA convergence on the
    closed enum (was `int`).
    """
    linkedin_url: str
    title: str
    company: str
    location: str
    icp_lane: Literal[1, 2]
    scoring_lane: str


# ---- Prospect-weekly: recent_outreach_map_empty payload ----


class RecentOutreachMapEmptyPayload(TypedDict):
    """`recent_outreach_map_empty` — the 14-day re-prospect guard's outreach
    map came back empty against a NON-EMPTY pipeline list. This is the exact
    fingerprint of the weekly re-stamp silent bug: `canonical_linkedin_url` was
    NULL on 100% of list entries, so `_load_recent_outreach_map` always yielded
    `{}` and the guard never fired.

    Emitted by `workflows.weekly_prospect._load_recent_outreach_map`. Logging
    alone hid this for months; the queue row makes the no-op operator-visible
    within one run. Idempotency key is the run/cutoff date so re-runs on the
    same day collapse to one row.

    Only emitted when ``entries_with_canonical == 0`` (the dead-guard
    fingerprint) — a map that is empty merely because no one was contacted in
    the window is a benign quiet window and is NOT escalated.
    """
    entries_scanned: int
    entries_with_canonical: int
    cutoff_date: str


# ---- Prospect-weekly PR-29: target_list_stale payload ----


class TargetListStalePayload(TypedDict):
    """`target_list_stale` — target-list JSON file under
    `content/{key}-targets.json` is >60 days old, halting the weekly
    batch (PR-29, B-PW-TARGETS-FRESH).

    Emitted by `models.freshness.check_target_list_freshness` BEFORE
    `WeeklyTargetListStaleError` is raised — queue-write-before-raise
    so the operator's terminal session ending doesn't lose the signal.
    Idempotency key is `f"{target_list_path}|{last_modified_at}"`:
    same stale state across re-runs is the same queue row; refresh
    bumps mtime → new key on the next stale event.

    `days_stale` is the integer-day delta. `freshness_check_at` is
    the operator-invocation date the check ran against.
    """
    target_list_path: str
    last_modified_at: str
    days_stale: int
    freshness_check_at: str


# ---- Prospect-daily — missing language (PR-12) ----

# ---- Salesman-weekly PR-30: resend_delivery_failed payload ----


class ResendDeliveryFailedPayload(TypedDict):
    """`resend_delivery_failed` — Resend rejected an outbound email send.

    Two emitters:
      * weekly_report (PR-30) — sets `kpi_snapshot_week_starting` to the
        report's ISO Monday. The weekly KPI sidecar is now a filesystem
        file (`reports/weekly-kpi/<week_starting>.json`, Wave-2-A);
        operators correlate via `kpi_snapshot_week_starting`. Idempotency
        key is `f"weekly_report|{week_starting}"`.
      * hot_lead_alert — has no weekly report context, so
        `kpi_snapshot_week_starting` is not applicable there.

    Wave-2-A: `kpi_snapshot_week_starting` is `NotRequired` (it was a
    hard-required key, which forced the hot-lead path to supply an empty
    sentinel). Marking it optional removes that coupling; the
    weekly_report path still passes the real week.
    """
    recipient_email: str
    send_attempt_at: str
    resend_error_code: str
    kpi_snapshot_week_starting: NotRequired[str]


class AwaitingReplyResolvedPayload(TypedDict):
    """`awaiting_reply_resolved` — a WAITING-lane cycle ended: the prospect
    replied to a send the radar was tracking (awaiting_reply_since) (PR-247).

    Emitter: the `followup-await --clear --resolved` CLI path, BEFORE the
    attrs are cleared (clearing first would destroy the latency data).
    Idempotency key is `f"awaiting-resolved|{record_id}|{resolved}"`. This is
    the reply-latency learning signal — how long prospects take to answer,
    and whether nudges (nudge_count > 0) preceded the reply — for the future
    nudge-timing feed. `latency_days` = resolved − awaiting_reply_since.
    """
    record_id: str
    object: str
    awaiting_reply_since: str
    resolved: str
    latency_days: int
    nudge_count: int


# ---- Salesman-daily Phase 0.6: email_response_detected payload ----


class EmailResponseDetectedPayload(TypedDict):
    """`email_response_detected` — Phase 0.6 detected a prospect reply to
    an outreach email and auto-flipped their email_campaign_stage.

    Emitter: workflows/detect_email_responses.py. Idempotency key is
    `f"email-reply|{record_id}|{received_date}"`. The row is the
    operator's cue to read the full reply (CRM note on the person) and
    take over the thread — the agent never auto-replies to email.
    """
    record_id: str
    email: str
    classification: str
    new_stage: str
    reply_excerpt: str
    detected_on: str


class MissingLanguagePayload(TypedDict):
    """`missing_language` — emitted by consumers of
    `models.resolution.resolve_language` when the Attio `language`
    attribute is missing, null, or not in {es, en, pt}.

    The fail-loud replacement for the pre-PR-12 silent default to
    English. `language_value` carries the rejected raw value (None if
    the field was absent) so operators can distinguish "no language
    set" from "language set to something we don't recognize". Mirrors
    the structured shape of `MissingLanguageError` for clean
    serialization at the call site.
    """
    record_id: str
    persona: str | None
    language_value: str | None
    dm_step: str
    error_msg: str


# ---- Prospect-daily — language guard (fail-closed, PR-240) ----

class LanguageMismatchPayload(TypedDict):
    """`language_mismatch` — emitted by the fail-closed language guard in
    `workflows.daily_check.run_dm_sequencing` /
    `run_connection_requests` when the entry's stored `language`
    disagrees with the language re-derived from its canonical source
    (the linked company's HQ country, via
    `models.email_campaign.detect_language_from_country`; or the
    us_mode scoring lane → English short-circuit).

    Motivation: nothing validated the stored `language` attribute
    against its seeding signal before send, so a wrong value could ship a
    wrong-language DM. On a GENUINE mismatch the guard fails CLOSED — the
    prospect is skipped and this row is opened for an operator to
    reconcile the attribute — rather than guessing which side is right.

    `stored_language` is the (valid) enum value resolve_language
    returned; `expected_language` is the source-derived value they
    disagree with. `scoring_lane` and `dm_step` carry triage context.

    Idempotency key: f"language_mismatch|{record_id}|{step}" — one row
    per prospect per step, matching the sibling missing_language /
    missing_copy guards so retries within a run don't duplicate rows.
    """
    record_id: str
    persona: str | None
    stored_language: str
    expected_language: str
    scoring_lane: str | None
    dm_step: str


# ---- Prospect-daily — per-company throttle (PR-13) ----

class CompanyThrottledPayload(TypedDict):
    """`company_throttled` — emitted by
    `workflows.daily_check.run_connection_requests` and
    `run_dm_sequencing` when a prospect is skipped because their
    linked Company has had outbound contact within the §3.8 throttle
    window (default 30 days).

    The throttle is the §3.1 second line of defense. Each row carries
    `company_id` (empty string when the prospect has no linked
    company — defensively permissive, no queue row in that case) and
    `throttle_date` (the daily-run anchor) for operator triage.
    """
    record_id: str
    company_id: str
    throttle_date: str
    window_days: int


# ---- Salesman-daily — per-prospect degree-unknown signal (PR-15) ----

class DegreeUnknownPayload(TypedDict):
    """`degree_unknown` — emitted PER MISSING PROSPECT by
    `workflows.pre_invite_check._pre_invite_degree_check` when the
    pre-invite scrape CSV returns fewer rows than were requested.

    PR-15 (B-SD-010) is the SIGNAL source — operators triage per-row
    via the queue UI. PR-17 (run-end summary) is the AGGREGATOR that
    writes the `daily_run.degree_unknown_count` attribute. Together
    they form the producer-consumer pair per Round-4 D12.

    Aggregated/summary queue rows are forbidden — operators MUST be
    able to triage per-prospect; one row per missing prospect.
    """
    record_id: str
    linkedin_url: str
    last_known_degree: str
    scrape_attempt_id: str
    requested_at: str
    csv_row_count_observed: int
    csv_row_count_expected: int


# ---- Prospect-daily — missing copy (PR-16) ----

class MissingMessagePayload(TypedDict):
    """`missing_copy` — emitted by consumers of
    `models.campaign.get_message` when the requested
    `(persona, language, dm_step, variant)` key returns no body.

    PR-16 (B-PD-005 + B-PD-008) replaces the pre-PR-16 silent Spanish
    fallback with a typed `MissingMessageError`. Every consumer
    catches the error and opens this queue row so the operator can
    triage the gap — no prospect silently receives a wrong-language
    message body.

    `variant` defaults to "default" since v1 messages.json doesn't
    expose explicit variants. Reserved for PR-32+ weekly_brain
    proposals; the operator queue UI can already filter by it.
    """
    record_id: str
    persona: str
    language: str
    dm_step: str
    variant: str
    error_msg: str


class PbCsvEmptyPayload(TypedDict):
    """`pb_csv_empty` — Phase 0.5 SN Inbox Scraper returned no CSV
    (or a CSV with zero rows) when reply-detection expected results.

    Emitted by ``workflows.detect_responses`` (PR-19 B-SD-005). The
    detection function then writes ``daily_run.reply_detection_status
    = "failed"`` and raises ``NoCSVHalt`` — cli.py catches and exits
    with code 2 (operator-visible non-success; distinct from
    EX_TEMPFAIL=75 used for lock contention).

    Halting at this point prevents DM3 from firing while an unread
    reply might be sitting in the inbox — a direct §3.1 no-resend
    protection. Operators triage the row, investigate the PB run
    (``container_id``), and re-run the daily check once resolved.
    """
    container_id: str
    scrape_attempt_id: str
    expected_min_rows: int
    observed_rows: int


class DMSequencingBlockedOnReplyFailurePayload(TypedDict):
    """`dm_sequencing_blocked_on_reply_failure` — Part-B DM sequencing
    short-circuited because Phase 0.5 reply detection failed
    (``daily_run.reply_detection_status='failed'``).

    PR-19 B-SD-005 emits this row from
    ``workflows.daily_check.run_dm_sequencing`` before any send-path
    work. The upstream ``pb_csv_empty`` row is the root cause; this
    row is the downstream consequence marker so operators see both
    halves of the §3.1 protection (CSV halt at detect_responses → DM
    sequencing skipped here). Idempotent on ``f"{run_date}|dm_sequencing"``.
    """
    run_date: str
    reason: str
    upstream_signal: str


class ManualReplyUnclassifiedPayload(TypedDict):
    """`manual_reply_unclassified` — emitted by
    ``workflows.detect_responses._handle_manual_reply`` (PR-20 B-SD-007)
    when a prospect's reply text is unavailable for automated
    classification.

    The SN Inbox Scraper only surfaces the LAST message in a thread.
    When we replied manually after the prospect's reply (so the last
    message is ours), the prospect's reply body isn't accessible —
    automated classification cannot run. The row routes to RESPONDED
    with ``response_classification="manual_unclassified"`` so operators
    see it in the queue and classify manually.

    If ``prospect_score >= 70``, PR-18's ``emit_hot_lead`` also fires on
    the manual-unclassified path (the hot-lead trigger gate accepts
    this classification when score is high enough).

    Idempotency key: ``f"manual_reply|{record_id}"`` — re-detecting the
    same manual reply across daily runs refreshes the row.
    """
    record_id: str
    entry_id: str
    prospect_name: str
    our_last_message: str
    total_messages: int
    expected_messages: int


class HotLeadPositiveReplyPayload(TypedDict):
    """`hot_lead_positive_reply` — emitted by
    `workflows.hot_lead_alert.emit_hot_lead` when a prospect's reply
    classifies as positive (or manual_unclassified with prospect_score
    >= 70). PR-18 (B-SD-002) is the primary writer; PR-20 invokes the
    same emit path on the manual-unclassified branch.

    Channel ordering is load-bearing: queue row FIRST (this payload),
    then Resend email SECOND (async best-effort). ``fallback_used=True``
    indicates the Resend send failed — operators still have this row
    as the durable record.

    Idempotency key: ``record_id`` — re-emitting for the same prospect
    is a no-op refresh, not a duplicate alert.
    """
    record_id: str
    response_classification: str
    # PR-18 fold-in (salesman-daily, prospect-daily): `prospect_score`
    # is nullable in spec — the manual_unclassified path (PR-20) can
    # emit before scoring lands. emit_hot_lead coerces None→0 in the
    # payload it writes, but the schema documents the wider contract
    # so PR-20's emit call doesn't read as a type violation.
    prospect_score: int
    message_excerpt: str
    thread_url: str
    fallback_used: bool


class DailyRunCollisionPayload(TypedDict):
    """`daily_run_collision` — cross-machine race on (run_date, machine_id).

    Opened by `cli.py::daily` when F-PR-8's `open_daily_run` raises
    `ConcurrentRunInAttio` because another machine already opened a
    daily_run row for the same `(run_date, machine_id)` uniqueness key.
    The colliding process exits EX_TEMPFAIL=75 so a wrapping retry /
    launchd backs off without overwriting the existing run's state.

    Idempotency: `f"{run_date}|{machine_id}"` — a second collision on
    the same key refreshes the existing queue row rather than opening
    duplicates. Operator triage: investigate which machine holds the
    row (see `existing.hostname`, `existing.started_at`,
    `existing.run_id`), confirm the other process is making progress,
    and either let it complete or `attio update` the row's status to
    `aborted` if it has hung.
    """
    run_date: str
    machine_id: str
    attempted_run_id: str
    existing: dict


# ---- Industry classifier: low-confidence result (PR-25) ----

class IndustryLowConfidencePayload(TypedDict):
    """`industry_low_confidence` — Haiku returned a valid label but with
    confidence below the threshold (default 0.7).

    Emitted by ``workflows.industry_classifier.classify_with_confidence``
    when ``status="low_confidence"``. Operators can review the LLM's best
    guess and confirm or override in Attio.

    Fields:
      - ``company_name``: the company that was classified.
      - ``suggested_vertical``: the LLM's best-guess label (one of the 11
        ``INDUSTRY_LABELS`` keys).
      - ``confidence``: float in [0.0, 1.0] — the LLM's raw confidence score.
      - ``reasoning``: LLM's one-sentence explanation for its choice.
      - ``model_used``: model ID that produced the verdict (for traceability).

    Idempotency: ``f"industry_low_confidence|{company_name}|{suggested_vertical}|{confidence_band}"``
    where ``confidence_band`` is the confidence rounded to the nearest 0.1
    (prevents jitter from creating duplicate rows on re-runs).
    """
    company_name: str
    suggested_vertical: str
    confidence: float
    reasoning: str
    model_used: str


# ---- Advertiser: synthetic prescreen per-call failure (PR-24) ----

class SynthPrescreenCallFailedPayload(TypedDict):
    """`synth_prescreen_call_failed` — a single (variant, persona) cell in
    ``score_variant_matrix`` failed due to a dispatch error or a
    ``CoerceRangeViolation`` from ``_coerce_range``.

    Emitted by ``workflows.synthetic_prescreen.score_variant_against_persona``
    before re-raising so the operator can identify which cell failed
    and why without reading logs.

    Fields:
      - ``persona_key``: the synthetic evaluator persona that failed.
      - ``variant_id``: which DM1 variant was being scored (added
        post-QA: 4-agent convergence on missing variant context;
        without it two variants failing against the same persona
        would dedup into a single queue row).
      - ``error_class``: closed-set Literal so mypy catches typos at
        the call site.
      - ``error_msg``: human-readable str(err) for operator triage.

    Idempotency: ``f"synth_prescreen|{variant_id}|{persona_key}|{error_class}[|{value}]"``.
    """
    persona_key: str
    variant_id: str
    error_class: Literal[
        "LLMDispatchTimeout",
        "LLMDispatchFailed",
        "CoerceRangeViolation",
    ]
    error_msg: str


# ---- PR-38: deal_creation_confirm ----

class DealCreationCandidateShape(TypedDict):
    """Sidecar inside ``DealCreationConfirmPayload`` — the shape of the
    deal that ``workflows.deal_creation`` would create on operator confirm."""
    name: str
    stage: str
    associated_company_id: str | None
    creation_idempotency_key: str


class DealCreationConfirmPayload(TypedDict):
    """``deal_creation_confirm`` — opened by
    ``workflows.deal_creation.create_deal_from_response`` when the
    classifier_confidence is in the confirm-band
    (``[DEAL_CONFIRM_THRESHOLD, DEAL_AUTO_THRESHOLD)``). Operator
    confirms → backfill creates the deal with ``created_via=agent_confirm``.

    Idempotency: per Round-4 D25,
    ``f'{person_record_id}_{response_classification}_{response_received_at}'``.
    """
    person_record_id: str
    response_classification: str
    response_received_at: str  # isoformat datetime
    classifier_confidence: float
    candidate_deal: DealCreationCandidateShape
    opened_by: str


# ---- DM desync invariant sweep (2026-06-09 design §2) ----

class DmPersonAdvanceDesyncPayload(TypedDict):
    """`dm_person_advance_desync` — emitted by
    `workflows.consistency_sweep.run_company_tally_consistency_sweep`
    when a divergent (company tally ahead of person entry) row cannot
    be auto-repaired: no list entry found, the person's current stage
    is outside the safe repair set (would regress monotonicity), or the
    guarded advance write itself failed.

    `reason` discriminates the five cases:
      - ``"no_list_entry"`` — no linkedin_outreach list entry for the
        stamped person_record_id; likely a data gap at PROSPECT-commit.
      - ``"stage_unsafe"`` — person.stage is outside {expected_prior,
        target_stage} (e.g., Responded) — a write would regress the
        funnel. Operator decides if the tally or the stage is canonical.
      - ``"repair_write_failed"`` — the guarded advance path (via
        _attio_advance_with_escalation) returned False after exhausting
        retries; the `attio_write_failed` DLQ row is the upstream
        signal.
      - ``"repair_circuit_open"`` — the repair circuit breaker opened
        after REPAIR_CIRCUIT_THRESHOLD consecutive repair_write_failed
        outcomes; this row was divergent and repair-safe but no write
        was attempted (each failed write can burn the full AttioWriter
        retry budget).
      - ``"invariant_violation"`` — a halt-class exception
        (AttioMonotonicityViolation, AttioTerminalClassRegression,
        UnauthorizedAttioWriteError) was raised during a repair write,
        indicating the sweep itself was regressing state. The sweep is
        aborted; operator triage required.

    Idempotency key: ``f"dm-advance-desync|{entry_id}|{stamp_date}"``
    (or ``person_record_id`` when ``entry_id`` is empty). The
    invariant_violation row keys
    ``f"dm-advance-desync|invariant-violation|{error_class}|{date}"``
    so two distinct halt-class violations on the same day open
    separate operator queue rows.
    """
    company_id: str
    person_record_id: str
    stamped_step: str
    stamp_date: str
    entry_id: str
    person_dm_step: int
    person_stage: str
    intended_attrs: dict
    reason: str
    # Set only for invariant_violation rows — the halt-class exception name.
    error_class: NotRequired[str]


# ---- Phase 0 PROSPECT sweep (Defect 2) ----

class ProspectFirstDegreeWithDepthPayload(TypedDict):
    """`prospect_first_degree_with_depth` — emitted by
    `workflows.daily_check.detect_accepted_connections`'s PROSPECT sweep when
    a record sitting at PROSPECT is confirmed 1st-degree by the degree check
    BUT already carries DM engagement (dm_step>0, or any of dm1/dm2/dm3_sent_at
    / response_received_at set).

    This is a Pattern-A regression: a record that was DM'd and then knocked
    back to PROSPECT (manual edit, parallel writer, or a cadence-desync bug).
    The sweep MUST NOT flip it to ACCEPTED — that would reset dm_step and wipe
    the cadence depth — and it MUST NOT guess a DM stage. It leaves the record
    at PROSPECT and opens this queue row so the repair tooling (which knows the
    canonical depth from the timestamps) reconciles it.

    `dm_step` is the raw stored slug. The four `*_set` booleans tell the
    operator which timestamps are populated without a secondary lookup.
    Idempotency key: `f"prospect-1st-depth|{record_id}|{date}"` — one row per
    record per day.
    """
    record_id: str
    entry_id: str
    linkedin_url: str
    dm_step: str
    dm1_sent_at_set: bool
    dm2_sent_at_set: bool
    dm3_sent_at_set: bool
    response_received_at_set: bool


# ---- Dup-prospect ingest cascade (PR-241 René RCA) ----

class PatternASuspectedDuplicatePayload(TypedDict):
    """`pattern_a_suspected_duplicate` — emitted by
    `workflows.pre_invite_check` when a 1st-degree row that would normally
    Pattern-A flip to ACCEPTED only became a prospect within the last
    `PATTERN_A_QUARANTINE_DAYS` (14).

    This is the daily-run half of the PR-241 René cascade: the weekly ingest
    re-created an existing prospect under a new LinkedIn vanity slug, the daily
    run found the "new" prospect already 1st-degree, and the Pattern-A flip
    re-started a cadence on a person who'd already completed a DM3. Recently-
    created + already-1st-degree is the URL-variant-duplicate fingerprint. The
    row is NOT flipped and NOT invited; the operator confirms or dismisses.

    A missing/unparseable `prospect_committed_at` is NOT quarantined (old
    records must keep flipping — the legitimate silent-acceptance Pattern-A
    case), so this row only fires when recency is provable.

    `prior_cadence_entries` (slug-variant cadence-leak fix): existing pipeline
    entries whose LinkedIn URL shares this row's profile-id suffix (slug
    variants of the same person), each
    `{entry_id, record_id, linkedin_url, stage, dm_step, dm1_sent_at,
    dm2_sent_at, dm3_sent_at, last_contact_date}`. The upstream incident's
    quarantine row was flagged correctly but misjudged as a real accept
    because the operator had no view of the prior DM3-complete entry — this
    field puts that cadence in the payload. Empty when no sibling exists;
    `prior_cadence_lookup_error` is set instead when the lookup itself failed
    (the escalation still opens — degraded context beats no row).

    Idempotency key: `record_id`.
    """
    record_id: str
    entry_id: str
    linkedin_url: str
    name: str
    company: str
    prospect_committed_at: str
    degree: str
    prior_cadence_entries: list[dict]
    prior_cadence_lookup_error: NotRequired[str]


class ManualReplySuppressedSelfEchoPayload(TypedDict):
    """`manual_reply_suppressed_self_echo` — emitted by
    `workflows.detect_responses` when the manual-reply count heuristic would
    fire (`isLastMessageFromMe=true` + `totalMessageCount > expected`) but the
    last message body matches one of OUR OWN DM templates.

    The reply-detection half of the PR-241 René cascade: a duplicate DM1
    (from the re-prospecting) left a thread whose last message was our own copy
    echoed twice, arithmetically indistinguishable from a real reply. The row
    is NOT flipped to Responded; the operator confirms the thread.

    A body that doesn't match any template passes through unchanged (a real
    reply still flips).

    Idempotency key: `entry_id|matched_template_id|total_messages` — the
    thread SIGNATURE, not the date. A self-echo thread's last message
    permanently matches our own template, so the old `entry_id|date` key
    re-opened an identical row every day after the operator resolved it.
    Keyed on the signature, `escalate()` finds the prior row (open or
    resolved) and no-ops; any change to the thread (a real reply, a further
    DM) changes `total_messages` and opens a fresh row.
    """
    record_id: str
    entry_id: str
    name: str
    stage: str
    total_messages: int
    expected: int
    matched_template_id: str


# ---- audit-silent-skip-escalations TypedDicts ----


class MissingLinkedinUrlPayload(TypedDict):
    """`missing_linkedin_url` — a PROSPECT-stage record has no LinkedIn
    URL in the RecordCache (blank string or None). Without a URL the
    invite-path cannot send and the record was previously silently
    skipped with only an internal counter bump.

    Emitted by `workflows.daily_check._build_invite_send_data` so the
    operator can triage the missing URL in Attio. Idempotency key is
    ``f"missing_url|{record_id}"`` — one row per record, stable across
    daily re-runs until the URL is filled.
    """
    record_id: str
    name: str | None
    company: str | None


class MissingQualityScorePayload(TypedDict):
    """`missing_quality_score` — a PROSPECT record's `quality_score`
    attribute is None, meaning the weekly pipeline (weekly_prospect)
    never stamped it. Distinct from `score < 60` (legitimate filter)
    which remains a silent skip.

    Emitted by `workflows.daily_check.run_connection_requests` before
    the invite-eligible filter. Idempotency key is
    ``f"missing_qs|{record_id}"`` — one row per record.
    """
    record_id: str


class AcceptedMissingLastContactDatePayload(TypedDict):
    """`accepted_missing_last_contact_date` — an ACCEPTED prospect has
    no `last_contact_date`, making DM1 eligibility (computed from that
    date) permanently impossible. These rows are permanently invisible
    in the DM queue.

    Emitted by `workflows.daily_check.run_dm_sequencing` at the
    stage-filter pass. No auto-backfill is performed; the operator
    must investigate the missing accept date in Attio.

    Idempotency key: ``f"accepted_no_lcd|{record_id}"`` — one row per
    record.
    """
    record_id: str
    entry_id: str


class StaleConnectionSentPayload(TypedDict):
    """`stale_connection_sent` — aggregated daily sweep of CONNECTION_SENT
    rows whose `last_contact_date` is older than
    `STALE_CONNECTION_SENT_ESCALATE_DAYS`. These rows are permanently
    excluded from Phase 0 acceptance detection (which only scans within
    ACCEPTANCE_CHECK_WINDOW_DAYS=14) so they silently sit in the pipeline
    forever.

    ONE row per day (idempotency_key ``f"stale_cs|{date.today()}"``).
    Visibility only — no stage changes, no re-checks.

    `count` is the number of stale rows found.
    `record_ids` carries up to 20 record IDs for operator triage.
    """
    count: int
    record_ids: list


# Registry: maps escalation type slug → TypedDict class.
# Unregistered types accept any dict at runtime; the CI guard tracks
# coverage as downstream PRs add their own TypedDicts.
ESCALATION_SCHEMAS: dict[str, type] = {
    "configuration_decision": ConfigurationDecisionPayload,
    "email_response_detected": EmailResponseDetectedPayload,
    "awaiting_reply_resolved": AwaitingReplyResolvedPayload,
    "attio_write_failed": AttioWriteFailedPayload,
    "mcp_scope_insufficient": McpScopeInsufficientPayload,
    "dedup_review": DedupReviewPayload,
    "dedup_experiment_id_conflict": DedupExperimentIdConflictPayload,
    "deal_creation_confirm": DealCreationConfirmPayload,
    "weekly_brain_proposal": WeeklyBrainProposalPayload,
    "cost_ceiling_breached": CostCeilingBreachedPayload,
    "pb_silent_no_op": PBSilentNoOpPayload,
    "pb_invite_unconfirmed": PBInviteUnconfirmedPayload,
    "pb_inmail_dead_end": PbInmailDeadEndPayload,
    "pipeline_starvation": PipelineStarvationPayload,
    "pipeline_starvation_check_failed": PipelineStarvationCheckFailedPayload,
    "attio_schema_missing": AttioSchemaMissingPayload,
    "phase0_scrape_timeout": Phase0ScrapeTimeoutPayload,
    "phase0_scrape_failed": Phase0ScrapeFailedPayload,
    "phase0_stale_scrape": Phase0StaleScrapePayload,
    "phase0_suspected_stale_degree": Phase0SuspectedStaleDegreePayload,
    "experiment_id_immutability_violation": ExperimentIdImmutabilityViolationPayload,
    "disqualifier_match": DisqualifierMatchPayload,
    "missing_language": MissingLanguagePayload,
    "language_mismatch": LanguageMismatchPayload,
    "missing_copy": MissingMessagePayload,
    "icp2_geo_violation": Icp2GeoViolationPayload,
    "recent_outreach_map_empty": RecentOutreachMapEmptyPayload,
    "target_list_stale": TargetListStalePayload,
    "resend_delivery_failed": ResendDeliveryFailedPayload,
    "threshold_recommendation": ThresholdRecommendationPayload,
    "company_throttled": CompanyThrottledPayload,
    "degree_unknown": DegreeUnknownPayload,
    "daily_run_collision": DailyRunCollisionPayload,
    "hot_lead_positive_reply": HotLeadPositiveReplyPayload,
    "pb_csv_empty": PbCsvEmptyPayload,
    "dm_sequencing_blocked_on_reply_failure": DMSequencingBlockedOnReplyFailurePayload,
    "manual_reply_unclassified": ManualReplyUnclassifiedPayload,
    "synth_prescreen_call_failed": SynthPrescreenCallFailedPayload,
    "industry_low_confidence": IndustryLowConfidencePayload,
    # DM desync invariant sweep (2026-06-09 design §2)
    "dm_person_advance_desync": DmPersonAdvanceDesyncPayload,
    # audit-silent-skip-escalations
    "missing_linkedin_url": MissingLinkedinUrlPayload,
    "missing_quality_score": MissingQualityScorePayload,
    "accepted_missing_last_contact_date": AcceptedMissingLastContactDatePayload,
    "stale_connection_sent": StaleConnectionSentPayload,
    # Phase 0 PROSPECT sweep (Defect 2)
    "prospect_first_degree_with_depth": ProspectFirstDegreeWithDepthPayload,
    # Dup-prospect ingest cascade (PR-241 René RCA)
    "pattern_a_suspected_duplicate": PatternASuspectedDuplicatePayload,
    "manual_reply_suppressed_self_echo": ManualReplySuppressedSelfEchoPayload,
}


class MissingAttioCredentials(RuntimeError):
    """Raised when escalate(attio=None) is called and ATTIO_API_KEY is
    unset. Replaces the bare KeyError from AttioClient construction
    (engineer-QA-build3 #3)."""


# ==================================================================
# Exceptions
# ==================================================================

class UnknownEscalationType(ValueError):
    """Raised by `escalate(type=...)` when `type` is not in ESCALATION_TYPES.

    This is loud-by-design: silently dropping an escalation would
    violate the §3 hard constraint #9 (no silent fallbacks).
    """


class MissingDecisionKey(ValueError):
    """Raised when `type="configuration_decision"` is opened without a
    `decision_key` in the payload (or with an unknown decision_key)."""


class EscalationSchemaError(TypeError):
    """Raised when the payload doesn't match the registered TypedDict
    for the escalation type."""
