"""Per-attribute write-owner registry (F-PR-3.7, plan §3.15).

This module is the canonical map from Attio attribute to its SOLE
write-owner module path. F-PR-4's `AttioWriter.apply()` will consult
this registry on every Attio write and raise
`UnauthorizedAttioWriteError` when the calling module is not declared
as the owner.

# Why this exists

Every multi-writer attribute in this codebase has been the source of a
silent-collision bug at least once. `dm_step` was advanced by both
`run_dm_sequencing` and `scripts/repair_*` simultaneously; the
2026-04-21 dedup damaged 70+ enterprise cadence states. The §3.1
no-resend hard red line depends on every state mutation being
attributable to a known authorized writer.

# Shape

The registry is a dict from `(object, slug)` → `WriteOwner`. A
`WriteOwner` is either a single module path string (sole writer) or a
list of strings (multi-writer with explicit declaration).

# Adding a new attribute

1. Add the attribute to `docs/attio_schema_deltas.yaml` with its
   `write_owner_module`.
2. Add the same entry to `WRITE_OWNER_REGISTRY` below.
3. CI guard (`tests/test_writer_registry.py`) asserts the manifest
   and registry agree on every attribute.

# Bootstrap path / special aliases

Some attributes are written by paths the registry can't enumerate
ahead of time:

- The F-PR-3 bootstrap migration writes `operator_review_queue` attrs
  during object creation — registered under `workflows.escalation.escalate`
  even though the bootstrap script itself runs once.
- F-PR-4's `AttioWriter.apply()` uses an `__authorized_alias__` keyword
  to bypass the registry for cases like the §3.15 Pattern-A flip in
  `pre_invite_check.py`. That hook lands in F-PR-4.
"""

from __future__ import annotations

# A write owner is either a single dotted module path (sole writer) or
# a list of paths (explicit multi-writer with rationale in the manifest).
WriteOwner = str | list[str]

# Special writer aliases — F-PR-4's AttioWriter.apply() accepts these as
# blanket authorized writers for ANY attribute on ANY object. Used for
# bootstrap and infrastructure operations that can't be enumerated as
# a normal write_owner_module.
SPECIAL_WRITER_ALIASES: frozenset[str] = frozenset({
    # §3.20 MCP canary bootstrap. The canary script creates AND deletes
    # a single transient note on a sentinel record to verify Attio
    # read+write+delete scope is live. Per plan §3.20:
    # "Write owner: __bootstrap_canary__ (explicit alias registered in
    # clients/attio_writer_registry.py)."
    "__bootstrap_canary__",
})

# §3.20 MCP scope canary — pinned target record.
#
# The Step-0 canary (every skill's "Verify Attio MCP scope" preflight) does a
# create-note → delete-note round-trip through the REST client to prove the
# `ATTIO_API_KEY` credential — the SAME credential the daily run mutates
# prospect data with — has live read+write+delete scope before any real write.
#
# The round-trip MUST target a dedicated, inert record, never a real prospect:
# an earlier MCP-based canary (now removed) left an orphan note on a live
# prospect because no target was pinned. Create a Person record that exists
# ONLY as the canary anchor:
#   - a clearly-labelled "do not contact" name
#   - deliberately NOT a member of the `linkedin_outreach` list, so the daily
#     run never fetches it (selection is list-entry-scoped — see
#     workflows/daily_check.py `_get_all_entries_parsed`), hence it can never be
#     invited or DM'd.
# Pin its record id here (version-controlled, not an env var) so every machine
# and skill resolves the same target. Left empty by default — the canary
# command fails closed with `canary_record_unconfigured` until an operator
# creates the inert Person and sets this constant.
CANARY_PERSON_RECORD_ID: str = ""

WRITE_OWNER_REGISTRY: dict[tuple[str, str], WriteOwner] = {
    # ---- LinkedIn Outreach: cadence + step state ----
    ("linkedin_outreach", "dm_step"): [
        "workflows.daily_check.run_dm_sequencing",
        "workflows.detect_responses._apply_cadence_repairs",
        "scripts.attio_dedup",
        # Crash-recovery scan re-records a DM advance PB confirmed but the
        # local process never wrote (routes through the same AttioWriter path).
        "workflows.pb_send_recovery.recover_unrecorded_dm_sends",
        # 2026-06-09 desync sweep: converges entries toward the company
        # tally (source of truth for confirmed sends) when the in-run
        # advance failed. Same guarded write path.
        "workflows.consistency_sweep.run_company_tally_consistency_sweep",
        # Event-confirmed advance from the OPTIONAL Botdog transport:
        # the poll-based drain applies the same advance PB's scrape
        # phases apply, through the same AttioWriter path, but only
        # for entries stamped send_channel=botdog.
        "workflows.botdog_ingest.apply_lead_events",
    ],
    ("linkedin_outreach", "stage"): [
        "workflows.daily_check.run_dm_sequencing",
        "workflows.daily_check.run_connection_requests",
        "workflows.daily_check.run_nurture_re_engagement",
        "workflows.daily_check.detect_accepted_connections",
        "workflows.daily_check.check_responses_manual",
        "workflows.pre_invite_check._pre_invite_degree_check",
        "workflows.detect_responses.detect_responses",
        "workflows.detect_responses._handle_manual_reply",
        "workflows.detect_responses._apply_cadence_repairs",
        "scripts.attio_dedup",
        "scripts.backfill_DEFENSIVE_HOLD",
        "workflows.pb_send_recovery.recover_unrecorded_dm_sends",
        # 2026-06-09 desync sweep: converges entries toward the company
        # tally (source of truth for confirmed sends) when the in-run
        # advance failed. Same guarded write path.
        "workflows.consistency_sweep.run_company_tally_consistency_sweep",
        # Phase C reconciliation sweep: flips PROSPECT→CONNECTION_SENT for
        # rows a fresh Sales Nav scrape confirms are already pending on
        # LinkedIn (clears the pre-fix re-selection backlog).
        "workflows.pending_invite_reconciliation.run_pending_invite_reconciliation",
        # One-shot: park CONNECTION_SENT rows older than
        # STALE_CONNECTION_SENT_ESCALATE_DAYS at UNREACHABLE (forward-only;
        # they are permanently past the acceptance-detection window).
        "scripts.remediate_stale_connection_sent_20260615",
        # Event-confirmed advance from the OPTIONAL Botdog transport:
        # the poll-based drain applies the same advance PB's scrape
        # phases apply, through the same AttioWriter path, but only
        # for entries stamped send_channel=botdog.
        "workflows.botdog_ingest.apply_lead_events",
    ],
    # last_contact_date — multi-writer: DM cadence (run_dm_sequencing),
    # PR-9.5 dedup union-merge MAX-of-duplicates per §3.11, and PR-15
    # Pattern-A flip from pre_invite_check (cache-hit "1st" → ACCEPTED
    # atomic write via AttioWriter).
    ("linkedin_outreach", "last_contact_date"): [
        "workflows.daily_check.run_dm_sequencing",
        "workflows.daily_check.run_connection_requests",
        "workflows.daily_check.detect_accepted_connections",
        "workflows.pre_invite_check._pre_invite_degree_check",
        "workflows.detect_responses._apply_cadence_repairs",
        "scripts.attio_dedup",
        "workflows.pb_send_recovery.recover_unrecorded_dm_sends",
        # 2026-06-09 desync sweep: converges entries toward the company
        # tally (source of truth for confirmed sends) when the in-run
        # advance failed. Same guarded write path.
        "workflows.consistency_sweep.run_company_tally_consistency_sweep",
        # Phase C reconciliation sweep (stamps last_contact_date alongside the
        # PROSPECT→CONNECTION_SENT flip).
        "workflows.pending_invite_reconciliation.run_pending_invite_reconciliation",
        # Event-confirmed advance from the OPTIONAL Botdog transport:
        # the poll-based drain applies the same advance PB's scrape
        # phases apply, through the same AttioWriter path, but only
        # for entries stamped send_channel=botdog.
        "workflows.botdog_ingest.apply_lead_events",
        # Manual-DM touch detection (Phase 0.5): when the inbox scrape shows
        # OUR non-template last message on a RESPONDED entry, advance
        # last_contact_date to that message's date so the follow-up radar
        # stops ranking hand-worked prospects as neglected.
        # Brownfield attr — registry-only.
        "workflows.detect_responses._detect_manual_touches",
    ],
    ("linkedin_outreach", "quality_score"):
        "workflows.quality_gate.score_prospect",

    # ---- LinkedIn Outreach: per-step send timestamps (PR-9a) ----
    # Plan §3.15: "dmN_sent_at → daily_check:run_dm_sequencing only
    # (backfill exception: scripts.backfill_per_step_timestamps)."
    # Multi-writer with explicit declaration — the backfill is one-shot
    # at PR-9b and ONLY writes when the field is currently null.
    # PR-9.5 adds attio_dedup as the MAX-non-null union-merge writer
    # per §3.11 (at merge time the attrs are typically null on
    # duplicates; dedup writes from inferred per-duplicate
    # last_contact_date when any non-null is present).
    # 2026-06-10 fix: the consistency sweep records confirmed sends the
    # in-run advance missed, so it stamps the converged step's sent_at
    # (NULL-fill only — never overwrites an existing stamp).
    ("linkedin_outreach", "dm1_sent_at"): [
        "workflows.daily_check.run_dm_sequencing",
        "scripts.backfill_per_step_timestamps",
        "scripts.attio_dedup",
        "workflows.pb_send_recovery.recover_unrecorded_dm_sends",
        "workflows.consistency_sweep.run_company_tally_consistency_sweep",
        # Event-confirmed advance from the OPTIONAL Botdog transport:
        # the poll-based drain applies the same advance PB's scrape
        # phases apply, through the same AttioWriter path, but only
        # for entries stamped send_channel=botdog.
        "workflows.botdog_ingest.apply_lead_events",
    ],
    ("linkedin_outreach", "dm2_sent_at"): [
        "workflows.daily_check.run_dm_sequencing",
        "scripts.backfill_per_step_timestamps",
        "scripts.attio_dedup",
        "workflows.pb_send_recovery.recover_unrecorded_dm_sends",
        "workflows.consistency_sweep.run_company_tally_consistency_sweep",
        # Event-confirmed advance from the OPTIONAL Botdog transport:
        # the poll-based drain applies the same advance PB's scrape
        # phases apply, through the same AttioWriter path, but only
        # for entries stamped send_channel=botdog.
        "workflows.botdog_ingest.apply_lead_events",
    ],
    ("linkedin_outreach", "dm3_sent_at"): [
        "workflows.daily_check.run_dm_sequencing",
        "scripts.backfill_per_step_timestamps",
        "scripts.attio_dedup",
        "workflows.pb_send_recovery.recover_unrecorded_dm_sends",
        "workflows.consistency_sweep.run_company_tally_consistency_sweep",
        # Event-confirmed advance from the OPTIONAL Botdog transport:
        # the poll-based drain applies the same advance PB's scrape
        # phases apply, through the same AttioWriter path, but only
        # for entries stamped send_channel=botdog.
        "workflows.botdog_ingest.apply_lead_events",
    ],
    # response_received_at — primary writer is classify_reply; PR-9.5
    # union-merge takes MAX-non-null across duplicates.
    ("linkedin_outreach", "response_received_at"): [
        "workflows.detect_responses.classify_reply",
        "scripts.attio_dedup",
        # Event-confirmed advance from the OPTIONAL Botdog transport:
        # the poll-based drain applies the same advance PB's scrape
        # phases apply, through the same AttioWriter path, but only
        # for entries stamped send_channel=botdog.
        "workflows.botdog_ingest.apply_lead_events",
    ],

    # ---- LinkedIn Outreach: canonical url + dedup bookkeeping ----
    ("linkedin_outreach", "canonical_linkedin_url"): [
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "scripts.backfill_canonical_linkedin_url",
    ],
    ("linkedin_outreach", "vanity_url_slug"): [
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "scripts.backfill_vanity_url_slug",
    ],
    ("linkedin_outreach", "merged_into"): "scripts.attio_dedup",

    # ---- LinkedIn Outreach: experiment cohort identity (§3.1 red line) ----
    # PR-9.5 dedup may STAMP experiment_id on a winner that holds NULL
    # when exactly one duplicate carries a non-null value (no conflict).
    # Multiple distinct non-null values escalate
    # `dedup_experiment_id_conflict` and never auto-merge — the dedup
    # is then NOT a writer for that group. experiment_id_frozen_at is
    # stamped in lockstep with experiment_id.
    #
    # PR-21 multi-writer list (6 writers):
    #   1. weekly_prospect._build_prospect_entry_attrs — PROSPECT-commit
    #   2. daily_check.run_connection_requests — thread-through to pre_invite_check;
    #      the run_connection_requests call site owns the to_send_data shape
    #   3. pre_invite_check._pre_invite_degree_check — Pattern-A flip WriteIntent
    #      writer_module; registered alongside stage + last_contact_date
    #   4. daily_check.detect_accepted_connections — Phase 0 ACCEPTED flip
    #   5. scripts.migrate_experiments_tsv_to_attio — F-PR-6 seed migration
    #   6. scripts.attio_dedup — PR-9.5 union-merge no-conflict stamp
    #
    # Note: experiment_id is NOT written by pre_invite_check (it's read-only
    # in the WriteIntent — the flip only updates experiment_id_frozen_at).
    # experiment_id stays as-is (5 writers); frozen_at has 6 (adds pre_invite_check).
    # PR-22 adds scripts.backfill_experiment_id_archaeology as a 6th writer
    # for experiment_id (stamps inferred value on legacy rows) and 7th writer
    # for experiment_id_frozen_at (stamps legacy_inferred_by_archaeology or
    # legacy_pure_unknown). See plan §3.15 + §3.10 archaeology hybrid.
    # The archaeology script NEVER overwrites rows where frozen_at is already
    # non-None — it writes only to NULL frozen_at rows (§3.1 immutability).
    ("linkedin_outreach", "experiment_id"): [
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "workflows.daily_check.run_connection_requests",
        "workflows.daily_check.detect_accepted_connections",
        "scripts.migrate_experiments_tsv_to_attio",
        "scripts.attio_dedup",
        "scripts.backfill_experiment_id_archaeology",  # PR-22: 6th writer
    ],
    ("linkedin_outreach", "experiment_id_frozen_at"): [
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "workflows.daily_check.run_connection_requests",
        "workflows.pre_invite_check._pre_invite_degree_check",
        "workflows.daily_check.detect_accepted_connections",
        "scripts.migrate_experiments_tsv_to_attio",
        "scripts.attio_dedup",
        "scripts.backfill_experiment_id_archaeology",  # PR-22: 7th writer
        # Phase C reconciliation sweep: re-stamps connection_sent on the
        # PROSPECT→CONNECTION_SENT flip (same guard as pre_invite_check).
        "workflows.pending_invite_reconciliation.run_pending_invite_reconciliation",
        # Event-confirmed advance from the OPTIONAL Botdog transport:
        # the poll-based drain applies the same advance PB's scrape
        # phases apply, through the same AttioWriter path, but only
        # for entries stamped send_channel=botdog.
        "workflows.botdog_ingest.apply_lead_events",
    ],
    ("linkedin_outreach", "experiment_id_backfill_confidence"):
        "scripts.backfill_experiment_id_archaeology",

    # ---- LinkedIn Outreach: response classification + suppression ----
    # PR-9.5 dedup applies §3.11 most-pessimistic-wins
    # (defensive > negative > positive > null) for response_classification,
    # and logical-OR for suppress_re_engagement + had_connection_note.
    # A row that ever said "no" stays "no" through any merge — protects §3.1.
    # PR-20 fold-in: ``run_manual_reply_envelope`` placeholder was never
    # built — the manual-reply path lives at
    # ``workflows.detect_responses._handle_manual_reply`` (PR-20 extended
    # it to write response_classification + open queue row). The
    # automated-classifier writer (``workflows.detect_responses.detect_responses``)
    # is registered as the LLM/keyword path's writer; the helper
    # ``_handle_manual_reply`` is invoked from within the same module
    # so module-granularity registration covers both.
    ("linkedin_outreach", "response_classification"): [
        "workflows.detect_responses.detect_responses",
        "workflows.detect_responses._handle_manual_reply",
        "scripts.attio_dedup",
    ],
    ("linkedin_outreach", "suppress_re_engagement"): [
        "workflows.cross_channel_suppression",
        "scripts.attio_dedup",
    ],

    # ---- LinkedIn Outreach: delivery-transport routing (OPTIONAL) ----
    # `send_channel` marks which transport owns a prospect's LinkedIn
    # sends. PhantomBuster owns sending, so no SEND path writes it: the
    # routing code only READS the field (missing/unset resolves to "pb"),
    # and a row stamped "botdog" is held out of every send and of the
    # Phase 0 / 0.5 scrape detectors until it is re-stamped "pb".
    # Sole writer: the union-merge, which must carry a "botdog" stamp from
    # ANY merged member onto the surviving entry. Dropping it there would
    # silently make the survivor PB-sendable again — the double-send this
    # attribute exists to prevent.
    ("linkedin_outreach", "send_channel"): [
        "scripts.attio_dedup",
    ],

    # ---- LinkedIn Outreach: pain-signal discovery lane (OPTIONAL) ----
    # Stamped ONCE at PROSPECT-commit by the pain lane, through
    # `_commit_prospect(lane_entry_attrs=...)`. Nothing else in the engine
    # writes them: the daily invite builder only READS prospect_source /
    # pain_source_type to pick the note frame, and no send path touches
    # them. The lane is off by default and the attributes are provisioned
    # only with `setup_attio_schema.py --feature pain_signal`, so a fresh
    # install carries none of them (parse_entry then reads None).
    ("linkedin_outreach", "prospect_source"): [
        "workflows.pain_signal.run_pain_signal_discovery",
    ],
    ("linkedin_outreach", "pain_source_type"): [
        "workflows.pain_signal.run_pain_signal_discovery",
    ],
    ("linkedin_outreach", "pain_snippet"): [
        "workflows.pain_signal.run_pain_signal_discovery",
    ],
    ("linkedin_outreach", "source_post_url"): [
        "workflows.pain_signal.run_pain_signal_discovery",
    ],
    ("linkedin_outreach", "source_post_at"): [
        "workflows.pain_signal.run_pain_signal_discovery",
    ],
    # PR-20 B-SD-008: written by the false-positive guard branch in
    # detect_responses (when the prospect's only inbox message looks
    # like LinkedIn's auto-acceptance note).
    ("linkedin_outreach", "had_connection_note"): [
        "workflows.detect_responses.detect_responses",
        "scripts.attio_dedup",
    ],

    # ---- LinkedIn Outreach: cadence + lane (PR-12, PR-39) ----
    # `language` is set at PROSPECT-commit by weekly_prospect; PR-12's
    # `models.resolution.resolve_language` is a READ-validator only — it
    # parses the existing attribute value into a Language enum and raises
    # MissingLanguageError when the value is unset/invalid. It NEVER
    # calls any Attio write path, so it's not a write-owner here. The
    # plan §3.15 text calling it a "PRIMARY writer" is inaccurate; the
    # actual contract is read-validator vs. PROSPECT-commit writer.
    # PR-26 one-shot backfill (scripts.backfill_language) registered as
    # co-writer alongside the PROSPECT-commit path; only writes when
    # language is currently null/empty (natural-filter idempotency).
    ("linkedin_outreach", "language"): [
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "scripts.backfill_language",  # PR-26 one-shot backfill
    ],
    ("linkedin_outreach", "next_eligible_send_date"): [
        "workflows.daily_check.run_dm_sequencing",
        # 2026-06-09 desync sweep: converges entries toward the company
        # tally (source of truth for confirmed sends) when the in-run
        # advance failed. Same guarded write path.
        "workflows.consistency_sweep.run_company_tally_consistency_sweep",
        # Event-confirmed advance from the OPTIONAL Botdog transport:
        # the poll-based drain applies the same advance PB's scrape
        # phases apply, through the same AttioWriter path, but only
        # for entries stamped send_channel=botdog.
        "workflows.botdog_ingest.apply_lead_events",
    ],
    ("linkedin_outreach", "nurture_re_eligible_at"): [
        "workflows.gtm.nurture_backfill",
        "workflows.daily_check.run_dm_sequencing",
    ],
    ("linkedin_outreach", "cadence_lane"):
        "workflows.weekly_prospect._build_prospect_entry_attrs",

    # ---- Companies: industry classification confidence (PR-25, FU-2) ----
    # Object corrected from linkedin_outreach → companies (see attio_schema_deltas.yaml note).
    # Industry is a company-level property; multiple outreach threads per company
    # would otherwise each duplicate the same value.
    # FU-2 adds `industry_vertical` (operator override on approval) and registers
    # `workflows.industry_approve` as a co-writer of the status slug so an
    # operator can lift the abstain gate by confirming the LLM's best guess.
    # PR-25 wraps backfill_missing_industries via scripts/reclassify_industry.py
    # with MigrationRunWriter + ReclassificationRunWriter support.
    ("companies", "industry_vertical"): [
        "workflows.industry_approve",
        "scripts.reclassify_industry",
    ],
    ("companies", "industry_vertical_status"): [
        "workflows.industry_classifier.classify_with_confidence",
        "workflows.industry_approve",  # FU-2: operator override
        "scripts.reclassify_industry",  # PR-25 backfill script
    ],
    ("companies", "industry_vertical_confidence"): [
        "workflows.industry_classifier.classify_with_confidence",
        "scripts.reclassify_industry",  # PR-25 backfill script
    ],

    # ---- LinkedIn Outreach: weekly batch + quarantine (PR-28, PR-43) ----
    ("linkedin_outreach", "week_starting"):
        "workflows.weekly_prospect.weekly_finalize_idempotent",
    ("linkedin_outreach", "prospect_committed_at"):
        "workflows.weekly_prospect._build_prospect_entry_attrs",
    ("linkedin_outreach", "invite_eligible_after"):
        [
            "workflows.weekly_prospect._build_prospect_entry_attrs",
            "workflows.daily_check.run_connection_requests",
        ],

    # ---- LinkedIn Outreach: provenance pointers (F-PR-3.7) ----
    ("linkedin_outreach", "last_classified_by"):
        "workflows.reclassification_run_writer.ReclassificationRunWriter",
    ("linkedin_outreach", "last_migrated_by"):
        "workflows.migration_run_writer.MigrationRunWriter",

    # ---- Companies: provenance pointer (#18) ----
    # Mirrors the linkedin_outreach pointer above; lets company-targeted
    # migrations stamp their §3.13 back-pointer (the attribute was missing
    # on companies, so every company back-pointer PATCH 404'd).
    ("companies", "last_migrated_by"):
        "workflows.migration_run_writer.MigrationRunWriter",

    # ---- People: provenance pointer ----
    # Third mirror of the linkedin_outreach pointer above. Person-targeted
    # migrations (e.g. backfill_prospect_committed_at,
    # backfill_per_step_timestamps) call mark_modified(object="people"); the
    # attribute was missing on people, so every person back-pointer PATCH was
    # rejected. The run still exited 0 (back-pointer failures never touch
    # rows_failed), so the only symptom was a back-pointer WARNING on 100% of
    # otherwise-successful runs — alarm fatigue that also hides a genuine gap.
    ("people", "last_migrated_by"):
        "workflows.migration_run_writer.MigrationRunWriter",

    # ---- Migration Run: cross-reference to Reclassification Run (PR-9b) ----
    # Backfill scripts that open both writers pass rec_run.run_id via the
    # MigrationRunWriter constructor; this attribute records that pointer
    # on the Migration Run row so forensics consumers can join the two.
    ("migration_run", "reclassification_run_id"):
        "workflows.migration_run_writer.MigrationRunWriter",

    # ---- LinkedIn Outreach: recheck-cache architecture (F-PR-8, §3.5) ----
    ("linkedin_outreach", "last_observed_degree"):
        "workflows.daily_check.run_connection_requests",
    ("linkedin_outreach", "last_observed_at"):
        "workflows.daily_check.run_connection_requests",

    # ---- LinkedIn Outreach: Phase-1 auto-research brownfield attrs ----
    # These 11 attrs predate §3.15 and existed in production Attio before
    # the registry/manifest contract was retrofitted. Wave-2-A surfaces
    # them so F-PR-4's AttioWriter.apply() can authorize the existing
    # write paths instead of 404-raising on every PROSPECT-commit + every
    # reply classification. See lens-QA pass-1 round-2 data-steward M5.
    #
    # Primary writer for the score-derived attrs is
    # _build_prospect_entry_attrs (PROSPECT-commit). scripts.rescore_prospect_cohort
    # is a backfill co-writer that re-stamps the score fields after a
    # quality-gate config bump. icp_lane on the entry has no active writer
    # today (legacy column kept for the operator's manual UI overrides); register it
    # under the canonical Phase-1 writer so a future re-stamp path lands
    # against an already-registered slug.
    ("linkedin_outreach", "persona"):
        "workflows.weekly_prospect._build_prospect_entry_attrs",
    ("linkedin_outreach", "score_breakdown"): [
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "scripts.rescore_prospect_cohort",
    ],
    ("linkedin_outreach", "scoring_lane"): [
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "scripts.rescore_prospect_cohort",
    ],
    ("linkedin_outreach", "icp_lane"):
        "workflows.weekly_prospect._build_prospect_entry_attrs",
    ("linkedin_outreach", "llm_rationale"): [
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "scripts.rescore_prospect_cohort",
    ],
    ("linkedin_outreach", "verdict_path"): [
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "scripts.rescore_prospect_cohort",
    ],
    ("linkedin_outreach", "icp_lane_persisted"):
        "workflows.weekly_prospect._build_prospect_entry_attrs",
    ("linkedin_outreach", "quality_score_band"):
        "workflows.weekly_prospect._build_prospect_entry_attrs",
    # Reply-side Phase-1 attrs: written by the main detect_responses loop
    # when stamping response_classification on a newly-classified reply.
    ("linkedin_outreach", "last_response_text"):
        "workflows.detect_responses.detect_responses",
    ("linkedin_outreach", "defensive_score"):
        "workflows.detect_responses.detect_responses",
    ("linkedin_outreach", "engagement_score"):
        "workflows.detect_responses.detect_responses",

    # ---- Companies: per-company throttle (PR-13) ----
    # Multi-writer per §3.15: both daily_check send paths
    # (run_connection_requests for invites, run_dm_sequencing for DMs)
    # call the shared `_write_company_throttle_tally` helper post-
    # confirmed-send. Listing both explicit writers keeps the registry
    # truthful and prevents a future CI guard from mis-flagging the
    # invite-path write as unauthorized.
    # PR-13 one-shot backfill (scripts.backfill_per_company_outreach_state)
    # populates rows pre-dating incremental tracking; registered as
    # co-writer alongside the runtime send paths.
    ("companies", "last_outreach_at"): [
        "workflows.daily_check.run_dm_sequencing",
        "workflows.daily_check.run_connection_requests",
        "scripts.backfill_per_company_outreach_state",  # PR-13 one-shot backfill
    ],
    ("companies", "last_outreach_person_id"): [
        "workflows.daily_check.run_dm_sequencing",
        "workflows.daily_check.run_connection_requests",
        "scripts.backfill_per_company_outreach_state",  # PR-13 one-shot backfill
    ],
    ("companies", "last_outreach_step"): [
        "workflows.daily_check.run_dm_sequencing",
        "workflows.daily_check.run_connection_requests",
        "scripts.backfill_per_company_outreach_state",  # PR-13 one-shot backfill
    ],
    ("companies", "last_outreach_experiment_id"): [
        "workflows.daily_check.run_dm_sequencing",
        "workflows.daily_check.run_connection_requests",
        "scripts.backfill_per_company_outreach_state",  # PR-13 one-shot backfill
    ],
    ("companies", "company_hq_country"):
        "workflows.weekly_prospect.classify_company_hq",
    ("companies", "company_hq_confidence"):
        "workflows.weekly_prospect.classify_company_hq",

    # ---- People: cross-channel outreach (PR-40) ----
    # Object slug is plural ("people") to match the Attio API URL —
    # `/objects/people/records/`. WriteIntent.object goes straight into
    # that URL path, so the registry key must match. Multi-writer:
    # stamp_outreach_channel is the runtime stamper; the migration
    # script seeds the historical pool.
    ("people", "outreach_channel"): [
        "workflows.cross_channel_suppression",
        "scripts.migrate_association_outreach_to_attio",
    ],

    # ---- Deals (PR-38) ----
    ("deals", "created_via"): [
        "workflows.deal_creation.create_deal_from_response",
        "scripts.backfill_operator_unknown_deals",
    ],
    # creation_idempotency_key (PR-38): the Round-4 D25 idempotency key.
    # Only the create_deal_from_response writer stamps it; the future
    # confirm-backfill (operator-resolved deal_creation_confirm queue
    # rows) uses the same key for its CREATE so it shares the writer.
    ("deals", "creation_idempotency_key"):
        "workflows.deal_creation.create_deal_from_response",

    # ---- daily_run: identity + lease state (F-PR-8) ----
    ("daily_run", "run_date"): "workflows.daily_run.open_daily_run",
    ("daily_run", "machine_id"): "workflows.daily_run.open_daily_run",
    ("daily_run", "uniqueness_key"): "workflows.daily_run.open_daily_run",
    ("daily_run", "hostname"): "workflows.daily_run.open_daily_run",
    ("daily_run", "started_at"): "workflows.daily_run.open_daily_run",
    ("daily_run", "completed_at"): "workflows.daily_run.open_daily_run",
    ("daily_run", "status"): "workflows.daily_run.open_daily_run",
    ("daily_run", "process_id"): "workflows.daily_run.open_daily_run",
    ("daily_run", "run_id"): "workflows.daily_run.open_daily_run",
    ("daily_run", "connections_sent"): "workflows.daily_run.record_send",
    ("daily_run", "messages_sent"): "workflows.daily_run.record_send",
    ("daily_run", "visits_sent"): "workflows.daily_run.record_send",
    ("daily_run", "failure_details"): "workflows.daily_run.open_daily_run",

    # ---- daily_run: run-end summary (PR-17 + §3.18) ----
    # Note: ``nurture_silent_skipped_count`` is owned by PR-39's nurture
    # re-engagement path (different writer module).
    ("daily_run", "prospect_pool_size"): "workflows.daily_check.run_end_summary",
    ("daily_run", "due_dm1_count"): "workflows.daily_check.run_end_summary",
    ("daily_run", "due_dm2_count"): "workflows.daily_check.run_end_summary",
    ("daily_run", "due_dm3_count"): "workflows.daily_check.run_end_summary",
    ("daily_run", "degree_unknown_count"): "workflows.daily_check.run_end_summary",
    # PR-19 B-SD-005: written mid-flight by detect_responses via
    # ``DailyRun.set_reply_detection_status`` ('failed' on no-CSV halt
    # before SystemExit(2); 'ok' on success). Part-B
    # (``run_dm_sequencing``) reads via ``get_reply_detection_status``
    # to short-circuit. run_end_summary does NOT write this attr —
    # the status was already durable on the row by the time the summary
    # runs (and on the failed path the summary never runs).
    ("daily_run", "reply_detection_status"): "workflows.daily_run.DailyRun.set_reply_detection_status",
    ("daily_run", "starvation_signal"): "workflows.daily_check.run_end_summary",
    ("daily_run", "nurture_silent_skipped_count"):
        "workflows.daily_check.run_nurture_re_engagement",

    # ---- Operator Review Queue (F-PR-3 — registered for AttioWriter scope) ----
    ("operator_review_queue", "type"): "workflows.escalation.escalate",
    ("operator_review_queue", "decision_key"): "workflows.escalation.escalate",
    ("operator_review_queue", "idempotency_key"): "workflows.escalation.escalate",
    ("operator_review_queue", "uniqueness_key"): "workflows.escalation.escalate",
    ("operator_review_queue", "payload_json"): "workflows.escalation.escalate",
    ("operator_review_queue", "status"): [
        "workflows.escalation.escalate",
        "workflows.escalation_resolver",
        "workflows.sales_approve",  # PR-32: approve/reject closes queue rows
        "workflows.industry_approve",  # FU-2: operator override for industry_low_confidence
    ],
    ("operator_review_queue", "opened_at"): "workflows.escalation.escalate",
    ("operator_review_queue", "resolved_at"): [
        "workflows.escalation.escalate",
        "workflows.escalation_resolver",
        "workflows.sales_approve",  # PR-32
        "workflows.industry_approve",  # FU-2
    ],
    ("operator_review_queue", "deadline"): "workflows.escalation.escalate",
    ("operator_review_queue", "decision_json"): [
        "workflows.escalation.escalate",
        "workflows.escalation_resolver",
        "workflows.sales_approve",  # PR-32
        "workflows.industry_approve",  # FU-2
    ],
    ("operator_review_queue", "decision_source"): [
        "workflows.escalation.escalate",
        "workflows.escalation_resolver",
    ],
    ("operator_review_queue", "agent_host"): "workflows.escalation.escalate",

    # ---- Experiment (F-PR-6 base schema) ----
    # The migration script is the sole writer at F-PR-6 ship time. Future
    # PRs will extend specific attrs to additional writers via explicit
    # multi-writer declaration (e.g., `status` is expected to gain
    # `workflows.sales_approve` at PR-32 for verdict transitions; the
    # per-step rate fields will gain a learn/weekly-brain writer when
    # those PRs land). Each extension lands together with its manifest
    # update per the F-PR-3.5 protocol.
    ("experiment", "experiment_id"): "scripts.migrate_experiments_tsv_to_attio",
    # PR-31 adds workflows.learn.apply_verdict as the second authorized writer
    # for Experiment.status. The migration script seeds initial rows; the
    # weekly learn cycle transitions running → {WON, LOST, REJECTED_NULL,
    # REJECTED_DEFENSIVE} on mature cohorts via apply_verdict, and the
    # REJECTED_DEFENSIVE path additionally opens a variant_paused_defensive
    # queue row for operator review (single-shot per experiment_id).
    ("experiment", "status"): [
        "scripts.migrate_experiments_tsv_to_attio",
        "workflows.learn.apply_verdict",
    ],
    ("experiment", "started"): "scripts.migrate_experiments_tsv_to_attio",
    ("experiment", "completed"): "scripts.migrate_experiments_tsv_to_attio",
    ("experiment", "variable"): "scripts.migrate_experiments_tsv_to_attio",
    ("experiment", "description"): "scripts.migrate_experiments_tsv_to_attio",
    # PR-11: recompute_baseline_v0 also appends `superseded` events to
    # state_history and overwrites the per-step rate attrs on baseline-v0.
    # Both writers are append-/overwrite-only on disjoint cohort scopes
    # (TSV migration: F-PR-6 one-shot seed; recompute: baseline-v0 only).
    ("experiment", "state_history"): [
        "scripts.migrate_experiments_tsv_to_attio",
        "scripts.recompute_baseline_v0",
    ],
    ("experiment", "cohort_size"): "scripts.migrate_experiments_tsv_to_attio",
    ("experiment", "dm_response_rate"): "scripts.migrate_experiments_tsv_to_attio",
    ("experiment", "dm1_response_rate"): [
        "scripts.migrate_experiments_tsv_to_attio",
        "scripts.recompute_baseline_v0",
    ],
    ("experiment", "dm2_response_rate"): [
        "scripts.migrate_experiments_tsv_to_attio",
        "scripts.recompute_baseline_v0",
    ],
    ("experiment", "dm3_response_rate"): [
        "scripts.migrate_experiments_tsv_to_attio",
        "scripts.recompute_baseline_v0",
    ],
    ("experiment", "baseline_rate"): "scripts.migrate_experiments_tsv_to_attio",

    # ---- Experiment (PR-33 — DM3 revisit verdict attrs) ----
    ("experiment", "verdict_v1"):
        "workflows.weekly_brain.apply_dm3_revisit_verdict",
    ("experiment", "verdict_v2"):
        "workflows.weekly_brain.apply_dm3_revisit_verdict",
    ("experiment", "dm3_window_settles_at"):
        "workflows.weekly_brain.apply_dm3_revisit_verdict",

    # ---- LLM Budget Ledger (PR-35) ----
    # NOTE: refactor-QA Rec #2 proposes replacing this Attio object with a
    # local JSON file. Pending operator decision; registry entries kept
    # so the §3.15 contract holds whichever way the operator goes.
    ("llm_budget_ledger", "step"): "workflows.llm_budget.LLMBudgetLedger.try_reserve",
    ("llm_budget_ledger", "week_starting"): "workflows.llm_budget.LLMBudgetLedger.try_reserve",
    ("llm_budget_ledger", "cap_remaining_this_week"): "workflows.llm_budget.LLMBudgetLedger.try_reserve",
    ("llm_budget_ledger", "cost_usd_actual"): "workflows.llm_budget.LLMBudgetLedger.try_reserve",
    ("llm_budget_ledger", "consumed"): "workflows.llm_budget.LLMBudgetLedger.try_reserve",

    # ---- Weekly KPI Snapshot (PR-30) ----
    # Sole writer is `run_weekly_report` per snapshot upsert. The HTML
    # email is a derived view of this object; subagents read only.
    ("weekly_kpi_snapshot", "week_starting"):
        "workflows.weekly_report.run_weekly_report",
    ("weekly_kpi_snapshot", "kpi_snapshot_json"):
        "workflows.weekly_report.run_weekly_report",
    ("weekly_kpi_snapshot", "persona_funnels_json"):
        "workflows.weekly_report.run_weekly_report",
    ("weekly_kpi_snapshot", "active_deals_json"):
        "workflows.weekly_report.run_weekly_report",
    ("weekly_kpi_snapshot", "measurement_basis"):
        "workflows.weekly_report.run_weekly_report",
    ("weekly_kpi_snapshot", "report_email_sent_to"):
        "workflows.weekly_report.run_weekly_report",
    ("weekly_kpi_snapshot", "report_resend_message_id"):
        "workflows.weekly_report.run_weekly_report",

    # ---- Data Quality Report (PR-43.5) ----
    # All written by scripts.data_quality_report.write_report — one row
    # per weekly run. The script is registered in
    # tests/test_migration_writer_compliance.py::EXEMPT_SCRIPTS because
    # the eight metric collectors are pure Attio reads; only the
    # write_report function mutates the Data Quality Report object.
    ("data_quality_report", "run_id"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "generated_at"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "period_start"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "period_end"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "cohort_tagging_regression_count"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "write_owner_invariant_violated_count"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "migration_idempotency_regression_count"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "manual_reply_classification_gap_count"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "nurture_silent_skipped_count_7d"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "nurture_count_parse_errors_7d"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "pipeline_starvation_open_count"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "back_pointer_failures_count_7d"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "legacy_archaeology_pool_count"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "p0_alarms_fired"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "p1_alarms_fired"):
        "scripts.data_quality_report.write_report",
    ("data_quality_report", "report_text"):
        "scripts.data_quality_report.write_report",

    # ---- Follow-up Radar state (PR-211/214/247) ----
    # Sole writer for every radar-state attribute is workflows.followup_state
    # (the stamp/snooze/mute/callback/touch/await CLI paths route through it via
    # AttioWriter). The five followup_* attrs land on BOTH warm object types
    # (the linkedin_outreach list + the deals object); deals also carries the
    # partner-attribution + verified-touch extras; the WAITING lane adds
    # awaiting_reply_* on both.
    ("linkedin_outreach", "followup_draft_at"): "workflows.followup_state",
    ("linkedin_outreach", "followup_draft_id"): "workflows.followup_state",
    ("linkedin_outreach", "followup_snooze_until"): "workflows.followup_state",
    ("linkedin_outreach", "followup_muted"): "workflows.followup_state",
    ("linkedin_outreach", "followup_callback_date"): "workflows.followup_state",
    ("deals", "followup_draft_at"): "workflows.followup_state",
    ("deals", "followup_draft_id"): "workflows.followup_state",
    ("deals", "followup_snooze_until"): "workflows.followup_state",
    ("deals", "followup_muted"): "workflows.followup_state",
    ("deals", "followup_callback_date"): "workflows.followup_state",
    # referred_by (PR-214): the referring partner's canonical lowercase email,
    # stamped by the skill layer (via the `followup-refer` CLI) onto the deal
    # through workflows.followup_state.stamp_referred_by.
    ("deals", "referred_by"): "workflows.followup_state",
    # last_verified_touch (PR-214): the skill-verified true last-touch date,
    # stamped via `followup-touch` after each Phase C verification.
    ("deals", "last_verified_touch"): "workflows.followup_state",
    # ---- WAITING lane: awaiting_reply_* state (PR-247) ----
    ("linkedin_outreach", "awaiting_reply_since"): "workflows.followup_state",
    ("linkedin_outreach", "awaiting_reply_thread_id"): "workflows.followup_state",
    ("linkedin_outreach", "awaiting_reply_note_id"): "workflows.followup_state",
    ("linkedin_outreach", "awaiting_reply_nudge_count"): "workflows.followup_state",
    ("deals", "awaiting_reply_since"): "workflows.followup_state",
    ("deals", "awaiting_reply_thread_id"): "workflows.followup_state",
    ("deals", "awaiting_reply_note_id"): "workflows.followup_state",
    ("deals", "awaiting_reply_nudge_count"): "workflows.followup_state",

    # language: the person-level outreach-language override that outranks
    # company-HQ inference in models.resolution. A NARROW human-curated
    # exception list — empty for almost everyone — so the script is the
    # only code writer; operators may also set it by hand in the CRM UI,
    # which the resolver reads the same way. Written via direct
    # attio.update_person, NOT AttioWriter.apply, so this registry never gates
    # it; the entry exists for manifest parity.
    ("people", "language"): "scripts.set_person_language",

    # ---- People: email response detection (Phase 0.6, PR-243) ----
    # Sole writer: the daily-run email reply detector. Note that
    # email_campaign_stage itself stays OUTSIDE the registry (the email
    # lane's writes go through attio.update_person directly, matching
    # workflows/email_campaign.py's existing convention).
    ("people", "email_response_classification"):
        "workflows.detect_email_responses.detect_email_responses",
    ("people", "email_response_received_at"):
        "workflows.detect_email_responses.detect_email_responses",
    ("people", "last_email_response_text"):
        "workflows.detect_email_responses.detect_email_responses",
    # Written via direct attio.update_person alongside email_campaign_stage
    # (email-lane convention); registered for manifest parity.
    ("people", "email_last_resend_id"):
        "workflows.email_campaign.run_email_daily",
}


class UnauthorizedAttioWriteError(PermissionError):
    """Raised by F-PR-4's AttioWriter when a caller writes an attribute
    not declared in `WRITE_OWNER_REGISTRY`. Lands fully in F-PR-4; the
    exception class lives here so the registry and the error stay in
    one place.
    """


def get_authorized_writers(object: str, slug: str) -> list[str] | None:
    """Return the list of authorized write-owner modules for an attribute.

    Returns `None` if the attribute is not registered (unknown attribute
    — F-PR-4 raises `UnauthorizedAttioWriteError` in that case). Always
    returns a list, even for sole-writer entries, so callers can iterate
    uniformly.
    """
    entry = WRITE_OWNER_REGISTRY.get((object, slug))
    if entry is None:
        return None
    return [entry] if isinstance(entry, str) else list(entry)


def is_authorized_writer(object: str, slug: str, module_path: str) -> bool:
    """Quick membership check used by F-PR-4's AttioWriter.

    Returns True if `module_path` is registered as a writer for
    `(object, slug)` OR is one of the `SPECIAL_WRITER_ALIASES` (blanket
    write-anywhere aliases for bootstrap operations like the §3.20
    MCP canary)."""
    if module_path in SPECIAL_WRITER_ALIASES:
        return True
    owners = get_authorized_writers(object, slug)
    return owners is not None and module_path in owners
