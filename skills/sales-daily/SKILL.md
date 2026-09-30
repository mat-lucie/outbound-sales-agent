---
name: sales-daily
description: Run attended acceptance and reply checks, confirmed invitations, reviewed weekday DMs, and a read-only follow-up radar.
---

# /sales-daily

Use a current checkout with configured operator content and CRM. This skill is
attended: it never schedules a run or grants send permission.

## Read-only preview

Run `sales daily --dry-run`. It queries pipeline inventory only: no scrapes,
Sheets or CRM writes, execution locks, cookie probes, daily ledger, LLM calls
or follow-up detection. Counts are cadence inventory; identities, copy and
live delivery eligibility remain unchecked. Verify read access for this preview;
do not perform a create/delete canary.

## Step 0 — Verify Attio MCP scope

Read the operator's sales program. Confirm the intended sender, channels,
batch and current send authorization. Verify the installed CRM connector's
documented read/list/write/delete capabilities with the configured canary.
Halt on incomplete scope with `mcp_scope_insufficient`.

Resolve open P0 alarms in the Operator Review Queue before DMs. An unavailable
queue is a halt. Acceptance and reply detection must run before sends; use
verified profile identity. A participant name does not establish identity.
Review exact recipients and rendered copy. `--yes` skips the prompt after
review; it never supplies authorization.

`sales daily --skip-dms --preview-dms-after-invites` sends approved invitations
and then rehearses DMs with the shared cache. It is a live invitation run.

## Attended LLM dispatch

Use the attending agent's native subagent capabilities within the configured
budget. Never enable an API-key fallback. For Codex, initialize a private session
with `python -m workflows.codex_dispatch init <private-parent>`. Set
`OUTBOUND_USE_LLM_DISPATCH=1` and `OUTBOUND_LLM_DISPATCH_SESSION=<session>`
only in the engine child. Keep the parent available to service typed requests
using the bounded `wait` command. Submit explicit success/error results with
`respond`; honor expiry and never answer a request twice.

Stop/wait for the child, confirm an empty inbox, and `close` the session.
Never reuse closed sessions or export dispatch globally. Claude's inbox/outbox
transport remains supported. See `docs/llm_dispatch_skill_handoff.md`.

## Execute and reconcile

The daily run detects accepts and replies, performs optional event/email
ingestion, checks starvation, delivers invitations, sequences DMs and builds
the read-only warm follow-up radar. Invites run any day; DMs run Monday–Friday.
`--force-weekend` overrides only the DM calendar gate; `--skip-followups`
skips the radar.

Acceptance monitoring occurs in Phase 0. Never replay already-invited contacts
through Network Booster as acceptance checks. Network Booster launches at most
10 new invites at a time, draining eligible capacity under one daily ledger.
Only exact provider-confirmed recipients advance; unconfirmed rows remain held
for reconciliation. DM tails drain under a bounded launch cap.

Report queued, provider-confirmed, CRM-advanced and failed counts separately.
A successful health probe or canary is not delivery proof. Inspect the run
summary and failure details before declaring completion.

## Follow-up review

Review the radar by cadence and who owes the next action. Use incremental Gmail
inventory when email evidence is needed; preserve incomplete-read errors.
A missing thread does not prove there is no reply. Fresh meeting commitments
may be included when the configured meeting source is available.
See `references/followup-review.md` for the review checklist.

Every follow-up requires verified identity, current evidence, exact copy review
and send authorization. The radar and inventory do not send or create drafts.
A local lock and machine-keyed CRM ledger prevent duplicate execution. Lock
contention exits 75; template refusal exits 78 with honest partial counts.
Never reset a hold, gate or ledger to force a rerun.
