# Known limitations

These are tracked, known edges in the current state of the outbound-agent
engine. They are documented here so a non-Attio operator knows what to expect
before committing to an integration, and so maintainers can find them in one
place. None are hidden — each has a clear remediation path.

---

## 1. Field / stage mapping in `config/crm.yaml` — wired, with two scoped residuals

`config/crm.yaml`'s `field_mapping` and `stage_mapping` sections are now
**consumed** by the Attio adapter (loaded once via `clients/crm/mapping.py`'s
`load_crm_mapping()` and injected into the provider + inner client by the
factory):

```yaml
field_mapping:                 # engine field -> vendor slug  [CONSUMED]
  linkedin_url: linkedin
  full_name: name
  company_domain: domains
stage_mapping:                 # engine stage -> vendor label + rank  [CONSUMED]
  CONNECTION_SENT: { name: "Invite sent", rank: 1 }
  ...
stage_field: stage             # which slug carries the stage  [NOT YET CONSUMED]
```

- **`stage_mapping`** translates your CRM's pipeline-stage option **labels** to
  the engine's canonical `PipelineStage` names (read) and back (write). A
  partial mapping merges over the identity default, so unnamed stages keep their
  canonical label + `STAGE_RANK` rank.
- **`field_mapping`** routes the three documented engine fields (`linkedin_url`,
  `full_name`, `company_domain`) to your workspace's attribute slugs for the
  adapter's own reads/filters. A partial mapping merges over the canonical Attio
  defaults.
- The bundled defaults are the **identity** mapping, so an Attio deployment with
  no `config/crm.yaml` (or one that leaves these sections at their canonical
  values) behaves byte-identically to before this wiring landed.

**Two scoped residuals remain:**

1. **`stage_field` (the slug carrying the stage) is still inert.** Reading the
   stage off a non-`stage` slug requires de-static-ing the pervasive
   `AttioClient.parse_entry` helper (~30 source + ~140 test call sites) — a
   separate refactor. A write-only `stage_field` would silently lose the stage
   on round-trip, so it stays inert: the engine reads **and** writes the stage
   under the literal `stage` slug.
2. **`field_mapping` for `linkedin_url` covers reads but not the upsert write
   path.** `search_person_by_linkedin` / `extract_record_info` / the corruption
   guard honor a renamed LinkedIn slug, but `upsert_person` still matches and
   writes the literal `linkedin` slug. A workspace that has *both* a renamed slug
   and the canonical `linkedin` slug could split person dedup silently. Routing
   the write path is deferred (it touches caller-supplied vendor-shaped attrs).

**For a generated non-Attio adapter:** the adapter's normalization layer still
owns mapping your CRM's shapes to the engine's canonical values; `config/crm.yaml`
covers the Attio adapter's slug/label resolution, not an arbitrary vendor's.

---

## 2. Filter DSL leak in `query_object_records`

`CRMProvider.query_object_records(object_type, *, filters, sorts, limit)` and
the `search_*` methods pass the `filters` / `sorts` arguments **through to the
adapter as a vendor-native query body**. For Attio that body is Attio's filter
DSL (`{"$and": [...]}`, `{"$not": {"$in": [...]}}`).

**What this means for you:**

- A **non-Attio adapter** must translate incoming Attio-shaped filter bodies
  into its own vendor's query language, or every caller that passes a `filters`
  body remains Attio-coupled.
- Only flat-equality filters are tested by the conformance suite
  (`tests/crm/test_provider_contract.py`). Richer filter shapes (`$and`,
  `$or`, nested operators) are per-adapter; the suite makes no universal
  assertion about them.
- Return values are still normalized to `Record` regardless of vendor, so reads
  **downstream** of the query are vendor-agnostic — only the query body leaks.

This is documented in `clients/crm/CONTRACT.md` ("Known limitation — the
filter-shape leak").

---

## 3. Write path is not yet fully vendor-neutral

`clients/attio_writer.py` (`AttioWriter`) handles write fanout, stage monotonicity
enforcement, and compensating rollback on partial failure. Its internals are
Attio-coupled in two ways:

- **Exception model / compensating rollback** (`_compensating_rollback`): the
  atomicity logic is written against Attio's error shapes. This is above the
  provider seam and not yet vendor-neutralized.
- **Raw PATCH fallback**: for object types without a dedicated typed helper,
  `AttioWriter` reaches into the inner `AttioClient._client.request(...)` directly
  (a private-channel bypass). This path is not routed through `CRMProvider`.

**What this means for you:** the provider seam covers the typed read/write
methods (`update_person`, `update_list_entry`, `query_object_records`, etc.).
The writer's retry/rollback exception model is a separate later increment.
Non-Attio operators using the typed provider methods are unaffected; operators
who extend or replace the writer need to adapt the Attio-specific exception
handling.

---

## 4. Email and Google Sheets are not abstracted behind a provider

Two integrations are not yet behind a vendor-neutral seam or covered by the
`/onboard` skill:

- **Email** (`clients/resend_client.py`): hot-lead alert emails use the Resend
  API directly, keyed by `RESEND_API_KEY`. If you use a different transactional
  email provider, replace or wrap this client. The engine degrades gracefully
  when `RESEND_API_KEY` is absent — the alert-email path is skipped.
- **Google Sheets export** (`clients/google_sheets.py`): the `backfill-export`
  command writes to a Google Sheet via OAuth2 credentials stored at
  `credentials/google-oauth.json` and `credentials/google-authorized-user.json`.
  This is not covered by onboarding and is not behind a provider interface.

---

## 5. Some workflows still hold `AttioClient` directly

The CRM seam migration is in progress (see `clients/crm/CONTRACT.md` →
"Migration pattern"). A subset of `workflows/` receive the raw `AttioClient`
via `bundle.attio` rather than the vendor-neutral `CRMProvider` via
`bundle.provider`. These workflows are fully functional with Attio; for a
non-Attio CRM they would need the migration slice applied first.

The migration pattern is documented in `clients/crm/CONTRACT.md`; each slice
is a mechanical substitution with no behavior change.

`workflows/botdog_ingest.py` is one of them: it takes an `AttioClient`, reads
`ATTIO_LIST_ID` directly, and applies its advances through
`daily_check._attio_advance_with_escalation`. That is deliberate — it shares the
`daily` slice's escape hatch rather than inventing a second write path — but it
means the optional Botdog event drain is Attio-only until that slice migrates.

---

## 6. The optional Botdog transport is shipped but not wired to send

PhantomBuster is the only wired send transport. `clients/sender.py` defines a
`Sender` protocol with two implementations — `PBSender` (the default, used by
both send loops) and `BotdogSender` — but nothing in the engine constructs a
`BotdogSender` for a send. Its surface (`clients/botdog.py`,
`workflows/botdog_ingest.py`, `botdog_ledger.py`, `botdog_limits.py`,
`scripts/seed_botdog_blacklist.py`) ships complete and tested for operators who
want to route sends through it, but wiring that up is your work, not a config
flag: `BOTDOG_SEND_ENABLED` only turns on the read-only event drain.

The `send_channel` stamp is the interlock. A row stamped `botdog` is held out of
PB sends *and* of the Phase 0 / 0.5 scrape detectors, so it gets no outreach
from any transport — loudly counted, never silent. Unset resolves to `pb`, so a
fresh install is unaffected. See GETTING_STARTED.md → *(Optional) Botdog
delivery transport*.

There is no operations runbook for it: the upstream one is written around a
specific operator's campaigns, accounts and teardown history, and was not worth
neutralising. The module docstrings carry the operational contract instead —
`clients/sender.py:BOTDOG_INVITE_NOTE_VARIABLE` in particular is half of a
contract whose other half lives in the Botdog UI.

---

## 7. The pain-signal discovery lane ships OFF and needs your own keywords

`workflows/pain_signal.py` is a complete second supply lane — LinkedIn post
keyword search → posters/commenters/likers → Sales Nav enrichment → the normal
qualify pipeline — but it is inert on a fresh install and cannot be woken by a
single flag. Two independent gates hold it closed: `OUTBOUND_PAIN_SIGNAL_ENABLED`
must be the literal `1`, AND `content/pain_keywords.json` must be
operator-approved. The shipped registry carries the `REPLACE_THIS_TEMPLATE`
sentinel and `status: "placeholder"`, and the approval check refuses on the
sentinel **independently of the status stamp** — so flipping the status without
replacing the queries still cannot scrape (or invite off) the engine's own
template text.

Three things are your work, not config:

- **The keyword registry is yours to write.** The bundled queries are
  placeholders; `examples/acme/content/pain_keywords.json` is a synthetic
  reference showing only the file's shape. Real queries come from real buyer
  language — call transcripts, not translated playbook English — and every
  query you add is a claim your invite note makes about the post.
- **The topic gate proves the phrase, not the note.** A post's people are
  accepted only when the post text literally contains an enabled query's phrase
  (accent/case-folded, word-bounded, all terms for a paired query). That stops
  LinkedIn's inexact "exact phrase" search from feeding you work-anniversary
  posts and job ads. It cannot verify that your fixed note copy actually
  describes the post — deciding a stretched query is not worth shipping is an
  operator judgement at the wet gate.
- **The engager workers' launch contract is version-dependent.** Only the posts
  worker's contract is pinned by tests against a known column set. The
  commenters/likers workers are optional by construction: leave their ids blank
  and the lane runs posters-only and says so on every run. Their CSV column
  fallbacks cover the shapes this phantom family has shipped, and a column
  rename surfaces as a counted, echoed drop rather than a silent "nobody
  engaged".

There is no operations runbook for it: the upstream one is written around one
operator's phantoms, container ids and live-verification history. The module
docstring carries the architecture and the safety posture instead, and
GETTING_STARTED.md → *(Optional) Pain-signal discovery lane* carries the
operator-facing setup.

---

## 8. Shipped content requires configuration

The runtime content files contain neutral replacement sentinels. Configure your
own personas, targets, messages, emails, and ICP before production use. The
`/onboard` skill prepares your configuration; `examples/acme/` provides a
synthetic worked example. Live sends refuse replacement or blank templates.

---

## Email compliance: operator responsibilities + known gaps

The email path is compliance-*capable* but requires operator configuration and
operation (see GETTING_STARTED.md §6). Two gaps to be aware of:

- **No hosted one-click unsubscribe / webhook (by design).** The engine emits a
  `List-Unsubscribe` *mailto* header and a footer opt-out line, and provides
  `sales email-unsubscribe <email>` to honor opt-outs. The RFC 8058 one-click
  HTTP endpoint and the Resend bounce/complaint webhook are **operator-hosted
  infra** — a CLI repo can't run an internet-reachable service. Until you stand
  those up, you must monitor the unsubscribe inbox and run the CLI to flip each
  opt-out to `UNSUBSCRIBED`.
- **Association outreach needs configured Attio authority.** The CLI now supplies
  it, applies cross-channel suppression and shared sent state, and shares the
  email execution lock. Direct library callers that omit an Attio client retain
  local-only deduplication and must curate suppression manually. Unmatched or
  ambiguous CRM email identities are held when authority is supplied.

---

## Summary table

| Limitation | Impact for non-Attio operators | Remediation |
|---|---|---|
| `field_mapping`/`stage_mapping` wired; `stage_field` + upsert-write residuals | Stage labels + 3 field slugs configurable; stage slug + linkedin write path still literal | `stage_field` needs the `parse_entry` de-static refactor; upsert-write routing deferred |
| Filter DSL leak | Non-Attio adapter must translate Attio filter bodies | Adapter-level translation; documented in CONTRACT.md |
| Write path Attio-coupled | `AttioWriter` rollback model is Attio-specific | Provider seam covers typed writes; exception model is a later increment |
| Email + Sheets not abstracted | Replace `resend_client.py` / `google_sheets.py` manually | Engine degrades gracefully on missing `RESEND_API_KEY` |
| Partial workflow migration | Some workflows still use raw `AttioClient` | Follow migration pattern in CONTRACT.md |
| Botdog transport ships unwired | Optional alternative sender exists but no send path routes to it; the event drain is Attio-only | Wire `BotdogSender` yourself; see GETTING_STARTED.md |
| Pain-signal lane ships OFF with placeholder keywords | The second supply lane is inert until you write your own keyword registry and approve it | Replace `content/pain_keywords.json`, set `OUTBOUND_PAIN_SIGNAL_ENABLED=1`; see GETTING_STARTED.md |
| Content requires configuration | `content/` files contain replacement sentinels | Configure before production use |
| Email one-click unsubscribe / webhook | No hosted endpoint; opt-outs are manual via mailto + `email-unsubscribe` CLI | Operator stands up an HTTP endpoint + Resend webhook for full automation |
| Association library callers without Attio | Local-only deduplication cannot establish cross-channel suppression | Use the configured CLI or supply Attio authority explicitly |
