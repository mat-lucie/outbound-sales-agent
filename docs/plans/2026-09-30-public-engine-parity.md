# Public engine parity — 2026-09-30

LUC-428 ports eligible engine improvements from upstream revision 4f79fb7 (PR #333),
relative to the previous public synchronization at daec30b. Public base: cfaf746.

Scope: verified inbox identity and acceptance, exact delivery confirmation,
bounded invite/DM draining, fresh stage checks, ambiguous-write handling,
query-only previews, preparation evidence/checkpoints, attended Codex dispatch,
paced incremental Gmail reads, complete skill installation and neutral guidance.

Preserve CRMProvider, configured fields and stages, operator content directories,
placeholder safeguards and machine-keyed single-operator ledgers. Acceptance
monitoring occurs in Phase 0; invite delivery never replays previously invited
contacts as profile checks. Multioperator ownership and private US campaign
policy are outside this synchronization. Private copy, identities, credentials,
provider exports and local operator state are excluded.

Review lenses: prospect identity/duplicate-contact protection; operator honesty
about partial delivery; retry and evidence semantics; pipeline and learning-loop
integrity. Verification: lint, static typing, offline tests, public-data scan and
independent delivery/control review. This port does not authorize live outreach
or merging.
