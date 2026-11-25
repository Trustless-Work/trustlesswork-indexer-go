# Where Core API and Indexer context lives

Reference for the context-sync skill and the context-sync workflow. It maps a change
in this repo to the place in `Trustless-Work/trustlesswork-context` that already holds
that knowledge, and states the conventions an update must keep.

The context repo has no file per repository. Knowledge is split by topic across
canonical files. An update edits the section that already owns the topic; it does not
add a new section, file or vocabulary to hold it.

## Destinations

| What changed | Where it goes |
| --- | --- |
| Repository role, class, default branch, visibility, documentation status | `repositories/index.md`, the repository's row |
| Build → sign → send flow, transaction families, submission | `repositories/system-map.md`, "Core Stellar Execution Model" |
| What the Indexer detects and publishes, envelopes, RabbitMQ, the Core API consumer, the read model, consistency | `repositories/system-map.md`, "Read / Indexing Model" |
| A numbered open question that the change answers or opens | `repositories/system-map.md`, "Open Architecture Questions" |
| What an integrator sees: hosts, auth, error codes, conventions, reads | `product/developer-platform.md`, "Version 2 Infrastructure Map", "Auth (Version 2)", "Conventions developers must not unlearn" |
| Responsibilities per layer, audit boundary, architectural priorities | `product/architecture.md`, "API / SDK", "Event layer", "Indexing layer", "Audit Boundary" |
| Contract generations and approved WASM hashes the Indexer discovers escrows by | `product/contract-versions.md` |
| Invariants visible through the SDKs | `product/sdk-clients.md` |
| Risk, drift or a security finding | `repositories/health-findings.md` |
| A dated incident or resolution | `updates/YYYY-MM-DD-<topic>.md`, then promoted into the canonical file above |
| A choice that must not be reversed by accident | `decision-logs/YYYY-MM-DD-<topic>.md` |

Read `meta/context-map.md` first: it is the routing map and wins over this table when
they disagree. If a change fits no existing section, say so in the pull request body
and put it where `meta/context-map.md` routes the closest topic; do not create a
section to hold it.

## What never goes in

- Living operating state: tasks, priorities, status, whether a job ran, metric
  snapshots. That lives in Notion (see `AGENTS.md`, "System of Record Split").
- This repo's own tooling, agent rules or CI, unless it changes how the system behaves
  for integrators or operators.
- A changelog of commits, a mirror of source files, or anything already stated cleanly.
- Secrets, `.env` values, credentials, unnecessary personal data.

## Vocabulary

Use the standard terms, defined in
`decision-logs/2026-09-14-production-and-audit-generation-model.md`:

- **Indexer**: this service, the event and read-model infrastructure. Not "the Go
  indexer service" or "indexer-go" in prose; the repository name
  `trustlesswork-indexer-go` appears only when the repository itself is the subject.
- **Core API v2**: the rewritten backend (`trustlesswork-core-api`) that consumes the
  Indexer's envelopes.
- **V1 API / Legacy API**: the production backend on mainnet.
- **V1 Production Contracts**, **V1 Current Contracts**, **V2 Contracts**: never a
  bare "V1" or "V2" when assurance is implied.
- **Version 2**: the developer platform built on Core API v2.
- **read model**, **build → sign → send**, **contract v1 / v2 transaction families**.

Do not state or imply that Core API v2 or the Indexer are audited.

Before writing, read how the target section already names things and reuse those
names. Do not coin a term when one exists.

## Nature and form

- Durable interpretation: what exists, what owns what, how the pieces connect, and
  where exact truth lives. Implementation detail stays in the implementation repo.
- Keep each file's own header (`Sensitivity`, `Updated` or `Snapshot`, `Status`) and
  update its date only when the content changed.
- Flows are drawn as text blocks with arrows, like the existing ones.
- A resolved open question is marked "Resolved YYYY-MM-DD" with what resolved it; it
  is not deleted.
- Preserve conflicts and uncertainty explicitly instead of picking a side.
- Match the surrounding text: same tense, same level of detail, same list style.
- English, absolute dates, no mention of assistants or automation.

## Commit and pull request

- Branch `docs/indexer-<head short sha>`.
- Subject: a plain imperative sentence, "Record the …" or "Update the …", without a
  conventional-commit prefix.
- Body, in this order: one bullet per file changed (`` `path`: what it now says ``);
  then `### Environment variables` with "None, documentation only." or the names of
  the variables the change introduces or renames (names and public on-chain
  identifiers only, never secret values); then `### Source` with the compare link of
  the reconciled range.
