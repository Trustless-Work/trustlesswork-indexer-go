# Trustless Work Indexer — agent context

Go service that follows the Stellar ledger stream, detects activity on Trustless Work
escrow contracts and publishes versioned envelopes to RabbitMQ for the core API.
`README.md` is the operator onboarding (pipeline, failover, health, replay, deploy) —
read it once. This file holds only what is NOT derivable from the code.

## Purpose (this is the design criterion, not the README blurb)

Reading ledgers is the mechanism. The purpose is to be **the source of truth for
everything on-chain that concerns us**, so that any fact can be traced back to the
ledger if it ever has to be. Derived rule for every design decision: *deliver, from
every ledger, everything that concerns us, and keep the ability to redefine "concerns
us" backwards in time.* That is stricter than "do not lose messages": a silent gap is
a failure even if nothing was technically lost. The service must do ONE thing, perfectly.

## Invariants (do not break; each one exists because of an incident or an audit finding)

- **The ingest loop is the single writer** of the watchlist and the state files.
  Commands from `/admin/*` only validate and enqueue; the loop executes them between
  ledgers. Never write state from another goroutine, handler or subcommand.
- **At-least-once with deterministic `message_id`s** (`internal/events/envelope.go`);
  the consumer dedupes. The cursor **never advances past an unpublished fact** —
  publisher confirms are awaited per message (audit A2).
- **Removals need evidence.** An empty RPC answer for a batch never publishes the batch
  as removed (audit A3, `suppressed_removals` in `/status`).
- **No AMQP command channel.** It was removed (audit A1); the only door in is the
  bearer-authenticated `/admin/*` HTTP surface, which must never be public. Any proposal
  of "let the core publish a message to the indexer over the broker" reopens A1 — the
  answer is pull, not push, and reconcile sets, not events (`docs/control-plane.md`).
- **Thin indexer, smart consumer.** Forward raw XDR; never decode escrow-specific data
  here. Identity is by approved WASM hash, never by address list or topic enumeration.
- **Chain continuity by parent hash** on resume and on endpoint rotation; a fallback
  serving a different chain must be fatal, not tolerated.

## Shared contract with core-api

The envelope shape (`schema_version` currently `1.3`), routing keys and the
`stellar.events` exchange are consumed by `trustlesswork-core-api`
(`src/presentation/ingestion` + `src/application/ingestion`). A change here is a change
there. `docs/event-schema.md` is the spec and is current.
See `.claude/skills/envelope-contract-change`.

`docs/threat-model.md` is the audit-facing statement of trust assumptions, defenses
and declared ceilings — keep it in the same PR as any change to a mechanism it cites.

## Silent-loss modes (nothing in Postgres, nothing in the gap record)

- `INDEXER_START_LEDGER=0` on a **first boot** (or after losing the state volume) means
  "start at the tip": everything before is never indexed and NO gap is recorded.
- A contract whose WASM hash is not in `ESCROW_APPROVED_WASM_HASHES` is invisible:
  zero envelopes, zero logs.
- Deleting the state file while the process runs: the loop rewrites it every ledger.
  Remove it and stop pid 1 atomically (`rm -f ... && kill -TERM 1`), never rm-then-redeploy.
  Deleting ONLY the cursor file is recovered, not lossy: boot preserves the surviving
  watchlist (escrows, tombstones, gaps) with a warning; a full reset removes the
  `.watchlist.json` file too.
- Messages parked in the core's DLQ are the core's problem to redrive, but the indexer
  side of the story is `indexer replay --from N --to M`.

## Known limits (true today; fix deliberately, not in passing)

- `internal/ingest/failover.go` clamps the start ledger using `getHealth`'s
  `OldestLedger`, but several providers serve far more history through `getLedgers`.
  So `replay` can refuse a range the endpoint would actually serve — with an error that
  suggests using an archive endpoint even when the rejected endpoint IS archive.
- Mainnet needs `INDEXER_GET_LEDGERS_LIMIT=10` (with 100 catch-up asks ~265 MB per
  answer). RPC plan decided: Validation Cloud primary + free archive fallbacks.
  Testnet has no free public ledger lake; deep testnet history may cost more than mainnet.

## Environments and operations

- Prod: Railway, one project with the broker; state on a mounted volume (`STATE_PATH`).
  `railway ssh -- CMD` flattens arguments — pass compound commands as ONE quoted string.
- The testnet indexer stays as a permanent canary after mainnet go-live.
- Local `make run` and `make up` both bind :8080 — stop one before the other.
- Ops procedures (reseed, replay, pause, state reset): `.claude/skills/ops-runbook`.

## Workflow

- Every change starts on a new branch from `main` and lands through a GitHub PR + merge.
  Never push to `main` directly. `main` is the deployed branch (Railway, testnet today;
  mainnet later — see core-api's branch plan, mirrored here when it lands).
- Definition of done: `gofmt -l .` prints nothing, `go vet ./...`,
  `CGO_ENABLED=0 go build ./...`, `go test ./...` green. Update `CHANGELOG.md`
  (`[Unreleased]`) for anything user-visible or operational.
- Code, comments, docs and ALL git artefacts are in **English**, even when the
  conversation is in Spanish. No AI trailers or mentions anywhere.
  The closing package and the cross-repo rules come from the `caleb-workflow` plugin
  (`finish-work`, `workflow-rules`, push-to-main hook) — install it from
  `github.com/zkCaleb-dev/claude-plugins`; repo-specific skills stay in `.claude/skills/`.
- Auxiliary tooling lives in a SEPARATE repo, never under this one. Ask where first.
- Sibling repos: `trustlesswork-core-api` (consumer), `trustlesswork-probe` (e2e against
  deployed environments; measures indexer→read-model lag, ~0.6 s on testnet).
- `.claude/skills/{assets,data,smart-contracts}` are Stellar reference skills, kept for
  contributors without the Anthropic Stellar plugin. Project procedures are the others.
