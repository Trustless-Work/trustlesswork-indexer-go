# Changelog

All notable changes to the Indexer are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The envelope wire contract is versioned independently of the binary;
see `docs/event-schema.md`.

## [Unreleased]

### Fixed
- Upgrade `go-stellar-sdk` v0.6.0 to v0.7.3 so the XDR codec understands the
  new protocol ledger format. The old SDK could not decode `ScValType`
  variant 22 (`SCV_EXECUTABLE_TAG`), which froze ingestion on any ledger
  that carried it — testnet stalled at ledger 4372189 retrying the same
  unmarshal indefinitely. Mainnet has not upgraded yet but would hit the
  same wall; this lifts it for both networks. No source changes were needed
  beyond guarding a pre-existing test clock against the race detector.

### Added
- Envelope signing: every message published to the broker (all types,
  replays included) now carries an `x-tw-sig` AMQP header — lowercase hex
  HMAC-SHA256 of the exact published body under the `ENVELOPE_HMAC_KEY`
  shared with the core API, which verifies it on consumption. Transport
  metadata only: the envelope JSON and `schema_version` are unchanged, so
  this needs no consumer migration. With no key configured the indexer
  publishes unsigned and warns at boot.
- Prometheus `/metrics` on the health server: a collector over the same
  tracker snapshot that backs `/status` (current ledger, ledger age,
  publish totals, gaps, ready/paused, uptime — all labelled by network)
  plus `indexer_ingest_ledger_fetch_duration_seconds`, registered once
  at the RPC pool level so it survives endpoint rotations (the SDK's
  `ledgerbackend.WithMetrics` registers per call and would panic on the
  first rotation — same metric, rotation-safe implementation).
- `indexer replay --from N --to M`: one-shot bounded re-publication of
  a ledger range (the Horizon `db reingest range` pattern). Runs next
  to the live indexer: nothing persisted, no flock, no command
  consumption (it would compete on the live queue), no heartbeat (its
  pings would mask a dead live indexer). Borrows the live watchlist as
  a lock-free point-in-time copy, refuses ranges no endpoint serves in
  full, and is events/deposits-only by design (bounded runs suppress
  state snapshots). Downstream dedupe absorbs the re-published copies.
- Split state files: the hot cursor (~150 bytes, every checkpoint)
  separated from the cold watchlist (escrows + tombstones + gaps,
  rewritten only when its content changes), both marshaled compactly.
  Before, every tip checkpoint rewrote and fsynced the whole combined
  record — ~150KB per ledger at 2.5k escrows to move a 4-byte cursor.
  Pre-split files migrate transparently on first load.
- The reconciliation sweep's published states now count into
  `state_changes_published_total` (they previously bypassed the
  tracker).
- TW-03 origin-side: deposit classification requires a well-formed
  transfer (`from` must decode as an Address) and the envelope's escrow
  is locked by test to the raw event's `to` — mirroring the consumer's
  check so both ends of the pipe enforce the invariant independently.
- Command plane (`internal/commands`): the indexer can now be told
  things at runtime instead of redeployed. Commands arrive over AMQP
  (direct exchange `stellar.commands` → queue
  `indexer.commands.<network>`, own connection with redial) and over an
  authenticated admin HTTP surface on the health server (`ADMIN_TOKEN`,
  bearer auth in constant time, disabled when unset): POST
  `/admin/escrows`, `/admin/escrows/{id}/refresh`, DELETE
  `/admin/escrows/{id}`, `/admin/reseed`, `/admin/pause` (TTL-capped at
  1h, auto-resume, deliberately noisy: readyz 503 + heartbeat silence +
  periodic warnings), `/admin/resume`, `/admin/reconcile`, GET
  `/admin/registry`. Every entry point only validates and enqueues; the
  ingest loop drains between ledgers and executes, staying the single
  writer of registry and state, and each execution leaves one audit log
  line. `track_escrow` seeds idempotently and publishes current state
  within seconds; `reconcile` restarts the sweep with the changed-since
  filter disarmed. Removal is a persisted TOMBSTONE: discovery and seed
  files cannot resurrect a removed escrow — only an explicit track can.
- Backpressure-aware publishing: a full queue now makes the indexer WAIT
  instead of die. Broker rejections that leave the channel usable (a
  nack from a reject-publish overflow policy, a confirm timeout) retry
  in place with 1s→60s backoff and no attempt cap, holding the cursor —
  idempotent message ids plus consumer dedupe make indefinite
  republishing safe, and the heartbeat's silence alert fires while the
  loop waits. A new `ErrSinkUnroutable` distinguishes `basic.return`
  (broken topology — fatal, retrying would hot-loop) from queue-full
  nacks; `ErrSinkUnavailable` stays fatal-fast because a failed AMQP
  channel never recovers in-process — the crash-restart owns the redial
  (in-process reconnection evaluated and declined).
- The indexer's command queue (`indexer.commands.<network>`) is a
  QUORUM queue from birth, matching the core consumer's migrated
  queues.

### Removed
- Dead configuration that announced nonexistent features:
  `INDEXER_WORKERS`, `INDEXER_SKIP_TX_META`, `INDEXER_SKIP_TX_ENVELOPE`,
  `STATE_RESET` and `WATCHLIST_SEED_PATH` were parsed and validated but
  never read by any code path. (`ESCROW_SEED_PATH` remains the real
  seed mechanism.)

### Added (earlier this cycle)
- Multi-RPC failover (`RPC_FALLBACK_URLS`): the RPC connection is now an
  ordered endpoint pool feeding both the ledger backend and the
  getLedgerEntries state fetches. When the active endpoint exhausts its
  bounded retries — or its tip stalls (a context-deadline watchdog, since
  the SDK's `GetLedger` blocks internally at a frozen tip) — the loop
  rotates to the next endpoint: it re-verifies the network passphrase,
  clamps the cursor against the new endpoint's retention window
  (recording a gap only if no endpoint serves the range), re-prepares
  the range, and re-arms the chain-continuity hash check so a fallback
  serving a different chain cannot poison the read-model. Retries are
  bounded per endpoint, infinite across the pool; the process dies only
  when every endpoint failed. Boot connects through the same pool, so a
  restart during a primary outage comes up on a fallback. `/status` now
  reports the active endpoint host as `rpc_endpoint`, and
  `config.String()` redacts credentials in URL lists.
- Centralized configuration in `internal/config/` loaded via
  `github.com/caarlos0/env/v11`. `Load()` validates cross-field rules
  (e.g. `SINK_TYPE=rabbitmq` requires `RABBITMQ_URL`) and `String()`
  dumps the effective config at boot with URL passwords redacted.
- `internal/events/` package: the envelope wire contract, the 12 TW
  topic Symbols, the topic filter, deterministic `MessageID` builder,
  and sentinel errors.
- `internal/state/` package: atomic file-backed persistence for the
  cursor + watchlist as one record. Uses write-temp + fsync + rename +
  parent-dir fsync; enforces single-writer via `flock`. Optional seed
  file (`WATCHLIST_SEED_PATH`) for first boot.
- `internal/detector/`: two-pass per-ledger scan. Pass 1 (sequential)
  discovers `tw_init` events and updates the watchlist. Pass 2 emits
  envelopes for the 12 TW topics plus SAC `transfer` events whose
  recipient matches the watchlist.
- `internal/publisher/`: thin adapter from `detector.DetectedEvent` to
  `events.Envelope`, with publish-duration metric.
- `internal/metrics/`: 11 Prometheus metrics with low-cardinality
  labels (never `contract_id`), surfaced via named recorder functions.
- `internal/health/`: HTTP server exposing `/healthz` (liveness),
  `/readyz` (sink ping), `/metrics` (Prometheus scrape), `/status`
  (JSON snapshot). Graceful shutdown on context cancel.
- `internal/rpc/`: sentinel errors and a `Classify` helper that maps
  SDK / HTTP / JRPC errors to stable categories the main loop can
  dispatch on.
- `internal/errs/`: category predicates `IsTransient`, `IsFatal`,
  `IsSkippable`. Disjoint invariant verified by tests.
- `internal/sink/`: new envelope-based `Sink.Publish` interface,
  sentinel errors (`ErrSinkUnavailable`, `ErrSinkPublishRejected`).
- Real publisher confirms in `internal/sink/rabbitmq/`: every `Publish`
  blocks on a positive broker ack before returning success, bounded by
  `PublishConfirmTimeout`.
- `.env.example` documenting every variable, `.env` with dev defaults,
  and a Makefile that sources `.env` via POSIX shell at `make run`.
- `STRICT_MODE` configuration: when `true` (production default),
  skippable errors halt the loop with full context; when `false` (dev),
  they are logged at ERROR and the cursor advances.
- `LOG_FORMAT` configuration with `auto|json|text`. `auto` detects a
  TTY and switches between human-readable and JSON.

### Changed
- **Breaking (env var renames)**: variables now use domain prefixes so
  the surface is auditable from one place (`internal/config/config.go`):
  - `START_LEDGER` → `INDEXER_START_LEDGER`
  - `END_LEDGER` → `INDEXER_END_LEDGER`
  - `GET_LEDGERS_LIMIT` → `INDEXER_GET_LEDGERS_LIMIT`
  - `LEDGER_BACKEND_TYPE` → `INDEXER_LEDGER_BACKEND_TYPE`

  Variables kept as-is: `RPC_URL`, `NETWORK_NAME`, `NETWORK_PASSPHRASE`,
  `SINK_TYPE`, `RABBITMQ_*`, `LOG_LEVEL`.

- **Breaking (sink contract)**: `Sink.Write(buffer, ledgerSeq)`
  replaced by `Sink.Publish(ctx, events.Envelope)`. One message per
  detected event instead of six batched messages per ledger.
- **Breaking (routing keys)**: from `stellar.<network>.<entity>` to
  `stellar.<network>.escrow.<event_kind>`. Single-segment wildcard
  bindings now work for any individual event kind.
- **Breaking (`EventKindTokenTransfer` value)**: `token.transfer` →
  `token_transfer`. Dots in the value broke single-segment AMQP wildcard
  bindings; snake_case aligns with the other event kinds.
- The buffer-based indexer (`internal/indexer/processors/`) is no
  longer reachable from the live pipeline, but is intentionally
  retained as capture machinery for future envelope kinds. Per the
  design decision recorded on 2026-05-13, the processors parse generic
  blockchain metadata (participants, classic operations, trustlines,
  SAC events) that is stable across contract evolution and may be
  useful when new envelope kinds (state changes, transactions, etc.)
  are added. The only sub-file slated for surgery is
  `processors/contracts/escrow_parser.go`, which violates the
  filter-and-forward principle by decoding contract-specific structs
  and will be reduced to identity-only detection.
- Default RPC ledger fetch retry policy: exponential backoff
  (1s → 2s → … → 30s cap), 10 attempts, with `rpc.Classify` driving
  the retry/fail decision.

### Removed
- The old hand-rolled env loader (`internal/ingest/config_env.go`),
  replaced by `internal/config`. File emptied in this commit and slated
  for `git rm` in the next housekeeping pass.
- The legacy `ingest.Config` struct, replaced by `*config.Config`.
- The `Sink.Write(buffer, ...)` interface and all references to the
  six-batch-per-ledger pattern.

### Fixed
- The previous publisher-confirms toggle did not actually wait for
  acks; it enabled the broker channel in confirms mode but advanced
  the cursor without inspecting the confirmation stream. Now confirms
  are observed and the cursor only advances on a positive ack.
- Off-by-one: the bounded backfill loop was `currentLedger < endLedger`,
  which skipped the last ledger of a `[start, end]` range. Corrected
  to `<=`.

### Known issues / not addressed
- `withdraw_remaining_funds` in `single-release-*` and
  `multi-release-v2` contracts does not emit a Soroban event;
  withdrawals via that function are not captured by the Indexer.
  Fix belongs in the contracts repo.
- The "datastore" ledger backend type is reserved in config but not
  implemented; selecting it errors at boot.
