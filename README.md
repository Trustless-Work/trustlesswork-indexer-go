# Trustless Work Indexer

[![CI](https://github.com/Trustless-Work/trustlesswork-indexer-go/actions/workflows/ci.yml/badge.svg)](https://github.com/Trustless-Work/trustlesswork-indexer-go/actions/workflows/ci.yml)

Blockchain indexer for [Trustless Work](https://www.trustlesswork.com) escrows on the
[Stellar](https://stellar.org) network. It follows the ledger stream in real time,
detects every on-chain fact that concerns a Trustless Work escrow contract —
lifecycle events, token deposits, state changes — and publishes them as versioned
JSON envelopes to RabbitMQ for any consumer to project into its own read model.

Built to run unattended: multi-RPC failover with chain-continuity checks, a durable
cursor and watchlist, recorded gap evidence for anything it could not serve,
HMAC-signed messages, Prometheus metrics, and an authenticated admin surface for
day-2 operations.

## Table of contents

- [Architecture at a glance](#architecture-at-a-glance)
- [What it publishes](#what-it-publishes)
- [Quick start (Docker)](#quick-start-docker)
- [Running from source](#running-from-source)
- [How it works](#how-it-works)
- [Configuration](#configuration)
- [Consuming the stream](#consuming-the-stream)
- [Health and monitoring](#health-and-monitoring)
- [Admin control surface](#admin-control-surface)
- [Replaying a ledger range](#replaying-a-ledger-range)
- [Deploying to production](#deploying-to-production)
- [Project structure](#project-structure)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Further reading](#further-reading)

## Architecture at a glance

```mermaid
flowchart LR
    subgraph stellar [Stellar network]
        RPC1["Soroban RPC (primary)"]
        RPC2["RPC fallbacks<br/>(archive providers)"]
    end

    subgraph indexer [Indexer]
        LOOP["Ingest loop<br/>(single writer)"]
        STATE[("state.json<br/>cursor + watchlist")]
        HTTP["HTTP :8080<br/>/healthz /readyz /status<br/>/metrics /admin/*"]
    end

    MQ[("RabbitMQ<br/>stellar.events<br/>topic exchange")]
    CONSUMER["Your consumer<br/>(e.g. Trustless Work core API)"]

    RPC1 -->|getLedgers| LOOP
    RPC2 -.->|failover| LOOP
    LOOP <--> STATE
    LOOP -->|signed envelopes| MQ
    MQ -->|"stellar.#"| CONSUMER
```

The Indexer is deliberately **thin**: it detects facts and forwards the **raw XDR**,
never decoding escrow-specific data (milestones, roles, amounts). Consumers decode
with the Stellar SDK. Escrows are identified by their **approved WASM code hash** —
not by address lists or event-topic enumeration — so shipping a new contract version
is a config change (one hash), never a code change.

## What it publishes

A single envelope shape with a `type` discriminator, one stream per network:

| `type` | What happened on-chain | Example routing key |
|---|---|---|
| `event` | The escrow itself emitted a contract event (`tw_init`, `tw_fund`, `tw_release`, …) | `stellar.testnet.escrow.event.tw_fund` |
| `deposit` | A SAC/SEP-41 token `transfer` whose recipient is a known escrow | `stellar.testnet.escrow.deposit.token_transfer` |
| `state` | The escrow's `ContractData` entry was created / updated / removed | `stellar.testnet.escrow.state.updated` |
| `control` | A fact about the pipeline itself — today `gap_detected` (a ledger range the RPCs could not serve) | `stellar.testnet.escrow.control.gap_detected` |

Delivery is **at-least-once** with deterministic `message_id`s and a total order key
(`ledger_seq`, `tx_index`, `event_index`); consumers dedupe and stay idempotent.
The full wire contract (fields, `message_id` construction, signature headers,
versioning policy) is specified in [docs/event-schema.md](docs/event-schema.md).

## Quick start (Docker)

The fastest way to see the Indexer working end to end. Requires only Docker.

```bash
git clone https://github.com/Trustless-Work/trustlesswork-indexer-go.git
cd trustlesswork-indexer-go
make up
```

`make up` brings up three containers:

1. **rabbitmq** — broker with the management UI on <http://localhost:15672>
   (user `admin`, password `admin`).
2. **rabbitmq-init** — one-shot job that declares the `indexer.events` queue bound
   to the `stellar.events` exchange with routing key `stellar.#`, then exits.
   Without a bound queue, published messages are dropped silently by AMQP design.
3. **indexer** — built from this repo, following Stellar **testnet** with the
   four published Trustless Work escrow WASM hashes pre-configured.

Verify it is alive:

```bash
curl -s localhost:8080/status | python3 -m json.tool
```

You should see the ledger cursor advancing (`current_ledger`), the active RPC endpoint
and publish totals. Envelopes appear in the `indexer.events` queue (management UI →
Queues) whenever a Trustless Work escrow has activity on testnet.

Day-to-day targets:

```bash
make logs        # follow the indexer
make rebuild     # rebuild + restart the indexer after editing Go source
make down        # stop everything (state and broker volumes preserved)
make reset-state # stop + wipe the cursor/watchlist volume ("fresh first boot")
```

## Running from source

Requires **Go 1.26+** ([download](https://go.dev/dl/)).

```bash
git clone https://github.com/Trustless-Work/trustlesswork-indexer-go.git
cd trustlesswork-indexer-go
go mod download

cp .env.example .env   # edit values as needed
make run               # builds bin/ingest and runs it with .env loaded
```

Or without Make:

```bash
go build -o bin/ingest cmd/ingest.go
RPC_URL=https://soroban-testnet.stellar.org ./bin/ingest
```

With the default `SINK_TYPE=noop` the Indexer exercises the whole detection path but
discards envelopes instead of publishing — no broker needed. Point `SINK_TYPE=rabbitmq`
and `RABBITMQ_URL` at a broker when you want real output.

> Port note: the local `make run` and the Docker stack both bind `:8080`.
> Stop one before starting the other.

## How it works

One goroutine drives the whole pipeline: fetch a ledger from the active RPC
endpoint, run it through the processors (event detection, escrow discovery by
approved WASM hash, state fetches via `getLedgerEntries`, reconciliation sweep),
publish every detected fact to the sink, then persist the cursor. Publisher
confirms are awaited per message, and the Indexer **never advances the cursor past
an unpublished fact** — a crash between broker-ack and cursor-save redelivers, and
the consumer dedupes on `message_id`.

The RPC connection is an ordered endpoint pool (`RPC_URL` + `RPC_FALLBACK_URLS`).
When the active endpoint fails or its tip stalls, the loop rotates to the next one:
it re-verifies the network passphrase, clamps the cursor against the new endpoint's
retention window (recording a gap — and publishing a `control` envelope — only if
no endpoint serves the range), and re-checks chain continuity by parent hash so a
fallback serving a different chain cannot poison the read model. The process exits
only when every endpoint failed; the supervisor's restart retries the pool from
the top.

State is two JSON files next to `STATE_PATH` (cursor, and watchlist + tombstones +
gap record), written atomically and guarded by a `flock` sidecar — **single writer,
one replica**. On restart the Indexer resumes from the persisted cursor and catches
up; nothing is skipped silently.

## Configuration

Configuration is **100% environment variables**, loaded and validated at boot by
`internal/config` — fail-fast: the process refuses to start on invalid or
inconsistent values, and logs the effective config with credentials redacted.
[`.env.example`](.env.example) is the commented reference used by `make run`.

### Full reference

| Variable | Default | Description |
|---|---|---|
| **RPC** | | |
| `RPC_URL` | *(required)* | Soroban RPC endpoint. Must serve the configured network. |
| `RPC_FALLBACK_URLS` | *(empty)* | Comma-separated, **ordered** failover endpoints. Put deep-retention (archive) providers here: when the cursor falls outside the failing endpoint's retention, an archive fallback serves the ledgers for real instead of recording a gap. |
| `RPC_REQUEST_TIMEOUT` | `30s` | Per-request timeout for small RPC calls. |
| `RPC_LEDGER_FETCH_TIMEOUT` | `120s` | Timeout for `getLedgers` batches (tens of MB on mainnet). |
| **Network** | | |
| `NETWORK_NAME` | `testnet` | `testnet` or `mainnet`. Used in routing keys and log fields. |
| `NETWORK_PASSPHRASE` | testnet passphrase | Every endpoint is verified against it at connect time. Mainnet: `Public Global Stellar Network ; September 2015`. |
| **Indexer** | | |
| `INDEXER_START_LEDGER` | `0` (= tip) | First boot only; afterwards the state file is the source of truth. See the warning below. |
| `INDEXER_END_LEDGER` | `0` (= unbounded) | Set to bound a backfill run. |
| `INDEXER_LEDGER_BACKEND_TYPE` | `rpc` | Only supported value today. |
| `INDEXER_GET_LEDGERS_LIMIT` | `100` | Ledgers per backend batch. **Use `10` on mainnet** — mainnet ledgers are heavy (~2.65 MB each) and 100 asks for ~265 MB per answer. |
| `INDEXER_SWEEP_ENABLED` | `true` | Reconciliation sweep (one budgeted `getLedgerEntries` batch per ledger, rotating over the watchlist). Kill switch for RPC rate-limit emergencies. |
| **Escrow identity** | | |
| `ESCROW_APPROVED_WASM_HASHES` | *(empty)* | Comma-separated WASM code hashes that identify Trustless Work escrows. See below. |
| `ESCROW_SEED_PATH` | *(empty)* | Optional file of escrow contract IDs (one per line) loaded at first boot — bootstrap for escrows created before the indexed range. |
| **Sink** | | |
| `SINK_TYPE` | `noop` | `noop` (dev, discards everything) or `rabbitmq` (production). |
| `RABBITMQ_URL` | — | AMQP connection string; required when `SINK_TYPE=rabbitmq`. Use `amqps://` for TLS whenever the broker exposes it. |
| `RABBITMQ_EXCHANGE` | `stellar.events` | Topic exchange; declared durable at boot. |
| `RABBITMQ_PUBLISHER_CONFIRMS` | `true` | Block each publish until the broker acks. Keep `true` in production. |
| `ENVELOPE_HMAC_KEY` | *(empty)* | HMAC-SHA256 key for the `x-tw-sig` header on every published message; must be the **exact same string** as the consumer's. Min 32 bytes when set. Empty publishes unsigned (warned at boot). Rotation: [docs/hmac-key-rotation.md](docs/hmac-key-rotation.md). |
| `SINK_REQUIRE_SIGNING` | `false` | When `true`, boot refuses to start without an HMAC key. Flip only after the consumer enforces verification. |
| **State** | | |
| `STATE_PATH` | `./indexer.state.json` | Durable cursor file; the watchlist lives next to it (`.watchlist.json`) and a `.lock` sidecar enforces single-writer semantics. |
| **Health / observability** | | |
| `HEALTH_ENABLED` | `true` | Disable to run without the HTTP server (one-shot backfills). |
| `HEALTH_PORT` | `8080` | Port for `/healthz`, `/readyz`, `/status`, `/metrics` (and `/admin/*`). |
| `HEALTH_HEARTBEAT_URL` | *(empty)* | Dead-man's-switch ping URL (Better Stack, Healthchecks.io, …); the external monitor alerts on silence. The URL is a secret — only scheme+host are logged. |
| `ADMIN_TOKEN` | *(empty)* | Bearer token enabling the `/admin/*` surface. Min 16 chars. Empty = surface disabled. |
| **Logging** | | |
| `LOG_LEVEL` | `info` | `panic` … `trace`. |
| `LOG_FORMAT` | `auto` | `auto` (JSON when stdout is not a TTY), `json`, or `text`. |

### Escrow identity: approved WASM hashes

A contract is recognised as a Trustless Work escrow because its WASM **code hash**
is in this set — one hash per published contract version. The four current versions
(single-release and multi-release, v1 and v2; same code hashes on testnet and
mainnet):

```bash
ESCROW_APPROVED_WASM_HASHES=38c42f5476192aabe5ae14aa773231a6e08c013ce4683aa387f2dc9e69376474,1bbce13523346de574214b8c6cc829a71a409d524e9a3435427da1c935885c04,0136f8fcc6e65dfe4aa3f8abe8dc386e5ec6ed503a123ee6275461ad2d9bd8df,3d9241655390f03c80536a2ef29dc350fb6c50f4d29dec380c366ab7ec24263c
```

A contract whose hash is **not** in this set is invisible: zero envelopes, zero
logs. When Trustless Work publishes a new contract version, add its hash here and
restart — no code change, no redeploy of anything else.

### ⚠️ First-boot and state-loss semantics

Two configurations lose data **silently** (nothing in the gap record):

- `INDEXER_START_LEDGER=0` on a **first boot** (or after losing the state volume)
  means "start at the current tip": everything before is never indexed and no gap
  is recorded. Set an explicit start ledger when history matters.
- Deleting the state file while the process runs does nothing — the loop rewrites
  it every ledger. To reset for real, stop the process and remove the state files
  atomically (locally: `make reset-state`).

Escrows that already existed before your start ledger are bootstrapped via
`ESCROW_SEED_PATH` or the `/admin/reseed` endpoint, and their missed history
recovered with [`replay`](#replaying-a-ledger-range).

### Mainnet example

```bash
RPC_URL=https://mainnet.sorobanrpc.com
RPC_FALLBACK_URLS=https://soroban-rpc.mainnet.stellar.gateway.fm,https://soroban.api.onfinality.io/public
NETWORK_NAME=mainnet
NETWORK_PASSPHRASE=Public Global Stellar Network ; September 2015
INDEXER_GET_LEDGERS_LIMIT=10
SINK_TYPE=rabbitmq
RABBITMQ_URL=amqps://user:pass@your-broker:5671/
ESCROW_APPROVED_WASM_HASHES=<the four hashes above>
LOG_FORMAT=json
```

## Consuming the stream

The Indexer publishes to a durable **topic** exchange (`stellar.events` by default).
Consumers own their queues: declare a queue, bind it with the pattern you need, and
dedupe on `message_id`.

| You want | Binding |
|---|---|
| Everything for escrows | `stellar.*.escrow.#` |
| Only lifecycle events (history) | `stellar.*.escrow.event.#` |
| Only state snapshots | `stellar.*.escrow.state.#` |
| Only deposits | `stellar.*.escrow.deposit.#` |
| One specific kind | `stellar.mainnet.escrow.event.tw_fund` |

A lifecycle event envelope looks like this:

```jsonc
{
  "schema_version": "1.1",
  "type": "event",
  "network": "testnet",
  "contract_id": "CESCROW...",          // the escrow this fact concerns
  "ledger_seq": 58762521,
  "ledger_closed_at": "2026-05-21T18:04:11Z",
  "tx_hash": "a1b2...",
  "tx_index": 7,
  "event_kind": "tw_fund",
  "event_index": 3,
  "message_id": "a1b2...:3",            // deterministic idempotency key
  "published_at": "2026-05-21T18:04:12Z",
  "raw_xdr": "AAAA..."                  // base64 xdr.ContractEvent — you decode it
}
```

Consumer rules (the contract in one paragraph): be **idempotent** on `message_id`
(at-least-once delivery), order history by `(ledger_seq, tx_index, event_index)`,
upsert state keeping the highest `ledger_seq`, ignore unknown fields and unknown
kinds (additive changes don't bump the version), and verify the `x-tw-sig` HMAC
header when you share a key with the Indexer. Decode `raw_xdr` with the Stellar SDK
(`xdr.ContractEvent` for `event`/`deposit`, `xdr.LedgerEntry` for `state`).

Everything above, in full detail: [docs/event-schema.md](docs/event-schema.md).

## Health and monitoring

Served on `HEALTH_PORT` (default 8080) while the loop runs:

- `GET /healthz` — liveness: the process is up. Always 200.
- `GET /readyz` — progress: 200 while ledgers advance, 503 after 60s of silence,
  whatever the cause. Point platform healthchecks and uptime monitors here.
- `GET /status` — the incident numbers: cursor, ledger age, active RPC endpoint
  (`rpc_endpoint`), pause state, escrows tracked, publish totals, recorded gaps,
  `suppressed_removals`, uptime, version. `suppressed_removals` above zero means an
  RPC endpoint answered with an empty result set and the plausibility guard stopped
  it from publishing the whole batch as removed — look at the endpoint, not at the
  escrows.
- `GET /metrics` — the same numbers in Prometheus format, plus the ledger-fetch
  duration summary.

For alerting without a scraper, set `HEALTH_HEARTBEAT_URL`: the Indexer pings it
after processed ledgers and the provider alerts on silence (dead-man's switch).

## Admin control surface

When `ADMIN_TOKEN` is set, the health server also mounts `/admin/*`
(bearer auth):

| Endpoint | Purpose |
|---|---|
| `POST /admin/escrows` | Track an escrow by contract ID |
| `POST /admin/escrows/{id}/refresh` | Force a state re-fetch for one escrow |
| `DELETE /admin/escrows/{id}` | Stop tracking (tombstone) |
| `POST /admin/reseed` | Bulk-load escrow IDs into the watchlist |
| `POST /admin/pause` / `POST /admin/resume` | TTL-bounded pause of the pipeline |
| `POST /admin/reconcile` | Trigger a reconciliation sweep |
| `GET /admin/registry` | Dump the current watchlist |

This is the **only** way a command enters the pipeline, and commands are only
queued at the edge — the ingest loop executes them between ledgers, so it stays the
single writer of the watchlist and the state file. The health port must **never**
be public: the token is defence in depth, not a substitute for a private network.
Background and rationale: [docs/control-plane.md](docs/control-plane.md).

## Replaying a ledger range

```bash
bin/ingest replay --from 58760000 --to 58770000
```

Re-publishes the events and deposits of a bounded range — e.g. to backfill a
recorded gap or an escrow seeded after its history happened. Safe to run **next
to** the live indexer: it persists nothing, takes no lock, consumes no commands and
never pings the heartbeat. It borrows the live watchlist as a point-in-time copy
and refuses ranges no configured RPC endpoint serves in full — point
`RPC_FALLBACK_URLS` at an archive provider for old history. Downstream dedupe
absorbs the duplicates.

## Deploying to production

The [Dockerfile](Dockerfile) builds a static binary on a non-root Alpine image with
`tini` as PID 1 (proper SIGTERM handling for graceful shutdown). Any container
platform works. The checklist that matters:

1. **Persist the state.** Mount a volume and point `STATE_PATH` at it
   (e.g. `/var/lib/indexer/state.json`). Without it, every redeploy restarts from
   the tip and loses the watchlist — the one unacceptable failure mode.
2. **One replica.** The state file is guarded by a `flock`; two writers cannot
   coexist. Do not scale horizontally.
3. **Healthcheck on `/readyz`**, restart policy "always". The process exits when
   every RPC endpoint failed; the restart retries the pool from the top.
4. **Broker topology.** The Indexer declares the exchange (idempotent) but **not**
   queues or bindings — consumers own their queues. Until a real consumer declares
   its queue, published messages are dropped silently: declare one manually when
   smoke-testing.
5. **Secrets.** Set `ENVELOPE_HMAC_KEY` (shared with the consumer) and
   `ADMIN_TOKEN`; keep the health port off the public internet; prefer `amqps://`.
6. **Watch it.** Scrape `/metrics` or set `HEALTH_HEARTBEAT_URL`.

Trustless Work's own deployment runs on [Railway](https://railway.com) (indexer +
RabbitMQ in one project, state on a volume): notes and the mainnet variable set
live in [docs/railway-deployment.md](docs/railway-deployment.md).

## Project structure

```
.
├── cmd/
│   └── ingest.go              # Entry point: config load, logger, signals, replay flags
├── internal/
│   ├── config/                # Env-driven configuration (load, validate, redacted dump)
│   ├── ingest/                # Composition root + ledger loop + RPC failover pool
│   ├── indexer/               # Per-ledger processing engine
│   │   ├── processors/        # Event detection, discovery, state fetches, sweep
│   │   └── registry/          # Escrow watchlist (approved WASM hashes)
│   ├── events/                # Envelope wire contract (versioned schema, message ids)
│   ├── sink/                  # Delivery interface + noop / rabbitmq implementations
│   ├── commands/              # /admin/* surface + command queue
│   ├── state/                 # Atomic state files: cursor + watchlist + gaps (flock)
│   ├── health/                # HTTP endpoints + outbound heartbeat
│   └── utils/                 # Small shared helpers
├── docs/                      # Event schema, control plane, threat model, deployment
├── docker-compose.yml         # Local RabbitMQ + indexer stack
├── Makefile
└── go.mod
```

## Development

```bash
gofmt -l .      # formatting (must print nothing)
go vet ./...    # static analysis
go test ./...   # unit tests
make build      # compile to bin/ingest
```

CI runs the same checks on every PR. Changes land through PRs against `main`
(the deployed branch); anything user-visible or operational gets a
[CHANGELOG](CHANGELOG.md) entry under `[Unreleased]`.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Boot fails with a passphrase mismatch | An endpoint in `RPC_URL`/`RPC_FALLBACK_URLS` serves a different network than `NETWORK_PASSPHRASE`. Every endpoint must serve the same chain. |
| `/readyz` returns 503 | No ledger processed for 60s. Check `/status` → `rpc_endpoint` and ledger age; usually the RPC provider is stalled or rate-limiting. |
| No messages in the queue | Either `SINK_TYPE=noop` (the dev default), or no queue is bound to the exchange — AMQP drops unroutable messages silently. `make up` declares `indexer.events` for you locally. |
| An escrow produces no envelopes | Its WASM hash is not in `ESCROW_APPROVED_WASM_HASHES`, or it was created before your indexed range — seed it (`ESCROW_SEED_PATH`, `/admin/reseed`) and `replay` its history. |
| `replay` refuses a range | No configured endpoint serves it in full. Add an archive provider to `RPC_FALLBACK_URLS`. (Known limit: retention is probed via `getHealth`'s `OldestLedger`, which under-reports some archive providers.) |
| Huge RPC responses / memory spikes on mainnet | Set `INDEXER_GET_LEDGERS_LIMIT=10` — mainnet ledgers weigh ~2.65 MB each. |
| `bind: address already in use` on 8080 | Local `make run` and the Docker stack both bind `:8080`. Stop one. |
| `suppressed_removals` > 0 in `/status` | An RPC answered a state batch with an empty result and the plausibility guard refused to publish mass removals. Investigate the endpoint, not the escrows. |

## Further reading

- [docs/event-schema.md](docs/event-schema.md) — the envelope wire contract
  (versioned independently of the binary).
- [docs/control-plane.md](docs/control-plane.md) — the admin surface's design, and
  why there is no command channel over AMQP.
- [docs/pluggable-sink-architecture.md](docs/pluggable-sink-architecture.md) — why
  delivery is an interface and how to add a sink.
- [docs/threat-model.md](docs/threat-model.md) — trust assumptions, defenses,
  deliberate non-implementations, and the declared operational ceilings.
- [docs/hmac-key-rotation.md](docs/hmac-key-rotation.md) — rotating the envelope
  signing key without downtime.
- [CHANGELOG.md](CHANGELOG.md) — notable changes per release.
