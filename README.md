# Trustless Work Indexer

Blockchain indexer for the Stellar network. Follows the ledger stream in
real time, detects activity on Trustless Work escrow contracts (events,
deposits, state changes), and publishes versioned envelopes to RabbitMQ
for the core API to consume. Built to run unattended: multi-RPC
failover, durable cursor + watchlist, gap evidence for anything it had
to skip, and health endpoints an external monitor can watch.

## How it works

One goroutine drives the whole pipeline: fetch a ledger from the active
RPC endpoint, run it through the processors (event detection, escrow
discovery by approved WASM hash, state fetches via `getLedgerEntries`,
reconciliation sweep), publish every detected fact to the sink, then
persist the cursor. Delivery is at-least-once with deterministic message
ids — the consumer dedupes; the indexer never advances the cursor past
an unpublished fact.

The RPC connection is an ordered endpoint pool (`RPC_URL` +
`RPC_FALLBACK_URLS`). When the active endpoint fails or its tip stalls,
the loop rotates to the next one: it re-verifies the network passphrase,
clamps the cursor against the new endpoint's retention window (recording
a gap only if no endpoint serves the range), and re-checks chain
continuity by parent hash so a fallback serving a different chain cannot
poison the read-model. The process exits only when every endpoint
failed; the supervisor's restart retries the pool from the top.

## Prerequisites

- **Go 1.25+** - [Download](https://go.dev/dl/)
- **Make** (optional) - Usually pre-installed on macOS/Linux
- **Docker** (optional) - Only for the local RabbitMQ stack (`make up`)

### Verify installation

```bash
go version
# Should display: go version go1.25.x or higher
```

## Installation

1. Clone the repository:

```bash
git clone https://github.com/Trustless-Work/Indexer.git
cd Indexer
```

2. Download dependencies:

```bash
go mod download
```

## Running the project

### Option 1: Using Make (loads .env automatically)

```bash
cp .env.example .env   # first time: edit values as needed
make run
```

### Option 2: Go commands

```bash
# Build
go build -o bin/ingest cmd/ingest.go

# Run (reads configuration from environment variables)
RPC_URL=https://soroban-testnet.stellar.org ./bin/ingest
```

### Option 3: Full local stack (RabbitMQ + indexer, via Docker)

```bash
make up          # broker + queue/binding init + indexer
make logs        # follow the indexer
make down        # stop everything (state volume preserved)
make reset-state # stop + wipe the cursor/watchlist volume
```

## Configuration

Configuration is **100% environment variables**, loaded and validated at
boot by `internal/config` (fail-fast: the process refuses to start on
invalid or inconsistent values, and logs the effective config with
credentials redacted).

**[`.env.example`](.env.example) is the canonical, commented reference
for every variable.** The ones you will touch most:

| Variable | Default | Purpose |
|----------|---------|---------|
| `RPC_URL` | *(required)* | Soroban RPC endpoint |
| `RPC_FALLBACK_URLS` | *(empty)* | Ordered failover endpoints; list archive-grade providers here |
| `NETWORK_NAME` / `NETWORK_PASSPHRASE` | testnet | Target network; every endpoint is verified against the passphrase |
| `ESCROW_APPROVED_WASM_HASHES` | *(empty)* | Code hashes that identify TW escrow contracts |
| `SINK_TYPE` | `noop` | `noop` (dev) or `rabbitmq` (production) |
| `RABBITMQ_URL` | — | AMQP connection string (required when `SINK_TYPE=rabbitmq`); use `amqps://` for TLS when the broker exposes it |
| `ENVELOPE_HMAC_KEY` | *(empty)* | Shared key for the `x-tw-sig` signature on every published message; must equal the core API's. Empty publishes unsigned (warned at boot) |
| `STATE_PATH` | `./indexer.state.json` | Durable cursor + watchlist + gap record |
| `INDEXER_START_LEDGER` | `0` (= tip) | First boot only; afterwards the state file is the source of truth |
| `HEALTH_HEARTBEAT_URL` | *(empty)* | Dead-man's switch ping; the monitor alerts on silence |

## Health endpoints

Served on `HEALTH_PORT` (default 8080) while the loop runs:

- `GET /healthz` — liveness: the process is up. Always 200.
- `GET /readyz` — progress: 200 while ledgers advance, 503 after 60s of
  silence, whatever the cause. Point platform healthchecks and uptime
  monitors here.
- `GET /status` — the incident numbers: cursor, ledger age, active RPC
  endpoint (`rpc_endpoint`), pause state, escrows tracked, publish
  totals, recorded gaps, `suppressed_removals`, uptime.
  `suppressed_removals` above zero means an RPC endpoint answered with an
  empty result set and the plausibility guard stopped it from publishing
  the whole batch as removed — look at the endpoint, not at the escrows.
- `GET /metrics` — the same numbers in Prometheus format, plus the
  ledger-fetch duration summary.

When `ADMIN_TOKEN` is set, the same server also mounts the `/admin/*`
control surface (bearer auth): track / remove / refresh escrows, bulk
reseed, TTL-bounded pause / resume, reconcile, and `GET /admin/registry`.
This is the only way a command enters the pipeline, and commands are only
queued at the edge — the ingest loop executes them between ledgers, so it
stays the single writer of the watchlist and the state file. See
[docs/control-plane.md](docs/control-plane.md) for what the surface is
for, and why the AMQP command channel that used to sit beside it was
removed.

## Project structure

```
.
├── cmd/
│   └── ingest.go              # Entry point: config load, logger, signals
├── internal/
│   ├── config/                # Env-driven configuration (load, validate, redacted dump)
│   ├── ingest/                # Composition root + ledger loop + RPC failover pool
│   ├── indexer/               # Per-ledger processing engine
│   │   ├── processors/        # Event detection, discovery, state fetches, sweep
│   │   └── registry/          # Escrow watchlist (approved WASM hashes)
│   ├── events/                # Envelope wire contract (versioned schema, message ids)
│   ├── sink/                  # Delivery interface + noop / rabbitmq implementations
│   ├── state/                 # Atomic state file: cursor + watchlist + gaps (flock)
│   ├── health/                # HTTP endpoints + outbound heartbeat
│   └── utils/                 # Small shared helpers
├── docs/                      # Event schema, sink architecture, Railway deployment
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

## Replaying a ledger range

`bin/ingest replay --from N --to M` re-publishes the events and
deposits of a bounded range (e.g. to backfill a recorded gap) and can
run NEXT TO the live indexer: it persists nothing, takes no lock,
consumes no commands and never pings the heartbeat. It borrows the live
watchlist as a point-in-time copy and refuses ranges no configured RPC
endpoint serves in full — point `RPC_FALLBACK_URLS` at an archive
provider for old history. Downstream dedupe absorbs the duplicates.

## Deployment

Production runs on Railway (indexer + RabbitMQ in one project, state on
a volume). The step-by-step guide, including the mainnet environment
variables and the failover endpoint list, lives in
[docs/railway-deployment.md](docs/railway-deployment.md).

## Further reading

- [docs/event-schema.md](docs/event-schema.md) — the envelope wire
  contract consumed by the core API (versioned independently).
- [docs/pluggable-sink-architecture.md](docs/pluggable-sink-architecture.md)
  — why delivery is an interface and how to add a sink.
- [CHANGELOG.md](CHANGELOG.md) — notable changes per release.
