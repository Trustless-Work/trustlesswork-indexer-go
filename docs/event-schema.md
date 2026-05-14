# Event schema (envelope contract)

This document is the canonical specification of the messages the Indexer
publishes to its configured sink. It is the contract consumers
(Trustless Work Core, downstream ETL, etc.) rely on.

The schema is intentionally minimal and version-tagged. The Indexer
deliberately does **not** parse contract-specific data structures
(escrow milestones, roles, flags); those are forwarded as base64-encoded
XDR for the consumer to decode with the Stellar SDK. This keeps the
Indexer stable across TW contract changes — only adding new event topics
to track requires touching the Indexer.

## Transport overview

- **Broker**: RabbitMQ. Topic exchange, durable.
- **Exchange name**: `RABBITMQ_EXCHANGE` (default `stellar.events`).
- **Routing key**: `stellar.<network>.escrow.<event_kind>`.
  - `<network>` is the value of `NETWORK_NAME` (`testnet`, `mainnet`).
  - `<event_kind>` is the Symbol Soroban emitted (e.g. `tw_init`) or an
    Indexer-synthesized constant (`token_transfer`).
- **Content type**: `application/json`.
- **Delivery mode**: persistent (`amqp.Persistent`).
- **Publisher confirms**: enabled by default. Every publish blocks on a
  broker ack before the Indexer advances its cursor. At-least-once
  delivery guaranteed; consumers must be idempotent on `message_id`.

Sample binding for "all events": `stellar.*.escrow.#`.
Sample binding for "only deposits": `stellar.*.escrow.tw_fund`.

## Envelope shape (v1)

Every published message has the same envelope. Fields are emitted in
snake_case.

```json
{
  "schema_version": "v1",
  "message_id": "abc123...-3",
  "network": "testnet",
  "event_kind": "tw_init",
  "contract_id": "C...",
  "tx_hash": "abc123...",
  "ledger_seq": 1234567,
  "event_index": 3,
  "ledger_closed_at": "2026-05-13T19:42:00Z",
  "published_at": "2026-05-13T19:42:00.512Z",
  "raw_xdr": "AAAA..."
}
```

### Field reference

| Field | Type | Description |
|---|---|---|
| `schema_version` | string | Always equal to the constant currently shipping (today: `"v1"`). Consumers use it to dispatch to version-specific handlers. |
| `message_id` | string | Deterministic identifier of the form `{tx_hash}-{event_index}`. Same physical event ⇒ same id. Suitable as a Postgres `ON CONFLICT DO NOTHING` key. |
| `network` | string | Short label, `testnet` or `mainnet`. Matches the network the Indexer is configured for. |
| `event_kind` | string | The semantic category. See the table below. |
| `contract_id` | string | The TW escrow contract this event concerns (a `C...` strkey). For `tw_*` events this is the emitter; for `token_transfer` it is the **recipient** (the `to` address that matched the watchlist). The original Soroban event header carrying the emitter is preserved in `raw_xdr`. |
| `tx_hash` | string (hex) | Transaction hash. |
| `ledger_seq` | uint32 | Ledger sequence. |
| `event_index` | uint32 | Position in the transaction's full event list (zero-indexed across all operations). |
| `ledger_closed_at` | RFC 3339 timestamp | Close time of the ledger (from the chain). Deterministic and replayable. |
| `published_at` | RFC 3339 timestamp | Wall-clock at the Indexer when the envelope was assembled. Observability only — never use it for ordering. |
| `raw_xdr` | string (base64) | Base64 of the full `xdr.ContractEvent` (header + body). Consumers decode with `xdr.SafeUnmarshalBase64`. |

### `event_kind` values

#### TW-emitted (pass-through Symbols)

These are the Soroban contract event Symbols emitted by TW escrow
contracts directly. The value of `event_kind` is identical to the
on-chain Symbol.

| event_kind | When emitted (function in the TW contract) |
|---|---|
| `tw_init` | `initialize_escrow` |
| `tw_fund` | `fund_escrow` |
| `tw_release` | `release_funds` / `release_milestone_funds` |
| `tw_update` | `update_escrow` |
| `tw_ms_change` | `change_milestone_status` |
| `tw_ms_approve` | `approve_milestone(s)` |
| `tw_ms_manage` | `manage_milestones` (v2 only) |
| `tw_disp_resolve` | `resolve_dispute` / `resolve_milestone_dispute` |
| `tw_dispute` | `dispute_escrow` (single-release) |
| `tw_ms_dispute` | `dispute_milestones` (multi-release v2) |
| `tw_ttl_extend` | `extend_contract_ttl` |
| `tw_withdraw` | `withdraw_remaining_funds` (multi-release v1 only — known blind spot in v2 contracts) |

#### Indexer-synthesized

The Indexer recognizes SAC/SEP-41 token `transfer` events whose `to`
address is in its runtime watchlist (i.e. transfers landing on a TW
escrow). These are emitted with a namespaced kind so consumers can
distinguish them from TW-emitted events.

| event_kind | Detection rule |
|---|---|
| `token_transfer` | First topic Symbol is `transfer`, the third topic decodes as an `ScAddress`, and that address is in the Indexer's watchlist of known TW escrows. |

Outgoing transfers from a watchlist escrow (`from` in watchlist) are
intentionally **not** emitted; they are already covered by the escrow's
own `tw_release` / `tw_withdraw` events.

## Versioning policy

`schema_version` is a single string. Evolution rules:

- **Additive changes** stay at `v1`:
  - New optional fields on the envelope.
  - New `event_kind` values (new TW topics, new Indexer-synthesized
    kinds).
  - New routing-key segments (e.g. a future `.v2` suffix would itself be
    a breaking change handled by parallel publishing — see below).

  Consumers MUST ignore unknown fields and unknown `event_kind` values
  gracefully.

- **Breaking changes** bump to `v2`:
  - Renaming or removing a field.
  - Changing a field's type (e.g. `ledger_seq` from uint32 to uint64).
  - Changing the semantic of a field (e.g. swapping `contract_id`'s
    meaning for `token_transfer`).

  During a `v1 → v2` migration the Indexer publishes in parallel to two
  routing-key spaces (`stellar.<net>.escrow.<kind>` for v1 and
  `stellar.<net>.escrow.v2.<kind>` or similar for v2) until all
  consumers have cut over. The migration plan and timeline are
  coordinated with consumer teams via the project's CHANGELOG.

## Idempotency contract

Consumers MUST be idempotent on `message_id`. The Indexer guarantees
at-least-once delivery (via RabbitMQ publisher confirms): a single
message may be redelivered after a reconnection, after a consumer-side
broker rebalance, or after the Indexer crash-restart cycle if the
crash happened between sink-ack and state-save.

Recommended implementation in the Core: a unique index on `message_id`
in the events table, and `INSERT ... ON CONFLICT DO NOTHING` on every
incoming envelope.

## Ordering guarantees

Events from the same `tx_hash` arrive in `event_index` order within the
same routing key. Events across `tx_hash` or across `event_kind` are
NOT globally ordered — the broker does not guarantee inter-key order
in a topic exchange, and the Indexer publishes them in detection order
within a ledger but does not synchronize across event kinds.

The `(ledger_seq, event_index)` tuple is unique and monotonic per
`tx_hash`. The `(ledger_seq, tx_hash, event_index)` tuple is unique
network-wide.

## Raw XDR consumption

`raw_xdr` is the base64-encoded marshal of the full
`xdr.ContractEvent` (header + body) as defined by Stellar XDR schemas.
Decode with the official SDK:

- **Go**: `xdr.SafeUnmarshalBase64(env.RawXDR, &ev)` into a
  `xdr.ContractEvent`.
- **TypeScript** (Stellar SDK): `StellarSdk.xdr.ContractEvent.fromXDR(env.raw_xdr, "base64")`.

The header (`ev.contract_id`, `ev.type`) is the Soroban event source.
The body (`ev.body.v0.topics`, `ev.body.v0.data`) carries the payload
of the event itself — for `tw_init` that's the full Escrow struct as
the contract emitted it.

## Compatibility checklist for new TW topics

When TW adds a new Soroban event topic to its contracts:

1. Add the Symbol to `internal/events/kind.go` (constant +
   `AllTWTopics`).
2. Add a row to the "TW-emitted" table above with the function name
   that emits it.
3. Bump `Version` if the addition is breaking; otherwise leave at
   `v1`.
4. Coordinate the deployment order: the Indexer must be running with
   the new topic in its filter BEFORE the contract is deployed,
   otherwise the first emissions of the new topic are silently
   dropped.
