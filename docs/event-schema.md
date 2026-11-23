# Envelope contract (output schema)

This document is the canonical specification of the messages the Indexer
publishes to RabbitMQ. It is the contract that consumers — primarily the
Trustless Work NestJS API — rely on to build their views.

> **Status (updated 2026-11-23):** this is the **live** wire contract. The
> Indexer publishes to RabbitMQ in production (testnet) and the core API
> consumes these shapes (`src/presentation/ingestion` +
> `src/application/ingestion` in `trustlesswork-core-api`). Every stream
> described below — `event`, `deposit`, `withdrawal` (1.3), `state`,
> `gap` — is implemented,
> published with per-message publisher confirms and deterministic
> `message_id`s, and signed with the `x-tw-sig` AMQP header when
> `ENVELOPE_HMAC_KEY` is configured. A change to this document is a change
> to the consumer: follow `.claude/skills/envelope-contract-change`.

## Design principles

1. **Thin indexer, smart consumer.** The Indexer detects facts and
   forwards the **raw XDR**; it never decodes contract-specific data
   (escrow milestones, roles, amounts). The consumer decodes with the
   Stellar SDK. This keeps the Indexer agnostic to contract evolution.
2. **Identity by approved WASM hash.** A contract is recognised as a TW
   escrow because its code hash is in a small, configured set of
   approved hashes (one per published contract version) — *not* by
   enumerating event topics, and *not* by a hand-maintained address
   list. Shipping a new contract version is a config change (one hash),
   never a code change.
3. **One envelope, four fact types.** A single envelope shape carries a
   `type` discriminator (`event` / `deposit` / `withdrawal` / `state`).
   The consumer uses `type` to choose a projection.
4. **Deterministic identity and order.** Every message has a
   deterministic `message_id` (idempotency) and a total order key
   (`ledger_seq`, `tx_index`, `event_index`).

## Transport

- **Broker**: RabbitMQ, durable **topic** exchange.
- **Routing key**: `stellar.<network>.escrow.<type>.<kind>` (see below).
- **Content type**: `application/json`.
- **Delivery mode**: persistent.
- **Publisher confirms**: enabled. The Indexer advances its cursor only
  after a positive broker ack ⇒ **at-least-once** delivery. Consumers
  MUST be idempotent on `message_id`.
- **Signature**: every published message (all `type`s, replays included)
  carries an AMQP header `x-tw-sig`: lowercase hex HMAC-SHA256 of the
  **exact body bytes** as published, keyed with the raw string value of
  `ENVELOPE_HMAC_KEY` (shared with the consumer; no hex/base64 decoding
  on either side). Transport metadata only — the envelope JSON and its
  `schema_version` are unaffected. The header is absent when the Indexer
  has no key configured; the consumer decides per its verification mode
  (off / log / enforce) whether a missing or invalid signature is fatal.
- **Key id**: signed messages also carry `x-tw-sig-kid` — the first 8
  bytes of `SHA-256(key)`, lowercase hex (16 chars) — so the consumer
  can hold a verify-set of several keys during a rotation and select
  the right one per message. Additive: a verifier that ignores it keeps
  working with a single key, and messages published before this header
  existed verify via try-all-keys fallback. Procedure and consumer
  requirements: `docs/hmac-key-rotation.md`.

## Envelope: common header

Every message, regardless of `type`, carries these fields (snake_case):

| Field | Type | Description |
|---|---|---|
| `schema_version` | string | Wire-contract version (today `"1.3"`). Consumers dispatch to version-specific handlers and MUST ignore unknown fields. |
| `type` | string | Discriminator: `event`, `deposit`, `withdrawal` (1.3), `state`, or `control` (1.1). |
| `network` | string | `testnet` or `mainnet`. |
| `contract_id` | string | **The TW escrow** this message concerns (`C…` strkey). The uniform key to group "everything for escrow X". For `event` it is the emitter; for `deposit` it is the recipient (`to`); for `withdrawal` it is the sender (`from`); for `state` it is the owner of the `ContractData`. |
| `ledger_seq` | uint32 | Ledger sequence the fact occurred in. |
| `ledger_closed_at` | RFC 3339 | Close time of the ledger (from the chain). Deterministic and replayable. |
| `tx_hash` | string (hex) | Transaction that produced the fact. |
| `tx_index` | uint32 | Application order of the transaction within the ledger (1-indexed). Part of the total order key. |
| `message_id` | string | Deterministic idempotency key. Construction is type-specific (see below). |
| `published_at` | RFC 3339 | Wall-clock at the Indexer when the envelope was assembled. Observability only — never used for ordering. |
| `raw_xdr` | string (base64) | Base64 marshal of the raw payload. Consumer decodes with the Stellar SDK. Payload type depends on `type` (see below). |

## Type-specific fields

### `type: "event"`, `type: "deposit"` and `type: "withdrawal"`

`raw_xdr` is the full `xdr.ContractEvent` (header + body). Extra fields:

| Field | Type | Description |
|---|---|---|
| `event_kind` | string | For `event`: the contract's `topic[0]` Symbol (e.g. `tw_fund`). For `deposit` and `withdrawal`: `token_transfer`. |
| `event_index` | uint32 | Position of the event within the transaction's full contract-event list (across all operations). Part of the total order key. |

The distinction between `event` and `deposit`: an `event` is emitted by
the escrow itself (the emitter IS the escrow); a `deposit` is a
SAC/SEP-41 `transfer` emitted by a **token** contract whose `to` is a
known escrow. In both cases `contract_id` is normalised to the escrow.

### `type: "withdrawal"` (1.3)

A SAC/SEP-41 `transfer` emitted by a **token** contract whose `from`
(`topics[1]`) is a known escrow: releases, dispute-resolution
distributions and `withdraw_remaining_funds` all move tokens out this
way. `contract_id` is the escrow (the transfer's `from`); the required
fields and the `raw_xdr` payload are identical to `deposit`. Like a
deposit, the Indexer reads only the topic addresses for identity and
never decodes the amount.

One physical transfer between two registered escrows (or from an escrow
to itself) yields BOTH a `deposit` and a `withdrawal`, in that order,
with the same `event_index` — hence the distinct `message_id` suffix
below. An event emitted by an escrow itself is always exactly one
`event` fact, never a deposit or withdrawal.

### `type: "state"`

`raw_xdr` is the raw `ContractData` ledger entry data
(`xdr.LedgerEntryData`) that carries the escrow's `DataKey::Escrow`
state after the change. Two on-wire shapes exist depending on the
contract version's storage model — the consumer dispatches on the
entry's value type:

- **`.persistent()` / `.temporary()` storage**: a dedicated entry keyed
  by `Vec[Symbol("Escrow")]`; its value **is** the `Escrow` struct map.
- **`.instance()` storage**: no dedicated entry exists. The entry is the
  contract **instance**; `DataKey::Escrow` lives inside its
  `ScContractInstance.storage` map under the `Vec[Symbol("Escrow")]`
  key. The consumer reaches into `storage` to read the `Escrow` map.

The Indexer stays agnostic and forwards whichever entry carries the
state, preferring a dedicated data entry when both are present.

Extra field:

| Field | Type | Description |
|---|---|---|
| `state_change_type` | string | `created`, `updated`, or `removed` (the last meaning the entry was archived / expired). |

`state` messages carry no `event_index` (a state change is not an
event); use `0` for the order key's third component.

**`removed` (1.1):** when the entry no longer exists on-chain (TTL
expiry, or `withdraw_remaining_funds`, which emits no Soroban event),
the Indexer publishes `state_change_type: "removed"` with an **empty**
`raw_xdr` — there is no entry left to carry; the signal is the payload.
Consumers MUST branch on `state_change_type` before decoding.

### `type: "control"` (1.1)

An out-of-band fact about the PIPELINE itself, not about one escrow:
`contract_id` and `raw_xdr` are empty. Today the only kind is
`gap_detected`: the Indexer clamped its cursor past a ledger range it
could not serve (RPC retention) and that range's history is missing
until an operator replays it.

| Field | Type | Description |
|---|---|---|
| `control_kind` | string | `gap_detected` (unknown kinds should be dead-lettered for post-upgrade replay). |
| `gap_from_ledger` | number | First skipped ledger (inclusive). |
| `gap_to_ledger` | number | Last skipped ledger (inclusive). |
| `reason` | string | Machine-readable cause, e.g. `rpc_retention`. |
| `detected_at` | string | RFC 3339 detection time (observability anchor). |

`ledger_seq` is stamped with `gap_to_ledger + 1` (where processing
resumed). `message_id` is deterministic — `gap:{network}:{from}:{to}` —
because the Indexer republishes recorded gaps on EVERY boot
(at-least-once); consumers dedupe on it and only the first arrival
should alert.

## `message_id` construction

| type | `message_id` | Consumer semantics |
|---|---|---|
| `event` | `{tx_hash}:{event_index}` | Unique per physical event. Insert-once into the history table. |
| `deposit` | `{tx_hash}:{event_index}` | Same as `event`. |
| `withdrawal` | `{tx_hash}:{event_index}:withdrawal` | Same as `event`; the suffix keeps it distinct from a `deposit` produced by the same physical transfer (the sink matches broker returns by message id). |
| `state` | `{contract_id}:{ledger_seq}` | "State of escrow X at ledger N." Upsert; keep the row with the highest `ledger_seq`. |

## Ordering

The tuple **`(ledger_seq, tx_index, event_index)`** is a total, stable
order across the whole stream — including events from different
transactions in the same ledger. The history view orders by this tuple.
State messages are latest-wins by `ledger_seq` and do not need
inter-message ordering.

## Routing keys

Structure: `stellar.<network>.escrow.<type>.<kind>`, every segment a
single snake_case token (no dots inside a segment — dots break AMQP
single-segment wildcards).

```
stellar.mainnet.escrow.event.tw_init
stellar.mainnet.escrow.event.tw_fund
stellar.mainnet.escrow.event.tw_release
stellar.mainnet.escrow.deposit.token_transfer
stellar.mainnet.escrow.withdrawal.token_transfer
stellar.mainnet.escrow.state.updated
stellar.mainnet.escrow.state.removed
stellar.mainnet.escrow.control.gap_detected
```

Binding examples:

| Consumer wants | Binding |
|---|---|
| Everything for escrows | `stellar.*.escrow.#` |
| Only lifecycle events (history) | `stellar.*.escrow.event.#` |
| Only state snapshots | `stellar.*.escrow.state.#` |
| Only deposits | `stellar.*.escrow.deposit.#` |
| Only withdrawals | `stellar.*.escrow.withdrawal.#` |
| One specific kind | `stellar.mainnet.escrow.event.tw_fund` |

**Extensibility:** because escrows are identified by WASM hash and the
Indexer forwards *all* events a TW escrow emits, a new contract version
that emits a new topic (e.g. `tw_new_thing`) publishes to
`…event.tw_new_thing` automatically. Consumers bound to `event.#`
receive it with no Indexer change. New event kinds are therefore an
**additive** change (no version bump).

## Concrete examples

```jsonc
// 1) Lifecycle event (history)
{
  "schema_version": "1.1", "type": "event", "network": "testnet",
  "contract_id": "CESCROW...", "ledger_seq": 58762521,
  "ledger_closed_at": "2026-05-21T18:04:11Z",
  "tx_hash": "a1b2", "tx_index": 7,
  "event_kind": "tw_fund", "event_index": 3,
  "message_id": "a1b2:3", "published_at": "2026-05-21T18:04:12Z",
  "raw_xdr": "AAAA..."
}

// 2) External deposit (SAC transfer to the escrow)
{
  "schema_version": "1.1", "type": "deposit", "network": "testnet",
  "contract_id": "CESCROW...", "ledger_seq": 58762530,
  "ledger_closed_at": "2026-05-21T18:05:01Z",
  "tx_hash": "c3d4", "tx_index": 2,
  "event_kind": "token_transfer", "event_index": 0,
  "message_id": "c3d4:0", "published_at": "2026-05-21T18:05:02Z",
  "raw_xdr": "AAAA..."
}

// 3) Withdrawal (SAC transfer out of the escrow)
{
  "schema_version": "1.3", "type": "withdrawal", "network": "testnet",
  "contract_id": "CESCROW...", "ledger_seq": 58762540,
  "ledger_closed_at": "2026-05-21T18:06:01Z",
  "tx_hash": "e5f6", "tx_index": 1,
  "event_kind": "token_transfer", "event_index": 2,
  "message_id": "e5f6:2:withdrawal", "published_at": "2026-05-21T18:06:02Z",
  "raw_xdr": "AAAA..."
}

// 4) State snapshot (ContractData changed)
{
  "schema_version": "1.1", "type": "state", "network": "testnet",
  "contract_id": "CESCROW...", "ledger_seq": 58762530,
  "ledger_closed_at": "2026-05-21T18:05:01Z",
  "tx_hash": "c3d4", "tx_index": 2,
  "state_change_type": "updated",
  "message_id": "CESCROW...:58762530",
  "published_at": "2026-05-21T18:05:02Z",
  "raw_xdr": "AAAA..."
}
```

## Consumer contract (the two projections)

From this single stream the consumer materialises two views:

- **History** (`type` in `event`, `deposit`, `withdrawal`): append-only. A unique
  index on `message_id` + `INSERT ... ON CONFLICT DO NOTHING`. Order by
  `(ledger_seq, tx_index, event_index)`.
- **State** (`type: state`): upsert keyed by `contract_id`, keeping the
  highest `ledger_seq`. This is the authoritative current snapshot;
  never reconstruct it from events.

Consumers MUST be idempotent: at-least-once delivery means a message can
be redelivered after a reconnect or after an Indexer crash-restart
between broker-ack and cursor-save.

## Decoding `raw_xdr`

- **`event` / `deposit` / `withdrawal`** → `xdr.ContractEvent`:
  - Go: `xdr.SafeUnmarshalBase64(env.RawXDR, &ev)`
  - TS: `StellarSdk.xdr.ContractEvent.fromXDR(env.raw_xdr, "base64")`
- **`state`** → `xdr.LedgerEntry` (a `ContractData` entry):
  - Go: `xdr.SafeUnmarshalBase64(env.RawXDR, &entry)`
  - TS: `StellarSdk.xdr.LedgerEntry.fromXDR(env.raw_xdr, "base64")`

## Versioning policy

`schema_version` is a single string (`MAJOR.MINOR`).

- **Additive (MINOR bump or no bump):** new optional header fields, new
  `event_kind` values, new `state_change_type` values, new routing-key
  kinds. Consumers MUST ignore unknown fields and unknown kinds.
- **Breaking (MAJOR bump):** renaming/removing a field, changing a
  field's type or semantics. During migration the Indexer publishes in
  parallel to a versioned routing-key space until consumers cut over;
  the plan is coordinated via the CHANGELOG.

Version history:

- **1.1** — state `removed` and the `control` type (`gap_detected`).
- **1.2** — not used by the Indexer: core-api names its consumer-side
  provenance labels (`source`, `api_version`, set by the backfill
  publisher) "schema 1.2".
- **1.3** — adds the `withdrawal` type (additive). Deploy the consumer
  first: an older consumer dead-letters an unknown `type` as malformed.

## Intentionally NOT in the envelope

No decoded business data — `engagement_id`, `amount`, `from`, milestone
state, roles, flags. All of it lives inside `raw_xdr` and is the
consumer's job to decode. Adding decoded fields would couple the
Indexer to a specific contract version and break principle (1).
