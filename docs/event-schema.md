# Envelope contract (output schema)

This document is the canonical specification of the messages the Indexer
publishes to RabbitMQ. It is the contract that consumers — primarily the
Trustless Work NestJS API — rely on to build their views.

> **Status (updated 2026-05-21):** this is the **target** wire contract the
> processor pipeline is being built toward; the Indexer does not publish
> yet. Both the Indexer and the consuming API code against this shape.
>
> **Implemented so far:** escrow identity by approved WASM hash
> (`internal/indexer/registry` + `ESCROW_APPROVED_WASM_HASHES`); on-chain
> discovery of escrows (`processors.EscrowDiscovery`); and registry-filtered
> detection of `event`/`deposit` facts (`processors.EscrowEventDetector`,
> type `EscrowEvent`). Those facts already carry the envelope's identity,
> kind, index, tx-order and raw-XDR fields.
>
> **Pending (resume here):** the `state` stream (ContractData snapshots via
> `LedgerTransaction.GetChanges()`); serialising `EscrowEvent` facts into the
> JSON envelope below (incl. `message_id`, `schema_version`); the RabbitMQ
> sink + routing keys + publisher confirms; and escrow bootstrap (API seed +
> `getLedgerEntries` fallback for escrows created before the indexed range).

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
3. **One envelope, three fact types.** A single envelope shape carries a
   `type` discriminator (`event` / `deposit` / `state`). The consumer
   uses `type` to choose a projection.
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

## Envelope: common header

Every message, regardless of `type`, carries these fields (snake_case):

| Field | Type | Description |
|---|---|---|
| `schema_version` | string | Wire-contract version (today `"1.1"`). Consumers dispatch to version-specific handlers and MUST ignore unknown fields. |
| `type` | string | Discriminator: `event`, `deposit`, `state`, or `control` (1.1). |
| `network` | string | `testnet` or `mainnet`. |
| `contract_id` | string | **The TW escrow** this message concerns (`C…` strkey). The uniform key to group "everything for escrow X". For `event` it is the emitter; for `deposit` it is the recipient (`to`); for `state` it is the owner of the `ContractData`. |
| `ledger_seq` | uint32 | Ledger sequence the fact occurred in. |
| `ledger_closed_at` | RFC 3339 | Close time of the ledger (from the chain). Deterministic and replayable. |
| `tx_hash` | string (hex) | Transaction that produced the fact. |
| `tx_index` | uint32 | Application order of the transaction within the ledger (1-indexed). Part of the total order key. |
| `message_id` | string | Deterministic idempotency key. Construction is type-specific (see below). |
| `published_at` | RFC 3339 | Wall-clock at the Indexer when the envelope was assembled. Observability only — never used for ordering. |
| `raw_xdr` | string (base64) | Base64 marshal of the raw payload. Consumer decodes with the Stellar SDK. Payload type depends on `type` (see below). |

## Type-specific fields

### `type: "event"` and `type: "deposit"`

`raw_xdr` is the full `xdr.ContractEvent` (header + body). Extra fields:

| Field | Type | Description |
|---|---|---|
| `event_kind` | string | For `event`: the contract's `topic[0]` Symbol (e.g. `tw_fund`). For `deposit`: `token_transfer`. |
| `event_index` | uint32 | Position of the event within the transaction's full contract-event list (across all operations). Part of the total order key. |

The distinction between `event` and `deposit`: an `event` is emitted by
the escrow itself (the emitter IS the escrow); a `deposit` is a
SAC/SEP-41 `transfer` emitted by a **token** contract whose `to` is a
known escrow. In both cases `contract_id` is normalised to the escrow.

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

// 3) State snapshot (ContractData changed)
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

- **History** (`type` in `event`, `deposit`): append-only. A unique
  index on `message_id` + `INSERT ... ON CONFLICT DO NOTHING`. Order by
  `(ledger_seq, tx_index, event_index)`.
- **State** (`type: state`): upsert keyed by `contract_id`, keeping the
  highest `ledger_seq`. This is the authoritative current snapshot;
  never reconstruct it from events.

Consumers MUST be idempotent: at-least-once delivery means a message can
be redelivered after a reconnect or after an Indexer crash-restart
between broker-ack and cursor-save.

## Decoding `raw_xdr`

- **`event` / `deposit`** → `xdr.ContractEvent`:
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

## Intentionally NOT in the envelope

No decoded business data — `engagement_id`, `amount`, `from`, milestone
state, roles, flags. All of it lives inside `raw_xdr` and is the
consumer's job to decode. Adding decoded fields would couple the
Indexer to a specific contract version and break principle (1).
