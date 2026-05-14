package events

import "fmt"

// NewMessageID builds the deterministic identifier for a detected event.
//
// The combination (txHash, eventIndex) is unique by blockchain physics: a
// transaction hash never repeats, and within a transaction the event index
// is a monotonic counter over the meta's ContractEvent list. The function is
// pure: same inputs always produce the same output. This makes message IDs
// safe to use as idempotency keys for downstream consumers (e.g. Postgres
// UPSERT ON CONFLICT DO NOTHING).
//
// We deliberately avoid hashing the inputs. Both are already unique and
// short; a plaintext format preserves human readability in logs and
// debugging without adding a single bit of safety.
//
// Format: "<txHash>-<eventIndex>" (e.g. "abc123...def-3").
func NewMessageID(txHash string, eventIndex uint32) string {
	return fmt.Sprintf("%s-%d", txHash, eventIndex)
}
