package errs

import (
	"errors"

	"github.com/Trustless-Work/Indexer/internal/rpc"
	"github.com/Trustless-Work/Indexer/internal/sink"
)

// IsTransient reports whether err is recoverable by retrying after a
// delay. The main loop uses this to decide between backoff-and-retry
// (true) and other policies.
//
// Transient sentinels:
//   - rpc.ErrLedgerNotYetAvailable: the network has not closed the
//     requested ledger yet. Normal at-tip; wait and retry.
//   - rpc.ErrRPCUnreachable: transport-level failure. The RPC may be
//     restarting or briefly unreachable.
//   - rpc.ErrRPCInvalidResponse: the RPC responded but with a body we
//     could not interpret. Often a server-side incident that recovers
//     within seconds; retry with backoff is appropriate.
//   - sink.ErrSinkUnavailable: the configured sink (e.g. RabbitMQ) is
//     not reachable. Retry; the cursor must NOT advance.
//   - sink.ErrSinkPublishRejected: the broker received the publish but
//     did not confirm it. Treat as transient: re-publish on the next
//     loop iteration. The downstream consumer's idempotency key
//     (envelope.MessageID) makes duplicate delivery safe.
func IsTransient(err error) bool {
	switch {
	case errors.Is(err, rpc.ErrLedgerNotYetAvailable),
		errors.Is(err, rpc.ErrRPCUnreachable),
		errors.Is(err, rpc.ErrRPCInvalidResponse),
		errors.Is(err, sink.ErrSinkUnavailable),
		errors.Is(err, sink.ErrSinkPublishRejected):
		return true
	}
	return false
}
