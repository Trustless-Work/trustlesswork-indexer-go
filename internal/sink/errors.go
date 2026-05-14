package sink

import "errors"

// Sentinel errors emitted by the sink package and its concrete
// implementations. Callers (notably the main ingestion loop) use
// errors.Is to dispatch on these.
//
// Categories (see internal/errs):
//   - ErrSinkUnavailable: TRANSIENT. Retry with backoff.
//   - ErrSinkPublishRejected: TRANSIENT. The broker explicitly nack'd
//     or did not confirm in time; the underlying cause may be transient
//     (queue full, broker overload) or permanent (auth, missing exchange).
//     Treated as transient by default; persistent failures escalate via
//     the retry policy in the main loop.
var (
	// ErrSinkUnavailable indicates the sink could not be reached at all
	// (network error, broker down, channel closed unexpectedly).
	ErrSinkUnavailable = errors.New("sink unavailable")

	// ErrSinkPublishRejected indicates the broker received the publish
	// but did not confirm it as accepted. With publisher confirms
	// enabled, this is a Nack from the broker or a confirmation
	// timeout.
	ErrSinkPublishRejected = errors.New("sink rejected publish")
)
