package errs

// IsTransient reports whether err is recoverable by retrying after a delay.
//
// Phase 1 placeholder: no transient sentinels exist yet. They will be added
// in Phase 2 when the RPC and sink packages introduce their own sentinels
// (ErrLedgerNotYetAvailable, ErrRPCUnreachable, ErrSinkUnavailable, etc.).
// Until then this function always returns false, which is the conservative
// answer — the main loop falls back to its default policy (fail) for
// errors it cannot classify, which is what we want until we can answer
// "is it safe to retry?" with confidence.
//
// To add a transient sentinel: edit this function to include it via
// errors.Is. Keep the godoc above in sync. Do NOT split transient
// classification across multiple files — this is the one place.
func IsTransient(err error) bool {
	_ = err
	return false
}
