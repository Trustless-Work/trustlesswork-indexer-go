package health

import "time"

// Status is the payload of GET /status. Field naming is snake_case,
// matching the convention of the envelope wire contract — operators
// who curl /status see the same casing they see in envelope payloads.
//
// LastPublishedAt and LastMessageID may be zero/empty values
// (LastPublishedAt: "0001-01-01T00:00:00Z", LastMessageID: "")
// when the Indexer has just started and has not yet completed a
// ledger that produced an envelope. This is intentional and not an
// error condition.
type Status struct {
	Version         string    `json:"version"`
	Network         string    `json:"network"`
	SinkType        string    `json:"sink_type"`
	LastLedgerSeq   uint32    `json:"last_ledger_seq"`
	LastMessageID   string    `json:"last_message_id"`
	LastPublishedAt time.Time `json:"last_published_at"`
	WatchlistSize   int       `json:"watchlist_size"`
	UptimeSeconds   int64     `json:"uptime_seconds"`
	StartedAt       time.Time `json:"started_at"`
}

// buildStatus assembles the Status payload from the server's static
// configuration plus a live snapshot from the Snapshotter. now is
// injected for testability.
func buildStatus(cfg Config, snap LiveSnapshot, now time.Time) Status {
	return Status{
		Version:         cfg.Version,
		Network:         cfg.Network,
		SinkType:        cfg.SinkType,
		LastLedgerSeq:   snap.LastLedgerSeq,
		LastMessageID:   snap.LastMessageID,
		LastPublishedAt: snap.LastPublishedAt,
		WatchlistSize:   snap.WatchlistSize,
		UptimeSeconds:   int64(now.Sub(cfg.StartedAt).Seconds()),
		StartedAt:       cfg.StartedAt,
	}
}
