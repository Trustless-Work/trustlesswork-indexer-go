// Package factory builds Sink implementations from validated configuration.
// It lives outside internal/sink to break the import cycle that would
// otherwise form between the sink interface package and its concrete
// implementations (each of which imports sink).
//
// The factory does not read environment variables directly — that is the
// responsibility of internal/config. The factory takes a typed config
// value, dispatches on Sink.Type, and constructs the appropriate
// implementation. This keeps env-handling in one place (internal/config)
// and makes the sink layer easy to test with hand-built config values.
package factory

import (
	"fmt"

	"github.com/Trustless-Work/Indexer/internal/config"
	"github.com/Trustless-Work/Indexer/internal/sink"
	"github.com/Trustless-Work/Indexer/internal/sink/noop"
	"github.com/Trustless-Work/Indexer/internal/sink/rabbitmq"
)

// Type identifies a concrete sink implementation. Its values match the
// case-insensitive strings accepted by SinkConfig.Type.
type Type string

const (
	TypeNoop     Type = "noop"
	TypeRabbitMQ Type = "rabbitmq"
)

// New constructs a Sink based on cfg. The Network is read separately so
// the sink can build correct routing keys without needing access to the
// full Config tree.
//
// New does not perform a second-pass validation of cfg — Config.Validate
// has already enforced the cross-field rules. New does perform the
// minimal "is the URL non-empty" check the implementations require, so
// a caller who builds a Config by hand (e.g. tests) still gets a clear
// error.
func New(cfg *config.Config) (sink.Sink, error) {
	if cfg == nil {
		return nil, fmt.Errorf("sink factory: config is nil")
	}

	switch Type(cfg.Sink.Type) {
	case TypeNoop:
		return noop.New(), nil
	case TypeRabbitMQ:
		return rabbitmq.New(rabbitmq.Config{
			URL:               cfg.RabbitMQ.URL,
			Exchange:          cfg.RabbitMQ.Exchange,
			Network:           cfg.Network.Name,
			PublisherConfirms: cfg.RabbitMQ.PublisherConfirms,
		})
	default:
		return nil, fmt.Errorf("sink factory: unsupported sink type %q (expected one of: %s, %s)",
			cfg.Sink.Type, TypeNoop, TypeRabbitMQ)
	}
}
