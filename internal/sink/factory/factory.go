// Package factory builds the configured Sink from the global config. It
// is the one place that knows the concrete sink implementations, so the
// rest of the pipeline depends only on the sink.Sink interface.
package factory

import (
	"fmt"

	"github.com/Trustless-Work/Indexer/internal/config"
	"github.com/Trustless-Work/Indexer/internal/sink"
	"github.com/Trustless-Work/Indexer/internal/sink/noop"
	"github.com/Trustless-Work/Indexer/internal/sink/rabbitmq"
)

// New returns the Sink selected by cfg.Sink.Type. config.Validate already
// guarantees the type is supported and that RABBITMQ_URL is set when
// rabbitmq is selected; the default branch is purely defensive.
func New(cfg *config.Config) (sink.Sink, error) {
	switch cfg.Sink.Type {
	case "noop":
		return noop.New(), nil
	case "rabbitmq":
		s, err := rabbitmq.New(rabbitmq.Config{
			URL:               cfg.RabbitMQ.URL,
			Exchange:          cfg.RabbitMQ.Exchange,
			PublisherConfirms: cfg.RabbitMQ.PublisherConfirms,
		})
		if err != nil {
			return nil, err
		}
		return s, nil
	default:
		return nil, fmt.Errorf("unsupported sink type %q", cfg.Sink.Type)
	}
}
