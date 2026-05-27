// Package rabbitmq publishes envelopes to a RabbitMQ topic exchange, one
// message per envelope. With publisher confirms enabled (the production
// default), Publish blocks on a positive broker ack before returning
// success — that ack is what justifies advancing the cursor downstream.
//
// The routing key comes from the envelope (stellar.<net>.escrow.<type>.<kind>).
// The sink owns only the exchange declaration; consumers declare and bind
// their own queues.
//
// Concurrency: amqp.Channel is not safe for concurrent use and confirms
// arrive in publish order, so Publish is serialized with a mutex. The
// intended caller is the single-goroutine ingest loop.
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/sink"
)

// Sink delivers envelopes to a RabbitMQ topic exchange.
type Sink struct {
	cfg Config

	mu       sync.Mutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	confirms chan amqp.Confirmation // nil when PublisherConfirms is false
}

var _ sink.Sink = (*Sink)(nil)

// New constructs a Sink and establishes the initial connection. A failure
// here should be treated as fail-fast at boot.
func New(cfg Config) (*Sink, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("rabbitmq: URL is required")
	}
	if cfg.Exchange == "" {
		return nil, fmt.Errorf("rabbitmq: Exchange is required")
	}
	s := &Sink{cfg: cfg.withDefaults()}
	if err := s.connect(); err != nil {
		return nil, fmt.Errorf("rabbitmq: initial connect: %w", err)
	}
	return s, nil
}

// connect dials, opens a channel, declares the durable topic exchange and
// (when configured) registers a publisher-confirm listener.
func (s *Sink) connect() error {
	conn, err := amqp.Dial(s.cfg.URL)
	if err != nil {
		return fmt.Errorf("%w: dial: %v", sink.ErrSinkUnavailable, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("%w: open channel: %v", sink.ErrSinkUnavailable, err)
	}

	if err := ch.ExchangeDeclare(s.cfg.Exchange, "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("%w: declare exchange %q: %v", sink.ErrSinkUnavailable, s.cfg.Exchange, err)
	}

	var confirms chan amqp.Confirmation
	if s.cfg.PublisherConfirms {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return fmt.Errorf("%w: enable confirms: %v", sink.ErrSinkUnavailable, err)
		}
		// Buffer of 1: Publish is serialized, so at most one confirm is
		// in flight at a time.
		confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	s.conn = conn
	s.channel = ch
	s.confirms = confirms
	return nil
}

// Publish marshals env to JSON and publishes it to the exchange under the
// envelope's routing key. When confirms are enabled, it blocks on the
// broker ack (bounded by PublishConfirmTimeout).
func (s *Sink) Publish(ctx context.Context, env events.Envelope) error {
	if err := env.Validate(); err != nil {
		return err // wrapped events.ErrEnvelopeInvalid
	}

	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshaling envelope %s: %w", env.MessageID, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    env.MessageID,
		Timestamp:    env.PublishedAt,
		Body:         body,
	}
	if err := s.channel.PublishWithContext(ctx, s.cfg.Exchange, env.RoutingKey(), false, false, pub); err != nil {
		return fmt.Errorf("%w: publishing %s: %v", sink.ErrSinkUnavailable, env.MessageID, err)
	}

	if s.confirms == nil {
		return nil
	}

	select {
	case confirm := <-s.confirms:
		if !confirm.Ack {
			return fmt.Errorf("%w: broker nacked %s", sink.ErrSinkPublishRejected, env.MessageID)
		}
		return nil
	case <-time.After(s.cfg.PublishConfirmTimeout):
		return fmt.Errorf("%w: confirm timeout for %s", sink.ErrSinkPublishRejected, env.MessageID)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close tears down the channel and connection. Idempotent.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channel != nil {
		_ = s.channel.Close()
		s.channel = nil
	}
	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		return err
	}
	return nil
}
