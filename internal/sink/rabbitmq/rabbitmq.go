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
	"github.com/stellar/go-stellar-sdk/support/log"

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
	// returns receives basic.return frames for unroutable mandatory
	// publishes. CRITICAL subtlety: the broker CONFIRMS a returned message
	// (it was received, just not routed), so the confirm alone would let
	// the cursor advance over silently-dropped data — exactly the loss
	// mandatory publishing exists to prevent. Publish checks this channel
	// after every confirm.
	returns chan amqp.Return
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

	// Buffer of 1 for the same serialization reason. The library delivers
	// the basic.return BEFORE the confirm of the same publish (frames are
	// dispatched in wire order), so by the time Publish sees the confirm,
	// any return for that message is already buffered here.
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))

	// Surface broker-wide flow control (memory/disk alarm) in the logs the
	// moment it happens: without this, a blocked broker looks like a bare
	// confirm timeout and the real cause is invisible. The goroutine ends
	// when the connection closes (the library closes the channel).
	blocked := conn.NotifyBlocked(make(chan amqp.Blocking, 1))
	go func() {
		for b := range blocked {
			if b.Active {
				log.Warnf("RabbitMQ blocked publishing: %s (broker alarm — publishes will stall)", b.Reason)
			} else {
				log.Info("RabbitMQ unblocked publishing")
			}
		}
	}()

	s.conn = conn
	s.channel = ch
	s.confirms = confirms
	s.returns = returns
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
	// mandatory=true: an envelope that matches NO queue binding comes back
	// as a basic.return instead of being dropped silently. Before this,
	// publishing with the consumer's queue missing confirmed fine and the
	// cursor advanced over lost data (audit Sprint 5).
	if err := s.channel.PublishWithContext(ctx, s.cfg.Exchange, env.RoutingKey(), true, false, pub); err != nil {
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
		// A returned message is CONFIRMED (received but unroutable), so
		// the ack alone is not success. Publish is serialized, so any
		// buffered return belongs to this publish.
		select {
		case ret := <-s.returns:
			return fmt.Errorf("%w: broker returned %s unroutable (reply %d: %s) — is the consumer's queue bound?",
				sink.ErrSinkPublishRejected, env.MessageID, ret.ReplyCode, ret.ReplyText)
		default:
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
