// Package rabbitmq publishes Indexer envelopes to a RabbitMQ topic
// exchange, one message per envelope. With publisher confirms enabled
// (the production default), Publish blocks on a positive ack from the
// broker before returning success — that ack is what justifies advancing
// the cursor.
//
// Routing key shape: stellar.<network>.escrow.<event_kind>
// Examples:
//   stellar.testnet.escrow.tw_init
//   stellar.testnet.escrow.tw_fund
//   stellar.testnet.escrow.token_transfer
//
// The Indexer does NOT declare queues. Consumers (the Core) declare and
// bind their own queues against this exchange. The Indexer only owns the
// exchange declaration and assumes that fan-out semantics live downstream.
//
// Concurrency: amqp.Channel is NOT safe for concurrent use, and publisher
// confirms arrive in publish order. RabbitMQSink serializes Publish calls
// with an internal mutex; callers may invoke Publish from multiple
// goroutines, but contention will degrade throughput. The intended use is
// the single-goroutine publisher in the main loop.
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/sink"
)

// reconnectMaxAttempts bounds how many times we retry the AMQP dial+open
// sequence on a single Publish failure before giving up. The Publisher in
// the main loop has its own retry-with-backoff above this, so this is just
// inner-loop resilience for transient blips, not the global retry policy.
const reconnectMaxAttempts = 5

// RabbitMQSink delivers envelopes to a RabbitMQ topic exchange.
type RabbitMQSink struct {
	cfg Config

	// mu serializes Publish calls and protects the conn/channel/confirms
	// fields against concurrent access during reconnects.
	mu       sync.Mutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	confirms chan amqp.Confirmation // nil when PublisherConfirms is false
}

// Compile-time check that RabbitMQSink satisfies the sink contracts.
var (
	_ sink.Sink          = (*RabbitMQSink)(nil)
	_ sink.HealthChecker = (*RabbitMQSink)(nil)
)

// New constructs a RabbitMQSink and establishes the initial AMQP
// connection. Returns a wrapped error if the connection cannot be
// established; the caller should treat that as fail-fast at boot.
func New(cfg Config) (*RabbitMQSink, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("rabbitmq: URL is required")
	}
	if cfg.Exchange == "" {
		return nil, fmt.Errorf("rabbitmq: Exchange is required")
	}
	if cfg.Network == "" {
		return nil, fmt.Errorf("rabbitmq: Network is required")
	}

	s := &RabbitMQSink{cfg: cfg.withDefaults()}
	if err := s.connect(); err != nil {
		return nil, fmt.Errorf("rabbitmq: initial connect: %w", err)
	}
	return s, nil
}

// connect establishes a fresh AMQP connection, opens a channel, declares
// the exchange, and (when configured) registers a publisher-confirm
// listener. Caller must hold s.mu OR be running before the sink is
// shared with anyone (i.e. inside New).
func (s *RabbitMQSink) connect() error {
	conn, err := amqp.Dial(s.cfg.URL)
	if err != nil {
		return fmt.Errorf("%w: dial: %v", sink.ErrSinkUnavailable, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("%w: open channel: %v", sink.ErrSinkUnavailable, err)
	}

	if err := ch.ExchangeDeclare(
		s.cfg.Exchange,
		"topic", // type
		true,    // durable
		false,   // auto-delete
		false,   // internal
		false,   // no-wait
		nil,     // args
	); err != nil {
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
		// Buffer of 1 is enough since we serialize Publishes — at most
		// one confirmation is in flight at a time.
		confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	s.conn = conn
	s.channel = ch
	s.confirms = confirms
	return nil
}

// Publish serializes env to JSON, builds its routing key, and publishes
// to the configured exchange. Behavior depends on PublisherConfirms:
//
//   - true (recommended): Publish blocks until either the broker acks
//     the publish (returns nil), the broker nacks it (returns wrapped
//     ErrSinkPublishRejected), ctx is cancelled (returns ctx.Err()), or
//     PublishConfirmTimeout elapses (returns wrapped ErrSinkPublishRejected).
//   - false: Publish returns as soon as the channel accepts the frame
//     (at-most-once semantics). Not recommended for production.
//
// On a transport-level publish failure (channel closed, network blip),
// Publish attempts one reconnect-and-retry cycle before propagating the
// error.
func (s *RabbitMQSink) Publish(ctx context.Context, env events.Envelope) error {
	if err := env.Validate(); err != nil {
		return err
	}

	body, err := json.Marshal(&env)
	if err != nil {
		// JSON of our own struct cannot fail unless we changed a field
		// to something unmarshalable — caller bug.
		return fmt.Errorf("%w: marshaling envelope: %v", events.ErrEnvelopeInvalid, err)
	}

	routingKey := BuildRoutingKey(s.cfg.Network, env.EventKind)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.publishLocked(ctx, routingKey, body); err != nil {
		// Try one reconnect-and-retry. Transient connection blips are
		// common; we want to absorb them inside a single Publish call
		// rather than propagate to the loop on every glitch.
		if reconnErr := s.reconnectLocked(); reconnErr != nil {
			return reconnErr
		}
		if err := s.publishLocked(ctx, routingKey, body); err != nil {
			return err
		}
	}

	logrus.WithFields(logrus.Fields{
		"ledger_seq":  env.LedgerSeq,
		"event_kind":  env.EventKind,
		"contract_id": env.ContractID,
		"message_id":  env.MessageID,
		"routing_key": routingKey,
		"component":   "sink.rabbitmq",
	}).Debug("envelope published")

	return nil
}

// publishLocked publishes the body and, if confirms are enabled, awaits
// the confirmation. Caller must hold s.mu.
func (s *RabbitMQSink) publishLocked(ctx context.Context, routingKey string, body []byte) error {
	err := s.channel.PublishWithContext(
		ctx,
		s.cfg.Exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now().UTC(),
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("%w: publish %q: %v", sink.ErrSinkUnavailable, routingKey, err)
	}

	if !s.cfg.PublisherConfirms {
		return nil
	}
	return s.awaitConfirmLocked(ctx)
}

// awaitConfirmLocked waits for the next publisher confirmation. Returns
// nil on a positive ack; a wrapped ErrSinkPublishRejected on Nack, on a
// closed confirm channel, or on timeout. Caller must hold s.mu.
func (s *RabbitMQSink) awaitConfirmLocked(ctx context.Context) error {
	select {
	case c, ok := <-s.confirms:
		if !ok {
			return fmt.Errorf("%w: confirm channel closed", sink.ErrSinkUnavailable)
		}
		if !c.Ack {
			return fmt.Errorf("%w: broker nacked delivery tag %d", sink.ErrSinkPublishRejected, c.DeliveryTag)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.cfg.PublishConfirmTimeout):
		return fmt.Errorf("%w: timeout after %v waiting for publisher confirm", sink.ErrSinkPublishRejected, s.cfg.PublishConfirmTimeout)
	}
}

// reconnectLocked tears down the current AMQP session (best-effort) and
// dials a fresh one. Returns a wrapped ErrSinkUnavailable if all attempts
// fail. Caller must hold s.mu.
func (s *RabbitMQSink) reconnectLocked() error {
	s.closeLocked()

	var lastErr error
	backoff := time.Second
	for attempt := 1; attempt <= reconnectMaxAttempts; attempt++ {
		if err := s.connect(); err == nil {
			logrus.WithField("attempt", attempt).Info("rabbitmq: reconnected")
			return nil
		} else {
			lastErr = err
			logrus.WithError(err).WithField("attempt", attempt).Warn("rabbitmq: reconnect attempt failed")
		}
		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	return fmt.Errorf("%w: reconnect failed after %d attempts: %v", sink.ErrSinkUnavailable, reconnectMaxAttempts, lastErr)
}

// closeLocked closes the channel and connection, ignoring errors (we're
// already in an error path; best-effort is the right policy).
func (s *RabbitMQSink) closeLocked() {
	if s.channel != nil {
		_ = s.channel.Close()
		s.channel = nil
	}
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.confirms = nil
}

// Ping reports liveness for /readyz. Cheap: only inspects the cached
// connection state, no network call. A broken connection is detected on
// the next Publish via the standard reconnect path.
func (s *RabbitMQSink) Ping(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil || s.conn.IsClosed() {
		return fmt.Errorf("%w: connection closed", sink.ErrSinkUnavailable)
	}
	return nil
}

// Close closes the channel and connection. Safe to call multiple times.
// After Close, Publish will fail with ErrSinkUnavailable.
func (s *RabbitMQSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	if s.channel != nil {
		if err := s.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			firstErr = fmt.Errorf("closing channel: %w", err)
		}
		s.channel = nil
	}
	if s.conn != nil {
		if err := s.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			if firstErr == nil {
				firstErr = fmt.Errorf("closing connection: %w", err)
			}
		}
		s.conn = nil
	}
	s.confirms = nil
	return firstErr
}

// BuildRoutingKey returns the AMQP topic key the Indexer publishes to
// for a given network and event_kind. Exposed so tests and the factory
// can compute the expected routing key without duplicating the
// formatting rule.
//
// Format: "stellar.<network>.escrow.<event_kind>".
func BuildRoutingKey(network string, eventKind string) string {
	return fmt.Sprintf("stellar.%s.escrow.%s", network, eventKind)
}
