// Package rabbitmq publishes envelopes to a RabbitMQ topic exchange, one
// message per envelope. With publisher confirms enabled (the production
// default), Publish blocks on a positive broker ack for THAT message
// before returning success — that ack is what justifies advancing the
// cursor downstream.
//
// The routing key comes from the envelope (stellar.<net>.escrow.<type>.<kind>).
// The sink owns only the exchange declaration; consumers declare and bind
// their own queues.
//
// Concurrency: amqp.Channel is not safe for concurrent use, so Publish is
// serialized with a mutex. Note this is the ONLY reason left: confirms no
// longer have to arrive in publish order, because each publish now waits
// on its own DeferredConfirmation rather than on the next confirmation to
// appear on a shared channel (audit A2). The intended caller is still the
// single-goroutine ingest loop.
package rabbitmq

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/sink"
)

// confirmWarnEvery paces the "still waiting" warning while a publish is
// held on the broker. Waiting is not an error — it is backpressure, and
// the loop deliberately holds the cursor rather than advance past an
// unconfirmed message — but a silent indefinite wait would be
// indistinguishable from a hang.
const confirmWarnEvery = 30 * time.Second

// returnsBuffer sizes the basic.return notification channel. The library
// delivers returns with a BLOCKING send, so an undersized buffer stalls
// the connection's dispatcher — the same hazard that made the old
// NotifyPublish listener dangerous. Returns are rare (they mean broken
// topology, which is fatal here), so a small buffer is plenty; what
// matters is that it is never zero-slack.
const returnsBuffer = 8

// signatureHeader carries the envelope signature as transport metadata.
// The name is the consumer's contract (core-api `SIGNATURE_HEADER`);
// keeping the signature OUT of the envelope JSON is deliberate — the
// body stays byte-for-byte what schema 1.1 describes, so there is no
// canonicalization to get wrong and unsigned pre-rollout messages stay
// redriveable.
const signatureHeader = "x-tw-sig"

// signBody returns the lowercase hex HMAC-SHA256 of body under key. It
// must be fed the EXACT bytes handed to the broker — the consumer
// verifies over the raw delivery body, so signing a re-marshal (even a
// semantically equal one) would fail verification.
func signBody(body, key []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// confirmation is one publish's own receipt, replacing the positional
// "read the next confirmation off a shared channel" that lost data when
// the stream desynchronised (audit A2). *amqp.DeferredConfirmation
// satisfies this as-is — no wrapper needed — while a fake can resolve it
// on demand, which is what makes this path testable at all.
type confirmation interface {
	// Done closes once the broker has answered for THIS message.
	Done() <-chan struct{}
	// Acked reports the answer. Only meaningful after Done is closed.
	Acked() bool
}

// publishChannel is the slice of *amqp.Channel the sink needs. Kept
// deliberately tiny: everything else the channel does (declaring the
// exchange, enabling confirm mode, registering notifications) is boot
// wiring that runs once in connect and is not worth abstracting.
type publishChannel interface {
	publish(ctx context.Context, routingKey string, msg amqp.Publishing) (confirmation, error)
	// Close is the odd one out in casing, on purpose: naming it close
	// would shadow the builtin.
	Close() error
}

// amqpChannel adapts *amqp.Channel to publishChannel. The exchange is
// fixed for the sink's lifetime, so it lives here instead of being passed
// on every publish.
type amqpChannel struct {
	ch       *amqp.Channel
	exchange string
}

// publish sends one message and returns its receipt, or a nil
// confirmation when the channel is not in confirm mode.
func (a amqpChannel) publish(ctx context.Context, routingKey string, msg amqp.Publishing) (confirmation, error) {
	// mandatory=true is an invariant of this sink, not a caller's choice:
	// an envelope that matches NO queue binding must come back as a
	// basic.return instead of being dropped silently. Before it, publishing
	// with the consumer's queue missing confirmed fine and the cursor
	// advanced over lost data (audit Sprint 5).
	dc, err := a.ch.PublishWithDeferredConfirmWithContext(ctx, a.exchange, routingKey, true, false, msg)
	if err != nil {
		return nil, err
	}
	if dc == nil {
		// Confirm mode is off, so the broker will never answer. The
		// library reports that by handing back a nil confirmation, which
		// is why this needs no second source of truth to keep in sync.
		//
		// Returning a literal nil — NOT dc — matters: `return dc, nil`
		// would wrap a nil pointer in a NON-nil interface, sailing past
		// the caller's nil check and panicking on the first method call.
		return nil, nil
	}
	return dc, nil
}

func (a amqpChannel) Close() error { return a.ch.Close() }

// Sink delivers envelopes to a RabbitMQ topic exchange.
type Sink struct {
	cfg Config

	mu   sync.Mutex
	conn *amqp.Connection
	ch   publishChannel
	// returns receives basic.return frames for unroutable mandatory
	// publishes. CRITICAL subtlety: the broker CONFIRMS a returned message
	// (it was received, just not routed), so the ack alone would let the
	// cursor advance over silently-dropped data — exactly the loss
	// mandatory publishing exists to prevent. Publish checks this after
	// every confirm, matching on MessageId so a stray return can never be
	// charged to the wrong envelope.
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
	s := &Sink{cfg: cfg}
	if err := s.connect(); err != nil {
		return nil, fmt.Errorf("rabbitmq: initial connect: %w", err)
	}
	return s, nil
}

// connect dials, opens a channel, declares the durable topic exchange and
// (when configured) puts the channel into confirm mode.
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

	if s.cfg.PublisherConfirms {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return fmt.Errorf("%w: enable confirms: %v", sink.ErrSinkUnavailable, err)
		}
	}

	// No NotifyPublish listener on purpose. Deferred confirmations carry
	// each result to its own publisher, and registering a listener would
	// re-create the hazard this design removes: the library delivers
	// confirmations with a BLOCKING send, so a listener nobody is draining
	// stalls the connection's dispatcher, kills the heartbeats and drops
	// the connection — precisely while a publish sits in backoff.
	returns := ch.NotifyReturn(make(chan amqp.Return, returnsBuffer))

	// Surface broker-wide flow control (memory/disk alarm) in the logs the
	// moment it happens: without this, a blocked broker looks like a bare
	// slow confirm and the real cause is invisible. The goroutine ends
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
	s.ch = amqpChannel{ch: ch, exchange: s.cfg.Exchange}
	s.returns = returns
	return nil
}

// Publish marshals env to JSON and publishes it under the envelope's
// routing key, then blocks until the broker answers for THAT message.
func (s *Sink) Publish(ctx context.Context, env events.Envelope) error {
	if err := env.Validate(); err != nil {
		return err // wrapped events.ErrEnvelopeInvalid
	}

	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshaling envelope %s: %w", env.MessageID, err)
	}

	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    env.MessageID,
		Timestamp:    env.PublishedAt,
		Body:         body,
	}
	if s.cfg.SignatureKey != "" {
		msg.Headers = amqp.Table{signatureHeader: signBody(body, []byte(s.cfg.SignatureKey))}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	confirm, err := s.ch.publish(ctx, env.RoutingKey(), msg)
	if err != nil {
		return fmt.Errorf("%w: publishing %s: %v", sink.ErrSinkUnavailable, env.MessageID, err)
	}
	if confirm == nil {
		return nil // confirms disabled: nothing to wait on
	}

	if err := s.awaitConfirm(ctx, confirm, env.MessageID); err != nil {
		return err
	}
	return s.checkReturned(ctx, env.MessageID)
}

// awaitConfirm blocks until this message's own confirmation resolves.
//
// There is deliberately NO timeout. A deferred confirmation ALWAYS
// resolves: by a broker ack, by a nack, or — when the channel or
// connection dies — as a nack, because Channel.shutdown nacks everything
// still pending. So the wait cannot hang, and bounding it artificially is
// what desynchronised the old shared confirmation stream: abandoning a
// wait left the late answer to be charged to the next message forever
// after (audit A2).
//
// Waiting is the correct behaviour anyway. While it waits the loop does
// not advance its cursor, /readyz reports 503 and the heartbeat falls
// silent, so the external monitor alerts — that alerting is the
// prerequisite that makes waiting acceptable rather than a silent stall.
func (s *Sink) awaitConfirm(ctx context.Context, c confirmation, messageID string) error {
	started := time.Now()
	warn := time.NewTicker(confirmWarnEvery)
	defer warn.Stop()

	for {
		select {
		case <-c.Done():
			if !c.Acked() {
				return fmt.Errorf("%w: broker nacked %s", sink.ErrSinkPublishRejected, messageID)
			}
			return nil
		case <-warn.C:
			log.Ctx(ctx).Warnf(
				"Still waiting for the broker to confirm %s after %s — the ledger cursor is held until it answers",
				messageID, time.Since(started).Round(time.Second))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// checkReturned reports an unroutable publish. A returned message is
// CONFIRMED (received but not routed), so the ack alone is not success.
// Returns are matched by MessageId rather than assumed to belong to the
// publish in flight: charging a stray return to the wrong envelope would
// fail a perfectly routable message, the mirror image of A2.
func (s *Sink) checkReturned(ctx context.Context, messageID string) error {
	for {
		select {
		case ret, ok := <-s.returns:
			if !ok {
				// Closed along with the channel. Read as "nothing returned";
				// the dead channel surfaces on the next publish.
				return nil
			}
			if ret.MessageId != messageID {
				log.Ctx(ctx).Warnf("Ignoring a basic.return for %s while publishing %s", ret.MessageId, messageID)
				continue
			}
			return fmt.Errorf("%w: broker returned %s unroutable (reply %d: %s) — is the consumer's queue bound?",
				sink.ErrSinkUnroutable, messageID, ret.ReplyCode, ret.ReplyText)
		default:
			return nil
		}
	}
}

// Close tears down the channel and connection. Idempotent.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch != nil {
		_ = s.ch.Close()
		s.ch = nil
	}
	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		return err
	}
	return nil
}
