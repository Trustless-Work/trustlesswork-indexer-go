package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"

	"github.com/Trustless-Work/Indexer/internal/sink"
)

// RabbitMQSink publishes processed ledger data to a RabbitMQ topic exchange.
// Each entity category is published as a separate message with its own routing key.
//
// Routing key pattern: stellar.<network>.<entity>
// Examples:
//   - stellar.testnet.escrow
//   - stellar.testnet.state_change
//   - stellar.testnet.transaction
//   - stellar.testnet.operation
//   - stellar.testnet.trustline_change
//   - stellar.testnet.contract_change
type RabbitMQSink struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	cfg     Config
}

// envelope wraps a payload with ledger metadata so consumers have full context
// without needing to parse the payload.
type envelope struct {
	LedgerSeq uint32      `json:"ledger_seq"`
	Network   string      `json:"network"`
	EventType string      `json:"event_type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

var _ sink.Sink = (*RabbitMQSink)(nil)

// New creates a RabbitMQSink and establishes the initial connection.
func New(cfg Config) (*RabbitMQSink, error) {
	s := &RabbitMQSink{cfg: cfg}
	if err := s.connect(); err != nil {
		return nil, fmt.Errorf("rabbitmq sink: initial connection failed: %w", err)
	}
	return s, nil
}

// connect establishes the AMQP connection, opens a channel, and declares the exchange.
func (s *RabbitMQSink) connect() error {
	conn, err := amqp.Dial(s.cfg.URL)
	if err != nil {
		return fmt.Errorf("dialing %q: %w", s.cfg.URL, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("opening channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		s.cfg.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("declaring exchange %q: %w", s.cfg.Exchange, err)
	}

	if s.cfg.PublisherConfirms {
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("enabling publisher confirms: %w", err)
		}
	}

	s.conn = conn
	s.channel = ch
	return nil
}

// Write publishes each non-empty entity category from the buffer as a separate
// message on the configured exchange.
func (s *RabbitMQSink) Write(ctx context.Context, buffer sink.LedgerBuffer, ledgerSeq uint32) error {
	now := time.Now().UTC()

	type batch struct {
		eventType string
		payload   interface{}
	}

	batches := []batch{
		{"escrow", buffer.GetEscrows()},
		{"state_change", buffer.GetStateChanges()},
		{"transaction", buffer.GetTransactions()},
		{"operation", buffer.GetOperations()},
		{"trustline_change", buffer.GetTrustlineChanges()},
		{"contract_change", buffer.GetContractChanges()},
	}

	for _, b := range batches {
		if isEmpty(b.payload) {
			continue
		}

		routingKey := fmt.Sprintf("stellar.%s.%s", s.cfg.Network, b.eventType)

		msg, err := s.marshal(envelope{
			LedgerSeq: ledgerSeq,
			Network:   s.cfg.Network,
			EventType: b.eventType,
			Timestamp: now,
			Payload:   b.payload,
		})
		if err != nil {
			return fmt.Errorf("marshaling %s for ledger %d: %w", b.eventType, ledgerSeq, err)
		}

		if err := s.publish(ctx, routingKey, msg); err != nil {
			// Attempt a single reconnect before giving up
			logrus.WithError(err).Warnf("rabbitmq: publish failed, attempting reconnect")
			if reconnErr := s.reconnect(); reconnErr != nil {
				return fmt.Errorf("rabbitmq: publish failed and reconnect failed: %w", err)
			}
			if err := s.publish(ctx, routingKey, msg); err != nil {
				return fmt.Errorf("rabbitmq: publish failed after reconnect for ledger %d: %w", ledgerSeq, err)
			}
		}

		logrus.WithFields(logrus.Fields{
			"ledger":      ledgerSeq,
			"routing_key": routingKey,
		}).Debug("rabbitmq: published message")
	}

	return nil
}

func (s *RabbitMQSink) publish(ctx context.Context, routingKey string, body []byte) error {
	return s.channel.PublishWithContext(
		ctx,
		s.cfg.Exchange, // exchange
		routingKey,     // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

func (s *RabbitMQSink) marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// reconnect closes the existing connection (best effort) and re-establishes it
// with a simple retry using exponential backoff up to 5 attempts.
func (s *RabbitMQSink) reconnect() error {
	if s.channel != nil {
		s.channel.Close()
	}
	if s.conn != nil {
		s.conn.Close()
	}

	backoff := time.Second
	for attempt := 1; attempt <= 5; attempt++ {
		logrus.WithField("attempt", attempt).Info("rabbitmq: reconnecting...")
		if err := s.connect(); err == nil {
			logrus.Info("rabbitmq: reconnected successfully")
			return nil
		}
		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	return fmt.Errorf("rabbitmq: failed to reconnect after 5 attempts")
}

// Ping verifies the connection is alive. Implements sink.HealthChecker.
func (s *RabbitMQSink) Ping(_ context.Context) error {
	if s.conn == nil || s.conn.IsClosed() {
		return fmt.Errorf("rabbitmq: connection is closed")
	}
	return nil
}

func (s *RabbitMQSink) Close() error {
	if s.channel != nil {
		s.channel.Close()
	}
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

// isEmpty reports whether v is a nil or zero-length slice.
func isEmpty(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice {
		return rv.Len() == 0
	}
	return false
}
