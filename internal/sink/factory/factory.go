// Package factory builds Sink implementations from environment configuration.
// It is intentionally separate from the internal/sink package to keep the
// Sink interface and its concrete implementations free of import cycles.
package factory

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/Trustless-Work/Indexer/internal/sink"
	"github.com/Trustless-Work/Indexer/internal/sink/noop"
	"github.com/Trustless-Work/Indexer/internal/sink/rabbitmq"
)

// Type identifies a concrete sink implementation. It is loaded from the
// SINK_TYPE environment variable.
type Type string

const (
	TypeNoop     Type = "noop"
	TypeRabbitMQ Type = "rabbitmq"
)

// EnvSinkType is the name of the environment variable that selects the sink
// implementation. Defaults to TypeNoop when unset or empty.
const EnvSinkType = "SINK_TYPE"

// NewFromEnv builds a Sink based on the SINK_TYPE environment variable.
// Backend-specific configuration is also read from the environment by each
// implementation. Returns TypeNoop when SINK_TYPE is unset.
//
// Supported values (case-insensitive): "noop", "rabbitmq".
func NewFromEnv() (sink.Sink, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(EnvSinkType)))
	if raw == "" {
		raw = string(TypeNoop)
	}

	switch Type(raw) {
	case TypeNoop:
		return noop.New(), nil
	case TypeRabbitMQ:
		cfg, err := rabbitMQConfigFromEnv()
		if err != nil {
			return nil, fmt.Errorf("loading rabbitmq config: %w", err)
		}
		return rabbitmq.New(cfg)
	default:
		return nil, fmt.Errorf("unsupported sink type %q (expected one of: %s, %s)", raw, TypeNoop, TypeRabbitMQ)
	}
}

// RabbitMQ-specific environment variables.
const (
	EnvRabbitMQURL                = "RABBITMQ_URL"
	EnvRabbitMQExchange           = "RABBITMQ_EXCHANGE"
	EnvRabbitMQNetwork            = "RABBITMQ_NETWORK"
	EnvRabbitMQPublisherConfirms  = "RABBITMQ_PUBLISHER_CONFIRMS"
	defaultRabbitMQExchange       = "stellar.events"
	defaultRabbitMQNetwork        = "testnet"
)

// rabbitMQConfigFromEnv loads RabbitMQSink configuration from the environment.
// RABBITMQ_URL is required. The remaining variables have sensible defaults.
func rabbitMQConfigFromEnv() (rabbitmq.Config, error) {
	url := strings.TrimSpace(os.Getenv(EnvRabbitMQURL))
	if url == "" {
		return rabbitmq.Config{}, fmt.Errorf("%s is required when SINK_TYPE=rabbitmq", EnvRabbitMQURL)
	}

	cfg := rabbitmq.Config{
		URL:      url,
		Exchange: getenvDefault(EnvRabbitMQExchange, defaultRabbitMQExchange),
		Network:  getenvDefault(EnvRabbitMQNetwork, defaultRabbitMQNetwork),
	}

	if v := strings.TrimSpace(os.Getenv(EnvRabbitMQPublisherConfirms)); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return rabbitmq.Config{}, fmt.Errorf("parsing %s=%q as bool: %w", EnvRabbitMQPublisherConfirms, v, err)
		}
		cfg.PublisherConfirms = parsed
	}

	return cfg, nil
}

// getenvDefault returns the trimmed value of the environment variable name, or
// def when the variable is unset or empty.
func getenvDefault(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}
