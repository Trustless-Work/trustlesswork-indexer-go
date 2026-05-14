package factory

import (
	"strings"
	"testing"

	"github.com/Trustless-Work/Indexer/internal/config"
	"github.com/Trustless-Work/Indexer/internal/sink/noop"
)

func TestNew_NilConfig_Errors(t *testing.T) {
	_, err := New(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
}

func TestNew_Noop_ReturnsNoopSink(t *testing.T) {
	cfg := &config.Config{
		Network: config.NetworkConfig{Name: "testnet"},
		Sink:    config.SinkConfig{Type: "noop"},
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := s.(*noop.NoopSink); !ok {
		t.Fatalf("expected *noop.NoopSink; got %T", s)
	}
}

func TestNew_UnsupportedType_Errors(t *testing.T) {
	cfg := &config.Config{
		Sink: config.SinkConfig{Type: "kafka"},
	}
	_, err := New(cfg)
	if err == nil {
		t.Fatal("expected error for unsupported sink type")
	}
	if !strings.Contains(err.Error(), "kafka") {
		t.Fatalf("error should mention the unsupported type; got %v", err)
	}
}

func TestNew_RabbitMQ_RejectsEmptyURL(t *testing.T) {
	// The validation lives in rabbitmq.New; the factory propagates it.
	cfg := &config.Config{
		Network:  config.NetworkConfig{Name: "testnet"},
		Sink:     config.SinkConfig{Type: "rabbitmq"},
		RabbitMQ: config.RabbitMQConfig{URL: "", Exchange: "x"},
	}
	_, err := New(cfg)
	if err == nil {
		t.Fatal("expected error for empty rabbitmq URL")
	}
}
