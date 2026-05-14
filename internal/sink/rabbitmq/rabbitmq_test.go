package rabbitmq

import (
	"testing"
	"time"
)

// These tests cover the pure-function bits of the rabbitmq sink that do
// not require a broker: routing key construction, config defaults, and
// the New() argument validation. Integration tests with a live broker
// live separately (docker-compose) and are NOT part of `go test ./...`.

func TestBuildRoutingKey_Format(t *testing.T) {
	cases := []struct {
		network   string
		eventKind string
		want      string
	}{
		{"testnet", "tw_init", "stellar.testnet.escrow.tw_init"},
		{"mainnet", "tw_fund", "stellar.mainnet.escrow.tw_fund"},
		{"testnet", "token_transfer", "stellar.testnet.escrow.token_transfer"},
	}
	for _, c := range cases {
		got := BuildRoutingKey(c.network, c.eventKind)
		if got != c.want {
			t.Errorf("BuildRoutingKey(%q, %q): got %q, want %q", c.network, c.eventKind, got, c.want)
		}
	}
}

func TestBuildRoutingKey_EventKindHasNoDots(t *testing.T) {
	// Property: the resulting routing key must have exactly 4 segments
	// so consumers can bind with `stellar.<net>.escrow.*` and catch any
	// single-segment event_kind. If an event_kind contains a dot, this
	// invariant breaks. Document it via test.
	key := BuildRoutingKey("testnet", "tw_init")
	count := 0
	for _, c := range key {
		if c == '.' {
			count++
		}
	}
	if count != 3 {
		t.Fatalf("expected 3 dots in routing key; got %d in %q", count, key)
	}
}

func TestConfig_WithDefaults_FillsTimeout(t *testing.T) {
	c := Config{}.withDefaults()
	if c.PublishConfirmTimeout != 10*time.Second {
		t.Fatalf("expected default 10s; got %v", c.PublishConfirmTimeout)
	}
}

func TestConfig_WithDefaults_RespectsExplicitTimeout(t *testing.T) {
	c := Config{PublishConfirmTimeout: 3 * time.Second}.withDefaults()
	if c.PublishConfirmTimeout != 3*time.Second {
		t.Fatalf("expected explicit 3s preserved; got %v", c.PublishConfirmTimeout)
	}
}

func TestNew_RejectsEmptyURL(t *testing.T) {
	_, err := New(Config{Exchange: "x", Network: "testnet"})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestNew_RejectsEmptyExchange(t *testing.T) {
	_, err := New(Config{URL: "amqp://localhost/", Network: "testnet"})
	if err == nil {
		t.Fatal("expected error for empty Exchange")
	}
}

func TestNew_RejectsEmptyNetwork(t *testing.T) {
	_, err := New(Config{URL: "amqp://localhost/", Exchange: "x"})
	if err == nil {
		t.Fatal("expected error for empty Network")
	}
}
