package config

import (
	"strings"
	"testing"
)

func TestString_RedactsURLPassword(t *testing.T) {
	withEnv(t, map[string]string{
		"RPC_URL":      "https://soroban-testnet.stellar.org",
		"SINK_TYPE":    "rabbitmq",
		"RABBITMQ_URL": "amqp://admin:secretpassword@rabbit.example.com:5672/",
	})
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	out := cfg.String()
	if strings.Contains(out, "secretpassword") {
		t.Errorf("String must redact URL password; got:\n%s", out)
	}
	if !strings.Contains(out, "***") {
		t.Errorf("String must show redaction marker; got:\n%s", out)
	}
	if !strings.Contains(out, "admin") {
		t.Errorf("String must preserve username; got:\n%s", out)
	}
}

func TestString_PreservesURLWithoutPassword(t *testing.T) {
	withEnv(t, map[string]string{
		"RPC_URL": "https://soroban-testnet.stellar.org",
	})
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	out := cfg.String()
	if !strings.Contains(out, "https://soroban-testnet.stellar.org") {
		t.Errorf("URL without password must print as-is; got:\n%s", out)
	}
}

func TestString_IncludesAllTopLevelDomains(t *testing.T) {
	withEnv(t, map[string]string{
		"RPC_URL": "https://soroban-testnet.stellar.org",
	})
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	out := cfg.String()
	// Every nested struct should appear at least once. Spot-check the
	// names operators will look for.
	for _, needle := range []string{"Network.", "RPC.", "Indexer.", "Sink.", "RabbitMQ.", "State.", "Health.", "Logging.", "StrictMode"} {
		if !strings.Contains(out, needle) {
			t.Errorf("String output missing domain %q; got:\n%s", needle, out)
		}
	}
}

func TestString_ShowsStrictModeValue(t *testing.T) {
	withEnv(t, map[string]string{
		"RPC_URL":     "https://soroban-testnet.stellar.org",
		"STRICT_MODE": "false",
	})
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	out := cfg.String()
	if !strings.Contains(out, "StrictMode=false") {
		t.Errorf("expected StrictMode=false in output; got:\n%s", out)
	}
}
