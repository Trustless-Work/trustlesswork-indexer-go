package rabbitmq

// Config holds what the RabbitMQ sink needs. It is populated by the
// factory from the global config; the sink never reads env vars.
type Config struct {
	// URL is the AMQP connection string, e.g.
	// "amqp://guest:guest@localhost:5672/".
	URL string
	// Exchange is the durable topic exchange envelopes are published to.
	// Consumers bind their own queues to it.
	Exchange string
	// PublisherConfirms enables RabbitMQ publisher confirms (at-least-once):
	// Publish blocks on a positive broker ack before returning success.
	// Recommended for production — without it a publish is fire-and-forget
	// and the cursor advances over anything the broker quietly drops.
	PublisherConfirms bool
	// SignatureKey is the HMAC-SHA256 key shared with the consumer. When
	// non-empty every publish carries an `x-tw-sig` header signing the
	// exact body bytes; empty publishes unsigned (consumer rollout).
	// Raw string bytes, exactly as the env var holds them — the consumer
	// keys its HMAC with the same undecoded string.
	SignatureKey string
}
