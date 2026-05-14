package rabbitmq

// Config holds all configuration needed to connect and publish to RabbitMQ.
type Config struct {
	// URL is the AMQP connection string.
	// Example: "amqp://guest:guest@localhost:5672/"
	URL string

	// Exchange is the topic exchange where messages are published.
	// Example: "stellar.events"
	Exchange string

	// Network is used to build routing keys.
	// Example: "testnet", "mainnet"
	Network string

	// PublisherConfirms enables RabbitMQ publisher confirms (at-least-once delivery).
	// Recommended for production. Adds a small latency overhead per batch.
	PublisherConfirms bool
}