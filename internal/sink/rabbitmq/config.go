package rabbitmq

import "time"

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
	// Recommended for production.
	PublisherConfirms bool
	// PublishConfirmTimeout bounds how long Publish waits for an ack when
	// PublisherConfirms is enabled. Defaults to 10s when zero.
	PublishConfirmTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.PublishConfirmTimeout == 0 {
		c.PublishConfirmTimeout = 10 * time.Second
	}
	return c
}
