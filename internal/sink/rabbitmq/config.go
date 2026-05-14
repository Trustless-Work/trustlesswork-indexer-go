package rabbitmq

import "time"

// Config holds the configuration needed to connect to a RabbitMQ broker
// and publish envelopes. It is populated by the factory from the global
// *config.Config — the sink itself does not read env vars.
type Config struct {
	// URL is the AMQP connection string.
	// Example: "amqp://guest:guest@localhost:5672/"
	URL string

	// Exchange is the topic exchange envelopes are published to. It is
	// declared as durable + topic at construction; consumers bind their
	// queues to it.
	Exchange string

	// Network identifies the Stellar network and feeds into the routing
	// key (stellar.<network>.escrow.<event_kind>). Example values:
	// "testnet", "mainnet".
	Network string

	// PublisherConfirms enables RabbitMQ publisher confirms (at-least-once
	// delivery). When true, every Publish blocks on a positive ack from
	// the broker before returning success. Recommended for production.
	// When false, Publish returns as soon as the channel accepts the
	// frame (at-most-once semantics).
	PublisherConfirms bool

	// PublishConfirmTimeout bounds how long Publish waits for an ack
	// from the broker when PublisherConfirms is enabled. After this
	// timeout the Publish returns ErrSinkPublishRejected even if the
	// underlying frame may have been accepted later. Default 10s if
	// zero.
	PublishConfirmTimeout time.Duration
}

// withDefaults returns a copy of c with zero-valued fields filled in
// from sensible defaults. Called once at sink construction so the
// hot-path can rely on non-zero values.
func (c Config) withDefaults() Config {
	if c.PublishConfirmTimeout == 0 {
		c.PublishConfirmTimeout = 10 * time.Second
	}
	return c
}
