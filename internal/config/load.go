package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

// Load reads the runtime configuration from environment variables and
// validates it.
//
// On success returns a *Config ready to hand to subsystem constructors.
// On any failure — env parse error, missing required, cross-field rule
// violated — returns a wrapped error and a nil pointer. The binary's
// entry point should treat any Load error as fail-fast: the process must
// not start with partial config.
//
// Load is intentionally cheap and side-effect free. It does NOT touch
// the disk (state file path is recorded but not opened here), does NOT
// open network connections, and does NOT mutate any global state. Boot
// sequencing — open state, instantiate sink, start health server — is
// the caller's responsibility, using values from the returned Config.
func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parsing environment: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	return cfg, nil
}
