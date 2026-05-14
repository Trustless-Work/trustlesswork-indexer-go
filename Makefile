.PHONY: build run test vet clean \
        up down restart logs logs-rabbit ps rebuild reset-state

# --- Toolchain ---------------------------------------------------------------

GO ?= go
BIN ?= bin/ingest
COMPOSE ?= docker compose

# --- Local binary ------------------------------------------------------------

build:
	$(GO) build -o $(BIN) cmd/ingest.go

# `run` sources .env (if present) BEFORE invoking the binary so dev
# operators don't have to export variables in their shell. Production
# deployments rely on container/orchestrator-provided env vars, not
# on .env files — keep .env out of containers.
#
# Implementation: we shell out to /bin/sh and use POSIX-compatible
# `set -a` / `. ./.env`, which handles quoted values (e.g. the network
# passphrase, which contains spaces and a semicolon) correctly. Make's
# native `include` directive would split such values on the semicolon.
run: build
	@/bin/sh -c 'set -a; [ -f .env ] && . ./.env; set +a; ./$(BIN)'

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

clean:
	rm -rf bin/

# --- Docker stack ------------------------------------------------------------
# The compose stack brings up RabbitMQ, declares the indexer.events
# queue + binding (one-shot init container), then starts the Indexer
# wired to publish to that broker. See docker-compose.yml for details.
#
# Conflict note: the docker indexer exposes :8080 too. Stop the local
# `make run` before `make up` (or vice versa).

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

# Tear down + remove the indexer's state volume. Use when you want
# a "fresh first-boot" run against the same broker (cursor + watchlist
# reset). RabbitMQ data is preserved.
reset-state:
	$(COMPOSE) down
	docker volume rm indexer_indexer-state 2>/dev/null || true

restart:
	$(COMPOSE) restart indexer

logs:
	$(COMPOSE) logs -f indexer

logs-rabbit:
	$(COMPOSE) logs -f rabbitmq

ps:
	$(COMPOSE) ps

# Force a rebuild of the indexer image and restart it. Useful after
# editing Go source: `make rebuild` picks up the new binary without
# touching RabbitMQ.
rebuild:
	$(COMPOSE) build indexer
	$(COMPOSE) up -d indexer
