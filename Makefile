.PHONY: build run test vet lint clean

# --- Toolchain ---------------------------------------------------------------

GO ?= go
BIN ?= bin/ingest

# --- Targets -----------------------------------------------------------------

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
