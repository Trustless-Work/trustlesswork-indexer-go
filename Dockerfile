# syntax=docker/dockerfile:1.7

# --- Build stage -------------------------------------------------------------
# Alpine + Go because we want a small layer cache and a static binary.
# CGO is disabled so the resulting binary runs on any glibc/musl base.
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Cache the module download separately from the source so editing code
# doesn't bust the module layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# VERSION can be overridden at build time:
#   docker build --build-arg VERSION=v1.2.3 .
# Unstamped builds report "dev" via /status.
ARG VERSION=dev

RUN CGO_ENABLED=0 GOOS=linux \
    go build \
        -trimpath \
        -ldflags="-w -s -X github.com/Trustless-Work/Indexer/internal/ingest.Version=${VERSION}" \
        -o /out/ingest \
        ./cmd

# --- Runtime stage -----------------------------------------------------------
# Alpine (not distroless) so the named volume mount for the state file
# can be chowned to the non-root user at image build time. CA certs are
# needed for TLS to the Stellar RPC.
FROM alpine:3.20

# Non-root user owns the state directory. uid 10001 is a conventional
# "service" uid that avoids any collision with host users.
RUN apk add --no-cache ca-certificates tini && \
    addgroup -g 10001 indexer && \
    adduser -D -u 10001 -G indexer indexer && \
    mkdir -p /var/lib/indexer && \
    chown -R indexer:indexer /var/lib/indexer

USER indexer:indexer
WORKDIR /var/lib/indexer

COPY --from=builder /out/ingest /usr/local/bin/ingest

# tini reaps zombies and forwards SIGTERM properly so signal-driven
# graceful shutdown works the same inside Docker as it does locally.
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/ingest"]
