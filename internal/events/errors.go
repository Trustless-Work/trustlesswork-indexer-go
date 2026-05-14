// Package events defines the wire contract between the Indexer and downstream
// consumers. It is intentionally free of transport concerns: sinks (RabbitMQ,
// Kafka, etc.) import the types defined here and serialize them according to
// their own protocol, but the shape of an event is decided in one place.
//
// The contract follows a "filter and forward" philosophy: the Indexer
// identifies events of interest by their topic Symbol (for Soroban contract
// events) or by their destination address (for token transfers to known
// escrows), captures minimal identifying metadata, and forwards the raw XDR
// payload as a base64 string. Downstream consumers decode the XDR with the
// Stellar SDK on their side. This decouples the Indexer from contract-level
// schema changes.
package events

import "errors"

// Sentinel errors emitted by the events package. Callers should use
// errors.Is to match these rather than comparing strings.
var (
	// ErrUnknownTopic indicates a Soroban contract event whose first topic
	// Symbol is not in the configured filter set. This is the normal "skip"
	// signal during ledger scanning; it does not represent a failure.
	ErrUnknownTopic = errors.New("event topic outside configured filter")

	// ErrXDRDecodingFail indicates that raw XDR could not be decoded into
	// the expected Soroban structure during identity extraction. Typically
	// signals data corruption upstream or a Soroban protocol change the
	// Indexer has not yet been updated for.
	ErrXDRDecodingFail = errors.New("could not decode XDR from event")

	// ErrEnvelopeInvalid indicates that an Envelope failed its own
	// validation before being handed to a sink. This is always a caller
	// bug (forgot to populate a required field), never a data quality
	// issue, and should be treated as fatal.
	ErrEnvelopeInvalid = errors.New("envelope failed validation")
)
