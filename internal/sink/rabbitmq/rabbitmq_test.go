package rabbitmq

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/sink"
)

// fakeConfirmation stands in for *amqp.DeferredConfirmation, whose done
// channel is unexported and therefore impossible to resolve from a test.
// This is the whole reason the confirmation interface exists.
type fakeConfirmation struct {
	done chan struct{}
	ack  bool
}

func newConfirmation() *fakeConfirmation {
	return &fakeConfirmation{done: make(chan struct{})}
}

func (f *fakeConfirmation) Done() <-chan struct{} { return f.done }
func (f *fakeConfirmation) Acked() bool           { return f.ack }

// resolve answers this publish the way the broker would. Passing false
// also models what the library does to every pending confirmation when
// the channel or connection dies.
func (f *fakeConfirmation) resolve(ack bool) {
	f.ack = ack
	close(f.done)
}

// fakeChannel hands out a pre-seeded confirmation per publish, in order,
// so a test can decide independently how each message is answered.
type fakeChannel struct {
	answers  []*fakeConfirmation
	sent     []string     // routing keys, in publish order
	messages []string     // MessageId, in publish order
	headers  []amqp.Table // AMQP headers, in publish order (nil when unset)
	bodies   [][]byte     // published bodies, in publish order
	err      error
	nilConf  bool // model a channel that is not in confirm mode
}

func (c *fakeChannel) publish(_ context.Context, routingKey string, msg amqp.Publishing) (confirmation, error) {
	if c.err != nil {
		return nil, c.err
	}
	c.sent = append(c.sent, routingKey)
	c.messages = append(c.messages, msg.MessageId)
	c.headers = append(c.headers, msg.Headers)
	c.bodies = append(c.bodies, msg.Body)
	if c.nilConf {
		return nil, nil
	}
	next := c.answers[0]
	c.answers = c.answers[1:]
	return next, nil
}

func (c *fakeChannel) Close() error { return nil }

func newSink(ch publishChannel, returns chan amqp.Return) *Sink {
	if returns == nil {
		returns = make(chan amqp.Return, returnsBuffer)
	}
	return &Sink{
		cfg:     Config{Exchange: "stellar.events", PublisherConfirms: true},
		ch:      ch,
		returns: returns,
	}
}

func envelope(id string) events.Envelope {
	return events.Envelope{
		SchemaVersion:   events.CurrentSchemaVersion,
		Type:            "state",
		Network:         "testnet",
		ContractID:      "CAAA",
		LedgerSeq:       42,
		StateChangeType: "updated",
		MessageID:       id,
		RawXDR:          "AAAA",
		PublishedAt:     time.Unix(1750000000, 0).UTC(),
	}
}

// THE regression. Two publishes in a row where the FIRST is acked and the
// SECOND nacked: the second must be judged by its OWN answer. The old
// code read the next confirmation off a shared channel, so after any
// desync it reported the second as delivered on the strength of the
// first's ack — and the ingest loop advanced its cursor over a message
// the broker had rejected.
func TestPublish_EachMessageIsJudgedByItsOwnConfirmation(t *testing.T) {
	first, second := newConfirmation(), newConfirmation()
	ch := &fakeChannel{answers: []*fakeConfirmation{first, second}}
	s := newSink(ch, nil)

	first.resolve(true)   // the broker accepted message one
	second.resolve(false) // and rejected message two

	if err := s.Publish(context.Background(), envelope("one")); err != nil {
		t.Fatalf("first publish: %v", err)
	}

	err := s.Publish(context.Background(), envelope("two"))
	if !errors.Is(err, sink.ErrSinkPublishRejected) {
		t.Fatalf("second publish = %v, want ErrSinkPublishRejected — it must not inherit the first message's ack", err)
	}
}

func TestPublish_AckedMessageSucceeds(t *testing.T) {
	c := newConfirmation()
	c.resolve(true)
	ch := &fakeChannel{answers: []*fakeConfirmation{c}}
	s := newSink(ch, nil)

	if err := s.Publish(context.Background(), envelope("one")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(ch.sent) != 1 || ch.sent[0] != "stellar.testnet.escrow.state.updated" {
		t.Errorf("routing keys = %v, want the envelope's own key", ch.sent)
	}
	if len(ch.messages) != 1 || ch.messages[0] != "one" {
		t.Errorf("message ids = %v, want [one]", ch.messages)
	}
}

// A dead channel resolves every pending confirmation as a nack, which is
// how the library reports it. That must surface as a rejection rather
// than hang the loop forever.
func TestPublish_ChannelDeathResolvesAsRejection(t *testing.T) {
	c := newConfirmation()
	ch := &fakeChannel{answers: []*fakeConfirmation{c}}
	s := newSink(ch, nil)

	go func() {
		time.Sleep(10 * time.Millisecond)
		c.resolve(false) // what Channel.shutdown does to pending confirmations
	}()

	done := make(chan error, 1)
	go func() { done <- s.Publish(context.Background(), envelope("one")) }()

	select {
	case err := <-done:
		if !errors.Is(err, sink.ErrSinkPublishRejected) {
			t.Fatalf("err = %v, want ErrSinkPublishRejected", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish hung on a confirmation the broker will never ack")
	}
}

func TestPublish_ContextCancellationUnblocksTheWait(t *testing.T) {
	ch := &fakeChannel{answers: []*fakeConfirmation{newConfirmation()}} // never resolved
	s := newSink(ch, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := s.Publish(ctx, envelope("one"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// A returned message is confirmed by the broker, so the ack alone is not
// success — the envelope reached no queue at all.
func TestPublish_ReturnedMessageIsUnroutable(t *testing.T) {
	c := newConfirmation()
	c.resolve(true)
	ch := &fakeChannel{answers: []*fakeConfirmation{c}}
	returns := make(chan amqp.Return, returnsBuffer)
	returns <- amqp.Return{MessageId: "one", ReplyCode: 312, ReplyText: "NO_ROUTE"}
	s := newSink(ch, returns)

	err := s.Publish(context.Background(), envelope("one"))
	if !errors.Is(err, sink.ErrSinkUnroutable) {
		t.Fatalf("err = %v, want ErrSinkUnroutable", err)
	}
}

// The mirror image of A2: a return belonging to some other envelope must
// not fail this one. Charging it here would kill a perfectly routable
// message and, worse, mask the fact that a DIFFERENT one was dropped.
func TestPublish_ForeignReturnIsNotChargedToThisMessage(t *testing.T) {
	c := newConfirmation()
	c.resolve(true)
	ch := &fakeChannel{answers: []*fakeConfirmation{c}}
	returns := make(chan amqp.Return, returnsBuffer)
	returns <- amqp.Return{MessageId: "somebody-else", ReplyCode: 312, ReplyText: "NO_ROUTE"}
	s := newSink(ch, returns)

	if err := s.Publish(context.Background(), envelope("one")); err != nil {
		t.Fatalf("Publish = %v, want success: the return belongs to another envelope", err)
	}
}

// A closed returns channel must not spin: reading it yields zero values
// forever, and a drain loop that only skips on a MessageId mismatch would
// never terminate.
func TestPublish_ClosedReturnsChannelDoesNotSpin(t *testing.T) {
	c := newConfirmation()
	c.resolve(true)
	ch := &fakeChannel{answers: []*fakeConfirmation{c}}
	returns := make(chan amqp.Return)
	close(returns)
	s := newSink(ch, returns)

	done := make(chan error, 1)
	go func() { done <- s.Publish(context.Background(), envelope("one")) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish = %v, want success", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish spun on a closed returns channel")
	}
}

// Confirm mode off is fire-and-forget: the library reports it with a nil
// confirmation, and there is nothing to wait on.
func TestPublish_WithoutConfirmModeReturnsImmediately(t *testing.T) {
	ch := &fakeChannel{nilConf: true}
	s := newSink(ch, nil)

	if err := s.Publish(context.Background(), envelope("one")); err != nil {
		t.Fatalf("Publish = %v, want success", err)
	}
	if len(ch.messages) != 1 {
		t.Errorf("published %d messages, want 1", len(ch.messages))
	}
}

func TestPublish_InvalidEnvelopeNeverReachesTheBroker(t *testing.T) {
	ch := &fakeChannel{}
	s := newSink(ch, nil)

	err := s.Publish(context.Background(), events.Envelope{Type: "state"}) // missing everything else
	if !errors.Is(err, events.ErrEnvelopeInvalid) {
		t.Fatalf("err = %v, want ErrEnvelopeInvalid", err)
	}
	if len(ch.sent) != 0 {
		t.Errorf("a malformed envelope was published: %v", ch.sent)
	}
}

// The signature must reproduce the consumer's exactly — these expected
// digests are computed with core-api's signEnvelope (envelope-signature.ts)
// over the same key/body pairs its own spec uses. If this test and the
// core's disagree, the core's wins: it is the verifier.
func TestSignBody_MatchesTheConsumerVector(t *testing.T) {
	key := []byte("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk") // 'k' (0x6b) × 32, the core's test key
	for _, tc := range []struct {
		body string
		want string
	}{
		{`{"a":1}`, "1d3154043cab846e480d276b293ac8d8443514ce52ccd43a7c118d42f9bc5b5b"},
		{`{"type":"state"}`, "35e6131a4b9d32e2683eb00e71bf516a1e0ee4ee210ab7adf581fd3633372748"},
	} {
		if got := signBody([]byte(tc.body), key); got != tc.want {
			t.Errorf("signBody(%s) = %s, want %s", tc.body, got, tc.want)
		}
	}
}

// The header must sign the EXACT bytes the broker receives — a signature
// over anything else (a re-marshal, the envelope struct) verifies against
// nothing on the consumer side. And the body itself must stay untouched:
// the signature is transport metadata, not an envelope field.
func TestPublish_SignsTheExactPublishedBody(t *testing.T) {
	c := newConfirmation()
	c.resolve(true)
	ch := &fakeChannel{answers: []*fakeConfirmation{c}}
	s := newSink(ch, nil)
	s.cfg.SignatureKey = "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk"

	if err := s.Publish(context.Background(), envelope("one")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	sig, ok := ch.headers[0][signatureHeader].(string)
	if !ok {
		t.Fatalf("headers = %v, want a string %s", ch.headers[0], signatureHeader)
	}
	if want := signBody(ch.bodies[0], []byte(s.cfg.SignatureKey)); sig != want {
		t.Errorf("signature = %s, want %s (HMAC of the published body)", sig, want)
	}
	if len(sig) != 64 || strings.ToLower(sig) != sig {
		t.Errorf("signature %q is not 64-char lowercase hex", sig)
	}
	if bytes.Contains(ch.bodies[0], []byte(signatureHeader)) {
		t.Errorf("the envelope body leaked the signature: %s", ch.bodies[0])
	}
}

// No key, no header — not an empty signature, not a signature under an
// empty key. The consumer distinguishes `missing` (tolerated during
// rollout) from `invalid` (dropped), and an empty-key signature would
// land every message in the second bucket.
func TestPublish_WithoutKeyPublishesNoSignatureHeader(t *testing.T) {
	c := newConfirmation()
	c.resolve(true)
	ch := &fakeChannel{answers: []*fakeConfirmation{c}}
	s := newSink(ch, nil)

	if err := s.Publish(context.Background(), envelope("one")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, present := ch.headers[0][signatureHeader]; present {
		t.Errorf("headers = %v, want no %s header without a key", ch.headers[0], signatureHeader)
	}
}

func TestPublish_TransportFailureIsUnavailable(t *testing.T) {
	ch := &fakeChannel{err: errors.New("channel closed")}
	s := newSink(ch, nil)

	err := s.Publish(context.Background(), envelope("one"))
	if !errors.Is(err, sink.ErrSinkUnavailable) {
		t.Fatalf("err = %v, want ErrSinkUnavailable", err)
	}
}
