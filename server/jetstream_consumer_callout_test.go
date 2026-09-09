// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !skip_js_tests && !skip_js_consumer_tests

package server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamConsumerCalloutConfigValidation(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:   "C1",
		AckPolicy: AckExplicit,
		Callout:   &ConsumerCallout{Subject: _EMPTY_},
	})
	require_Error(t, err)
	if !IsNatsErr(err, JSConsumerCalloutSubjectRequiredErr) {
		t.Fatalf("want error %q, got %q", ApiErrors[JSConsumerCalloutSubjectRequiredErr], err)
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:   "C2",
		AckPolicy: AckExplicit,
		Callout:   &ConsumerCallout{Subject: "callout.>"},
	})
	require_Error(t, err)
	if !IsNatsErr(err, JSConsumerCalloutInvalidSubjectErr) {
		t.Fatalf("want error %q, got %q", ApiErrors[JSConsumerCalloutInvalidSubjectErr], err)
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:   "C3",
		AckPolicy: AckExplicit,
		Callout:   &ConsumerCallout{Subject: "callout.subj", Timeout: -1},
	})
	require_Error(t, err)
	if !IsNatsErr(err, JSConsumerCalloutTimeoutNegativeErr) {
		t.Fatalf("want error %q, got %q", ApiErrors[JSConsumerCalloutTimeoutNegativeErr], err)
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:   "C4",
		AckPolicy: AckExplicit,
		Callout:   &ConsumerCallout{Subject: "callout.subj"},
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:   "C5",
		AckPolicy: AckExplicit,
		Callout:   &ConsumerCallout{Subject: "callout.subj", Concurrency: -1},
	})
	require_Error(t, err)
	if !IsNatsErr(err, JSConsumerCalloutConcurrencyNegativeErr) {
		t.Fatalf("want error %q, got %q", ApiErrors[JSConsumerCalloutConcurrencyNegativeErr], err)
	}

	unordered := false
	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:   "C6",
		AckPolicy: AckExplicit,
		Callout:   &ConsumerCallout{Subject: "callout.subj", Concurrency: 8, Ordered: &unordered},
	})
	require_NoError(t, err)
}

// calloutResponder subscribes to subj and replies "go" or "no-go" based on an
// atomically-toggled flag, forwarding every received request onto reqCh so
// the test can inspect what the server actually sent.
func calloutResponder(t *testing.T, nc *nats.Conn, subj string, allow *atomic.Bool) chan *nats.Msg {
	t.Helper()
	reqCh := make(chan *nats.Msg, 64)
	_, err := nc.Subscribe(subj, func(m *nats.Msg) {
		// Non-blocking: a denied message is retried in a tight loop by the
		// server (bounded only by round-trip time), so don't let a full test
		// channel stall the responder from ever answering.
		select {
		case reqCh <- m:
		default:
		}
		if allow.Load() {
			m.Respond(nil)
			return
		}
		reply := nats.NewMsg(m.Reply)
		reply.Header.Set("Status", "403")
		reply.Header.Set("Description", "denied")
		m.RespondMsg(reply)
	})
	require_NoError(t, err)
	require_NoError(t, nc.Flush())
	return reqCh
}

func TestJetStreamConsumerCalloutPush(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	var allow atomic.Bool
	allow.Store(true)
	reqCh := calloutResponder(t, nc, "CALLOUT.PUSH", &allow)

	deliverSubj := "deliver.push"
	sub, err := nc.SubscribeSync(deliverSubj)
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "PUSH",
		DeliverSubject: deliverSubj,
		AckPolicy:      AckExplicit,
		Callout: &ConsumerCallout{
			Subject:        "CALLOUT.PUSH",
			IncludeSubject: true,
			IncludeHeaders: true,
			IncludePayload: true,
		},
	})
	require_NoError(t, err)

	// Go path: message should be delivered, and the callout request should
	// carry the subject and payload we asked for.
	sendStreamMsg(t, nc, "foo", "hello")
	req := require_ChanRead(t, reqCh, 2*time.Second)
	require_Equal(t, req.Header.Get(JSSubject), "foo")
	require_Equal(t, string(req.Data), "hello")

	m, err := sub.NextMsg(2 * time.Second)
	require_NoError(t, err)
	require_Equal(t, string(m.Data), "hello")

	// No-go path: message must not be delivered while denied...
	allow.Store(false)
	sendStreamMsg(t, nc, "foo", "world")
	require_ChanRead(t, reqCh, 2*time.Second)
	if _, err := sub.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("expected no delivery while callout denies the message")
	}

	// ...but once allowed again it should show up, redelivered.
	allow.Store(true)
	m, err = sub.NextMsg(2 * time.Second)
	require_NoError(t, err)
	require_Equal(t, string(m.Data), "world")
}

func TestJetStreamConsumerCalloutPull(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	var allow atomic.Bool
	allow.Store(true)
	reqCh := calloutResponder(t, nc, "CALLOUT.PULL", &allow)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:   "PULL",
		AckPolicy: AckExplicit,
		Callout: &ConsumerCallout{
			Subject:        "CALLOUT.PULL",
			IncludeSubject: true,
			IncludePayload: true,
		},
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe(_EMPTY_, "PULL", nats.Bind("TEST", "PULL"))
	require_NoError(t, err)

	// Go path.
	sendStreamMsg(t, nc, "foo", "hello")
	msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
	require_NoError(t, err)
	require_Equal(t, len(msgs), 1)
	require_Equal(t, string(msgs[0].Data), "hello")
	require_NoError(t, msgs[0].Ack())
	require_ChanRead(t, reqCh, time.Second)

	// No-go: fetch should time out with nothing delivered, and must not have
	// consumed the fetch's budget in a way that starves a later accepted message.
	allow.Store(false)
	sendStreamMsg(t, nc, "foo", "denied-1")
	if msgs, err = sub.Fetch(1, nats.MaxWait(500*time.Millisecond)); err == nil && len(msgs) > 0 {
		t.Fatalf("expected no message while callout denies, got %d", len(msgs))
	}
	require_ChanRead(t, reqCh, time.Second)

	// Flip back to allow and fetch a larger batch: both the earlier denied
	// message (now retried) and a fresh one should come back.
	allow.Store(true)
	sendStreamMsg(t, nc, "foo", "ok-2")
	msgs, err = sub.Fetch(2, nats.MaxWait(2*time.Second))
	require_NoError(t, err)
	require_Equal(t, len(msgs), 2)
	for _, m := range msgs {
		require_NoError(t, m.Ack())
	}
}

func TestJetStreamConsumerCalloutTimeout(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	deliverSubj := "deliver.timeout"
	sub, err := nc.SubscribeSync(deliverSubj)
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	// No one is listening on the callout subject at all.
	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "PUSH",
		DeliverSubject: deliverSubj,
		AckPolicy:      AckExplicit,
		Callout: &ConsumerCallout{
			Subject: "CALLOUT.NOBODY",
			Timeout: 250 * time.Millisecond,
		},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo", "hello")
	if _, err := sub.NextMsg(500 * time.Millisecond); err == nil {
		t.Fatalf("expected fail-closed: no delivery when nothing answers the callout")
	}

	// Once a responder shows up (fail-closed just delays, it doesn't drop),
	// the message should eventually be delivered.
	var allow atomic.Bool
	allow.Store(true)
	calloutResponder(t, nc, "CALLOUT.NOBODY", &allow)

	m, err := sub.NextMsg(2 * time.Second)
	require_NoError(t, err)
	require_Equal(t, string(m.Data), "hello")
}

func TestJetStreamConsumerCalloutUpdateConfig(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	deliverSubj := "deliver.update"
	sub, err := nc.SubscribeSync(deliverSubj)
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	// No callout configured initially: normal delivery.
	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "PUSH",
		DeliverSubject: deliverSubj,
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo", "one")
	m, err := sub.NextMsg(2 * time.Second)
	require_NoError(t, err)
	require_Equal(t, string(m.Data), "one")
	require_NoError(t, m.AckSync())

	// Now attach a callout that denies everything.
	var allowA atomic.Bool
	reqChA := calloutResponder(t, nc, "CALLOUT.A", &allowA)

	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "PUSH",
		DeliverSubject: deliverSubj,
		AckPolicy:      AckExplicit,
		Callout:        &ConsumerCallout{Subject: "CALLOUT.A"},
	}, ActionUpdate, false)
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo", "two")
	require_ChanRead(t, reqChA, 2*time.Second)
	if _, err := sub.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("expected delivery to be gated after attaching callout")
	}

	// Repoint the callout at a different subject that allows everything;
	// the old subject's responder should stop being consulted.
	var allowB atomic.Bool
	allowB.Store(true)
	reqChB := calloutResponder(t, nc, "CALLOUT.B", &allowB)

	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "PUSH",
		DeliverSubject: deliverSubj,
		AckPolicy:      AckExplicit,
		Callout:        &ConsumerCallout{Subject: "CALLOUT.B"},
	}, ActionUpdate, false)
	require_NoError(t, err)

	// CALLOUT.A always denies, so the only way "two" gets delivered is if the
	// consumer resubscribed its callout to CALLOUT.B, which always allows.
	m, err = sub.NextMsg(2 * time.Second)
	require_NoError(t, err)
	require_Equal(t, string(m.Data), "two")
	require_ChanRead(t, reqChB, 2*time.Second)
}

// delayedApproveResponder always approves, but sleeps first if the payload
// has a configured delay — lets a test force completion order to differ from
// dispatch order.
func delayedApproveResponder(t *testing.T, nc *nats.Conn, subj string, delays map[string]time.Duration) {
	t.Helper()
	// nats.Subscribe dispatches callbacks one at a time from a single
	// per-subscription goroutine, so without spawning here the configured
	// delays would just serialize the responder instead of actually racing
	// concurrent requests against each other.
	_, err := nc.Subscribe(subj, func(m *nats.Msg) {
		go func() {
			if d := delays[string(m.Data)]; d > 0 {
				time.Sleep(d)
			}
			m.Respond(nil)
		}()
	})
	require_NoError(t, err)
	require_NoError(t, nc.Flush())
}

func TestJetStreamConsumerCalloutPipelineOrdered(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	// "1" resolves slowest, "3" fastest — completion order is reversed from
	// dispatch order, so this only passes if release ordering is enforced.
	delayedApproveResponder(t, nc, "CALLOUT.ORDERED", map[string]time.Duration{
		"1": 300 * time.Millisecond,
		"2": 150 * time.Millisecond,
		"3": 0,
	})

	deliverSubj := "deliver.ordered"
	sub, err := nc.SubscribeSync(deliverSubj)
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "ORDERED",
		DeliverSubject: deliverSubj,
		AckPolicy:      AckExplicit,
		Callout: &ConsumerCallout{
			Subject:        "CALLOUT.ORDERED",
			IncludePayload: true,
			Concurrency:    3,
			// Ordered defaults to true.
		},
	})
	require_NoError(t, err)

	for _, m := range []string{"1", "2", "3"} {
		sendStreamMsg(t, nc, "foo", m)
	}

	for _, want := range []string{"1", "2", "3"} {
		m, err := sub.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_Equal(t, string(m.Data), want)
	}
}

func TestJetStreamConsumerCalloutPipelineUnordered(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	delayedApproveResponder(t, nc, "CALLOUT.UNORDERED", map[string]time.Duration{
		"1": 300 * time.Millisecond,
		"2": 150 * time.Millisecond,
		"3": 0,
	})

	deliverSubj := "deliver.unordered"
	sub, err := nc.SubscribeSync(deliverSubj)
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	unordered := false
	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "UNORDERED",
		DeliverSubject: deliverSubj,
		AckPolicy:      AckExplicit,
		Callout: &ConsumerCallout{
			Subject:        "CALLOUT.UNORDERED",
			IncludePayload: true,
			Concurrency:    3,
			Ordered:        &unordered,
		},
	})
	require_NoError(t, err)

	for _, m := range []string{"1", "2", "3"} {
		sendStreamMsg(t, nc, "foo", m)
	}

	// Fastest-resolving ("3") should come back first, not "1" — actually
	// observe reordering rather than merely tolerate it.
	want := []string{"3", "2", "1"}
	for i := range want {
		m, err := sub.NextMsg(2 * time.Second)
		require_NoError(t, err)
		if string(m.Data) != want[i] {
			t.Fatalf("expected out-of-order delivery %v, got %q at position %d", want, m.Data, i)
		}
	}
}
