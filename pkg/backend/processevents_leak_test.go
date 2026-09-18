// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"testing"
	"time"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/stretchr/testify/require"
)

// TestProcessEventsExitsOnCancelWhenConsumerStopped reproduces the goroutine
// leak: when a watch consumer (the backendShim transform goroutine) stops
// reading after its context is cancelled, processEvents would block forever on
// a bare `out <- evs` send once the buffer filled. It must instead observe
// ctx.Done and exit.
func TestProcessEventsExitsOnCancelWhenConsumerStopped(t *testing.T) {
	s, closer := newTestSuites(t, memKvStorage)
	defer closer()
	b := s.backend.(*backend)

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan []*proto.Event) // unbuffered and never read: the send blocks
	in := make(chan []*proto.Event, 4)
	done := make(chan struct{})
	go func() {
		b.processEvents(ctx, cancel, out, in, "/registry/", 0)
		close(done)
	}()

	// A matching event forces processEvents to block on `out <- evs`.
	in <- []*proto.Event{{Revision: 5, Kv: &proto.KeyValue{Key: []byte("/registry/a"), Revision: 5}}}
	time.Sleep(50 * time.Millisecond) // let it reach the blocked send

	cancel() // consumer is gone; processEvents must not leak

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processEvents leaked: did not exit after ctx cancel while the consumer was stopped and out was full")
	}
}

// A subscriber is attached before cache/history replay. Its live queue can
// therefore contain an older marker than the replay already delivered.
func TestProcessEventsDropsProgressBeforeReplayFloor(t *testing.T) {
	s, closer := newTestSuites(t, memKvStorage)
	defer closer()
	b := s.backend.(*backend)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	in := make(chan []*proto.Event, 5)
	out := make(chan []*proto.Event, 5)
	in <- NewProgressMarker(32) // queued before replay through revision 33
	in <- NewProgressMarker(33) // equal watermark is safe
	in <- []*proto.Event{{Revision: 34, Kv: &proto.KeyValue{Key: []byte("/registry/a"), Revision: 34}}}
	in <- NewProgressMarker(34)
	in <- NewProgressMarker(33) // preserve genuine live regression for RPC checks
	close(in)
	done := make(chan struct{})
	go func() {
		b.processEvents(ctx, cancel, out, in, "/registry/", 34)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processEvents did not finish")
	}
	var revisions []uint64
	for batch := range out {
		revisions = append(revisions, batch[0].Revision)
	}
	require.Equal(t, []uint64{33, 34, 34, 33}, revisions)
}
