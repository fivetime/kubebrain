// Copyright 2022 ByteDance and/or its affiliates
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

package scanner

import (
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

type resultReceiver interface {
	append(key, value []byte, revision uint64)
	flush()
	close()
	reset()
	needMore() bool

	// retriable reports whether a failed partition scan may be retried from the
	// partition start with this receiver. Buffering receivers are always
	// retriable (reset discards everything). A streaming receiver is NOT once it
	// has emitted a chunk: chunks already in the channel cannot be recalled, so
	// re-scanning the partition would re-send keys and break RangeStream's
	// disjoint-chunks contract (clients like clientv3's GetStreamToGetResponse
	// proto.Merge the chunks — duplicates corrupt Kvs/Count). Failing the scan
	// instead surfaces Unavailable and the client cleanly relists.
	retriable() bool

	// MapReduce
	fork() resultReceiver
	merge(receiver resultReceiver)
}

type emptyResultReceiver struct{}

func (e *emptyResultReceiver) needMore() bool {
	return true
}

func (e *emptyResultReceiver) append(key, value []byte, revision uint64) {
	// do nothing
}

func (e *emptyResultReceiver) flush() {
	// do nothing
}

func (e *emptyResultReceiver) close() {
	// do nothing
}

func (e *emptyResultReceiver) reset() {
	// do nothing
}

func (e *emptyResultReceiver) fork() resultReceiver {
	return &emptyResultReceiver{}
}

func (e *emptyResultReceiver) retriable() bool {
	return true
}
func (e *emptyResultReceiver) merge(receiver resultReceiver) {
	// do nothing
}

type commonResultReceiver struct {
	emptyResultReceiver
	result []*proto.KeyValue
	limit  int
}

func (c *commonResultReceiver) fork() resultReceiver {
	return &commonResultReceiver{limit: c.limit, result: make([]*proto.KeyValue, 0, c.limit)}
}

func (c *commonResultReceiver) merge(receiver resultReceiver) {
	subReceiver := receiver.(*commonResultReceiver)

	if !c.isLimited() || len(c.result)+len(subReceiver.result) <= c.limit {
		c.result = append(c.result, subReceiver.result...)
	} else {
		c.result = append(c.result, subReceiver.result[:c.limit-len(c.result)]...)
	}
}

func (c *commonResultReceiver) needMore() bool {
	if c.isLimited() {
		return len(c.result) < c.limit
	}
	return true
}

func (c *commonResultReceiver) isLimited() bool {
	return c.limit > 0
}

func (c *commonResultReceiver) append(key, value []byte, revision uint64) {
	c.result = append(c.result, &proto.KeyValue{
		Key:      key,
		Value:    value,
		Revision: revision,
	})
}

func (c *commonResultReceiver) reset() {
	c.result = make([]*proto.KeyValue, 0, len(c.result))
}

// rangeStreamBatchBytes caps a chunk by accumulated key+value bytes, mirroring
// etcd's byte-adaptive RangeStream chunking (it sizes chunks against
// MaxRequestBytes ≈ 1.5MiB). Without it the fixed 300-key chunk grows linearly
// with value size — 10KB objects already make 3MB chunks, and 100KB+ objects
// would build chunks beyond gRPC message comfort — and the stream channel's
// memory bound stops being a constant. Whichever threshold trips first flushes;
// a single value larger than the cap still travels as its own one-key chunk
// (a KV cannot be split).
const rangeStreamBatchBytes = 1536 * 1024

type streamResultReceiver struct {
	emptyResultReceiver
	readRev uint64
	stream  chan *proto.StreamRangeResponse
	batch   []*proto.KeyValue
	// batchBytes is the accumulated key+value payload of batch, driving the
	// byte half of the dual flush threshold.
	batchBytes int
	// emitted is set once a chunk has been pushed into stream; from then on this
	// receiver (a per-partition fork) is no longer retriable — see
	// resultReceiver.retriable.
	emitted bool
}

func newStreamReceiver(readRev uint64, stream chan *proto.StreamRangeResponse) *streamResultReceiver {
	return &streamResultReceiver{
		readRev: readRev,
		stream:  stream,
	}
}

func (e *streamResultReceiver) append(key, value []byte, revision uint64) {
	e.batch = append(e.batch, &proto.KeyValue{
		Key:      key,
		Value:    value,
		Revision: revision,
	})
	e.batchBytes += len(key) + len(value)
	if len(e.batch) >= rangeStreamBatch || e.batchBytes >= rangeStreamBatchBytes {
		e.emit()
	}
}

func (e *streamResultReceiver) flush() {
	if len(e.batch) != 0 {
		e.emit()
	}
}

// emit pushes the pending batch as one chunk and resets the batch state.
func (e *streamResultReceiver) emit() {
	// todo: use object pool
	batch := e.batch
	e.reset()
	e.stream <- &proto.StreamRangeResponse{
		RangeResponse: &proto.RangeResponse{
			Header: &proto.ResponseHeader{Revision: e.readRev},
			Kvs:    batch,
			More:   true,
		},
	}
	e.emitted = true
}

func (e *streamResultReceiver) close() {
	e.flush()
}

func (e *streamResultReceiver) reset() {
	e.batch = make([]*proto.KeyValue, 0, rangeStreamBatch)
	e.batchBytes = 0
}

func (e *streamResultReceiver) fork() resultReceiver {
	// readRev MUST be carried into the forked per-partition receiver: it is the
	// pinned read revision every emitted chunk stamps into its ResponseHeader.
	// Dropping it made every streamed chunk report revision 0 — latent until the
	// user-key RangeStream path first streamed real data (partition workers all
	// run on forked receivers).
	return &streamResultReceiver{
		readRev: e.readRev,
		stream:  e.stream,
	}
}

func (e *streamResultReceiver) retriable() bool {
	return !e.emitted
}
