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
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/streamerror"
)

const (
	decodedRangeSpillRunKeys  = 300
	decodedRangeSpillRunBytes = 1536 * 1024
	decodedRangeSpillMergeFan = 16
)

// decodedUserRangeStreamFromSpill is the bounded-memory correctness path when
// the in-memory ordering index cannot serve (overflow, old untrusted history,
// or an interrupted rebuild). The legacy object encoding is not user-key order
// preserving, so one keys-only snapshot scan creates bounded sorted runs and a
// fixed-fan-in external merge. We then consume the final run 300 keys at a time
// and resolve values against the same pinned TiKV snapshot.
//
// Only keys are spilled: values remain in TiKV and response memory stays
// O(decodedRangeStreamIndexPage). One spill is admitted per replica to avoid an
// overflow request herd multiplying full scans and temporary-disk consumption.
func (b *backend) decodedUserRangeStreamFromSpill(
	ctx context.Context,
	userStart, userEnd []byte,
	revision uint64,
) <-chan *proto.StreamRangeResponse {
	// A page can contain 16 near-limit values; do not queue multiple pages in
	// memory while the gRPC layer is still transmitting the previous one.
	stream := make(chan *proto.StreamRangeResponse)
	go func() {
		defer close(stream)
		send := func(response *proto.StreamRangeResponse) bool {
			select {
			case stream <- response:
				return true
			case <-ctx.Done():
				return false
			}
		}
		fail := func(err error) {
			if err == nil || ctx.Err() != nil {
				return
			}
			send(&proto.StreamRangeResponse{
				RangeResponse: &proto.RangeResponse{Header: responseHeader(revision)},
				Err:           streamerror.Encode(err),
			})
		}

		select {
		case b.decodedRangeSpillSem <- struct{}{}:
			defer func() { <-b.decodedRangeSpillSem }()
		case <-ctx.Done():
			return
		}

		started := time.Now()
		sorter, err := newExternalKeySorter()
		if err != nil {
			fail(fmt.Errorf("create decoded range ordering spill: %w", err))
			return
		}
		defer sorter.Close()

		end := userEnd
		if isFromKeyEnd(end) {
			end = nil
		}
		scanStart, scanEnd, exactKeys := b.decodedUserRangeScanPlan(userStart, userEnd)
		excluded := make(map[string]struct{}, len(exactKeys))
		for _, key := range exactKeys {
			excluded[string(key)] = struct{}{}
		}
		var observed int64
		add := func(key []byte) error {
			if bytes.Compare(key, userStart) < 0 || (len(end) != 0 && bytes.Compare(key, end) >= 0) {
				return nil
			}
			if _, skip := excluded[string(key)]; skip {
				return nil
			}
			observed++
			return sorter.Add(key)
		}

		raw := b.scanner.RangeStream(ctx, scanStart, scanEnd, revision, true)
		for response := range raw {
			if response.Err != "" {
				fail(streamerror.DecodeOrPlain(response.Err))
				return
			}
			for _, kv := range response.RangeResponse.Kvs {
				if err = add(kv.Key); err != nil {
					fail(fmt.Errorf("write decoded range ordering spill: %w", err))
					return
				}
			}
		}
		err = b.visitDecodedRangeExactKeyChunks(ctx, exactKeys, revision, func(chunkKeys [][]byte, exact []*proto.KeyValue) error {
			for index, kv := range exact {
				if kv == nil {
					continue
				}
				observed++
				if addErr := sorter.Add(chunkKeys[index]); addErr != nil {
					return fmt.Errorf("write decoded range exact-key spill: %w", addErr)
				}
			}
			return nil
		})
		if err != nil {
			fail(err)
			return
		}
		finalRun, err := sorter.Finish(ctx)
		if err != nil {
			fail(fmt.Errorf("merge decoded range ordering spill: %w", err))
			return
		}

		var spillBytes int64
		if finalRun != "" {
			if stat, statErr := os.Stat(finalRun); statErr == nil {
				spillBytes = stat.Size()
			}
			reader, openErr := openKeyRun(finalRun)
			if openErr != nil {
				fail(openErr)
				return
			}
			defer reader.Close()
			var carry []byte
			for {
				keys := make([][]byte, 0, decodedRangeStreamIndexPage)
				pageBytes := 0
				if carry != nil {
					keys = append(keys, carry)
					pageBytes = len(carry)
					carry = nil
				}
				for len(keys) < decodedRangeStreamIndexPage {
					key, readErr := reader.Next()
					if errors.Is(readErr, io.EOF) {
						break
					}
					if readErr != nil {
						fail(readErr)
						return
					}
					if len(keys) != 0 && pageBytes+len(key) > decodedRangeSpillRunBytes {
						carry = key
						break
					}
					keys = append(keys, key)
					pageBytes += len(key)
				}
				if len(keys) == 0 {
					break
				}
				readErr := b.visitDecodedRangeExactKeyChunks(ctx, keys, revision, func(chunkKeys [][]byte, kvs []*proto.KeyValue) error {
					for index, kv := range kvs {
						if kv == nil {
							return fmt.Errorf("decoded range spilled key %q is not live at revision %d", chunkKeys[index], revision)
						}
					}
					if !send(&proto.StreamRangeResponse{RangeResponse: &proto.RangeResponse{
						Header: responseHeader(revision), Kvs: kvs, More: true,
					}}) {
						return ctx.Err()
					}
					return nil
				})
				if readErr != nil {
					if ctx.Err() != nil {
						return
					}
					fail(readErr)
					return
				}
			}
		}
		b.metricCli.EmitGauge("backend.range_stream.decoded_spill_bytes", spillBytes)
		b.metricCli.EmitCounter("backend.range_stream.decoded_spill", 1)
		b.metricCli.EmitGauge("backend.range_stream.decoded_spill_keys", observed)
		b.metricCli.EmitHistogram("backend.range_stream.decoded_spill_latency_seconds", time.Since(started).Seconds())
		send(&proto.StreamRangeResponse{RangeResponse: &proto.RangeResponse{Header: responseHeader(revision)}})
	}()
	return stream
}

// externalKeySorter performs a multi-pass merge with fixed memory and file
// descriptor bounds. Run records are uvarint-length-prefixed raw keys, so it
// supports etcd keys larger than bbolt's ~32KiB key limit.
type externalKeySorter struct {
	dir          string
	runs         []string
	pending      [][]byte
	pendingBytes int
	sequence     uint64
}

func newExternalKeySorter() (*externalKeySorter, error) {
	dir, err := os.MkdirTemp("", ".kubebrain-range-order-*")
	if err != nil {
		return nil, err
	}
	return &externalKeySorter{dir: dir, pending: make([][]byte, 0, decodedRangeSpillRunKeys)}, nil
}

func (s *externalKeySorter) Close() error { return os.RemoveAll(s.dir) }

func (s *externalKeySorter) Add(key []byte) error {
	s.pending = append(s.pending, append([]byte(nil), key...))
	s.pendingBytes += len(key)
	if len(s.pending) >= decodedRangeSpillRunKeys || s.pendingBytes >= decodedRangeSpillRunBytes {
		return s.flushRun()
	}
	return nil
}

func (s *externalKeySorter) nextPath() string {
	s.sequence++
	return filepath.Join(s.dir, fmt.Sprintf("run-%020d", s.sequence))
}

func (s *externalKeySorter) flushRun() error {
	if len(s.pending) == 0 {
		return nil
	}
	sort.Slice(s.pending, func(i, j int) bool { return bytes.Compare(s.pending[i], s.pending[j]) < 0 })
	path := s.nextPath()
	writer, err := createKeyRun(path)
	if err != nil {
		return err
	}
	var previous []byte
	for _, key := range s.pending {
		if previous != nil && bytes.Equal(previous, key) {
			continue
		}
		if err = writer.Write(key); err != nil {
			_ = writer.Close()
			return err
		}
		previous = key
	}
	if err = writer.Close(); err != nil {
		return err
	}
	s.runs = append(s.runs, path)
	s.pending = s.pending[:0]
	s.pendingBytes = 0
	return nil
}

func (s *externalKeySorter) Finish(ctx context.Context) (string, error) {
	if err := s.flushRun(); err != nil {
		return "", err
	}
	for len(s.runs) > 1 {
		next := make([]string, 0, (len(s.runs)+decodedRangeSpillMergeFan-1)/decodedRangeSpillMergeFan)
		for start := 0; start < len(s.runs); start += decodedRangeSpillMergeFan {
			end := min(start+decodedRangeSpillMergeFan, len(s.runs))
			inputs := s.runs[start:end]
			output := s.nextPath()
			if err := mergeKeyRuns(ctx, inputs, output); err != nil {
				return "", err
			}
			for _, input := range inputs {
				if err := os.Remove(input); err != nil {
					return "", err
				}
			}
			next = append(next, output)
		}
		s.runs = next
	}
	if len(s.runs) == 0 {
		return "", nil
	}
	return s.runs[0], nil
}

type keyRunWriter struct {
	file   *os.File
	buffer *bufio.Writer
}

func createKeyRun(path string) (*keyRunWriter, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	return &keyRunWriter{file: file, buffer: bufio.NewWriterSize(file, 64<<10)}, nil
}

func (w *keyRunWriter) Write(key []byte) error {
	var size [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(size[:], uint64(len(key)))
	if _, err := w.buffer.Write(size[:n]); err != nil {
		return err
	}
	_, err := w.buffer.Write(key)
	return err
}

func (w *keyRunWriter) Close() error {
	flushErr := w.buffer.Flush()
	closeErr := w.file.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

type keyRunReader struct {
	file   *os.File
	buffer *bufio.Reader
}

func openKeyRun(path string) (*keyRunReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &keyRunReader{file: file, buffer: bufio.NewReaderSize(file, 64<<10)}, nil
}

func (r *keyRunReader) Next() ([]byte, error) {
	size, err := binary.ReadUvarint(r.buffer)
	if err != nil {
		return nil, err
	}
	if size > uint64(maxInt()) {
		return nil, fmt.Errorf("decoded range spill key length %d overflows int", size)
	}
	key := make([]byte, int(size))
	if _, err = io.ReadFull(r.buffer, key); err != nil {
		return nil, err
	}
	return key, nil
}

func (r *keyRunReader) Close() error { return r.file.Close() }

func maxInt() int { return int(^uint(0) >> 1) }

type keyRunHeapItem struct {
	key    []byte
	reader int
}

type keyRunHeap []keyRunHeapItem

func (h keyRunHeap) Len() int           { return len(h) }
func (h keyRunHeap) Less(i, j int) bool { return bytes.Compare(h[i].key, h[j].key) < 0 }
func (h keyRunHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *keyRunHeap) Push(value any)    { *h = append(*h, value.(keyRunHeapItem)) }
func (h *keyRunHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func mergeKeyRuns(ctx context.Context, inputs []string, output string) (retErr error) {
	readers := make([]*keyRunReader, 0, len(inputs))
	defer func() {
		for _, reader := range readers {
			if err := reader.Close(); retErr == nil && err != nil {
				retErr = err
			}
		}
	}()
	items := make(keyRunHeap, 0, len(inputs))
	for _, input := range inputs {
		reader, err := openKeyRun(input)
		if err != nil {
			return err
		}
		readers = append(readers, reader)
		key, err := reader.Next()
		if errors.Is(err, io.EOF) {
			continue
		}
		if err != nil {
			return err
		}
		items = append(items, keyRunHeapItem{key: key, reader: len(readers) - 1})
	}
	heap.Init(&items)
	writer, err := createKeyRun(output)
	if err != nil {
		return err
	}
	defer func() {
		if err := writer.Close(); retErr == nil && err != nil {
			retErr = err
		}
	}()
	var previous []byte
	for items.Len() != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := heap.Pop(&items).(keyRunHeapItem)
		if previous == nil || !bytes.Equal(previous, item.key) {
			if err := writer.Write(item.key); err != nil {
				return err
			}
			previous = append(previous[:0], item.key...)
		}
		next, err := readers[item.reader].Next()
		if err == nil {
			heap.Push(&items, keyRunHeapItem{key: next, reader: item.reader})
		} else if !errors.Is(err, io.EOF) {
			return err
		}
	}
	return nil
}
