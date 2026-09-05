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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/streamerror"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

const (
	decodedRangeSpillRunKeys  = 300
	decodedRangeSpillRunBytes = decodedRangeStreamIndexBytes
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
	metadataOnly bool,
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
		sorterClosed := false
		defer func() {
			if !sorterClosed {
				_ = sorter.Close()
			}
		}()

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
			readerClosed := false
			defer func() {
				if !readerClosed {
					_ = reader.Close()
				}
			}()
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
						if metadataOnly {
							kv.Value = projectMetadataValue(kv.Value)
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
			if closeErr := reader.Close(); closeErr != nil {
				fail(fmt.Errorf("close decoded range ordering spill: %w", closeErr))
				return
			}
			readerClosed = true
		}
		if closeErr := sorter.Close(); closeErr != nil {
			fail(fmt.Errorf("remove decoded range ordering spill: %w", closeErr))
			return
		}
		sorterClosed = true
		b.metricCli.EmitGauge("backend.range_stream.decoded_spill_bytes", spillBytes)
		b.metricCli.EmitCounter("backend.range_stream.decoded_spill", 1)
		b.metricCli.EmitGauge("backend.range_stream.decoded_spill_keys", observed)
		b.metricCli.EmitHistogram("backend.range_stream.decoded_spill_latency_seconds", time.Since(started).Seconds())
		send(&proto.StreamRangeResponse{RangeResponse: &proto.RangeResponse{Header: responseHeader(revision)}})
	}()
	return stream
}

// latestMetadataRangeStreamFromKeyScan is the bounded-memory fallback for a
// latest FastKeysOnly stream when the process-local count index is unavailable.
// A key-only engine snapshot scan discovers every physical object version,
// including rows from an older writer that has no latest-metadata entry. The
// external sorter deduplicates and restores user-key order without retaining
// the complete key set in memory; bounded pages are then joined with the small
// revision/latest-metadata directories at the exact same snapshot timestamp.
func (b *backend) latestMetadataRangeStreamFromKeyScan(
	ctx context.Context,
	userStart, userEnd []byte,
	revision uint64,
) (<-chan *proto.StreamRangeResponse, bool, error) {
	keyReader, supported := storage.FindCapability[storage.KeyIterator](b.kv)
	if !supported {
		return nil, false, nil
	}
	if _, supported = storage.FindCapability[storage.SnapshotGetter](b.kv); !supported {
		return nil, false, nil
	}
	pinnedCtx, err := b.withRangeSnapshotTimestamp(ctx)
	if err != nil {
		return nil, true, err
	}
	timestamp, pinned := storage.SnapshotTimestampFromContext(pinnedCtx)
	if !pinned {
		return nil, true, ErrSerializableCheckpointUnavailable
	}

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
		fail := func(streamErr error) {
			if streamErr != nil && ctx.Err() == nil {
				send(rangeStreamErrorEnd(revision, streamErr))
			}
		}

		select {
		case b.decodedRangeSpillSem <- struct{}{}:
			defer func() { <-b.decodedRangeSpillSem }()
		case <-ctx.Done():
			return
		}

		started := time.Now()
		sorter, createErr := newExternalKeySorter()
		if createErr != nil {
			fail(fmt.Errorf("create latest metadata key ordering spill: %w", createErr))
			return
		}
		sorterClosed := false
		defer func() {
			if !sorterClosed {
				_ = sorter.Close()
			}
		}()

		end := userEnd
		if isFromKeyEnd(end) {
			end = nil
		}
		scanStart, scanEnd, exactKeys := b.decodedUserRangeScanPlan(userStart, userEnd)
		var observed int64
		add := func(key []byte) error {
			if bytes.Compare(key, userStart) < 0 || (len(end) != 0 && bytes.Compare(key, end) >= 0) {
				return nil
			}
			observed++
			return sorter.Add(key)
		}
		for _, key := range exactKeys {
			if addErr := sorter.Add(key); addErr != nil {
				fail(fmt.Errorf("write latest metadata exact-key spill: %w", addErr))
				return
			}
		}

		iterator, iterErr := keyReader.IterKeys(pinnedCtx, scanStart, scanEnd, timestamp, 0)
		if iterErr != nil {
			fail(fmt.Errorf("create latest metadata key-only iterator: %w", iterErr))
			return
		}
		iteratorClosed := false
		defer func() {
			if !iteratorClosed {
				_ = iterator.Close()
			}
		}()
		for {
			iterErr = iterator.Next(pinnedCtx)
			if iterErr != nil {
				break
			}
			rawKey := iterator.Key()
			if b.ks.IsInternalStorageKey(rawKey) {
				continue
			}
			userKey, _, decodeErr := b.coder.Decode(rawKey)
			if decodeErr != nil {
				fail(fmt.Errorf("decode latest metadata key-only iterator key: %w", decodeErr))
				return
			}
			if addErr := add(userKey); addErr != nil {
				fail(fmt.Errorf("write latest metadata key ordering spill: %w", addErr))
				return
			}
		}
		if iterErr != nil && !errors.Is(iterErr, io.EOF) {
			fail(iterErr)
			return
		}
		if closeErr := iterator.Close(); closeErr != nil {
			fail(fmt.Errorf("close latest metadata key-only iterator: %w", closeErr))
			return
		}
		iteratorClosed = true

		finalRun, finishErr := sorter.Finish(pinnedCtx)
		if finishErr != nil {
			fail(fmt.Errorf("merge latest metadata key ordering spill: %w", finishErr))
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
			readerClosed := false
			defer func() {
				if !readerClosed {
					_ = reader.Close()
				}
			}()
			var carry []byte
			for {
				keys := make([][]byte, 0, latestRangeStreamIndexPage)
				pageBytes := 0
				if carry != nil {
					keys = append(keys, carry)
					pageBytes = len(carry)
					carry = nil
				}
				for len(keys) < latestRangeStreamIndexPage {
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
				kvs, readErr := b.readLatestMetadataKeys(pinnedCtx, keys, revision, 0)
				if readErr != nil {
					fail(readErr)
					return
				}
				if len(kvs) != 0 && !send(&proto.StreamRangeResponse{RangeResponse: &proto.RangeResponse{
					Header: responseHeader(revision), Kvs: kvs, More: true,
				}}) {
					return
				}
			}
			if closeErr := reader.Close(); closeErr != nil {
				fail(fmt.Errorf("close latest metadata key ordering spill: %w", closeErr))
				return
			}
			readerClosed = true
		}
		if closeErr := sorter.Close(); closeErr != nil {
			fail(fmt.Errorf("remove latest metadata key ordering spill: %w", closeErr))
			return
		}
		sorterClosed = true
		b.metricCli.EmitCounter("backend.range_stream.latest_metadata_key_scan_hit", 1)
		b.metricCli.EmitGauge("backend.range_stream.latest_metadata_key_spill_bytes", spillBytes)
		b.metricCli.EmitGauge("backend.range_stream.latest_metadata_key_spill_keys", observed)
		b.metricCli.EmitHistogram("backend.range_stream.latest_metadata_key_spill_latency_seconds", time.Since(started).Seconds())
		send(&proto.StreamRangeResponse{RangeResponse: &proto.RangeResponse{Header: responseHeader(revision)}})
	}()
	return stream, true, nil
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
			return errors.Join(err, writer.Close())
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
	file     *os.File
	buffer   *bufio.Writer
	checksum hash.Hash
}

func createKeyRun(path string) (*keyRunWriter, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	return &keyRunWriter{file: file, buffer: bufio.NewWriterSize(file, 64<<10), checksum: sha256.New()}, nil
}

func (w *keyRunWriter) Write(key []byte) error {
	var size [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(size[:], uint64(len(key)))
	if _, err := w.buffer.Write(size[:n]); err != nil {
		return err
	}
	_, _ = w.checksum.Write(size[:n])
	if _, err := w.buffer.Write(key); err != nil {
		return err
	}
	_, _ = w.checksum.Write(key)
	return nil
}

func (w *keyRunWriter) Close() error {
	flushErr := w.buffer.Flush()
	var checksumErr error
	if flushErr == nil {
		checksum := w.checksum.Sum(nil)
		if n, err := w.file.Write(checksum); err != nil {
			checksumErr = err
		} else if n != len(checksum) {
			checksumErr = io.ErrShortWrite
		}
	}
	closeErr := w.file.Close()
	return errors.Join(flushErr, checksumErr, closeErr)
}

type keyRunReader struct {
	file      *os.File
	buffer    *bufio.Reader
	remaining uint64
	previous  []byte
	checksum  hash.Hash
	wantHash  []byte
	verified  bool
}

func openKeyRun(path string) (*keyRunReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if info.Size() < sha256.Size {
		return nil, errors.Join(errors.New("range ordering spill is too short for checksum"), file.Close())
	}
	dataBytes := info.Size() - sha256.Size
	wantHash := make([]byte, sha256.Size)
	if _, err := file.ReadAt(wantHash, dataBytes); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	checksum := sha256.New()
	data := io.NewSectionReader(file, 0, dataBytes)
	return &keyRunReader{
		file: file, buffer: bufio.NewReaderSize(io.TeeReader(data, checksum), 64<<10),
		remaining: uint64(dataBytes), checksum: checksum, wantHash: wantHash,
	}, nil
}

func (r *keyRunReader) Next() ([]byte, error) {
	if r.remaining == 0 {
		if !r.verified {
			if !bytes.Equal(r.checksum.Sum(nil), r.wantHash) {
				return nil, errors.New("range ordering spill checksum mismatch")
			}
			r.verified = true
		}
		return nil, io.EOF
	}
	counted := countingByteReader{reader: r.buffer}
	size, err := binary.ReadUvarint(&counted)
	if err != nil {
		return nil, err
	}
	if counted.read > r.remaining {
		return nil, errors.New("range ordering spill length prefix exceeds remaining run bytes")
	}
	r.remaining -= counted.read
	if size > r.remaining {
		return nil, fmt.Errorf("range ordering spill key length %d exceeds remaining run bytes %d", size, r.remaining)
	}
	if size > uint64(maxInt()) {
		return nil, fmt.Errorf("range ordering spill key length %d overflows int", size)
	}
	if size == 0 {
		return nil, errors.New("range ordering spill contains an empty key")
	}
	key := make([]byte, int(size))
	if _, err = io.ReadFull(r.buffer, key); err != nil {
		return nil, err
	}
	r.remaining -= size
	if r.previous != nil && bytes.Compare(r.previous, key) >= 0 {
		return nil, errors.New("range ordering spill keys are not strictly increasing")
	}
	r.previous = append(r.previous[:0], key...)
	return key, nil
}

type countingByteReader struct {
	reader io.ByteReader
	read   uint64
}

func (r *countingByteReader) ReadByte() (byte, error) {
	value, err := r.reader.ReadByte()
	if err == nil {
		r.read++
	}
	return value, err
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
			retErr = errors.Join(retErr, reader.Close())
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
	defer func() { retErr = errors.Join(retErr, writer.Close()) }()
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
