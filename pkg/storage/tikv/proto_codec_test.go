// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

func scanResponseFixture(pairCount int) *kvrpcpb.ScanResponse {
	pairs := make([]*kvrpcpb.KvPair, pairCount)
	for i := range pairs {
		pairs[i] = &kvrpcpb.KvPair{
			Key:   []byte{byte(i >> 8), byte(i), 'k'},
			Value: bytes.Repeat([]byte{byte(i), 'v'}, 32),
		}
	}
	return &kvrpcpb.ScanResponse{Pairs: pairs}
}

func TestTiKVProtoCodecOwnsSuccessfulScanRows(t *testing.T) {
	original := scanResponseFixture(4)
	wire, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &kvrpcpb.ScanResponse{}
	require.NoError(t, newTiKVProtoCodec().Unmarshal(
		mem.BufferSlice{mem.SliceBuffer(wire)}, decoded,
	))
	require.Equal(t, original, decoded)

	wantKey := append([]byte(nil), decoded.Pairs[0].Key...)
	wantValue := append([]byte(nil), decoded.Pairs[0].Value...)
	for i := range wire {
		wire[i] = 0
	}
	require.Equal(t, wantKey, decoded.Pairs[0].Key,
		"decoded rows must not alias gRPC's releasable input buffer")
	require.Equal(t, wantValue, decoded.Pairs[0].Value)
}

func TestTiKVProtoCodecDecodesScanOnlyBatch(t *testing.T) {
	original := &tikvpb.BatchCommandsResponse{
		Responses: []*tikvpb.BatchCommandsResponse_Response{
			{Cmd: &tikvpb.BatchCommandsResponse_Response_Scan{Scan: scanResponseFixture(3)}},
			{Cmd: &tikvpb.BatchCommandsResponse_Response_Scan{Scan: scanResponseFixture(2)}},
		},
		RequestIds:         []uint64{17, 23},
		TransportLayerLoad: 280,
	}
	wire, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &tikvpb.BatchCommandsResponse{}
	require.NoError(t, newTiKVProtoCodec().Unmarshal(
		mem.BufferSlice{mem.SliceBuffer(wire)}, decoded,
	))
	require.Equal(t, original, decoded)
}

func TestTiKVProtoCodecFallsBackForMixedBatch(t *testing.T) {
	original := &tikvpb.BatchCommandsResponse{
		Responses: []*tikvpb.BatchCommandsResponse_Response{
			{Cmd: &tikvpb.BatchCommandsResponse_Response_Scan{Scan: scanResponseFixture(1)}},
			{Cmd: &tikvpb.BatchCommandsResponse_Response_Get{Get: &kvrpcpb.GetResponse{Value: []byte("value")}}},
		},
		RequestIds: []uint64{1, 2},
	}
	wire, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &tikvpb.BatchCommandsResponse{}
	require.NoError(t, newTiKVProtoCodec().Unmarshal(
		mem.BufferSlice{mem.SliceBuffer(wire)}, decoded,
	))
	require.Equal(t, original, decoded)
}

func TestTiKVProtoCodecFallsBackForErrorAndFragmentedResponses(t *testing.T) {
	tests := []proto.Message{
		&kvrpcpb.ScanResponse{RegionError: &errorpb.Error{Message: "not leader"}},
		&kvrpcpb.ScanResponse{Error: &kvrpcpb.KeyError{Abort: "aborted"}},
	}
	for _, original := range tests {
		wire, err := proto.Marshal(original)
		require.NoError(t, err)
		decoded := &kvrpcpb.ScanResponse{}
		require.NoError(t, newTiKVProtoCodec().Unmarshal(
			mem.BufferSlice{mem.SliceBuffer(wire)}, decoded,
		))
		require.True(t, proto.Equal(original, decoded))
	}

	original := scanResponseFixture(3)
	wire, err := proto.Marshal(original)
	require.NoError(t, err)
	split := len(wire) / 2
	decoded := &kvrpcpb.ScanResponse{}
	require.NoError(t, newTiKVProtoCodec().Unmarshal(mem.BufferSlice{
		mem.SliceBuffer(wire[:split]), mem.SliceBuffer(wire[split:]),
	}, decoded))
	require.Equal(t, original, decoded)
	want := append([]byte(nil), decoded.Pairs[0].Value...)
	for i := range wire {
		wire[i] = 0
	}
	require.Equal(t, want, decoded.Pairs[0].Value,
		"fragmented successful scans must use an owned contiguous frame")
}

func TestTiKVProtoCodecRejectsMalformedScan(t *testing.T) {
	malformed := []byte{0x12, 0x05, 0x01}
	err := newTiKVProtoCodec().Unmarshal(
		mem.BufferSlice{mem.SliceBuffer(malformed)}, &kvrpcpb.ScanResponse{},
	)
	require.Error(t, err)
}

func TestTiKVProtoCodecResetsReusedScanMessages(t *testing.T) {
	tests := []*kvrpcpb.ScanResponse{{}, scanResponseFixture(2)}
	for _, original := range tests {
		wire, err := proto.Marshal(original)
		require.NoError(t, err)
		decoded := &kvrpcpb.ScanResponse{
			RegionError: &errorpb.Error{Message: "stale"},
			Error:       &kvrpcpb.KeyError{Abort: "stale"},
			Pairs:       scanResponseFixture(1).Pairs,
		}
		require.NoError(t, newTiKVProtoCodec().Unmarshal(
			mem.BufferSlice{mem.SliceBuffer(wire)}, decoded,
		))
		require.True(t, proto.Equal(original, decoded))
	}

	originalBatch := &tikvpb.BatchCommandsResponse{
		Responses: []*tikvpb.BatchCommandsResponse_Response{
			{Cmd: &tikvpb.BatchCommandsResponse_Response_Scan{Scan: scanResponseFixture(2)}},
		},
		RequestIds: []uint64{42}, TransportLayerLoad: 180,
	}
	wire, err := proto.Marshal(originalBatch)
	require.NoError(t, err)
	decodedBatch := &tikvpb.BatchCommandsResponse{
		Responses:          originalBatch.Responses,
		RequestIds:         []uint64{1, 2, 3},
		TransportLayerLoad: 999,
		XXX_unrecognized:   []byte{0xa0, 0x06, 0x01},
	}
	require.NoError(t, newTiKVProtoCodec().Unmarshal(
		mem.BufferSlice{mem.SliceBuffer(wire)}, decodedBatch,
	))
	require.True(t, proto.Equal(originalBatch, decodedBatch))
}

func TestTiKVProtoCodecReducesScanAllocations(t *testing.T) {
	wire, err := proto.Marshal(scanResponseFixture(2048))
	require.NoError(t, err)
	data := mem.BufferSlice{mem.SliceBuffer(wire)}
	baselineCodec := encoding.GetCodecV2("proto")
	require.NotNil(t, baselineCodec)
	optimizedCodec := newTiKVProtoCodec()

	baseline := testing.AllocsPerRun(10, func() {
		require.NoError(t, baselineCodec.Unmarshal(data, &kvrpcpb.ScanResponse{}))
	})
	optimized := testing.AllocsPerRun(10, func() {
		require.NoError(t, optimizedCodec.Unmarshal(data, &kvrpcpb.ScanResponse{}))
	})
	require.Less(t, optimized, baseline/4,
		"successful Scan decoding must eliminate per-row key/value allocations")
}

func TestTiKVProtoCodecReducesScanBatchAllocations(t *testing.T) {
	responses := make([]*tikvpb.BatchCommandsResponse_Response, 8)
	requestIDs := make([]uint64, len(responses))
	for i := range responses {
		responses[i] = &tikvpb.BatchCommandsResponse_Response{
			Cmd: &tikvpb.BatchCommandsResponse_Response_Scan{Scan: scanResponseFixture(256)},
		}
		requestIDs[i] = uint64(i + 1)
	}
	wire, err := proto.Marshal(&tikvpb.BatchCommandsResponse{
		Responses: responses, RequestIds: requestIDs,
	})
	require.NoError(t, err)
	data := mem.BufferSlice{mem.SliceBuffer(wire)}
	baselineCodec := encoding.GetCodecV2("proto")
	optimizedCodec := newTiKVProtoCodec()

	baseline := testing.AllocsPerRun(10, func() {
		require.NoError(t, baselineCodec.Unmarshal(data, &tikvpb.BatchCommandsResponse{}))
	})
	optimized := testing.AllocsPerRun(10, func() {
		require.NoError(t, optimizedCodec.Unmarshal(data, &tikvpb.BatchCommandsResponse{}))
	})
	require.Less(t, optimized, baseline/4)
}

func BenchmarkTiKVProtoCodecScanResponse(b *testing.B) {
	wire, err := proto.Marshal(scanResponseFixture(2048))
	require.NoError(b, err)
	data := mem.BufferSlice{mem.SliceBuffer(wire)}
	baselineCodec := encoding.GetCodecV2("proto")
	codec := newTiKVProtoCodec()

	b.Run("generated", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := baselineCodec.Unmarshal(data, &kvrpcpb.ScanResponse{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("owned-frame", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := codec.Unmarshal(data, &kvrpcpb.ScanResponse{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func FuzzTiKVProtoCodecMatchesStandardScan(f *testing.F) {
	for _, fixture := range []*kvrpcpb.ScanResponse{
		{},
		scanResponseFixture(3),
		{RegionError: &errorpb.Error{Message: "not leader"}},
		{Error: &kvrpcpb.KeyError{Abort: "aborted"}},
	} {
		wire, err := proto.Marshal(fixture)
		require.NoError(f, err)
		f.Add(wire)
	}
	f.Add([]byte{0x12, 0x05, 0x01})

	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > 1<<20 {
			t.Skip()
		}
		standard := &kvrpcpb.ScanResponse{
			RegionError: &errorpb.Error{Message: "stale"},
			Pairs:       scanResponseFixture(1).Pairs,
		}
		optimized := proto.Clone(standard).(*kvrpcpb.ScanResponse)
		standardErr := encoding.GetCodecV2("proto").Unmarshal(
			mem.BufferSlice{mem.SliceBuffer(wire)}, standard,
		)
		optimizedErr := newTiKVProtoCodec().Unmarshal(
			mem.BufferSlice{mem.SliceBuffer(wire)}, optimized,
		)
		require.Equal(t, standardErr == nil, optimizedErr == nil)
		if standardErr == nil {
			require.True(t, proto.Equal(standard, optimized))
		}
	})
}

func FuzzTiKVProtoCodecMatchesStandardBatch(f *testing.F) {
	fixtures := []*tikvpb.BatchCommandsResponse{
		{},
		{
			Responses: []*tikvpb.BatchCommandsResponse_Response{
				{Cmd: &tikvpb.BatchCommandsResponse_Response_Scan{Scan: scanResponseFixture(2)}},
			},
			RequestIds: []uint64{42}, TransportLayerLoad: 180,
		},
		{
			Responses: []*tikvpb.BatchCommandsResponse_Response{
				{Cmd: &tikvpb.BatchCommandsResponse_Response_Get{Get: &kvrpcpb.GetResponse{Value: []byte("v")}}},
			},
			RequestIds: []uint64{7},
		},
	}
	for _, fixture := range fixtures {
		wire, err := proto.Marshal(fixture)
		require.NoError(f, err)
		f.Add(wire)
	}
	f.Add([]byte{0x0a, 0x04, 0x12, 0x05, 0x01})

	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > 1<<20 {
			t.Skip()
		}
		standard := &tikvpb.BatchCommandsResponse{
			RequestIds:         []uint64{1, 2, 3},
			TransportLayerLoad: 999,
			XXX_unrecognized:   []byte{0xa0, 0x06, 0x01},
		}
		optimized := proto.Clone(standard).(*tikvpb.BatchCommandsResponse)
		standardErr := encoding.GetCodecV2("proto").Unmarshal(
			mem.BufferSlice{mem.SliceBuffer(wire)}, standard,
		)
		optimizedErr := newTiKVProtoCodec().Unmarshal(
			mem.BufferSlice{mem.SliceBuffer(wire)}, optimized,
		)
		require.Equal(t, standardErr == nil, optimizedErr == nil)
		if standardErr == nil {
			require.True(t, proto.Equal(standard, optimized))
		}
	})
}
