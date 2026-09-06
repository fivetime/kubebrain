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
	"fmt"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

// tikvProtoCodec retains gRPC's standard protobuf codec except for successful
// transactional Scan responses. The pinned kvproto generator allocates one
// KvPair plus separate key and value buffers for every row. The fast path owns
// one copy of the complete gRPC frame and points immutable KvPair fields into
// it. Error-bearing, mixed-command, malformed, or unknown wire shapes use the
// standard decoder after the BufferSlice has been materialized once.
type tikvProtoCodec struct {
	fallback encoding.CodecV2
}

var defaultTiKVProtoCodec = newTiKVProtoCodec()

func newTiKVProtoCodec() *tikvProtoCodec {
	fallback := encoding.GetCodecV2("proto")
	if fallback == nil {
		panic("gRPC protobuf CodecV2 is not registered")
	}
	return &tikvProtoCodec{fallback: fallback}
}

func (*tikvProtoCodec) Name() string { return "proto" }

func (c *tikvProtoCodec) Marshal(value interface{}) (mem.BufferSlice, error) {
	return c.fallback.Marshal(value)
}

func (c *tikvProtoCodec) Unmarshal(data mem.BufferSlice, value interface{}) error {
	contiguous := data.MaterializeToBuffer(mem.DefaultBufferPool())
	defer contiguous.Free()
	raw := contiguous.ReadOnlyData()
	switch message := value.(type) {
	case *kvrpcpb.ScanResponse:
		pairCount, ok := successfulScanShape(raw)
		if ok {
			return decodeOwnedScanResponse(raw, pairCount, message)
		}
	case *tikvpb.BatchCommandsResponse:
		responseCount, pairCount, requestIDCount, ok := scanOnlyBatchShape(raw)
		if ok {
			return decodeOwnedScanBatch(raw, responseCount, pairCount, requestIDCount, message)
		}
	}
	// The standard codec also materializes a fragmented BufferSlice. Pass the
	// already-contiguous, ref-counted buffer so an unsupported shape pays no
	// second full-message copy.
	return c.fallback.Unmarshal(mem.BufferSlice{contiguous}, value)
}

func successfulScanShape(data []byte) (int, bool) {
	pairCount := 0
	for len(data) > 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(data)
		if tagBytes < 0 || number != 2 || wireType != protowire.BytesType {
			return 0, false
		}
		pairData, fieldBytes := protowire.ConsumeBytes(data[tagBytes:])
		if fieldBytes < 0 || !successfulPairShape(pairData) {
			return 0, false
		}
		pairCount++
		data = data[tagBytes+fieldBytes:]
	}
	return pairCount, true
}

func successfulPairShape(data []byte) bool {
	for len(data) > 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(data)
		if tagBytes < 0 || (number != 2 && number != 3) || wireType != protowire.BytesType {
			return false
		}
		_, fieldBytes := protowire.ConsumeBytes(data[tagBytes:])
		if fieldBytes < 0 {
			return false
		}
		data = data[tagBytes+fieldBytes:]
	}
	return true
}

func scanOnlyBatchShape(data []byte) (responseCount, pairCount, requestIDCount int, ok bool) {
	for len(data) > 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(data)
		if tagBytes < 0 {
			return 0, 0, 0, false
		}
		data = data[tagBytes:]
		switch number {
		case 1:
			if wireType != protowire.BytesType {
				return 0, 0, 0, false
			}
			responseData, fieldBytes := protowire.ConsumeBytes(data)
			if fieldBytes < 0 {
				return 0, 0, 0, false
			}
			count, valid := scanOnlyBatchResponseShape(responseData)
			if !valid {
				return 0, 0, 0, false
			}
			responseCount++
			pairCount += count
			data = data[fieldBytes:]
		case 2:
			switch wireType {
			case protowire.VarintType:
				_, fieldBytes := protowire.ConsumeVarint(data)
				if fieldBytes < 0 {
					return 0, 0, 0, false
				}
				requestIDCount++
				data = data[fieldBytes:]
			case protowire.BytesType:
				packed, fieldBytes := protowire.ConsumeBytes(data)
				if fieldBytes < 0 {
					return 0, 0, 0, false
				}
				count, valid := packedVarintCount(packed)
				if !valid {
					return 0, 0, 0, false
				}
				requestIDCount += count
				data = data[fieldBytes:]
			default:
				return 0, 0, 0, false
			}
		case 3:
			if wireType != protowire.VarintType {
				return 0, 0, 0, false
			}
			_, fieldBytes := protowire.ConsumeVarint(data)
			if fieldBytes < 0 {
				return 0, 0, 0, false
			}
			data = data[fieldBytes:]
		default:
			return 0, 0, 0, false
		}
	}
	return responseCount, pairCount, requestIDCount, responseCount > 0
}

func scanOnlyBatchResponseShape(data []byte) (int, bool) {
	number, wireType, tagBytes := protowire.ConsumeTag(data)
	if tagBytes < 0 || number != 2 || wireType != protowire.BytesType {
		return 0, false
	}
	scanData, fieldBytes := protowire.ConsumeBytes(data[tagBytes:])
	if fieldBytes < 0 || tagBytes+fieldBytes != len(data) {
		return 0, false
	}
	return successfulScanShape(scanData)
}

func packedVarintCount(data []byte) (int, bool) {
	count := 0
	for len(data) > 0 {
		_, fieldBytes := protowire.ConsumeVarint(data)
		if fieldBytes < 0 {
			return 0, false
		}
		count++
		data = data[fieldBytes:]
	}
	return count, true
}

func decodeOwnedScanResponse(data []byte, pairCount int, output *kvrpcpb.ScanResponse) error {
	output.Reset()
	if pairCount == 0 {
		return nil
	}
	owned := append([]byte(nil), data...)
	pairObjects := make([]kvrpcpb.KvPair, pairCount)
	pairPointers := make([]*kvrpcpb.KvPair, pairCount)
	pairIndex := 0
	for len(owned) > 0 {
		_, _, tagBytes := protowire.ConsumeTag(owned)
		pairData, fieldBytes := protowire.ConsumeBytes(owned[tagBytes:])
		decodeSuccessfulPair(pairData, &pairObjects[pairIndex])
		pairPointers[pairIndex] = &pairObjects[pairIndex]
		pairIndex++
		owned = owned[tagBytes+fieldBytes:]
	}
	output.Pairs = pairPointers
	return nil
}

func decodeSuccessfulPair(data []byte, output *kvrpcpb.KvPair) {
	for len(data) > 0 {
		number, _, tagBytes := protowire.ConsumeTag(data)
		field, fieldBytes := protowire.ConsumeBytes(data[tagBytes:])
		if number == 2 {
			output.Key = field
		} else {
			output.Value = field
		}
		data = data[tagBytes+fieldBytes:]
	}
}

func decodeOwnedScanBatch(
	data []byte,
	responseCount, pairCount, requestIDCount int,
	output *tikvpb.BatchCommandsResponse,
) error {
	output.Reset()
	owned := append([]byte(nil), data...)
	responseObjects := make([]tikvpb.BatchCommandsResponse_Response, responseCount)
	responsePointers := make([]*tikvpb.BatchCommandsResponse_Response, responseCount)
	scanObjects := make([]kvrpcpb.ScanResponse, responseCount)
	scanCommands := make([]tikvpb.BatchCommandsResponse_Response_Scan, responseCount)
	pairObjects := make([]kvrpcpb.KvPair, pairCount)
	pairPointers := make([]*kvrpcpb.KvPair, pairCount)
	requestIDs := make([]uint64, requestIDCount)
	responseIndex, pairIndex, requestIDIndex := 0, 0, 0

	for len(owned) > 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(owned)
		fieldData := owned[tagBytes:]
		switch number {
		case 1:
			responseData, fieldBytes := protowire.ConsumeBytes(fieldData)
			_, _, responseTagBytes := protowire.ConsumeTag(responseData)
			scanData, _ := protowire.ConsumeBytes(responseData[responseTagBytes:])
			thisPairCount, _ := successfulScanShape(scanData)
			for len(scanData) > 0 {
				_, _, pairTagBytes := protowire.ConsumeTag(scanData)
				pairData, pairFieldBytes := protowire.ConsumeBytes(scanData[pairTagBytes:])
				decodeSuccessfulPair(pairData, &pairObjects[pairIndex])
				pairPointers[pairIndex] = &pairObjects[pairIndex]
				pairIndex++
				scanData = scanData[pairTagBytes+pairFieldBytes:]
			}
			scanObjects[responseIndex].Pairs = pairPointers[pairIndex-thisPairCount : pairIndex]
			scanCommands[responseIndex].Scan = &scanObjects[responseIndex]
			responseObjects[responseIndex].Cmd = &scanCommands[responseIndex]
			responsePointers[responseIndex] = &responseObjects[responseIndex]
			responseIndex++
			owned = fieldData[fieldBytes:]
		case 2:
			if wireType == protowire.VarintType {
				requestIDs[requestIDIndex], tagBytes = protowire.ConsumeVarint(fieldData)
				requestIDIndex++
				owned = fieldData[tagBytes:]
			} else {
				packed, fieldBytes := protowire.ConsumeBytes(fieldData)
				for len(packed) > 0 {
					requestIDs[requestIDIndex], tagBytes = protowire.ConsumeVarint(packed)
					requestIDIndex++
					packed = packed[tagBytes:]
				}
				owned = fieldData[fieldBytes:]
			}
		case 3:
			output.TransportLayerLoad, tagBytes = protowire.ConsumeVarint(fieldData)
			owned = fieldData[tagBytes:]
		default:
			return fmt.Errorf("validated TiKV scan batch contains unexpected field %d", number)
		}
	}
	output.Responses = responsePointers
	output.RequestIds = requestIDs
	return nil
}
