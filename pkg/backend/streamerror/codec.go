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

// Package streamerror carries typed errors across the legacy
// StreamRangeResponse.err string without guessing from diagnostic text.
package streamerror

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
)

const envelopePrefix = "kubebrain.stream.error/v1:"

const (
	kindInvalidMVCC = "invalid_mvcc_metadata"
	kindGRPC        = "grpc_status"
)

type envelope struct {
	Kind    string `json:"kind"`
	Code    int32  `json:"code,omitempty"`
	Message string `json:"message"`
}

// Encode serializes recognized typed errors and leaves untyped errors exactly
// as they were for compatibility with older consumers.
func Encode(err error) string {
	if err == nil {
		return ""
	}
	value := envelope{Message: err.Error()}
	switch {
	case errors.Is(err, coder.ErrInvalidMVCCMetadata):
		value.Kind = kindInvalidMVCC
	default:
		grpcStatus, ok := status.FromError(err)
		if !ok || grpcStatus.Code() == codes.Unknown || grpcStatus.Code() == codes.OK {
			return err.Error()
		}
		value.Kind = kindGRPC
		value.Code = int32(grpcStatus.Code())
		value.Message = grpcStatus.Message()
	}
	payload, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return err.Error()
	}
	return envelopePrefix + base64.RawURLEncoding.EncodeToString(payload)
}

// Decode reconstructs a typed error only for a valid supported envelope.
// Unknown versions, kinds, and malformed payloads remain opaque to preserve
// forward compatibility and prevent user-controlled diagnostics selecting a
// status merely by containing familiar words.
func Decode(raw string) (error, bool) {
	if !strings.HasPrefix(raw, envelopePrefix) {
		return nil, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(raw, envelopePrefix))
	if err != nil {
		return nil, false
	}
	var value envelope
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, false
	}
	switch value.Kind {
	case kindInvalidMVCC:
		if value.Message == "" || value.Code != 0 {
			return nil, false
		}
		return coder.MarkInvalidMVCCMetadata(errors.New(value.Message)), true
	case kindGRPC:
		code := codes.Code(value.Code)
		if code == codes.OK || code == codes.Unknown || value.Message == "" {
			return nil, false
		}
		return status.Error(code, value.Message), true
	default:
		return nil, false
	}
}

// DecodeOrPlain returns a reconstructed typed error for a supported envelope,
// otherwise an ordinary error containing the legacy diagnostic string.
func DecodeOrPlain(raw string) error {
	if decoded, ok := Decode(raw); ok {
		return decoded
	}
	return errors.New(raw)
}
