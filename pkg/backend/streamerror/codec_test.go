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

package streamerror

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
)

func TestCodecRoundTripsTypedErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		is   error
		code codes.Code
	}{
		{
			name: "MVCC metadata",
			err:  coder.MarkInvalidMVCCMetadata(errors.New("decode object key 00ff")),
			is:   coder.ErrInvalidMVCCMetadata,
			code: codes.Unknown,
		},
		{
			name: "gRPC status",
			err:  status.Error(codes.Unavailable, "history backend unavailable"),
			code: codes.Unavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded := Encode(test.err)
			require.NotEqual(t, test.err.Error(), encoded)
			decoded, ok := Decode(encoded)
			require.True(t, ok)
			require.Equal(t, test.err.Error(), decoded.Error())
			if test.is != nil {
				require.ErrorIs(t, decoded, test.is)
			}
			require.Equal(t, test.code, status.Code(decoded))
		})
	}
}

func TestCodecLeavesUntypedAndMalformedPayloadsOpaque(t *testing.T) {
	plain := errors.New("ordinary backend error")
	require.Equal(t, plain.Error(), Encode(plain))
	_, ok := Decode(plain.Error())
	require.False(t, ok)
	_, ok = Decode(fmt.Sprintf("%s%s", envelopePrefix, "not-base64"))
	require.False(t, ok)
}
