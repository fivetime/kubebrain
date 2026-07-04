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

package etcd

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

// TestErrClass pins the bounded, low-cardinality error buckets used to label
// read/write metrics, so dashboards can separate benign client-retriable
// failures (revision/unavailable/fenced) from real trouble (deadline/other).
func TestErrClass(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{nil, "none"},
		{context.Canceled, "canceled"},
		{context.DeadlineExceeded, "deadline"},
		{backend.ErrLeadershipFenced, "fenced"},
		{fmt.Errorf("wrap: %w", backend.ErrLeadershipFenced), "fenced"},
		{status.Error(codes.OutOfRange, "compacted"), "revision"},
		{status.Error(codes.Unavailable, "no leader"), "unavailable"},
		{status.Error(codes.DeadlineExceeded, "timeout"), "deadline"},
		{status.Error(codes.NotFound, "absent"), "not_found"},
		{status.Error(codes.InvalidArgument, "bad"), "invalid"},
		{errors.New("something unexpected"), "other"},
	}
	// bound the label set
	seen := map[string]struct{}{}
	for _, c := range cases {
		got := errClass(c.err)
		require.Equal(t, c.want, got, "errClass(%v)", c.err)
		seen[got] = struct{}{}
	}
	require.LessOrEqual(t, len(seen), 9, "errclass must stay low-cardinality (bounded set)")
}
