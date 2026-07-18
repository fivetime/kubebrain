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
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReadBarrierFailuresAreRetryableAcrossRPCs(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	barrierErr := errors.New("leader revision transport failed")
	server.peers = testPeerService{
		isLeader:   true,
		syncReadFn: func(context.Context) error { return barrierErr },
	}
	ctx := context.Background()
	tests := []struct {
		name string
		call func() error
	}{
		{"range", func() error {
			_, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("key")})
			return err
		}},
		{"compact", func() error {
			_, err := server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: 1})
			return err
		}},
		{"delete range", func() error {
			_, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
				Key: []byte("a"), RangeEnd: []byte("z"),
			})
			return err
		}},
		{"status", func() error {
			_, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
			return err
		}},
		{"alarm get", func() error {
			_, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
				Action: etcdserverpb.AlarmRequest_GET,
			})
			return err
		}},
		{"hash", func() error {
			_, err := server.Hash(ctx, &etcdserverpb.HashRequest{})
			return err
		}},
		{"hash kv", func() error {
			_, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Equal(t, barrierErr.Error(), status.Convert(err).Message())
		})
	}
}

func TestReadBarrierStatusPreservation(t *testing.T) {
	for _, wantErr := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		status.Error(codes.ResourceExhausted, "barrier overloaded"),
	} {
		got := readBarrierStatusErr(wantErr)
		require.Equal(t, status.Code(wantErr), status.Code(got))
		require.Equal(t, status.Convert(wantErr).Message(), status.Convert(got).Message())
	}
}
