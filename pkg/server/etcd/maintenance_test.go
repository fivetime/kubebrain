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
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestMaintenanceBasicDiagnostics(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/maintenance/key"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)

	statusResp, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, maintenanceVersion, statusResp.Version)
	require.NotNil(t, statusResp.Header)

	hashResp, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{})
	require.NoError(t, err)
	require.NotZero(t, hashResp.Hash)

	alarmResp, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{})
	require.NoError(t, err)
	require.Empty(t, alarmResp.Alarms)

	_, err = server.Defragment(ctx, &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err)
}
