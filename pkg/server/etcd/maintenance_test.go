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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestStatusVersionEnablesRequestWatchProgress guards the exact gate the
// kube-apiserver applies: Maintenance.Status.Version must be semver-parseable
// and satisfy >= 3.5.13 (or [3.4.31, 3.5.0)) or RequestWatchProgress —
// consistent-list-from-cache / WatchList — stays disabled.
// See k8s.io/apiserver/pkg/storage/feature/feature_support_checker.go.
func TestStatusVersionEnablesRequestWatchProgress(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	resp, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.NoError(t, err)

	var maj, min, patch int
	n, err := fmt.Sscanf(resp.Version, "%d.%d.%d", &maj, &min, &patch)
	require.NoError(t, err, "Status.Version %q must be semver-parseable", resp.Version)
	require.Equal(t, 3, n, "Status.Version %q must be major.minor.patch", resp.Version)

	v := maj*1_000_000 + min*1_000 + patch
	ge3_4_31 := v >= 3*1_000_000+4*1_000+31
	lt3_5_0 := v < 3*1_000_000+5*1_000+0
	ge3_5_13 := v >= 3*1_000_000+5*1_000+13
	supported := ge3_5_13 || (ge3_4_31 && lt3_5_0)
	require.True(t, supported,
		"Status.Version %q does not satisfy the apiserver RequestWatchProgress gate (>=3.5.13 or [3.4.31,3.5.0))", resp.Version)
}

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
