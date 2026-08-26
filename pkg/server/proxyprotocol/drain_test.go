// Copyright 2026 The KubeBrain Authors.
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

package proxyprotocol

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsPeerDrainedBeforeAdmissionRequiresExactInternalSignal(t *testing.T) {
	require.True(t, IsPeerDrainedBeforeAdmission(ErrPeerDrainedBeforeAdmission))
	require.True(t, IsPeerDrainedBeforeAdmission(fmt.Errorf("forward: %w", ErrPeerDrainedBeforeAdmission)))
	require.False(t, IsPeerDrainedBeforeAdmission(status.Error(codes.Aborted, "another aborted request")))
	require.False(t, IsPeerDrainedBeforeAdmission(rpctypes.ErrGRPCLeaderChanged))
	require.False(t, IsPeerDrainedBeforeAdmission(status.Error(codes.Unavailable, peerDrainedBeforeAdmissionMessage)))
	require.False(t, IsPeerDrainedBeforeAdmission(nil))
}

func TestIsPeerStreamDrainedRequiresExactInternalSignal(t *testing.T) {
	require.True(t, IsPeerStreamDrained(ErrPeerStreamDrained))
	require.True(t, IsPeerStreamDrained(fmt.Errorf("forward: %w", ErrPeerStreamDrained)))
	require.False(t, IsPeerStreamDrained(ErrPeerDrainedBeforeAdmission))
	require.False(t, IsPeerStreamDrained(status.Error(codes.Aborted, "another aborted stream")))
	require.False(t, IsPeerStreamDrained(status.Error(codes.Unavailable, peerStreamDrainedMessage)))
	require.False(t, IsPeerStreamDrained(nil))
}

func TestClientDrainedBeforeAdmissionUsesMutableRetrySentinel(t *testing.T) {
	require.Equal(t, codes.Unavailable, status.Code(ErrClientDrainedBeforeAdmission))
	require.Equal(t, clientDrainedBeforeAdmissionMessage, status.Convert(ErrClientDrainedBeforeAdmission).Message())
}

func TestCountIndexNotReadyRequiresExactSignal(t *testing.T) {
	require.True(t, IsCountIndexNotReady(ErrCountIndexNotReady))
	require.True(t, IsCountIndexNotReady(status.Error(codes.Unavailable, countIndexNotReadyMessage)))
	require.False(t, IsCountIndexNotReady(status.Error(codes.Unavailable, "leader down")))
	require.False(t, IsCountIndexNotReady(nil))
}
