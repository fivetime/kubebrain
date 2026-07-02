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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAuthServiceIsExplicitlyUnsupported(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.AuthEnable(context.Background(), &etcdserverpb.AuthEnableRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	_, err = server.Authenticate(context.Background(), &etcdserverpb.AuthenticateRequest{
		Name:     "root",
		Password: "secret",
	})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
