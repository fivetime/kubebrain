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

package endpoint

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type closeErrorServer struct {
	err error
}

func (s closeErrorServer) name() string             { return "close-error" }
func (s closeErrorServer) matcher() cmux.Matcher    { return cmux.Any() }
func (s closeErrorServer) serve(net.Listener) error { return nil }
func (s closeErrorServer) close() error             { return s.err }

func TestNormalizeServeError(t *testing.T) {
	for _, err := range []error{
		nil,
		http.ErrServerClosed,
		grpc.ErrServerStopped,
		net.ErrClosed,
		cmux.ErrListenerClosed,
		cmux.ErrServerClosed,
		fmt.Errorf("wrapped: %w", cmux.ErrListenerClosed),
	} {
		require.NoError(t, normalizeServeError(err))
	}

	unexpected := errors.New("accept failed")
	require.ErrorIs(t, normalizeServeError(unexpected), unexpected)
}

func TestSecureServerCloseNormalizesAndReturnsErrors(t *testing.T) {
	unexpected := errors.New("close failed")
	server := &secureServer{internalServers: []exposedServer{
		closeErrorServer{err: net.ErrClosed},
		closeErrorServer{err: cmux.ErrServerClosed},
		closeErrorServer{err: unexpected},
	}}

	require.ErrorIs(t, server.close(), unexpected)
}

func TestNewHttpServerBoundsHeaderAdmission(t *testing.T) {
	exposed := newHttpServer(http.NotFoundHandler())
	server, ok := exposed.(*httpServer)
	require.True(t, ok)

	require.Equal(t, httpReadHeaderTimeout, server.svr.ReadHeaderTimeout)
	require.Equal(t, httpIdleTimeout, server.svr.IdleTimeout)
	require.Equal(t, httpMaxHeaderBytes, server.svr.MaxHeaderBytes)
	require.Zero(t, server.svr.ReadTimeout)
	require.Zero(t, server.svr.WriteTimeout)
}
