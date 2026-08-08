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

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/server/etcd"
)

// The /version endpoint must return etcd's {"etcdserver","etcdcluster","storage"}
// shape so kubeadm's ExternalEtcdVersion preflight parses it (a 404 body "404
// page not found" is mis-read as the JSON number 404). The handler uses no
// server state, so a zero-value server exercises it.
func TestVersionHandlerReturnsEtcdShape(t *testing.T) {
	rec := httptest.NewRecorder()
	(&server{}).versionHandler(rec, httptest.NewRequest(http.MethodGet, "/version", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.Equal(t, fmt.Sprintf(
		`{"etcdserver":%q,"etcdcluster":%q,"storage":%q}`,
		etcd.Version, etcd.ClusterVersion, etcd.ClusterVersion,
	), rec.Body.String())

	var body struct {
		EtcdServer  string `json:"etcdserver"`
		EtcdCluster string `json:"etcdcluster"`
		Storage     string `json:"storage"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, etcd.Version, body.EtcdServer)
	require.Equal(t, etcd.ClusterVersion, body.EtcdCluster)
	require.Equal(t, etcd.ClusterVersion, body.Storage)

	// Must be semver-parseable and satisfy the apiserver RequestWatchProgress
	// floor (>= 3.5.13), the same guarantee maintenance_test enforces on the gRPC
	// path — the two now share one constant.
	var maj, min, patch int
	n, err := fmt.Sscanf(etcd.Version, "%d.%d.%d", &maj, &min, &patch)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	v := maj*1_000_000 + min*1_000 + patch
	require.GreaterOrEqual(t, v, 3*1_000_000+5*1_000+13, "advertised version must stay >= 3.5.13")
}

func TestVersionHandlerRejectsNonGet(t *testing.T) {
	for _, method := range []string{
		http.MethodConnect,
		http.MethodTrace,
		http.MethodPut,
		http.MethodPost,
		http.MethodHead,
	} {
		t.Run(method, func(t *testing.T) {
			rec := httptest.NewRecorder()
			(&server{}).versionHandler(rec, httptest.NewRequest(method, "/version", nil))
			require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
			require.Equal(t, http.MethodGet, rec.Header().Get("Allow"))
			require.Equal(t, "Method Not Allowed\n", rec.Body.String())
			require.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
			require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
		})
	}
}

func TestPeerHTTPHandlersExposeVersion(t *testing.T) {
	handlers := (&server{}).GetPeerHttpHandlers()
	handler, ok := handlers["/version"]
	require.True(t, ok)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/version", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, fmt.Sprintf(
		`{"etcdserver":%q,"etcdcluster":%q,"storage":%q}`,
		etcd.Version, etcd.ClusterVersion, etcd.ClusterVersion,
	), rec.Body.String())
}
