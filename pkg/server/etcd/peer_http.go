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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

// PeerHashKVPath is the etcd peer HTTP endpoint used by corruption checkers.
const PeerHashKVPath = "/members/hashkv"

const etcdClusterIDHeader = "X-Etcd-Cluster-ID"

// GetPeerHttpHandlers returns etcd-compatible peer HTTP handlers.
func (s *RPCServer) GetPeerHttpHandlers() map[string]http.Handler {
	return map[string]http.Handler{
		PeerHashKVPath: http.HandlerFunc(s.peerHashKVHandler),
	}
}

func (s *RPCServer) peerHashKVHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path != PeerHashKVPath {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}

	clusterID := strconv.FormatUint(s.backend.ClusterID(), 16)
	if got := r.Header.Get(etcdClusterIDHeader); got != "" && got != clusterID {
		http.Error(w, "cluster ID mismatch", http.StatusPreconditionFailed)
		return
	}

	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error reading body", http.StatusBadRequest)
		return
	}
	req := &etcdserverpb.HashKVRequest{}
	if err := json.Unmarshal(body, req); err != nil {
		http.Error(w, "error unmarshalling request", http.StatusBadRequest)
		return
	}

	result, err := s.backend.HashKV(r.Context(), req.GetRevision())
	if err != nil {
		if errors.Is(err, backend.ErrHashKVCompacted) {
			err = compactedRevisionError()
		} else if errors.Is(err, backend.ErrHashKVFuture) {
			err = futureRevisionError()
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := &etcdserverpb.HashKVResponse{
		Header:          txnHeader(result.CurrentRevision),
		Hash:            result.Hash,
		CompactRevision: result.CompactRevision,
		HashRevision:    result.HashRevision,
	}
	respBytes, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(etcdClusterIDHeader, clusterID)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(respBytes)
}
