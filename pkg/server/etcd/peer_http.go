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
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

// PeerHashKVPath is the etcd peer HTTP endpoint used by corruption checkers.
const PeerHashKVPath = "/members/hashkv"

const etcdClusterIDHeader = "X-Etcd-Cluster-ID"

// HashKVRequest carries only a revision. Match upstream's peer HTTP request
// ceiling so a compromised peer cannot make the handler buffer an unbounded
// body before JSON validation.
const maxPeerHashKVRequestBytes = 64 * 1024

// GetPeerHttpHandlers returns etcd-compatible peer HTTP handlers.
func (s *RPCServer) GetPeerHttpHandlers() map[string]http.Handler {
	return map[string]http.Handler{
		"/downgrade/enabled": http.HandlerFunc(s.peerDowngradeEnabledHandler),
		"/members":           http.HandlerFunc(s.peerMembersHandler),
		"/members/promote/":  http.HandlerFunc(s.peerMemberPromoteHandler),
		PeerHashKVPath:       http.HandlerFunc(s.peerHashKVHandler),
	}
}

type peerHTTPMember struct {
	ID         uint64   `json:"id"`
	PeerURLs   []string `json:"peerURLs"`
	IsLearner  bool     `json:"isLearner,omitempty"`
	Name       string   `json:"name,omitempty"`
	ClientURLs []string `json:"clientURLs,omitempty"`
}

func (s *RPCServer) peerMembersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	clusterID := strconv.FormatUint(s.backend.ClusterID(), 16)
	w.Header().Set(etcdClusterIDHeader, clusterID)
	if r.URL.Path != "/members" {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}

	members := s.membersSnapshot()
	resp := make([]peerHTTPMember, 0, len(members))
	for _, member := range members {
		resp = append(resp, peerHTTPMember{
			ID:         member.ID,
			PeerURLs:   append([]string(nil), member.PeerURLs...),
			IsLearner:  member.IsLearner,
			Name:       member.Name,
			ClientURLs: append([]string(nil), member.ClientURLs...),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *RPCServer) peerDowngradeEnabledHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set(etcdClusterIDHeader, strconv.FormatUint(s.backend.ClusterID(), 16))
	if r.URL.Path != "/downgrade/enabled" {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("false"))
}

func (s *RPCServer) peerMemberPromoteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	const prefix = "/members/promote/"
	w.Header().Set(etcdClusterIDHeader, strconv.FormatUint(s.backend.ClusterID(), 16))
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	idString := strings.TrimPrefix(r.URL.Path, prefix)
	id, err := strconv.ParseUint(idString, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("member %s not found in cluster", idString), http.StatusNotFound)
		return
	}

	// etcd's peer handler reconstructs incoming gRPC metadata from this HTTP
	// header before running the same admin and member-state checks as the gRPC
	// MemberPromote endpoint.
	ctx := r.Context()
	if token := r.Header.Get("Authorization"); token != "" {
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, token))
	}
	_, err = s.MemberPromote(ctx, &etcdserverpb.MemberPromoteRequest{ID: id})
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}\n"))
		return
	}
	switch {
	case errors.Is(err, rpctypes.ErrGRPCMemberNotFound):
		http.Error(w, "membership: ID not found", http.StatusNotFound)
	case errors.Is(err, rpctypes.ErrGRPCMemberNotLearner):
		http.Error(w, "membership: can only promote a learner member", http.StatusPreconditionFailed)
	case status.Code(err) == codes.Unimplemented:
		http.Error(w, memberMutationUnsupportedMessage, http.StatusNotImplemented)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"Internal Server Error"}`))
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
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxPeerHashKVRequestBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "error reading body", http.StatusBadRequest)
		return
	}
	req := &etcdserverpb.HashKVRequest{}
	if err := json.Unmarshal(body, req); err != nil {
		http.Error(w, "error unmarshalling request", http.StatusBadRequest)
		return
	}

	// KubeBrain stores one logical MVCC keyspace in TiKV, while each service
	// replica caches the latest committed revision. Refresh when possible so
	// peer diagnostics do not falsely report a known revision as future on an
	// idle follower; keep etcd's local-diagnostic availability when the leader is
	// unavailable.
	_ = s.peers.SyncReadRevision(r.Context())
	result, err := s.backend.HashKV(r.Context(), req.GetRevision())
	if err != nil {
		switch {
		case errors.Is(err, backend.ErrHashKVCompacted):
			http.Error(w, "mvcc: required revision has been compacted", http.StatusBadRequest)
		case errors.Is(err, backend.ErrHashKVFuture):
			http.Error(w, "mvcc: required revision is a future revision", http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
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
