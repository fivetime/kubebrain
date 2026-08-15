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

// authorizedPeerHashKVProxyMetadataKey lets the peer HTTP corruption-check
// endpoint call the leader's gRPC HashKV implementation without inventing an
// etcd user credential. HashKV accepts it only together with the unforgeable
// PeerServerOptions context marker; public metadata with this name is ignored.
const authorizedPeerHashKVProxyMetadataKey = "kubebrain-authorized-peer-hashkv-proxy"

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

	_, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh && s.peers.EtcdProxyEnabled() {
		// etcd hashes each peer's local BoltDB even when that member cannot reach
		// the leader. KubeBrain replicas have no independent MVCC store: they all
		// expose the same TiKV history. Race the local shared-store hash with the
		// leader's trusted peer RPC so either a TiKV/PD-isolated ingress or a
		// leader-peer-isolated ingress retains the upstream member-local behavior.
		resp, hashErr := s.hedgedPeerHashKV(r.Context(), req)
		if hashErr != nil {
			writePeerHashKVError(w, hashErr)
			return
		}
		writePeerHashKVResponse(w, clusterID, resp)
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
		writePeerHashKVError(w, err)
		return
	}
	resp := &etcdserverpb.HashKVResponse{
		Header:          txnHeader(result.CurrentRevision),
		Hash:            result.Hash,
		CompactRevision: result.CompactRevision,
		HashRevision:    result.HashRevision,
	}
	writePeerHashKVResponse(w, clusterID, resp)
}

type peerHashKVResult struct {
	response  *etcdserverpb.HashKVResponse
	err       error
	forwarded bool
}

func (s *RPCServer) hedgedPeerHashKV(ctx context.Context, req *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan peerHashKVResult, 2)
	go func() {
		response, err := s.localPeerHashKV(ctx, req)
		results <- peerHashKVResult{response: response, err: err}
	}()
	go func() {
		proxyCtx := metadata.AppendToOutgoingContext(ctx, authorizedPeerHashKVProxyMetadataKey, "1")
		response, err := s.peers.HashKV(proxyCtx, req)
		results <- peerHashKVResult{response: response, err: err, forwarded: true}
	}()

	var firstErr error
	for range 2 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-results:
			if result.forwarded {
				s.observeForwardedRevision(result.response.GetHeader(), result.err)
			}
			if result.err == nil {
				return result.response, nil
			}
			if isPeerHashKVRevisionError(result.err) {
				return nil, result.err
			}
			if firstErr == nil {
				firstErr = result.err
			}
		}
	}
	return nil, firstErr
}

func (s *RPCServer) localPeerHashKV(ctx context.Context, req *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	result, err := s.backend.HashKV(ctx, req.GetRevision())
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.HashKVResponse{
		Header:          txnHeader(result.CurrentRevision),
		Hash:            result.Hash,
		CompactRevision: result.CompactRevision,
		HashRevision:    result.HashRevision,
	}, nil
}

func isPeerHashKVRevisionError(err error) bool {
	message := status.Convert(err).Message()
	return errors.Is(err, backend.ErrHashKVCompacted) || errors.Is(err, backend.ErrHashKVFuture) ||
		strings.Contains(message, "required revision has been compacted") ||
		strings.Contains(message, "required revision is a future revision")
}

func writePeerHashKVError(w http.ResponseWriter, err error) {
	message := status.Convert(err).Message()
	switch {
	case errors.Is(err, backend.ErrHashKVCompacted), strings.Contains(message, "required revision has been compacted"):
		http.Error(w, "mvcc: required revision has been compacted", http.StatusBadRequest)
	case errors.Is(err, backend.ErrHashKVFuture), strings.Contains(message, "required revision is a future revision"):
		http.Error(w, "mvcc: required revision is a future revision", http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

func writePeerHashKVResponse(w http.ResponseWriter, clusterID string, resp *etcdserverpb.HashKVResponse) {
	respBytes, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(etcdClusterIDHeader, clusterID)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(respBytes)
}
