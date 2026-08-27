// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	etcdutlsnapshot "go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	streamProbeSeedKeys              = 16
	streamProbeValueBytes            = 8 * 1024
	streamProbeHistoryLeaseTTL       = 15 * 60
	streamProbeSecondHistoryLeaseTTL = 16 * 60
	// Keep enough headroom for the two bounded voter and learner
	// reconfiguration lifecycles while remaining below the outer two-minute
	// Snapshot stream-attempt budget used by the production rollout gate.
	restoredSnapshotVerificationTimeout = 100 * time.Second
	// AppliedIndex can momentarily equal the committed index before the raft
	// Ready has been Advanced. A back-to-back ConfChange in that window is
	// silently converted to a no-op by raft and its etcd waiter times out (the
	// race covered upstream by etcd issue #15528). Require a quiescent interval
	// after equality so restored bootstrap ConfChanges have also been Advanced.
	restoredSnapshotRaftQuiescence = 500 * time.Millisecond
)

type streamProbeExpectation struct {
	key      string
	value    string
	hash     [sha256.Size]byte
	revision int64
	events   []streamProbeEventExpectation
}

type streamProbeEventExpectation struct {
	eventType       mvccpb.Event_EventType
	value           string
	hash            [sha256.Size]byte
	revision        int64
	subRevision     int64
	totalChanges    int64
	createRevision  int64
	version         int64
	lease           int64
	leaseGrantedTTL int64
}

func newStreamProbeExpectations(prefix string) []streamProbeExpectation {
	expected := make([]streamProbeExpectation, 0, streamProbeSeedKeys)
	for index := 0; index < streamProbeSeedKeys; index++ {
		key := fmt.Sprintf("%sstream/%04d", prefix, index)
		value := string(bytes.Repeat([]byte{byte(index + 1)}, streamProbeValueBytes))
		expected = append(expected, streamProbeExpectation{key: key, value: value, hash: sha256.Sum256([]byte(value))})
	}
	return expected
}

func streamProbeTxnSeeds(expected []streamProbeExpectation) ([]*streamProbeExpectation, error) {
	indexes := [...]int{4, 2, 3}
	if len(expected) <= indexes[0] {
		return nil, fmt.Errorf("Snapshot subrevision probe requires at least %d stream seeds", indexes[0]+1)
	}
	seeds := make([]*streamProbeExpectation, 0, len(indexes))
	for _, index := range indexes {
		seeds = append(seeds, &expected[index])
	}
	return seeds, nil
}

type streamProbeNestedSeeds struct {
	compare  *streamProbeExpectation
	outerPut *streamProbeExpectation
	deleted  *streamProbeExpectation
	innerPut *streamProbeExpectation
}

func streamProbeNestedTxnSeeds(expected []streamProbeExpectation) (streamProbeNestedSeeds, error) {
	const (
		compareIndex  = 6
		outerPutIndex = 9
		deletedIndex  = 7
		innerPutIndex = 8
	)
	if len(expected) <= outerPutIndex {
		return streamProbeNestedSeeds{}, fmt.Errorf("Snapshot nested transaction probe requires at least %d stream seeds", outerPutIndex+1)
	}
	return streamProbeNestedSeeds{
		compare:  &expected[compareIndex],
		outerPut: &expected[outerPutIndex],
		deleted:  &expected[deletedIndex],
		innerPut: &expected[innerPutIndex],
	}, nil
}

type streamProbeMultilevelSeeds struct {
	outerCompare *streamProbeExpectation
	innerCompare *streamProbeExpectation
	outerPut     *streamProbeExpectation
	middlePut    *streamProbeExpectation
	deleted      *streamProbeExpectation
	innerPut     *streamProbeExpectation
}

func streamProbeMultilevelTxnSeeds(expected []streamProbeExpectation) (streamProbeMultilevelSeeds, error) {
	const (
		outerCompareIndex = 10
		innerCompareIndex = 11
		outerPutIndex     = 15
		middlePutIndex    = 12
		deletedIndex      = 14
		innerPutIndex     = 13
	)
	if len(expected) <= outerPutIndex {
		return streamProbeMultilevelSeeds{}, fmt.Errorf("Snapshot multilevel transaction probe requires at least %d stream seeds", outerPutIndex+1)
	}
	return streamProbeMultilevelSeeds{
		outerCompare: &expected[outerCompareIndex],
		innerCompare: &expected[innerCompareIndex],
		outerPut:     &expected[outerPutIndex],
		middlePut:    &expected[middlePutIndex],
		deleted:      &expected[deletedIndex],
		innerPut:     &expected[innerPutIndex],
	}, nil
}

type rangeStreamReceiver interface {
	Recv() (*etcdserverpb.RangeStreamResponse, error)
}

type snapshotReceiver interface {
	Recv() (*etcdserverpb.SnapshotResponse, error)
}

type snapshotArtifactManager interface {
	Status(string) (etcdutlsnapshot.Status, error)
	Restore(etcdutlsnapshot.RestoreConfig) error
}

type restoredSnapshotConfig struct {
	members             []restoredSnapshotMemberConfig
	initialCluster      string
	initialClusterToken string
	tls                 restoredSnapshotTLSConfig
	auth                *restoredSnapshotAuthExpectation
}

type restoredSnapshotMemberConfig struct {
	name      string
	dataDir   string
	clientURL url.URL
	peerURL   url.URL
}

func (cfg restoredSnapshotConfig) validate() error {
	if len(cfg.members) == 0 || cfg.initialCluster == "" || cfg.initialClusterToken == "" {
		return errors.New("restored Snapshot cluster requires members, an initial cluster, and a token")
	}
	names := make(map[string]struct{}, len(cfg.members))
	dataDirs := make(map[string]struct{}, len(cfg.members))
	clientURLs := make(map[string]struct{}, len(cfg.members))
	peerURLs := make(map[string]struct{}, len(cfg.members))
	initialCluster := make([]string, 0, len(cfg.members))
	for index, member := range cfg.members {
		if member.name == "" || member.dataDir == "" || member.clientURL.Host == "" || member.peerURL.Host == "" {
			return fmt.Errorf("restored Snapshot member %d requires a name, data directory, client URL, and peer URL", index)
		}
		for label, identity := range map[string]struct {
			value string
			seen  map[string]struct{}
		}{
			"name": {member.name, names}, "data directory": {member.dataDir, dataDirs},
			"client URL": {member.clientURL.String(), clientURLs}, "peer URL": {member.peerURL.String(), peerURLs},
		} {
			if _, duplicate := identity.seen[identity.value]; duplicate {
				return fmt.Errorf("restored Snapshot member %d repeats %s %q", index, label, identity.value)
			}
			identity.seen[identity.value] = struct{}{}
		}
		initialCluster = append(initialCluster, member.name+"="+member.peerURL.String())
	}
	if got := strings.Join(initialCluster, ","); cfg.initialCluster != got {
		return fmt.Errorf("restored Snapshot initial cluster mismatch: got=%q want=%q", cfg.initialCluster, got)
	}
	return cfg.tls.validate()
}

type restoredSnapshotVerifier func(context.Context, restoredSnapshotConfig, []streamProbeExpectation, int64) error

type restoredSnapshotTLSConfig struct {
	caFile     string
	certFile   string
	keyFile    string
	serverName string
}

func (cfg restoredSnapshotTLSConfig) enabled() bool {
	return cfg.caFile != "" || cfg.certFile != "" || cfg.keyFile != "" || cfg.serverName != ""
}

func (cfg restoredSnapshotTLSConfig) validate() error {
	if !cfg.enabled() {
		return nil
	}
	if cfg.caFile == "" || cfg.certFile == "" || cfg.keyFile == "" || cfg.serverName == "" {
		return errors.New("restored Snapshot TLS requires CA, certificate, key, and server name")
	}
	return nil
}

func consumeRangeStream(stream rangeStreamReceiver, prefix string, expected []streamProbeExpectation, clusterID uint64) (bool, error) {
	expectedHashes := make(map[string][sha256.Size]byte, len(expected))
	for _, item := range expected {
		expectedHashes[item.key] = item.hash
	}
	found := make(map[string]struct{}, len(expected))
	var (
		lastKey      string
		delivered    int64
		maxRevision  int64
		terminalSeen bool
		partial      bool
	)
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if !terminalSeen {
				return partial, errors.New("RangeStream ended without terminal metadata")
			}
			if len(found) != len(expectedHashes) {
				return partial, fmt.Errorf("RangeStream delivered %d/%d seeded keys", len(found), len(expectedHashes))
			}
			return partial, nil
		}
		if err != nil {
			return partial, err
		}
		partial = true
		if response == nil || response.RangeResponse == nil {
			return partial, errors.New("RangeStream returned an empty frame")
		}
		if terminalSeen {
			return partial, errors.New("RangeStream continued after terminal metadata")
		}
		rangeResponse := response.RangeResponse
		for _, kv := range rangeResponse.Kvs {
			if kv == nil || !bytes.HasPrefix(kv.Key, []byte(prefix)) {
				return partial, errors.New("RangeStream returned a key outside the probe prefix")
			}
			key := string(kv.Key)
			if lastKey != "" && key <= lastKey {
				return partial, errors.New("RangeStream keys are not strictly increasing")
			}
			if kv.CreateRevision <= 0 || kv.ModRevision <= 0 || kv.Version <= 0 || kv.CreateRevision > kv.ModRevision {
				return partial, errors.New("RangeStream returned invalid MVCC metadata")
			}
			lastKey = key
			delivered++
			maxRevision = max(maxRevision, kv.ModRevision)
			if expectedHash, ok := expectedHashes[key]; ok {
				if sha256.Sum256(kv.Value) != expectedHash {
					return partial, fmt.Errorf("RangeStream changed seeded value for %q", key)
				}
				found[key] = struct{}{}
			}
		}
		if rangeResponse.Header == nil {
			if rangeResponse.Count != 0 || rangeResponse.More {
				return partial, errors.New("RangeStream published terminal fields before its terminal frame")
			}
			continue
		}
		if rangeResponse.Header.ClusterId != clusterID || rangeResponse.Header.MemberId == 0 ||
			rangeResponse.Header.Revision <= 0 || rangeResponse.Header.Revision < maxRevision ||
			rangeResponse.Count != delivered || rangeResponse.More {
			return partial, errors.New("RangeStream returned invalid terminal identity or count")
		}
		terminalSeen = true
	}
}

func consumeSnapshot(stream snapshotReceiver) (bool, error) {
	partial, _, err := consumeSnapshotTo(stream, io.Discard)
	return partial, err
}

func consumeSnapshotTo(stream snapshotReceiver, artifact io.Writer) (bool, string, error) {
	var (
		digest           hash.Hash = sha256.New()
		remaining        uint64
		version          string
		versionObserved  bool
		haveData         bool
		awaitingChecksum bool
		complete         bool
		partial          bool
	)
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if !complete {
				return partial, version, errors.New("Snapshot ended before its checksum frame")
			}
			return partial, version, nil
		}
		if err != nil {
			return partial, version, err
		}
		partial = true
		if response == nil {
			return partial, version, errors.New("Snapshot returned an empty frame")
		}
		if complete {
			return partial, version, errors.New("Snapshot continued after its checksum frame")
		}
		if versionObserved && response.Version != version {
			return partial, version, errors.New("Snapshot changed storage version")
		}
		version = response.Version
		versionObserved = true
		if awaitingChecksum {
			if response.RemainingBytes != 0 || len(response.Blob) != sha256.Size || !bytes.Equal(response.Blob, digest.Sum(nil)) {
				return partial, version, errors.New("Snapshot returned an invalid checksum frame")
			}
			if _, err = artifact.Write(response.Blob); err != nil {
				return partial, version, fmt.Errorf("write Snapshot checksum: %w", err)
			}
			complete = true
			continue
		}
		if len(response.Blob) == 0 {
			return partial, version, errors.New("Snapshot returned an empty data frame")
		}
		if haveData {
			if uint64(len(response.Blob)) > remaining || response.RemainingBytes != remaining-uint64(len(response.Blob)) {
				return partial, version, errors.New("Snapshot returned discontinuous remaining bytes")
			}
		}
		if _, err = artifact.Write(response.Blob); err != nil {
			return partial, version, fmt.Errorf("write Snapshot data: %w", err)
		}
		_, _ = digest.Write(response.Blob)
		remaining = response.RemainingBytes
		haveData = true
		awaitingChecksum = remaining == 0
	}
}

func allocateRestoredSnapshotURLs(clientTLS bool) (clientURL, peerURL url.URL, retErr error) {
	listeners := make([]net.Listener, 0, 2)
	defer func() {
		for _, listener := range listeners {
			if closeErr := listener.Close(); closeErr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("release restored Snapshot listener reservation: %w", closeErr))
			}
		}
	}()
	for range 2 {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return url.URL{}, url.URL{}, fmt.Errorf("reserve restored Snapshot listener: %w", err)
		}
		listeners = append(listeners, listener)
	}
	clientScheme := "http"
	if clientTLS {
		clientScheme = "https"
	}
	clientURL = url.URL{Scheme: clientScheme, Host: listeners[0].Addr().String()}
	peerURL = url.URL{Scheme: "http", Host: listeners[1].Addr().String()}
	return clientURL, peerURL, nil
}

func newRestoredSnapshotConfig(restoreRoot string, memberCount int, tlsCfg restoredSnapshotTLSConfig,
	auth *restoredSnapshotAuthExpectation,
) (restoredSnapshotConfig, error) {
	if memberCount != 1 && memberCount < 3 {
		return restoredSnapshotConfig{}, fmt.Errorf("restored Snapshot member count must be one or at least three: %d", memberCount)
	}
	const restoreName = "kubebrain-rollout-restore"
	members := make([]restoredSnapshotMemberConfig, memberCount)
	initialCluster := make([]string, memberCount)
	for index := range members {
		clientURL, peerURL, err := allocateRestoredSnapshotURLs(tlsCfg.enabled())
		if err != nil {
			return restoredSnapshotConfig{}, err
		}
		name := restoreName
		if memberCount > 1 {
			name = fmt.Sprintf("%s-%d", restoreName, index)
		}
		members[index] = restoredSnapshotMemberConfig{
			name: name, dataDir: filepath.Join(restoreRoot, fmt.Sprintf("member-%d", index)),
			clientURL: clientURL, peerURL: peerURL,
		}
		initialCluster[index] = name + "=" + peerURL.String()
	}
	cfg := restoredSnapshotConfig{
		members: members, initialCluster: strings.Join(initialCluster, ","), initialClusterToken: restoreName,
		tls: tlsCfg, auth: auth,
	}
	if err := cfg.validate(); err != nil {
		return restoredSnapshotConfig{}, err
	}
	return cfg, nil
}

func newRestoredSnapshotEmbedConfig(cfg restoredSnapshotConfig, member restoredSnapshotMemberConfig,
	initialCluster string,
) *embed.Config {
	embedCfg := embed.NewConfig()
	embedCfg.Name = member.name
	embedCfg.Dir = member.dataDir
	embedCfg.ClusterState = embed.ClusterStateFlagExisting
	embedCfg.ListenClientUrls = []url.URL{member.clientURL}
	embedCfg.AdvertiseClientUrls = []url.URL{member.clientURL}
	embedCfg.ListenPeerUrls = []url.URL{member.peerURL}
	embedCfg.AdvertisePeerUrls = []url.URL{member.peerURL}
	embedCfg.InitialCluster = initialCluster
	embedCfg.InitialClusterToken = cfg.initialClusterToken
	embedCfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.NewNop())
	if cfg.tls.enabled() {
		embedCfg.ClientTLSInfo = transport.TLSInfo{
			CertFile: cfg.tls.certFile, KeyFile: cfg.tls.keyFile, TrustedCAFile: cfg.tls.caFile, ClientCertAuth: true,
		}
	}
	return embedCfg
}

func waitForRestoredSnapshotMember(ctx context.Context, member restoredSnapshotMemberConfig, server *embed.Etcd) error {
	select {
	case <-server.Server.ReadyNotify():
		return nil
	case serveErr, ok := <-server.Err():
		if !ok || serveErr == nil {
			return fmt.Errorf("officially restored etcd member %q stopped before becoming ready", member.name)
		}
		return fmt.Errorf("officially restored etcd member %q stopped before becoming ready: %w", member.name, serveErr)
	case <-ctx.Done():
		return fmt.Errorf("wait for officially restored etcd member %q readiness: %w", member.name, context.Cause(ctx))
	}
}

type restoredClusterTopology struct {
	clusterID uint64
	leaderID  uint64
	memberIDs []uint64
}

func verifyRestoredClusterTopology(ctx context.Context, client *clientv3.Client, cfg restoredSnapshotConfig, revision int64,
) (restoredClusterTopology, error) {
	response, err := client.MemberList(ctx)
	if err != nil {
		return restoredClusterTopology{}, fmt.Errorf("list officially restored etcd members: %w", err)
	}
	if response == nil || response.Header == nil || response.Header.ClusterId == 0 || response.Header.MemberId == 0 ||
		response.Header.RaftTerm == 0 || response.Header.Revision != 0 {
		return restoredClusterTopology{}, fmt.Errorf("invalid officially restored etcd member-list identity: response=%+v snapshot_revision=%d", response, revision)
	}
	if len(response.Members) != len(cfg.members) {
		return restoredClusterTopology{}, fmt.Errorf("officially restored etcd member count mismatch: got=%d want=%d", len(response.Members), len(cfg.members))
	}
	expectedByName := make(map[string]int, len(cfg.members))
	for index, member := range cfg.members {
		expectedByName[member.name] = index
	}
	topology := restoredClusterTopology{clusterID: response.Header.ClusterId, memberIDs: make([]uint64, len(cfg.members))}
	seenIDs := make(map[uint64]struct{}, len(cfg.members))
	for _, member := range response.Members {
		if member == nil || member.ID == 0 || member.Name == "" || member.IsLearner {
			return restoredClusterTopology{}, fmt.Errorf("officially restored etcd returned an invalid voting member: %+v", member)
		}
		index, exists := expectedByName[member.Name]
		if !exists {
			return restoredClusterTopology{}, fmt.Errorf("officially restored etcd returned unexpected member name %q", member.Name)
		}
		if _, duplicate := seenIDs[member.ID]; duplicate {
			return restoredClusterTopology{}, fmt.Errorf("officially restored etcd repeated member ID %x", member.ID)
		}
		seenIDs[member.ID] = struct{}{}
		expected := cfg.members[index]
		if len(member.PeerURLs) != 1 || member.PeerURLs[0] != expected.peerURL.String() ||
			len(member.ClientURLs) != 1 || member.ClientURLs[0] != expected.clientURL.String() {
			return restoredClusterTopology{}, fmt.Errorf("officially restored etcd member %q URL mismatch: peer=%v client=%v want_peer=%q want_client=%q",
				member.Name, member.PeerURLs, member.ClientURLs, expected.peerURL.String(), expected.clientURL.String())
		}
		topology.memberIDs[index] = member.ID
	}
	for index, member := range cfg.members {
		statusResponse, statusErr := client.Status(ctx, member.clientURL.String())
		if statusErr != nil {
			return restoredClusterTopology{}, fmt.Errorf("read officially restored etcd member %q status: %w", member.name, statusErr)
		}
		if statusResponse == nil || statusResponse.Header == nil || statusResponse.Header.ClusterId != topology.clusterID ||
			statusResponse.Header.MemberId != topology.memberIDs[index] || statusResponse.Header.RaftTerm == 0 ||
			statusResponse.Header.Revision != revision || statusResponse.Leader == 0 || statusResponse.RaftTerm == 0 ||
			statusResponse.IsLearner || len(statusResponse.Errors) != 0 {
			return restoredClusterTopology{}, fmt.Errorf("invalid officially restored etcd member %q status: %+v snapshot_revision=%d", member.name, statusResponse, revision)
		}
		if _, exists := seenIDs[statusResponse.Leader]; !exists {
			return restoredClusterTopology{}, fmt.Errorf("officially restored etcd member %q reported unknown leader %x", member.name, statusResponse.Leader)
		}
		if topology.leaderID == 0 {
			topology.leaderID = statusResponse.Leader
		} else if topology.leaderID != statusResponse.Leader {
			return restoredClusterTopology{}, fmt.Errorf("officially restored etcd members disagree on leader: got=%x want=%x", statusResponse.Leader, topology.leaderID)
		}
	}
	return topology, nil
}

func newDirectRestoredClient(cfg clientv3.Config, endpoint string) (*clientv3.Client, error) {
	cfg.Endpoints = []string{endpoint}
	cfg.Logger = zap.NewNop()
	return clientv3.New(cfg)
}

func waitForRestoredMemberValue(ctx context.Context, client *clientv3.Client, key, value string, minimumRevision int64) error {
	for {
		response, err := client.Get(ctx, key, clientv3.WithSerializable())
		if err == nil && response != nil && response.Header != nil && response.Header.Revision >= minimumRevision &&
			len(response.Kvs) == 1 && string(response.Kvs[0].Key) == key && string(response.Kvs[0].Value) == value &&
			response.Kvs[0].ModRevision == minimumRevision {
			return nil
		}
		if err != nil && ctx.Err() != nil {
			return fmt.Errorf("wait for restored member value: %w", context.Cause(ctx))
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return fmt.Errorf("wait for restored member value: %w", context.Cause(ctx))
		}
	}
}

func waitForRestoredMemberMissing(ctx context.Context, client *clientv3.Client, key string, minimumRevision int64) error {
	for {
		response, err := client.Get(ctx, key, clientv3.WithSerializable())
		if err == nil && response != nil && response.Header != nil && response.Header.Revision >= minimumRevision &&
			response.Count == 0 && len(response.Kvs) == 0 {
			return nil
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return fmt.Errorf("wait for restored member key deletion: %w", context.Cause(ctx))
		}
	}
}

func waitForRestoredMemberListenerClosed(ctx context.Context, endpoint *url.URL) error {
	if endpoint == nil || endpoint.Host == "" {
		return errors.New("removed restored member requires a client listener address")
	}
	dialer := &net.Dialer{Timeout: 100 * time.Millisecond}
	for {
		probeCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		connection, err := dialer.DialContext(probeCtx, "tcp", endpoint.Host)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("wait for removed restored member listener to close: %w", context.Cause(ctx))
			}
			return nil
		}
		if closeErr := connection.Close(); closeErr != nil {
			return fmt.Errorf("close removed restored member listener probe: %w", closeErr)
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return fmt.Errorf("wait for removed restored member listener to close: %w", context.Cause(ctx))
		}
	}
}

func validateRestoredMemberAddResponse(response *clientv3.MemberAddResponse, cfg restoredSnapshotConfig,
	topology restoredClusterTopology, added restoredSnapshotMemberConfig,
) (uint64, error) {
	if response == nil || response.Header == nil || response.Header.ClusterId != topology.clusterID ||
		response.Header.MemberId == 0 || response.Header.RaftTerm == 0 || response.Header.Revision != 0 || response.Member == nil {
		return 0, fmt.Errorf("invalid officially restored etcd MemberAdd response identity: %+v", response)
	}
	servingMember := false
	for _, memberID := range topology.memberIDs {
		servingMember = servingMember || response.Header.MemberId == memberID
	}
	if !servingMember {
		return 0, fmt.Errorf("officially restored etcd MemberAdd response came from unknown member %x", response.Header.MemberId)
	}
	newMember := response.Member
	if newMember.ID == 0 || newMember.Name != "" || newMember.IsLearner || len(newMember.PeerURLs) != 1 ||
		newMember.PeerURLs[0] != added.peerURL.String() || len(newMember.ClientURLs) != 0 {
		return 0, fmt.Errorf("officially restored etcd returned an invalid added voter: %+v", newMember)
	}
	for _, memberID := range topology.memberIDs {
		if newMember.ID == memberID {
			return 0, fmt.Errorf("officially restored etcd reused member ID %x during MemberAdd", newMember.ID)
		}
	}
	if len(response.Members) != len(cfg.members)+1 {
		return 0, fmt.Errorf("officially restored etcd MemberAdd member count mismatch: got=%d want=%d", len(response.Members), len(cfg.members)+1)
	}
	expectedByID := make(map[uint64]restoredSnapshotMemberConfig, len(cfg.members))
	for index, memberID := range topology.memberIDs {
		expectedByID[memberID] = cfg.members[index]
	}
	seen := make(map[uint64]struct{}, len(response.Members))
	for _, member := range response.Members {
		if member == nil || member.ID == 0 || member.IsLearner {
			return 0, fmt.Errorf("officially restored etcd MemberAdd returned an invalid voter: %+v", member)
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return 0, fmt.Errorf("officially restored etcd MemberAdd repeated member ID %x", member.ID)
		}
		seen[member.ID] = struct{}{}
		if member.ID == newMember.ID {
			if member.Name != "" || len(member.PeerURLs) != 1 || member.PeerURLs[0] != added.peerURL.String() || len(member.ClientURLs) != 0 {
				return 0, fmt.Errorf("officially restored etcd MemberAdd member list disagrees on new voter: %+v", member)
			}
			continue
		}
		expected, exists := expectedByID[member.ID]
		if !exists || member.Name != expected.name || len(member.PeerURLs) != 1 || member.PeerURLs[0] != expected.peerURL.String() ||
			len(member.ClientURLs) != 1 || member.ClientURLs[0] != expected.clientURL.String() {
			return 0, fmt.Errorf("officially restored etcd MemberAdd changed an existing voter: %+v", member)
		}
	}
	if _, exists := seen[newMember.ID]; !exists {
		return 0, fmt.Errorf("officially restored etcd MemberAdd omitted new voter %x from member list", newMember.ID)
	}
	return newMember.ID, nil
}

func verifyRestoredMemberCurrentSeeds(ctx context.Context, source, added *clientv3.Client, expected []streamProbeExpectation,
	clusterID, memberID uint64, minimumRevision int64,
) error {
	for _, item := range expected {
		sourceResponse, err := source.Get(ctx, item.key, clientv3.WithSerializable())
		if err != nil {
			return fmt.Errorf("read current seed %q from source restored member: %w", item.key, err)
		}
		addedResponse, err := added.Get(ctx, item.key, clientv3.WithSerializable())
		if err != nil {
			return fmt.Errorf("read current seed %q from added restored member: %w", item.key, err)
		}
		if sourceResponse == nil || sourceResponse.Header == nil || sourceResponse.Header.ClusterId != clusterID ||
			sourceResponse.Header.MemberId == 0 || sourceResponse.Header.Revision < minimumRevision || sourceResponse.More ||
			sourceResponse.Count < 0 || sourceResponse.Count > 1 || int64(len(sourceResponse.Kvs)) != sourceResponse.Count {
			return fmt.Errorf("source restored member returned invalid current seed envelope for %q: %+v", item.key, sourceResponse)
		}
		if addedResponse == nil || addedResponse.Header == nil || addedResponse.Header.ClusterId != clusterID ||
			addedResponse.Header.MemberId != memberID || addedResponse.Header.Revision < minimumRevision || addedResponse.More ||
			addedResponse.Count != sourceResponse.Count || len(addedResponse.Kvs) != len(sourceResponse.Kvs) {
			return fmt.Errorf("added restored member returned divergent current seed envelope for %q: source=%+v added=%+v",
				item.key, sourceResponse, addedResponse)
		}
		if len(sourceResponse.Kvs) == 0 {
			continue
		}
		sourceKV, addedKV := sourceResponse.Kvs[0], addedResponse.Kvs[0]
		if sourceKV == nil || addedKV == nil || string(sourceKV.Key) != item.key ||
			!bytes.Equal(sourceKV.Key, addedKV.Key) || !bytes.Equal(sourceKV.Value, addedKV.Value) ||
			sourceKV.CreateRevision != addedKV.CreateRevision || sourceKV.ModRevision != addedKV.ModRevision ||
			sourceKV.Version != addedKV.Version || sourceKV.Lease != addedKV.Lease {
			return fmt.Errorf("added restored member returned divergent current seed data for %q", item.key)
		}
	}
	return nil
}

func sortedStrings(values []string) []string {
	copy := append([]string(nil), values...)
	sort.Strings(copy)
	return copy
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func verifyRestoredMemberAuthState(ctx context.Context, source, added *clientv3.Client,
	clusterID, memberID uint64, minimumRevision int64,
) error {
	validateHeaders := func(label string, sourceHeader, addedHeader *etcdserverpb.ResponseHeader) error {
		if sourceHeader == nil || sourceHeader.ClusterId != clusterID || sourceHeader.MemberId == 0 || sourceHeader.RaftTerm == 0 ||
			sourceHeader.Revision < minimumRevision || addedHeader == nil || addedHeader.ClusterId != clusterID ||
			addedHeader.MemberId != memberID || addedHeader.RaftTerm == 0 || addedHeader.Revision < minimumRevision {
			return fmt.Errorf("added restored member returned invalid %s identity: source=%+v added=%+v", label, sourceHeader, addedHeader)
		}
		return nil
	}
	sourceStatus, err := source.AuthStatus(ctx)
	if err != nil {
		return fmt.Errorf("read restored auth status before member expansion verification: %w", err)
	}
	addedStatus, err := added.AuthStatus(ctx)
	if err != nil {
		return fmt.Errorf("read restored auth status from added member: %w", err)
	}
	if sourceStatus == nil || addedStatus == nil || sourceStatus.Enabled != addedStatus.Enabled ||
		sourceStatus.AuthRevision != addedStatus.AuthRevision {
		return fmt.Errorf("added restored member auth status mismatch: source=%+v added=%+v", sourceStatus, addedStatus)
	}
	if err := validateHeaders("auth status", sourceStatus.Header, addedStatus.Header); err != nil {
		return err
	}
	sourceUsers, err := source.UserList(ctx)
	if err != nil {
		return fmt.Errorf("list restored users before member expansion verification: %w", err)
	}
	addedUsers, err := added.UserList(ctx)
	if err != nil {
		return fmt.Errorf("list restored users from added member: %w", err)
	}
	sourceRoles, err := source.RoleList(ctx)
	if err != nil {
		return fmt.Errorf("list restored roles before member expansion verification: %w", err)
	}
	addedRoles, err := added.RoleList(ctx)
	if err != nil {
		return fmt.Errorf("list restored roles from added member: %w", err)
	}
	if sourceUsers == nil || addedUsers == nil || sourceRoles == nil || addedRoles == nil ||
		!equalStrings(sortedStrings(sourceUsers.Users), sortedStrings(addedUsers.Users)) ||
		!equalStrings(sortedStrings(sourceRoles.Roles), sortedStrings(addedRoles.Roles)) {
		return fmt.Errorf("added restored member auth identity mismatch: source_users=%v added_users=%v source_roles=%v added_roles=%v",
			sourceUsers, addedUsers, sourceRoles, addedRoles)
	}
	if err := validateHeaders("auth user list", sourceUsers.Header, addedUsers.Header); err != nil {
		return err
	}
	if err := validateHeaders("auth role list", sourceRoles.Header, addedRoles.Header); err != nil {
		return err
	}
	for _, username := range sourceUsers.Users {
		sourceUser, err := source.UserGet(ctx, username)
		if err != nil {
			return fmt.Errorf("read restored user %q before member expansion verification: %w", username, err)
		}
		addedUser, err := added.UserGet(ctx, username)
		if err != nil {
			return fmt.Errorf("read restored user %q from added member: %w", username, err)
		}
		if sourceUser == nil || addedUser == nil ||
			!equalStrings(sortedStrings(sourceUser.Roles), sortedStrings(addedUser.Roles)) {
			return fmt.Errorf("added restored member user %q role bindings mismatch: source=%+v added=%+v", username, sourceUser, addedUser)
		}
		if err := validateHeaders("auth user "+username, sourceUser.Header, addedUser.Header); err != nil {
			return err
		}
	}
	for _, roleName := range sourceRoles.Roles {
		sourceRole, err := source.RoleGet(ctx, roleName)
		if err != nil {
			return fmt.Errorf("read restored role %q before member expansion verification: %w", roleName, err)
		}
		addedRole, err := added.RoleGet(ctx, roleName)
		if err != nil {
			return fmt.Errorf("read restored role %q from added member: %w", roleName, err)
		}
		if sourceRole == nil || addedRole == nil || len(sourceRole.Perm) != len(addedRole.Perm) {
			return fmt.Errorf("added restored member role %q permission count mismatch: source=%+v added=%+v", roleName, sourceRole, addedRole)
		}
		for index := range sourceRole.Perm {
			if !proto.Equal(sourceRole.Perm[index], addedRole.Perm[index]) {
				return fmt.Errorf("added restored member role %q permission mismatch at %d", roleName, index)
			}
		}
		if err := validateHeaders("auth role "+roleName, sourceRole.Header, addedRole.Header); err != nil {
			return err
		}
	}
	return nil
}

func addRestoredSnapshotMember(ctx context.Context, client *clientv3.Client, peerURL string) (*clientv3.MemberAddResponse, error) {
	for {
		response, err := client.MemberAdd(ctx, []string{peerURL})
		if err == nil {
			return response, nil
		}
		if !errors.Is(err, rpctypes.ErrUnhealthy) && !errors.Is(err, rpctypes.ErrMemberNotEnoughStarted) {
			return nil, err
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for officially restored etcd cluster to become safe for MemberAdd: %w", context.Cause(ctx))
		}
	}
}

func addRestoredSnapshotLearner(ctx context.Context, client *clientv3.Client, peerURL string) (*clientv3.MemberAddResponse, error) {
	for {
		response, err := client.MemberAddAsLearner(ctx, []string{peerURL})
		if err == nil {
			return response, nil
		}
		if !errors.Is(rpctypes.Error(err), rpctypes.ErrUnhealthy) &&
			!errors.Is(rpctypes.Error(err), rpctypes.ErrMemberNotEnoughStarted) {
			return nil, err
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for officially restored etcd cluster to become safe for MemberAddAsLearner: %w", context.Cause(ctx))
		}
	}
}

func validateRestoredLearnerMembers(header *etcdserverpb.ResponseHeader, members []*etcdserverpb.Member,
	cfg restoredSnapshotConfig, topology restoredClusterTopology, learner restoredSnapshotMemberConfig,
	learnerID uint64, started bool,
) error {
	if len(topology.memberIDs) != len(cfg.members) {
		return fmt.Errorf("officially restored etcd learner membership voter identity count mismatch: voters=%d ids=%d",
			len(cfg.members), len(topology.memberIDs))
	}
	if header == nil || header.ClusterId != topology.clusterID || header.MemberId == 0 ||
		header.RaftTerm == 0 || header.Revision != 0 {
		return fmt.Errorf("invalid officially restored etcd learner membership response identity: %+v", header)
	}
	servingMember := false
	for _, memberID := range topology.memberIDs {
		servingMember = servingMember || header.MemberId == memberID
	}
	if !servingMember {
		return fmt.Errorf("officially restored etcd learner membership response came from unknown member %x", header.MemberId)
	}
	if learnerID == 0 || len(members) != len(cfg.members)+1 {
		return fmt.Errorf("officially restored etcd learner membership count mismatch: learner=%x got=%d want=%d",
			learnerID, len(members), len(cfg.members)+1)
	}
	expectedByID := make(map[uint64]restoredSnapshotMemberConfig, len(cfg.members))
	for index, memberID := range topology.memberIDs {
		expectedByID[memberID] = cfg.members[index]
	}
	seen := make(map[uint64]struct{}, len(members))
	learnerSeen := false
	for _, member := range members {
		if member == nil || member.ID == 0 {
			return fmt.Errorf("officially restored etcd learner membership returned an invalid member: %+v", member)
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return fmt.Errorf("officially restored etcd learner membership repeated member ID %x", member.ID)
		}
		seen[member.ID] = struct{}{}
		if member.ID == learnerID {
			learnerSeen = true
			expectedName := ""
			expectedClientURLs := 0
			if started {
				expectedName = learner.name
				expectedClientURLs = 1
			}
			if !member.IsLearner || member.Name != expectedName || len(member.PeerURLs) != 1 ||
				member.PeerURLs[0] != learner.peerURL.String() || len(member.ClientURLs) != expectedClientURLs ||
				(expectedClientURLs == 1 && member.ClientURLs[0] != learner.clientURL.String()) {
				return fmt.Errorf("officially restored etcd returned an invalid learner identity: %+v", member)
			}
			continue
		}
		expected, exists := expectedByID[member.ID]
		if !exists || member.IsLearner || member.Name != expected.name || len(member.PeerURLs) != 1 ||
			member.PeerURLs[0] != expected.peerURL.String() || len(member.ClientURLs) != 1 ||
			member.ClientURLs[0] != expected.clientURL.String() {
			return fmt.Errorf("officially restored etcd learner membership changed an existing voter: %+v", member)
		}
	}
	if !learnerSeen {
		return fmt.Errorf("officially restored etcd learner membership omitted learner %x", learnerID)
	}
	return nil
}

func validateRestoredMemberAddLearnerResponse(response *clientv3.MemberAddResponse, cfg restoredSnapshotConfig,
	topology restoredClusterTopology, learner restoredSnapshotMemberConfig,
) (uint64, error) {
	if response == nil || response.Member == nil || response.Member.ID == 0 {
		return 0, fmt.Errorf("invalid officially restored etcd MemberAddAsLearner response: %+v", response)
	}
	learnerID := response.Member.ID
	for _, memberID := range topology.memberIDs {
		if learnerID == memberID {
			return 0, fmt.Errorf("officially restored etcd reused member ID %x during MemberAddAsLearner", learnerID)
		}
	}
	if !response.Member.IsLearner || response.Member.Name != "" || len(response.Member.PeerURLs) != 1 ||
		response.Member.PeerURLs[0] != learner.peerURL.String() || len(response.Member.ClientURLs) != 0 {
		return 0, fmt.Errorf("officially restored etcd returned an invalid added learner: %+v", response.Member)
	}
	if err := validateRestoredLearnerMembers(response.Header, response.Members, cfg, topology, learner, learnerID, false); err != nil {
		return 0, err
	}
	return learnerID, nil
}

func waitForRestoredLearnerMembership(ctx context.Context, client *clientv3.Client, cfg restoredSnapshotConfig,
	topology restoredClusterTopology, learner restoredSnapshotMemberConfig, learnerID uint64,
) error {
	var lastErr error
	for {
		response, err := client.MemberList(ctx)
		if err == nil && response != nil {
			lastErr = validateRestoredLearnerMembers(response.Header, response.Members, cfg, topology, learner, learnerID, true)
			if lastErr == nil {
				return nil
			}
		} else if err == nil {
			lastErr = errors.New("officially restored etcd returned a nil learner MemberList response")
		} else {
			lastErr = err
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return fmt.Errorf("wait for officially restored etcd learner membership: last_error=%v: %w", lastErr, context.Cause(ctx))
		}
	}
}

func promoteRestoredSnapshotLearner(ctx context.Context, client *clientv3.Client, learnerID uint64) (*clientv3.MemberPromoteResponse, error) {
	for {
		response, err := client.MemberPromote(ctx, learnerID)
		if err == nil {
			return response, nil
		}
		if !errors.Is(rpctypes.Error(err), rpctypes.ErrMemberLearnerNotReady) {
			return nil, err
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for officially restored etcd learner %x to become promotable: %w", learnerID, context.Cause(ctx))
		}
	}
}

func validateRestoredMemberPromoteResponse(response *clientv3.MemberPromoteResponse, cfg restoredSnapshotConfig,
	topology restoredClusterTopology, promotedID uint64,
) error {
	if len(topology.memberIDs) != len(cfg.members) {
		return fmt.Errorf("officially restored etcd MemberPromote voter identity count mismatch: voters=%d ids=%d",
			len(cfg.members), len(topology.memberIDs))
	}
	if response == nil || response.Header == nil || response.Header.ClusterId != topology.clusterID ||
		response.Header.MemberId == 0 || response.Header.RaftTerm == 0 || response.Header.Revision != 0 {
		return fmt.Errorf("invalid officially restored etcd MemberPromote response identity: %+v", response)
	}
	servingMember := false
	for _, memberID := range topology.memberIDs {
		servingMember = servingMember || response.Header.MemberId == memberID
	}
	if !servingMember || len(response.Members) != len(cfg.members) {
		return fmt.Errorf("invalid officially restored etcd MemberPromote responder or member count: response=%+v want_members=%d",
			response, len(cfg.members))
	}
	expectedByID := make(map[uint64]restoredSnapshotMemberConfig, len(cfg.members))
	for index, memberID := range topology.memberIDs {
		expectedByID[memberID] = cfg.members[index]
	}
	seen := make(map[uint64]struct{}, len(response.Members))
	promotedSeen := false
	for _, member := range response.Members {
		if member == nil || member.ID == 0 || member.IsLearner {
			return fmt.Errorf("officially restored etcd MemberPromote returned an invalid voter: %+v", member)
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return fmt.Errorf("officially restored etcd MemberPromote repeated member ID %x", member.ID)
		}
		seen[member.ID] = struct{}{}
		expected, exists := expectedByID[member.ID]
		if !exists || member.Name != expected.name || len(member.PeerURLs) != 1 || member.PeerURLs[0] != expected.peerURL.String() ||
			len(member.ClientURLs) != 1 || member.ClientURLs[0] != expected.clientURL.String() {
			return fmt.Errorf("officially restored etcd MemberPromote changed a voter identity: %+v", member)
		}
		promotedSeen = promotedSeen || member.ID == promotedID
	}
	if !promotedSeen {
		return fmt.Errorf("officially restored etcd MemberPromote omitted promoted member %x", promotedID)
	}
	return nil
}

func validateRestoredMemberRemoveResponse(response *clientv3.MemberRemoveResponse, cfg restoredSnapshotConfig,
	topology restoredClusterTopology, removedMemberID uint64,
) error {
	if response == nil || response.Header == nil || response.Header.ClusterId != topology.clusterID ||
		response.Header.MemberId == 0 || response.Header.RaftTerm == 0 || response.Header.Revision != 0 {
		return fmt.Errorf("invalid officially restored etcd MemberRemove response identity: %+v", response)
	}
	servingMember := false
	for _, memberID := range topology.memberIDs {
		servingMember = servingMember || response.Header.MemberId == memberID
	}
	if !servingMember {
		return fmt.Errorf("officially restored etcd MemberRemove response came from unknown member %x", response.Header.MemberId)
	}
	if len(response.Members) != len(cfg.members) {
		return fmt.Errorf("officially restored etcd MemberRemove member count mismatch: got=%d want=%d", len(response.Members), len(cfg.members))
	}
	expectedByID := make(map[uint64]restoredSnapshotMemberConfig, len(cfg.members))
	for index, memberID := range topology.memberIDs {
		expectedByID[memberID] = cfg.members[index]
	}
	seen := make(map[uint64]struct{}, len(response.Members))
	for _, member := range response.Members {
		if member == nil || member.ID == 0 || member.ID == removedMemberID || member.IsLearner {
			return fmt.Errorf("officially restored etcd MemberRemove returned an invalid remaining voter: %+v", member)
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return fmt.Errorf("officially restored etcd MemberRemove repeated member ID %x", member.ID)
		}
		seen[member.ID] = struct{}{}
		expected, exists := expectedByID[member.ID]
		if !exists || member.Name != expected.name || len(member.PeerURLs) != 1 || member.PeerURLs[0] != expected.peerURL.String() ||
			len(member.ClientURLs) != 1 || member.ClientURLs[0] != expected.clientURL.String() {
			return fmt.Errorf("officially restored etcd MemberRemove changed a remaining voter: %+v", member)
		}
	}
	return nil
}

func removeRestoredSnapshotMember(ctx context.Context, client *clientv3.Client, memberID uint64) (*clientv3.MemberRemoveResponse, error) {
	for {
		response, err := client.MemberRemove(ctx, memberID)
		if err == nil {
			return response, nil
		}
		if !errors.Is(err, rpctypes.ErrUnhealthy) && !errors.Is(err, rpctypes.ErrMemberNotEnoughStarted) {
			return nil, err
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for officially restored etcd cluster to become safe for MemberRemove: %w", context.Cause(ctx))
		}
	}
}

func verifyRestoredMemberPromoteRejections(ctx context.Context, followerClient *clientv3.Client,
	topology restoredClusterTopology,
) error {
	if followerClient == nil || len(topology.memberIDs) == 0 {
		return errors.New("restored MemberPromote rejection verification requires a follower client and member IDs")
	}
	if _, err := followerClient.MemberPromote(ctx, topology.memberIDs[0]); !errors.Is(rpctypes.Error(err), rpctypes.ErrMemberNotLearner) {
		return fmt.Errorf("promote existing restored voter must fail with ErrMemberNotLearner: %w", err)
	}
	existingMemberIDs := make(map[uint64]struct{}, len(topology.memberIDs))
	for _, memberID := range topology.memberIDs {
		existingMemberIDs[memberID] = struct{}{}
	}
	missingMemberID := ^uint64(0)
	for {
		if _, exists := existingMemberIDs[missingMemberID]; !exists {
			break
		}
		missingMemberID--
	}
	if _, err := followerClient.MemberPromote(ctx, missingMemberID); !errors.Is(rpctypes.Error(err), rpctypes.ErrMemberNotFound) {
		return fmt.Errorf("promote absent restored member must fail with ErrMemberNotFound: %w", err)
	}
	return nil
}

func validateRestoredMemberRaftAppliedStatus(response *clientv3.StatusResponse, topology restoredClusterTopology,
	memberID uint64,
) (bool, error) {
	if response == nil || response.Header == nil {
		return false, errors.New("officially restored etcd member returned a nil Raft apply-barrier status or header")
	}
	if response.Header.ClusterId != topology.clusterID || response.Header.MemberId != memberID ||
		response.Header.RaftTerm == 0 || response.RaftTerm == 0 || response.Header.RaftTerm != response.RaftTerm {
		return false, fmt.Errorf("officially restored etcd member returned invalid Raft apply-barrier identity: %+v", response)
	}
	knownLeader := false
	for _, knownMemberID := range topology.memberIDs {
		knownLeader = knownLeader || response.Leader == knownMemberID
	}
	if !knownLeader || response.Leader != topology.leaderID || response.IsLearner || len(response.Errors) != 0 {
		return false, fmt.Errorf("officially restored etcd member returned invalid Raft apply-barrier state: %+v", response)
	}
	if response.RaftAppliedIndex > response.RaftIndex {
		return false, fmt.Errorf("officially restored etcd member applied Raft index %d beyond committed index %d",
			response.RaftAppliedIndex, response.RaftIndex)
	}
	return response.RaftIndex > 0 && response.RaftAppliedIndex == response.RaftIndex, nil
}

func waitForRestoredMemberRaftApplied(ctx context.Context, client *clientv3.Client, endpoint string,
	topology restoredClusterTopology, memberID uint64,
) error {
	if client == nil || endpoint == "" || topology.clusterID == 0 || topology.leaderID == 0 || memberID == 0 {
		return errors.New("restored Raft apply barrier requires a client, endpoint, and complete cluster identity")
	}
	var lastResponse *clientv3.StatusResponse
	var lastErr error
	var stableSince time.Time
	var stableRaftIndex uint64
	for {
		response, err := client.Status(ctx, endpoint)
		lastResponse, lastErr = response, err
		if err == nil {
			ready, validationErr := validateRestoredMemberRaftAppliedStatus(response, topology, memberID)
			if validationErr != nil {
				return validationErr
			}
			if ready {
				now := time.Now()
				if stableSince.IsZero() || stableRaftIndex != response.RaftIndex {
					stableSince, stableRaftIndex = now, response.RaftIndex
				} else if now.Sub(stableSince) >= restoredSnapshotRaftQuiescence {
					return nil
				}
			} else {
				stableSince, stableRaftIndex = time.Time{}, 0
			}
		} else {
			stableSince, stableRaftIndex = time.Time{}, 0
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return fmt.Errorf("wait for officially restored etcd member %x Raft apply barrier: response=%+v last_error=%v: %w",
				memberID, lastResponse, lastErr, context.Cause(ctx))
		}
	}
}

func verifyRestoredClusterLearnerLifecycle(ctx context.Context, adminClient *clientv3.Client, adminConfig clientv3.Config,
	cfg restoredSnapshotConfig, topology restoredClusterTopology, directClients []*clientv3.Client,
	expected []streamProbeExpectation, catchUpKey, catchUpValue string, catchUpRevision int64,
) (probeKeys []string, finalRevision int64, retErr error) {
	if len(topology.memberIDs) != len(cfg.members) || len(directClients) != len(cfg.members) {
		return nil, 0, fmt.Errorf("restored learner verification requires matching voters, IDs, and clients: voters=%d ids=%d clients=%d",
			len(cfg.members), len(topology.memberIDs), len(directClients))
	}

	clientURL, peerURL, err := allocateRestoredSnapshotURLs(cfg.tls.enabled())
	if err != nil {
		return nil, 0, err
	}
	learner := restoredSnapshotMemberConfig{
		name: "kubebrain-rollout-restore-learner", dataDir: filepath.Join(filepath.Dir(cfg.members[0].dataDir), "member-learner"),
		clientURL: clientURL, peerURL: peerURL,
	}
	addResponse, err := addRestoredSnapshotLearner(ctx, adminClient, learner.peerURL.String())
	if err != nil {
		return nil, 0, fmt.Errorf("add learner to officially restored etcd cluster: %w", err)
	}
	learnerID, err := validateRestoredMemberAddLearnerResponse(addResponse, cfg, topology, learner)
	if err != nil {
		return nil, 0, err
	}
	if _, err := adminClient.MemberPromote(ctx, learnerID); !errors.Is(rpctypes.Error(err), rpctypes.ErrMemberLearnerNotReady) {
		return nil, 0, fmt.Errorf("promote unstarted restored learner must fail with ErrMemberLearnerNotReady: %w", err)
	}
	memberList, err := adminClient.MemberList(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("list restored members after rejected learner promotion: %w", err)
	}
	if err := validateRestoredLearnerMembers(memberList.Header, memberList.Members, cfg, topology, learner, learnerID, false); err != nil {
		return nil, 0, fmt.Errorf("verify membership after rejected learner promotion: %w", err)
	}

	expandedCfg := cfg
	expandedCfg.members = append(append([]restoredSnapshotMemberConfig(nil), cfg.members...), learner)
	initialCluster := make([]string, len(expandedCfg.members))
	for index, member := range expandedCfg.members {
		initialCluster[index] = member.name + "=" + member.peerURL.String()
	}
	expandedCfg.initialCluster = strings.Join(initialCluster, ",")
	if err := expandedCfg.validate(); err != nil {
		return nil, 0, fmt.Errorf("validate learner-expanded restored Snapshot cluster: %w", err)
	}
	learnerServer, err := embed.StartEtcd(newRestoredSnapshotEmbedConfig(expandedCfg, learner, expandedCfg.initialCluster))
	if err != nil {
		return nil, 0, fmt.Errorf("start officially restored etcd learner %q: %w", learner.name, err)
	}
	learnerServerClosed := false
	defer func() {
		if !learnerServerClosed {
			learnerServer.Close()
		}
	}()
	if err := waitForRestoredSnapshotMember(ctx, learner, learnerServer); err != nil {
		return nil, 0, err
	}

	learnerClientConfig := adminConfig
	if learnerClientConfig.Username != "" {
		authResponse, authErr := adminClient.Authenticate(ctx, learnerClientConfig.Username, learnerClientConfig.Password)
		if authErr != nil {
			return nil, 0, fmt.Errorf("obtain restored administrator token for direct learner verification: %w", authErr)
		}
		if authResponse == nil || authResponse.Token == "" {
			return nil, 0, fmt.Errorf("restored administrator authentication returned an empty token: %+v", authResponse)
		}
		learnerClientConfig.Username = ""
		learnerClientConfig.Password = ""
		learnerClientConfig.Token = authResponse.Token
	}
	learnerClient, err := newDirectRestoredClient(learnerClientConfig, learner.clientURL.String())
	if err != nil {
		return nil, 0, fmt.Errorf("create direct officially restored etcd learner client: %w", err)
	}
	learnerClientClosed := false
	defer func() {
		if !learnerClientClosed {
			if closeErr := learnerClient.Close(); closeErr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("close direct officially restored etcd learner client: %w", closeErr))
			}
		}
	}()
	if err := waitForRestoredLearnerMembership(ctx, adminClient, cfg, topology, learner, learnerID); err != nil {
		return nil, 0, err
	}
	if err := waitForRestoredMemberValue(ctx, learnerClient, catchUpKey, catchUpValue, catchUpRevision); err != nil {
		return nil, 0, fmt.Errorf("verify restored learner caught up before promotion: %w", err)
	}
	learnerStatus, err := learnerClient.Status(ctx, learner.clientURL.String())
	if err != nil {
		return nil, 0, fmt.Errorf("read officially restored etcd learner status: %w", err)
	}
	if learnerStatus == nil {
		return nil, 0, errors.New("officially restored etcd learner returned a nil status before promotion")
	}
	knownLeader := false
	for _, memberID := range topology.memberIDs {
		knownLeader = knownLeader || learnerStatus.Leader == memberID
	}
	if learnerStatus.Header == nil || learnerStatus.Header.ClusterId != topology.clusterID ||
		learnerStatus.Header.MemberId != learnerID || learnerStatus.Header.Revision < catchUpRevision || learnerStatus.Header.RaftTerm == 0 ||
		!learnerStatus.IsLearner || !knownLeader || len(learnerStatus.Errors) != 0 {
		return nil, 0, fmt.Errorf("invalid officially restored etcd learner status before promotion: %+v", learnerStatus)
	}
	serializableResponse, err := learnerClient.Get(ctx, catchUpKey, clientv3.WithSerializable())
	if err != nil || serializableResponse == nil || serializableResponse.Header == nil ||
		serializableResponse.Header.ClusterId != topology.clusterID || serializableResponse.Header.MemberId != learnerID ||
		serializableResponse.Header.Revision < catchUpRevision || serializableResponse.Count != 1 || len(serializableResponse.Kvs) != 1 ||
		string(serializableResponse.Kvs[0].Key) != catchUpKey || string(serializableResponse.Kvs[0].Value) != catchUpValue {
		return nil, 0, fmt.Errorf("restored learner serializable read contract failed: response=%+v err=%v", serializableResponse, err)
	}
	rejectedKey := "/kubebrain-rollout-restore/learner-rejected-mutation"
	rejectedOperations := []struct {
		name string
		op   clientv3.Op
	}{
		{name: "linearizable range", op: clientv3.OpGet(catchUpKey)},
		{name: "put", op: clientv3.OpPut(rejectedKey, "must-not-commit")},
		{name: "delete", op: clientv3.OpDelete(rejectedKey)},
		{name: "transaction", op: clientv3.OpTxn([]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(rejectedKey), "=", 0)}, nil, nil)},
	}
	for _, operation := range rejectedOperations {
		if _, err := learnerClient.Do(ctx, operation.op); !errors.Is(rpctypes.Error(err), rpctypes.Error(rpctypes.ErrGRPCNotSupportedForLearner)) {
			return nil, 0, fmt.Errorf("restored learner %s must fail with ErrGRPCNotSupportedForLearner: %w", operation.name, err)
		}
	}

	promoteResponse, err := promoteRestoredSnapshotLearner(ctx, adminClient, learnerID)
	if err != nil {
		return nil, 0, fmt.Errorf("promote caught-up officially restored etcd learner: %w", err)
	}
	expandedTopology := restoredClusterTopology{
		clusterID: topology.clusterID, leaderID: topology.leaderID,
		memberIDs: append(append([]uint64(nil), topology.memberIDs...), learnerID),
	}
	if err := validateRestoredMemberPromoteResponse(promoteResponse, expandedCfg, expandedTopology, learnerID); err != nil {
		return nil, 0, err
	}
	promotedKey := "/kubebrain-rollout-restore/learner-promoted-replication"
	promotedValue := fmt.Sprintf("promoted-%x", learnerID)
	promotedPut, err := adminClient.Put(ctx, promotedKey, promotedValue)
	if err != nil {
		return nil, 0, fmt.Errorf("write through learner-promoted officially restored etcd cluster: %w", err)
	}
	if promotedPut == nil || promotedPut.Header == nil || promotedPut.Header.ClusterId != topology.clusterID ||
		promotedPut.Header.Revision <= catchUpRevision {
		return nil, 0, fmt.Errorf("invalid learner-promoted officially restored etcd write response: %+v", promotedPut)
	}
	for index, directClient := range append(append([]*clientv3.Client(nil), directClients...), learnerClient) {
		if err := waitForRestoredMemberValue(ctx, directClient, promotedKey, promotedValue, promotedPut.Header.Revision); err != nil {
			return nil, 0, fmt.Errorf("verify learner-promoted cluster value on member %d: %w", index, err)
		}
	}
	verifiedExpandedTopology, err := verifyRestoredClusterTopology(ctx, adminClient, expandedCfg, promotedPut.Header.Revision)
	if err != nil {
		return nil, 0, fmt.Errorf("verify learner-promoted officially restored etcd topology: %w", err)
	}
	for index, memberID := range expandedTopology.memberIDs {
		if verifiedExpandedTopology.memberIDs[index] != memberID {
			return nil, 0, fmt.Errorf("officially restored etcd changed member ID during learner promotion at %d: got=%x want=%x",
				index, verifiedExpandedTopology.memberIDs[index], memberID)
		}
	}
	if err := verifyRestoredMemberCurrentSeeds(ctx, adminClient, learnerClient, expected, topology.clusterID, learnerID,
		promotedPut.Header.Revision); err != nil {
		return nil, 0, err
	}
	if err := verifyRestoredMemberAuthState(ctx, adminClient, learnerClient, topology.clusterID, learnerID,
		promotedPut.Header.Revision); err != nil {
		return nil, 0, err
	}

	removeResponse, err := removeRestoredSnapshotMember(ctx, adminClient, learnerID)
	if err != nil {
		return nil, 0, fmt.Errorf("remove promoted learner from officially restored etcd cluster: %w", err)
	}
	if err := validateRestoredMemberRemoveResponse(removeResponse, cfg, topology, learnerID); err != nil {
		return nil, 0, err
	}
	select {
	case <-learnerServer.Server.StopNotify():
	case <-ctx.Done():
		return nil, 0, fmt.Errorf("wait for removed promoted learner %q to stop: %w", learner.name, context.Cause(ctx))
	}
	learnerClientClosed = true
	if closeErr := learnerClient.Close(); closeErr != nil {
		return nil, 0, fmt.Errorf("close removed promoted learner client: %w", closeErr)
	}
	learnerServer.Close()
	learnerServerClosed = true
	if err := waitForRestoredMemberListenerClosed(ctx, &learner.clientURL); err != nil {
		return nil, 0, fmt.Errorf("verify removed promoted learner %q listener stopped: %w", learner.name, err)
	}

	contractedKey := "/kubebrain-rollout-restore/three-member-after-learner"
	contractedValue := fmt.Sprintf("removed-promoted-%x", learnerID)
	contractedPut, err := adminClient.Put(ctx, contractedKey, contractedValue)
	if err != nil {
		return nil, 0, fmt.Errorf("write after removing promoted learner: %w", err)
	}
	if contractedPut == nil || contractedPut.Header == nil || contractedPut.Header.ClusterId != topology.clusterID ||
		contractedPut.Header.Revision <= promotedPut.Header.Revision {
		return nil, 0, fmt.Errorf("invalid write after removing promoted learner: %+v", contractedPut)
	}
	for index, directClient := range directClients {
		if err := waitForRestoredMemberValue(ctx, directClient, contractedKey, contractedValue, contractedPut.Header.Revision); err != nil {
			return nil, 0, fmt.Errorf("verify post-learner contraction value on voter %d: %w", index, err)
		}
	}
	contractedTopology, err := verifyRestoredClusterTopology(ctx, adminClient, cfg, contractedPut.Header.Revision)
	if err != nil {
		return nil, 0, fmt.Errorf("verify topology after removing promoted learner: %w", err)
	}
	for index, memberID := range topology.memberIDs {
		if contractedTopology.memberIDs[index] != memberID {
			return nil, 0, fmt.Errorf("officially restored etcd changed member ID after learner contraction at %d: got=%x want=%x",
				index, contractedTopology.memberIDs[index], memberID)
		}
	}
	return []string{promotedKey, contractedKey}, contractedPut.Header.Revision, nil
}

func verifyRestoredClusterMemberReconfiguration(ctx context.Context, adminClient *clientv3.Client, adminConfig clientv3.Config,
	cfg restoredSnapshotConfig, topology restoredClusterTopology, directClients []*clientv3.Client,
	expected []streamProbeExpectation, preJoinKey, preJoinValue string, preJoinRevision int64,
) (retErr error) {
	if len(topology.memberIDs) != len(cfg.members) || len(directClients) != len(cfg.members) {
		return fmt.Errorf("restored member reconfiguration requires matching voters, IDs, and clients: voters=%d ids=%d clients=%d",
			len(cfg.members), len(topology.memberIDs), len(directClients))
	}
	for index, directClient := range directClients {
		if err := waitForRestoredMemberRaftApplied(ctx, directClient, cfg.members[index].clientURL.String(), topology,
			topology.memberIDs[index]); err != nil {
			return fmt.Errorf("establish restored member %q Raft apply barrier before reconfiguration: %w", cfg.members[index].name, err)
		}
	}
	followerIndex := -1
	for index, memberID := range topology.memberIDs {
		if memberID != topology.leaderID {
			followerIndex = index
			break
		}
	}
	if followerIndex < 0 || followerIndex >= len(directClients) {
		return errors.New("officially restored etcd cluster has no direct follower client for MemberPromote rejection verification")
	}
	if err := verifyRestoredMemberPromoteRejections(ctx, directClients[followerIndex], topology); err != nil {
		return err
	}
	clientURL, peerURL, err := allocateRestoredSnapshotURLs(cfg.tls.enabled())
	if err != nil {
		return err
	}
	added := restoredSnapshotMemberConfig{
		name: "kubebrain-rollout-restore-added", dataDir: filepath.Join(filepath.Dir(cfg.members[0].dataDir), "member-added"),
		clientURL: clientURL, peerURL: peerURL,
	}
	memberAddResponse, err := addRestoredSnapshotMember(ctx, adminClient, added.peerURL.String())
	if err != nil {
		return fmt.Errorf("add voter to officially restored etcd cluster: %w", err)
	}
	addedMemberID, err := validateRestoredMemberAddResponse(memberAddResponse, cfg, topology, added)
	if err != nil {
		return err
	}
	expandedCfg := cfg
	expandedCfg.members = append(append([]restoredSnapshotMemberConfig(nil), cfg.members...), added)
	initialCluster := make([]string, len(expandedCfg.members))
	for index, member := range expandedCfg.members {
		initialCluster[index] = member.name + "=" + member.peerURL.String()
	}
	expandedCfg.initialCluster = strings.Join(initialCluster, ",")
	if err := expandedCfg.validate(); err != nil {
		return fmt.Errorf("validate expanded restored Snapshot cluster: %w", err)
	}
	addedServer, err := embed.StartEtcd(newRestoredSnapshotEmbedConfig(expandedCfg, added, expandedCfg.initialCluster))
	if err != nil {
		return fmt.Errorf("start added officially restored etcd member %q: %w", added.name, err)
	}
	addedServerClosed := false
	defer func() {
		if !addedServerClosed {
			addedServer.Close()
		}
	}()
	if err := waitForRestoredSnapshotMember(ctx, added, addedServer); err != nil {
		return err
	}
	addedClient, err := newDirectRestoredClient(adminConfig, added.clientURL.String())
	if err != nil {
		return fmt.Errorf("create added officially restored etcd member client: %w", err)
	}
	addedClientClosed := false
	defer func() {
		if !addedClientClosed {
			if closeErr := addedClient.Close(); closeErr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("close added officially restored etcd member client: %w", closeErr))
			}
		}
	}()

	postJoinKey := "/kubebrain-rollout-restore/four-member-replication"
	postJoinValue := fmt.Sprintf("added-%x", addedMemberID)
	postJoinPut, err := adminClient.Put(ctx, postJoinKey, postJoinValue)
	if err != nil {
		return fmt.Errorf("write through expanded officially restored etcd cluster: %w", err)
	}
	if postJoinPut == nil || postJoinPut.Header == nil || postJoinPut.Header.ClusterId != topology.clusterID ||
		postJoinPut.Header.Revision <= preJoinRevision {
		return fmt.Errorf("invalid expanded officially restored etcd write response: %+v", postJoinPut)
	}
	for index, directClient := range append(append([]*clientv3.Client(nil), directClients...), addedClient) {
		if err := waitForRestoredMemberValue(ctx, directClient, postJoinKey, postJoinValue, postJoinPut.Header.Revision); err != nil {
			return fmt.Errorf("verify expanded cluster value on member %d: %w", index, err)
		}
	}
	expandedTopology, err := verifyRestoredClusterTopology(ctx, adminClient, expandedCfg, postJoinPut.Header.Revision)
	if err != nil {
		return fmt.Errorf("verify expanded officially restored etcd topology after replication barrier: %w", err)
	}
	for index, memberID := range topology.memberIDs {
		if expandedTopology.memberIDs[index] != memberID {
			return fmt.Errorf("officially restored etcd changed member ID during expansion at %d: got=%x want=%x",
				index, expandedTopology.memberIDs[index], memberID)
		}
	}
	if expandedTopology.memberIDs[len(expandedTopology.memberIDs)-1] != addedMemberID {
		return fmt.Errorf("officially restored etcd added member ID mismatch: got=%x want=%x",
			expandedTopology.memberIDs[len(expandedTopology.memberIDs)-1], addedMemberID)
	}
	if err := waitForRestoredMemberValue(ctx, addedClient, preJoinKey, preJoinValue, preJoinRevision); err != nil {
		return fmt.Errorf("verify pre-join value on added restored member: %w", err)
	}
	if err := verifyRestoredMemberCurrentSeeds(ctx, adminClient, addedClient, expected, topology.clusterID, addedMemberID,
		postJoinPut.Header.Revision); err != nil {
		return err
	}
	if err := verifyRestoredMemberAuthState(ctx, adminClient, addedClient, topology.clusterID, addedMemberID,
		postJoinPut.Header.Revision); err != nil {
		return err
	}
	memberRemoveResponse, err := removeRestoredSnapshotMember(ctx, adminClient, addedMemberID)
	if err != nil {
		return fmt.Errorf("remove added voter from officially restored etcd cluster: %w", err)
	}
	if err := validateRestoredMemberRemoveResponse(memberRemoveResponse, cfg, topology, addedMemberID); err != nil {
		return err
	}
	select {
	case <-addedServer.Server.StopNotify():
	case <-ctx.Done():
		return fmt.Errorf("wait for removed officially restored etcd member %q to stop: %w", added.name, context.Cause(ctx))
	}
	addedClientClosed = true
	if closeErr := addedClient.Close(); closeErr != nil {
		retErr = errors.Join(retErr, fmt.Errorf("close added officially restored etcd member client: %w", closeErr))
		return retErr
	}
	addedServer.Close()
	addedServerClosed = true
	if err := waitForRestoredMemberListenerClosed(ctx, &added.clientURL); err != nil {
		return fmt.Errorf("verify removed officially restored etcd member %q listener stopped: %w", added.name, err)
	}

	postRemoveKey := "/kubebrain-rollout-restore/three-member-replication"
	postRemoveValue := fmt.Sprintf("removed-%x", addedMemberID)
	postRemovePut, err := adminClient.Put(ctx, postRemoveKey, postRemoveValue)
	if err != nil {
		return fmt.Errorf("write through contracted officially restored etcd cluster: %w", err)
	}
	if postRemovePut == nil || postRemovePut.Header == nil || postRemovePut.Header.ClusterId != topology.clusterID ||
		postRemovePut.Header.Revision <= postJoinPut.Header.Revision {
		return fmt.Errorf("invalid contracted officially restored etcd write response: %+v", postRemovePut)
	}
	for index, directClient := range directClients {
		if err := waitForRestoredMemberValue(ctx, directClient, postRemoveKey, postRemoveValue, postRemovePut.Header.Revision); err != nil {
			return fmt.Errorf("verify contracted cluster value on member %d: %w", index, err)
		}
	}
	contractedTopology, err := verifyRestoredClusterTopology(ctx, adminClient, cfg, postRemovePut.Header.Revision)
	if err != nil {
		return fmt.Errorf("verify contracted officially restored etcd topology after replication barrier: %w", err)
	}
	for index, memberID := range topology.memberIDs {
		if contractedTopology.memberIDs[index] != memberID {
			return fmt.Errorf("officially restored etcd changed member ID during contraction at %d: got=%x want=%x",
				index, contractedTopology.memberIDs[index], memberID)
		}
	}
	learnerProbeKeys, learnerFinalRevision, err := verifyRestoredClusterLearnerLifecycle(ctx, adminClient, adminConfig,
		cfg, topology, directClients, expected, postRemoveKey, postRemoveValue, postRemovePut.Header.Revision)
	if err != nil {
		return err
	}

	probeKeys := append([]string{preJoinKey, postJoinKey, postRemoveKey}, learnerProbeKeys...)
	deleteOperations := make([]clientv3.Op, len(probeKeys))
	for index, key := range probeKeys {
		deleteOperations[index] = clientv3.OpDelete(key)
	}
	deleteResponse, err := adminClient.Txn(ctx).Then(deleteOperations...).Commit()
	if err != nil {
		return fmt.Errorf("delete officially restored etcd reconfiguration probes: %w", err)
	}
	if deleteResponse == nil || deleteResponse.Header == nil || deleteResponse.Header.ClusterId != topology.clusterID ||
		deleteResponse.Header.Revision <= learnerFinalRevision || len(deleteResponse.Responses) != len(probeKeys) {
		return fmt.Errorf("invalid officially restored etcd reconfiguration probe deletion response: %+v", deleteResponse)
	}
	for index, response := range deleteResponse.Responses {
		if response.GetResponseDeleteRange() == nil || response.GetResponseDeleteRange().Deleted != 1 {
			return fmt.Errorf("invalid officially restored etcd reconfiguration probe deletion at %d for %q: %+v",
				index, probeKeys[index], response)
		}
	}
	for index, directClient := range directClients {
		for _, key := range probeKeys {
			if err := waitForRestoredMemberMissing(ctx, directClient, key, deleteResponse.Header.Revision); err != nil {
				return fmt.Errorf("verify reconfiguration probe deletion on member %d key %q: %w", index, key, err)
			}
		}
	}
	return nil
}

func verifyRestoredClusterReplicationAndQuorum(ctx context.Context, adminClient *clientv3.Client, adminConfig clientv3.Config,
	cfg restoredSnapshotConfig, topology restoredClusterTopology, servers []*embed.Etcd, expected []streamProbeExpectation, revision int64,
) (retErr error) {
	if len(cfg.members) < 3 || len(topology.memberIDs) != len(cfg.members) || len(servers) != len(cfg.members) {
		return fmt.Errorf("restored Snapshot quorum verification requires matching cluster state with at least three members: members=%d ids=%d servers=%d",
			len(cfg.members), len(topology.memberIDs), len(servers))
	}
	directClients := make([]*clientv3.Client, len(cfg.members))
	defer func() {
		for index, directClient := range directClients {
			if directClient != nil {
				if closeErr := directClient.Close(); closeErr != nil {
					retErr = errors.Join(retErr, fmt.Errorf("close officially restored etcd member %q client: %w", cfg.members[index].name, closeErr))
				}
			}
		}
	}()
	for index, member := range cfg.members {
		var err error
		directClients[index], err = newDirectRestoredClient(adminConfig, member.clientURL.String())
		if err != nil {
			return fmt.Errorf("create officially restored etcd member %q client: %w", member.name, err)
		}
	}

	replicationKey := "/kubebrain-rollout-restore/three-member-replication"
	replicationValue := fmt.Sprintf("cluster-%x-revision-%d", topology.clusterID, revision)
	putResponse, err := adminClient.Put(ctx, replicationKey, replicationValue)
	if err != nil {
		return fmt.Errorf("write officially restored etcd replication probe: %w", err)
	}
	if putResponse == nil || putResponse.Header == nil || putResponse.Header.ClusterId != topology.clusterID || putResponse.Header.Revision <= revision {
		return fmt.Errorf("invalid officially restored etcd replication write response: %+v", putResponse)
	}
	for index, directClient := range directClients {
		if err := waitForRestoredMemberValue(ctx, directClient, replicationKey, replicationValue, putResponse.Header.Revision); err != nil {
			return fmt.Errorf("verify replicated value on officially restored member %q: %w", cfg.members[index].name, err)
		}
	}
	deleteResponse, err := adminClient.Delete(ctx, replicationKey)
	if err != nil || deleteResponse == nil || deleteResponse.Header == nil || deleteResponse.Header.ClusterId != topology.clusterID || deleteResponse.Deleted != 1 {
		return fmt.Errorf("delete officially restored etcd replication probe: response=%+v err=%v", deleteResponse, err)
	}

	stopped := -1
	for index, memberID := range topology.memberIDs {
		if memberID != topology.leaderID {
			stopped = index
			break
		}
	}
	if stopped < 0 || servers[stopped] == nil {
		return errors.New("officially restored etcd cluster has no stoppable follower")
	}
	if closeErr := directClients[stopped].Close(); closeErr != nil {
		return fmt.Errorf("close officially restored etcd follower %q client before stop: %w", cfg.members[stopped].name, closeErr)
	}
	directClients[stopped] = nil
	servers[stopped].Close()
	servers[stopped] = nil
	survivorEndpoints := make([]string, 0, len(cfg.members)-1)
	for index, member := range cfg.members {
		if index != stopped {
			survivorEndpoints = append(survivorEndpoints, member.clientURL.String())
		}
	}
	quorumConfig := adminConfig
	quorumConfig.Endpoints = survivorEndpoints
	quorumConfig.Logger = zap.NewNop()
	quorumClient, err := clientv3.New(quorumConfig)
	if err != nil {
		return fmt.Errorf("create officially restored etcd quorum client: %w", err)
	}
	defer func() {
		if closeErr := quorumClient.Close(); closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close officially restored etcd quorum client: %w", closeErr))
		}
	}()
	quorumKey := "/kubebrain-rollout-restore/two-of-three-quorum"
	quorumValue := fmt.Sprintf("stopped-%x", topology.memberIDs[stopped])
	quorumPut, err := quorumClient.Put(ctx, quorumKey, quorumValue)
	if err != nil {
		return fmt.Errorf("write through officially restored etcd two-of-three quorum: %w", err)
	}
	if quorumPut == nil || quorumPut.Header == nil || quorumPut.Header.ClusterId != topology.clusterID || quorumPut.Header.Revision <= deleteResponse.Header.Revision {
		return fmt.Errorf("invalid officially restored etcd quorum write response: %+v", quorumPut)
	}
	for index, directClient := range directClients {
		if index == stopped {
			continue
		}
		if err := waitForRestoredMemberValue(ctx, directClient, quorumKey, quorumValue, quorumPut.Header.Revision); err != nil {
			return fmt.Errorf("verify quorum value on surviving officially restored member %q: %w", cfg.members[index].name, err)
		}
	}

	restarted, err := embed.StartEtcd(newRestoredSnapshotEmbedConfig(cfg, cfg.members[stopped], cfg.initialCluster))
	if err != nil {
		return fmt.Errorf("restart stopped officially restored etcd follower %q: %w", cfg.members[stopped].name, err)
	}
	servers[stopped] = restarted
	if err := waitForRestoredSnapshotMember(ctx, cfg.members[stopped], restarted); err != nil {
		return err
	}
	directClients[stopped], err = newDirectRestoredClient(adminConfig, cfg.members[stopped].clientURL.String())
	if err != nil {
		return fmt.Errorf("recreate officially restored etcd follower %q client: %w", cfg.members[stopped].name, err)
	}
	if err := waitForRestoredMemberValue(ctx, directClients[stopped], quorumKey, quorumValue, quorumPut.Header.Revision); err != nil {
		return fmt.Errorf("verify recovered follower %q caught up to quorum write: %w", cfg.members[stopped].name, err)
	}
	recoveredTopology, err := verifyRestoredClusterTopology(ctx, adminClient, cfg, quorumPut.Header.Revision)
	if err != nil {
		return fmt.Errorf("verify recovered officially restored etcd topology: %w", err)
	}
	for index, memberID := range topology.memberIDs {
		if recoveredTopology.memberIDs[index] != memberID {
			return fmt.Errorf("officially restored etcd changed member ID during follower recovery at %d: got=%x want=%x",
				index, recoveredTopology.memberIDs[index], memberID)
		}
	}
	return verifyRestoredClusterMemberReconfiguration(ctx, adminClient, adminConfig, cfg, recoveredTopology, directClients,
		expected, quorumKey, quorumValue, quorumPut.Header.Revision)
}

func verifyRestoredSnapshot(ctx context.Context, cfg restoredSnapshotConfig, expected []streamProbeExpectation, revision int64) (retErr error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	verifyCtx, cancel := context.WithTimeout(ctx, restoredSnapshotVerificationTimeout)
	defer cancel()

	var (
		clientTLSConfig *tls.Config
		err             error
	)
	if cfg.tls.enabled() {
		clientTLSConfig, err = (transport.TLSInfo{
			TrustedCAFile: cfg.tls.caFile,
			CertFile:      cfg.tls.certFile,
			KeyFile:       cfg.tls.keyFile,
			ServerName:    cfg.tls.serverName,
		}).ClientConfig()
		if err != nil {
			return fmt.Errorf("configure client for officially restored etcd TLS: %w", err)
		}
	}
	restored := make([]*embed.Etcd, len(cfg.members))
	defer func() {
		for index := len(restored) - 1; index >= 0; index-- {
			if restored[index] != nil {
				restored[index].Close()
			}
		}
	}()
	for index, member := range cfg.members {
		restored[index], err = embed.StartEtcd(newRestoredSnapshotEmbedConfig(cfg, member, cfg.initialCluster))
		if err != nil {
			return fmt.Errorf("start officially restored etcd member %q: %w", member.name, err)
		}
	}
	for index, server := range restored {
		if err := waitForRestoredSnapshotMember(verifyCtx, cfg.members[index], server); err != nil {
			return err
		}
	}

	endpoints := make([]string, len(cfg.members))
	for index, member := range cfg.members {
		endpoints[index] = member.clientURL.String()
	}
	clientConfig := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
		Context:     verifyCtx,
		TLS:         clientTLSConfig,
		Logger:      zap.NewNop(),
	}
	if cfg.auth != nil {
		if (cfg.auth.adminUsername == "") != (cfg.auth.adminPassword == "") ||
			(!cfg.auth.enabled && cfg.auth.adminUsername != "") {
			return errors.New("restored auth administrator credentials require enabled auth and a non-empty username/password pair")
		}
		if cfg.auth.adminUsername != "" {
			clientConfig.Username = cfg.auth.adminUsername
			clientConfig.Password = cfg.auth.adminPassword
		}
	}
	if clientTLSConfig != nil {
		clientConfig.DialOptions = append(clientConfig.DialOptions, grpc.WithAuthority(cfg.tls.serverName))
	}
	client, err := clientv3.New(clientConfig)
	if err != nil {
		return fmt.Errorf("create client for officially restored etcd: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close officially restored etcd client: %w", closeErr))
		}
	}()
	topology, err := verifyRestoredClusterTopology(verifyCtx, client, cfg, revision)
	if err != nil {
		return err
	}
	verifyCluster := func(adminClient *clientv3.Client, adminConfig clientv3.Config) error {
		if len(cfg.members) == 1 {
			return nil
		}
		return verifyRestoredClusterReplicationAndQuorum(verifyCtx, adminClient, adminConfig, cfg, topology, restored, expected, revision)
	}

	validateHeader := func(header *etcdserverpb.ResponseHeader) error {
		if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.RaftTerm == 0 || header.Revision != revision {
			return fmt.Errorf("invalid restored response identity: header=%+v snapshot_revision=%d", header, revision)
		}
		return nil
	}
	if len(expected) == 0 {
		response, err := client.Get(verifyCtx, "\x00")
		if err != nil {
			return fmt.Errorf("read from officially restored etcd: %w", err)
		}
		if err := validateHeader(response.Header); err != nil {
			return err
		}
		return verifyRestoredSnapshotAuthWithAdmin(verifyCtx, client, clientConfig, cfg.auth, revision, verifyCluster)
	}
	type keyedExpectedEvent struct {
		key   string
		event streamProbeEventExpectation
	}
	seenKeys := make(map[string]struct{}, len(expected))
	expectedEvents := make([]keyedExpectedEvent, 0, len(expected))
	type historicalLeaseExpectation struct {
		grantedTTL   int64
		attachedKeys map[string]struct{}
	}
	expectedLeases := make(map[int64]*historicalLeaseExpectation)
	firstRevision := revision + 1
	commonPrefix := []byte(expected[0].key)
	for _, item := range expected {
		if item.key == "" || item.revision <= 0 || item.revision > revision {
			return fmt.Errorf("invalid restored seed expectation: key=%q revision=%d snapshot_revision=%d", item.key, item.revision, revision)
		}
		if _, duplicate := seenKeys[item.key]; duplicate {
			return fmt.Errorf("duplicate restored seed expectation for %q", item.key)
		}
		seenKeys[item.key] = struct{}{}
		events := item.events
		if len(events) == 0 {
			events = []streamProbeEventExpectation{{
				eventType: mvccpb.PUT, value: item.value, hash: item.hash, revision: item.revision,
				createRevision: item.revision, version: 1,
			}}
		}
		var previousRevision int64
		for _, event := range events {
			if event.revision <= 0 || event.revision > revision || event.revision <= previousRevision {
				return fmt.Errorf("invalid restored seed event expectation: key=%q revision=%d previous_revision=%d snapshot_revision=%d",
					item.key, event.revision, previousRevision, revision)
			}
			if event.subRevision < 0 || event.totalChanges < 0 ||
				(event.totalChanges == 0 && event.subRevision != 0) ||
				(event.totalChanges > 0 && event.subRevision >= event.totalChanges) {
				return fmt.Errorf("invalid restored seed event order expectation: key=%q revision=%d subrevision=%d total_changes=%d",
					item.key, event.revision, event.subRevision, event.totalChanges)
			}
			switch event.eventType {
			case mvccpb.PUT:
				if event.createRevision <= 0 || event.createRevision > event.revision || event.version <= 0 {
					return fmt.Errorf("invalid restored seed PUT expectation: key=%q revision=%d", item.key, event.revision)
				}
				if event.lease != 0 {
					if event.leaseGrantedTTL <= 0 {
						return fmt.Errorf("restored seed lease expectation is missing a granted TTL: key=%q revision=%d lease=%d",
							item.key, event.revision, event.lease)
					}
					expectedLease := expectedLeases[event.lease]
					if expectedLease == nil {
						expectedLeases[event.lease] = &historicalLeaseExpectation{
							grantedTTL: event.leaseGrantedTTL, attachedKeys: make(map[string]struct{}),
						}
					} else if expectedLease.grantedTTL != event.leaseGrantedTTL {
						return fmt.Errorf("inconsistent restored historical lease granted TTL expectation: lease=%d got=%d want=%d",
							event.lease, event.leaseGrantedTTL, expectedLease.grantedTTL)
					}
				} else if event.leaseGrantedTTL != 0 {
					return fmt.Errorf("unleased restored seed PUT has a granted TTL expectation: key=%q revision=%d",
						item.key, event.revision)
				}
			case mvccpb.DELETE:
				if event.value != "" || event.hash != ([sha256.Size]byte{}) || event.createRevision != 0 || event.version != 0 ||
					event.lease != 0 || event.leaseGrantedTTL != 0 {
					return fmt.Errorf("invalid restored seed DELETE expectation: key=%q revision=%d", item.key, event.revision)
				}
			default:
				return fmt.Errorf("invalid restored seed event type: key=%q type=%s", item.key, event.eventType)
			}
			expectedEvents = append(expectedEvents, keyedExpectedEvent{key: item.key, event: event})
			firstRevision = min(firstRevision, event.revision)
			previousRevision = event.revision
		}
		lastEvent := events[len(events)-1]
		if lastEvent.eventType != mvccpb.PUT || lastEvent.revision != item.revision ||
			lastEvent.hash != item.hash || lastEvent.value != item.value {
			return fmt.Errorf("restored seed event history does not end at current seed: key=%q", item.key)
		}
		if lastEvent.lease != 0 {
			expectedLeases[lastEvent.lease].attachedKeys[item.key] = struct{}{}
		}
		key := []byte(item.key)
		commonLength := min(len(commonPrefix), len(key))
		for commonLength > 0 && !bytes.Equal(commonPrefix[:commonLength], key[:commonLength]) {
			commonLength--
		}
		commonPrefix = commonPrefix[:commonLength]
	}
	if len(commonPrefix) == 0 {
		return errors.New("restored seed expectations do not share a non-empty Watch prefix")
	}
	sort.Slice(expectedEvents, func(left, right int) bool {
		if expectedEvents[left].event.revision != expectedEvents[right].event.revision {
			return expectedEvents[left].event.revision < expectedEvents[right].event.revision
		}
		return expectedEvents[left].event.subRevision < expectedEvents[right].event.subRevision
	})
	for start := 0; start < len(expectedEvents); {
		end := start + 1
		for end < len(expectedEvents) && expectedEvents[end].event.revision == expectedEvents[start].event.revision {
			end++
		}
		count := int64(end - start)
		for index := start; index < end; index++ {
			event := expectedEvents[index].event
			if count == 1 && event.totalChanges == 0 {
				continue
			}
			wantSub := int64(index - start)
			if event.totalChanges != count || event.subRevision != wantSub {
				return fmt.Errorf("incomplete restored seed event order expectation at revision %d: subrevision=%d total_changes=%d want_subrevision=%d want_total_changes=%d",
					event.revision, event.subRevision, event.totalChanges, wantSub, count)
			}
		}
		start = end
	}
	validateSeed := func(item streamProbeExpectation, current streamProbeEventExpectation, response *clientv3.GetResponse, historical bool) error {
		label := "current"
		if historical {
			label = "historical"
		}
		if response == nil {
			return fmt.Errorf("officially restored etcd returned an empty %s seed response for %q", label, item.key)
		}
		if err := validateHeader(response.Header); err != nil {
			return fmt.Errorf("validate restored %s seed %q: %w", label, item.key, err)
		}
		if response.More || response.Count != 1 || len(response.Kvs) != 1 {
			return fmt.Errorf("officially restored etcd returned invalid %s seed cardinality for %q: count=%d values=%d more=%t",
				label, item.key, response.Count, len(response.Kvs), response.More)
		}
		kv := response.Kvs[0]
		if kv == nil || string(kv.Key) != item.key || sha256.Sum256(kv.Value) != current.hash || kv.CreateRevision != current.createRevision ||
			kv.ModRevision != current.revision || kv.Version != current.version || kv.Lease != current.lease {
			return fmt.Errorf("officially restored etcd returned invalid %s seed data for %q", label, item.key)
		}
		return nil
	}
	for _, item := range expected {
		events := item.events
		if len(events) == 0 {
			events = []streamProbeEventExpectation{{
				eventType: mvccpb.PUT, value: item.value, hash: item.hash, revision: item.revision,
				createRevision: item.revision, version: 1,
			}}
		}
		for _, event := range events {
			response, err := client.Get(verifyCtx, item.key, clientv3.WithRev(event.revision))
			if err != nil {
				return fmt.Errorf("read historical seed %q at revision %d from officially restored etcd: %w", item.key, event.revision, err)
			}
			if err := validateHeader(response.Header); err != nil {
				return fmt.Errorf("validate restored historical seed %q at revision %d: %w", item.key, event.revision, err)
			}
			if event.eventType == mvccpb.DELETE {
				if response.More || response.Count != 0 || len(response.Kvs) != 0 {
					return fmt.Errorf("officially restored etcd returned data for deleted historical seed %q at revision %d", item.key, event.revision)
				}
				continue
			}
			if response.More || response.Count != 1 || len(response.Kvs) != 1 {
				return fmt.Errorf("officially restored etcd returned invalid historical seed cardinality for %q at revision %d: count=%d values=%d more=%t",
					item.key, event.revision, response.Count, len(response.Kvs), response.More)
			}
			kv := response.Kvs[0]
			if kv == nil || string(kv.Key) != item.key || sha256.Sum256(kv.Value) != event.hash || kv.CreateRevision != event.createRevision ||
				kv.ModRevision != event.revision || kv.Version != event.version || kv.Lease != event.lease {
				return fmt.Errorf("officially restored etcd returned invalid historical seed data for %q at revision %d", item.key, event.revision)
			}
		}
		response, err := client.Get(verifyCtx, item.key)
		if err != nil {
			return fmt.Errorf("read current seed %q from officially restored etcd: %w", item.key, err)
		}
		if err := validateSeed(item, events[len(events)-1], response, false); err != nil {
			return err
		}
	}
	leaseIDs := make([]int64, 0, len(expectedLeases))
	for leaseID := range expectedLeases {
		leaseIDs = append(leaseIDs, leaseID)
	}
	sort.Slice(leaseIDs, func(left, right int) bool { return leaseIDs[left] < leaseIDs[right] })
	for _, leaseID := range leaseIDs {
		expectedLease := expectedLeases[leaseID]
		response, err := client.TimeToLive(verifyCtx, clientv3.LeaseID(leaseID), clientv3.WithAttachedKeys())
		if err != nil {
			return fmt.Errorf("read restored historical lease %d: %w", leaseID, err)
		}
		if response == nil {
			return fmt.Errorf("officially restored etcd returned an empty historical lease response for %d", leaseID)
		}
		if err := validateHeader(response.ResponseHeader); err != nil {
			return fmt.Errorf("validate restored historical lease %d: %w", leaseID, err)
		}
		if int64(response.ID) != leaseID || response.TTL <= 0 || response.GrantedTTL != expectedLease.grantedTTL ||
			response.TTL > response.GrantedTTL {
			return fmt.Errorf("officially restored etcd returned invalid historical lease state for %d: id=%d ttl=%d granted_ttl=%d",
				leaseID, response.ID, response.TTL, response.GrantedTTL)
		}
		if len(response.Keys) != len(expectedLease.attachedKeys) {
			return fmt.Errorf("officially restored etcd returned invalid attached key count for historical lease %d: got=%d want=%d",
				leaseID, len(response.Keys), len(expectedLease.attachedKeys))
		}
		seenAttachedKeys := make(map[string]struct{}, len(response.Keys))
		for _, key := range response.Keys {
			if len(key) == 0 {
				return fmt.Errorf("officially restored etcd returned an empty attached key for historical lease %d", leaseID)
			}
			if _, duplicate := seenAttachedKeys[string(key)]; duplicate {
				return fmt.Errorf("officially restored etcd repeated attached key %q for historical lease %d", key, leaseID)
			}
			if _, expected := expectedLease.attachedKeys[string(key)]; !expected {
				return fmt.Errorf("officially restored etcd returned unexpected attached key %q for historical lease %d", key, leaseID)
			}
			seenAttachedKeys[string(key)] = struct{}{}
		}
	}

	watchCtx, stopWatch := context.WithCancel(verifyCtx)
	defer stopWatch()
	watch := client.Watch(watchCtx, string(commonPrefix), clientv3.WithPrefix(), clientv3.WithRev(firstRevision))
	observed := 0
	for observed < len(expectedEvents) {
		select {
		case response, ok := <-watch:
			if !ok {
				return fmt.Errorf("officially restored etcd historical seed Watch closed after %d/%d events", observed, len(expectedEvents))
			}
			if err := response.Err(); err != nil {
				return fmt.Errorf("watch restored historical seeds from revision %d: %w", firstRevision, err)
			}
			if response.Canceled || response.Created || response.CompactRevision != 0 || len(response.Events) == 0 {
				return fmt.Errorf("officially restored etcd returned an invalid historical seed Watch envelope: canceled=%t created=%t compact_revision=%d events=%d",
					response.Canceled, response.Created, response.CompactRevision, len(response.Events))
			}
			if err := validateHeader(response.Header); err != nil {
				return fmt.Errorf("validate restored historical seed Watch: %w", err)
			}
			for _, event := range response.Events {
				if event == nil || event.Kv == nil || event.PrevKv != nil {
					return errors.New("officially restored etcd returned an invalid historical seed Watch event")
				}
				if observed >= len(expectedEvents) {
					return fmt.Errorf("officially restored etcd historical seed Watch returned unexpected key %q", event.Kv.Key)
				}
				want := expectedEvents[observed]
				key := string(event.Kv.Key)
				if key != want.key {
					return fmt.Errorf("officially restored etcd historical seed Watch returned unexpected key %q", key)
				}
				if event.Type != want.event.eventType {
					return fmt.Errorf("officially restored etcd historical seed Watch returned unexpected event type for %q: got=%s want=%s", key, event.Type, want.event.eventType)
				}
				if want.event.eventType == mvccpb.DELETE {
					if len(event.Kv.Value) != 0 || event.Kv.CreateRevision != 0 || event.Kv.ModRevision != want.event.revision ||
						event.Kv.Version != 0 || event.Kv.Lease != 0 {
						return fmt.Errorf("officially restored etcd returned invalid historical seed Watch delete for %q", key)
					}
				} else if sha256.Sum256(event.Kv.Value) != want.event.hash || event.Kv.CreateRevision != want.event.createRevision ||
					event.Kv.ModRevision != want.event.revision || event.Kv.Version != want.event.version || event.Kv.Lease != want.event.lease {
					return fmt.Errorf("officially restored etcd returned invalid historical seed Watch data for %q", key)
				}
				observed++
			}
		case <-verifyCtx.Done():
			return fmt.Errorf("wait for officially restored etcd historical seed Watch: %w", context.Cause(verifyCtx))
		}
	}
	return verifyRestoredSnapshotAuthWithAdmin(verifyCtx, client, clientConfig, cfg.auth, revision, verifyCluster)
}

func validateSnapshotArtifactWithVerifier(ctx context.Context, manager snapshotArtifactManager, path, wireVersion, artifactDir string,
	expected []streamProbeExpectation, tlsCfg restoredSnapshotTLSConfig, verifier restoredSnapshotVerifier,
) error {
	return validateSnapshotArtifactWithAuthVerifier(ctx, manager, path, wireVersion, artifactDir, expected, tlsCfg, nil, verifier)
}

func validateSnapshotArtifactWithAuthVerifier(ctx context.Context, manager snapshotArtifactManager, path, wireVersion, artifactDir string,
	expected []streamProbeExpectation, tlsCfg restoredSnapshotTLSConfig, auth *restoredSnapshotAuthExpectation,
	verifier restoredSnapshotVerifier,
) error {
	return validateSnapshotArtifactWithClusterAuthVerifier(ctx, manager, path, wireVersion, artifactDir, expected, tlsCfg, auth, 1, verifier)
}

func validateSnapshotArtifactWithClusterAuthVerifier(ctx context.Context, manager snapshotArtifactManager, path, wireVersion, artifactDir string,
	expected []streamProbeExpectation, tlsCfg restoredSnapshotTLSConfig, auth *restoredSnapshotAuthExpectation, memberCount int,
	verifier restoredSnapshotVerifier,
) (retErr error) {
	if err := tlsCfg.validate(); err != nil {
		return err
	}
	artifactStatus, err := manager.Status(path)
	if err != nil {
		return fmt.Errorf("official etcdutl rejected Snapshot artifact: %w", err)
	}
	if artifactStatus.Revision <= 0 || artifactStatus.TotalSize <= 0 || artifactStatus.Version != wireVersion {
		return fmt.Errorf("official etcdutl returned invalid Snapshot status: revision=%d size=%d version=%q wire_version=%q",
			artifactStatus.Revision, artifactStatus.TotalSize, artifactStatus.Version, wireVersion)
	}

	restoreRoot, err := os.MkdirTemp(artifactDir, ".kubebrain-rollout-restore-*")
	if err != nil {
		return fmt.Errorf("create Snapshot restore directory: %w", err)
	}
	defer func() {
		if removeErr := os.RemoveAll(restoreRoot); removeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("remove Snapshot restore directory: %w", removeErr))
		}
	}()

	restoredCfg, err := newRestoredSnapshotConfig(restoreRoot, memberCount, tlsCfg, auth)
	if err != nil {
		return err
	}
	for _, member := range restoredCfg.members {
		if err := manager.Restore(etcdutlsnapshot.RestoreConfig{
			SnapshotPath: path, Name: member.name, OutputDataDir: member.dataDir, PeerURLs: []string{member.peerURL.String()},
			InitialCluster: restoredCfg.initialCluster, InitialClusterToken: restoredCfg.initialClusterToken, SkipHashCheck: false,
		}); err != nil {
			return fmt.Errorf("official etcdutl failed to restore Snapshot artifact for member %q: %w", member.name, err)
		}
		restoredDB := filepath.Join(member.dataDir, "member", "snap", "db")
		info, err := os.Stat(restoredDB)
		if err != nil {
			return fmt.Errorf("inspect officially restored Snapshot database for member %q: %w", member.name, err)
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 {
			return fmt.Errorf("official etcdutl produced an invalid restored Snapshot database for member %q: mode=%s size=%d",
				member.name, info.Mode(), info.Size())
		}
	}
	if verifier == nil {
		return errors.New("restored Snapshot verifier is required")
	}
	if err := verifier(ctx, restoredCfg, expected, artifactStatus.Revision); err != nil {
		return fmt.Errorf("official restored etcd validation failed: %w", err)
	}
	return nil
}

func consumeAndValidateSnapshot(ctx context.Context, stream snapshotReceiver, artifactDir string, expected []streamProbeExpectation,
	tlsCfg restoredSnapshotTLSConfig,
) (bool, error) {
	return consumeAndValidateSnapshotWithAuth(ctx, stream, artifactDir, expected, tlsCfg, nil)
}

func consumeAndValidateSnapshotWithAuth(ctx context.Context, stream snapshotReceiver, artifactDir string,
	expected []streamProbeExpectation, tlsCfg restoredSnapshotTLSConfig, auth *restoredSnapshotAuthExpectation,
) (bool, error) {
	return consumeAndValidateSnapshotWithClusterAuth(ctx, stream, artifactDir, expected, tlsCfg, auth, 1)
}

func consumeAndValidateSnapshotWithClusterAuth(ctx context.Context, stream snapshotReceiver, artifactDir string,
	expected []streamProbeExpectation, tlsCfg restoredSnapshotTLSConfig, auth *restoredSnapshotAuthExpectation, memberCount int,
) (partial bool, retErr error) {
	artifact, err := os.CreateTemp(artifactDir, ".kubebrain-rollout-snapshot-*.db")
	if err != nil {
		return false, fmt.Errorf("create Snapshot artifact: %w", err)
	}
	path := artifact.Name()
	defer func() {
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, fmt.Errorf("remove Snapshot artifact: %w", removeErr))
		}
	}()
	version := ""
	partial, version, err = consumeSnapshotTo(stream, artifact)
	if err == nil {
		err = artifact.Sync()
	}
	if closeErr := artifact.Close(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	if err != nil {
		return partial, err
	}
	if err := validateSnapshotArtifactWithClusterAuthVerifier(ctx, etcdutlsnapshot.NewV3(zap.NewNop()), path, version, artifactDir,
		expected, tlsCfg, auth, memberCount, verifyRestoredSnapshot); err != nil {
		return partial, err
	}
	return partial, nil
}

type streamProbeCounters struct {
	rangeOK        atomic.Int64
	snapshotOK     atomic.Int64
	retries        atomic.Int64
	partialRetries atomic.Int64
}

type streamProbeResult struct {
	rangeOK        int64
	snapshotOK     int64
	retries        int64
	partialRetries int64
}

type streamWorkerConfig struct {
	initialDelay    time.Duration
	interval        time.Duration
	attemptTimeout  time.Duration
	retryBackoff    time.Duration
	maxBackoff      time.Duration
	successLimit    int64
	artifactDir     string
	restoredTLS     restoredSnapshotTLSConfig
	restoredAuth    *restoredSnapshotAuthExpectation
	restoredMembers int
}

type streamAttempt func(context.Context) (partial bool, err error)

func runStreamWorker(ctx context.Context, cfg streamWorkerConfig, success *atomic.Int64, counters *streamProbeCounters, attempt streamAttempt) error {
	if cfg.initialDelay > 0 && !waitForStreamProbe(ctx, cfg.initialDelay) {
		return nil
	}
	backoff := cfg.retryBackoff
	for ctx.Err() == nil {
		attemptCtx, cancel := context.WithTimeout(ctx, cfg.attemptTimeout)
		partial, err := attempt(attemptCtx)
		cancel()
		if err == nil {
			completed := success.Add(1)
			if cfg.successLimit > 0 && completed >= cfg.successLimit {
				return nil
			}
			backoff = cfg.retryBackoff
			if !waitForStreamProbe(ctx, cfg.interval) {
				return nil
			}
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
		if !retryableStreamError(err) {
			return err
		}
		counters.retries.Add(1)
		if partial {
			counters.partialRetries.Add(1)
		}
		if !waitForStreamProbe(ctx, backoff) {
			return nil
		}
		backoff = nextStreamProbeBackoff(backoff, cfg.maxBackoff)
	}
	return nil
}

func retryableStreamError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func waitForStreamProbe(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextStreamProbeBackoff(current, maximum time.Duration) time.Duration {
	next := current * 2
	if next < current || next > maximum {
		return maximum
	}
	return next
}

type streamProbeGroup struct {
	cancel   context.CancelFunc
	wait     sync.WaitGroup
	stopOnce sync.Once
	fatalMu  sync.Mutex
	fatalErr error
	counters streamProbeCounters
}

func startStreamProbeGroup(ctx context.Context, client *clientv3.Client, prefix string, expected []streamProbeExpectation, clusterID uint64, rangeCfg, snapshotCfg streamWorkerConfig) *streamProbeGroup {
	probeCtx, cancel := context.WithCancel(ctx)
	group := &streamProbeGroup{cancel: cancel}
	fail := func(kind string, err error) {
		group.fatalMu.Lock()
		if group.fatalErr == nil {
			group.fatalErr = fmt.Errorf("%s integrity probe: %w", kind, err)
			cancel()
		}
		group.fatalMu.Unlock()
	}
	group.wait.Add(2)
	go func() {
		defer group.wait.Done()
		err := runStreamWorker(probeCtx, rangeCfg, &group.counters.rangeOK, &group.counters, func(callCtx context.Context) (bool, error) {
			stream, err := etcdserverpb.NewKVClient(client.ActiveConnection()).RangeStream(callCtx, &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
			})
			if err != nil {
				return false, err
			}
			return consumeRangeStream(stream, prefix, expected, clusterID)
		})
		if err != nil {
			fail("RangeStream", err)
		}
	}()
	go func() {
		defer group.wait.Done()
		err := runStreamWorker(probeCtx, snapshotCfg, &group.counters.snapshotOK, &group.counters, func(callCtx context.Context) (bool, error) {
			stream, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).Snapshot(callCtx, &etcdserverpb.SnapshotRequest{})
			if err != nil {
				return false, err
			}
			memberCount := snapshotCfg.restoredMembers
			if memberCount == 0 {
				memberCount = 1
			}
			return consumeAndValidateSnapshotWithClusterAuth(callCtx, stream, snapshotCfg.artifactDir, expected,
				snapshotCfg.restoredTLS, snapshotCfg.restoredAuth, memberCount)
		})
		if err != nil {
			fail("Snapshot", err)
		}
	}()
	return group
}

func (g *streamProbeGroup) err() error {
	g.fatalMu.Lock()
	defer g.fatalMu.Unlock()
	return g.fatalErr
}

func (g *streamProbeGroup) waitForMinimum(ctx context.Context, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := g.err(); err != nil {
			return err
		}
		if g.counters.rangeOK.Load() > 0 && g.counters.snapshotOK.Load() > 0 {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("stream integrity probe did not complete one RangeStream and Snapshot within %s: range=%d snapshot=%d: %w",
				timeout, g.counters.rangeOK.Load(), g.counters.snapshotOK.Load(), waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (g *streamProbeGroup) stop() (streamProbeResult, error) {
	g.stopOnce.Do(func() {
		g.cancel()
		g.wait.Wait()
	})
	result := streamProbeResult{
		rangeOK:        g.counters.rangeOK.Load(),
		snapshotOK:     g.counters.snapshotOK.Load(),
		retries:        g.counters.retries.Load(),
		partialRetries: g.counters.partialRetries.Load(),
	}
	if err := g.err(); err != nil {
		return result, err
	}
	if result.rangeOK == 0 || result.snapshotOK == 0 {
		return result, fmt.Errorf("stream integrity probe produced no complete result: range=%d snapshot=%d", result.rangeOK, result.snapshotOK)
	}
	return result, nil
}

func sortedExpectedKeys(expected []streamProbeExpectation) []string {
	keys := make([]string, 0, len(expected))
	for _, item := range expected {
		keys = append(keys, item.key)
	}
	sort.Strings(keys)
	return keys
}
