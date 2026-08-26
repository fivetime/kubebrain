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
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	etcdutlsnapshot "go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	streamProbeSeedKeys              = 16
	streamProbeValueBytes            = 8 * 1024
	streamProbeHistoryLeaseTTL       = 15 * 60
	streamProbeSecondHistoryLeaseTTL = 16 * 60
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

func verifyRestoredClusterReplicationAndQuorum(ctx context.Context, adminClient *clientv3.Client, adminConfig clientv3.Config,
	cfg restoredSnapshotConfig, topology restoredClusterTopology, servers []*embed.Etcd, revision int64,
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
	quorumDelete, err := quorumClient.Delete(ctx, quorumKey)
	if err != nil || quorumDelete == nil || quorumDelete.Header == nil || quorumDelete.Header.ClusterId != topology.clusterID || quorumDelete.Deleted != 1 {
		return fmt.Errorf("delete officially restored etcd quorum probe: response=%+v err=%v", quorumDelete, err)
	}
	return nil
}

func verifyRestoredSnapshot(ctx context.Context, cfg restoredSnapshotConfig, expected []streamProbeExpectation, revision int64) (retErr error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	verifyCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
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
		embedCfg := embed.NewConfig()
		embedCfg.Name = member.name
		embedCfg.Dir = member.dataDir
		embedCfg.ClusterState = embed.ClusterStateFlagExisting
		embedCfg.ListenClientUrls = []url.URL{member.clientURL}
		embedCfg.AdvertiseClientUrls = []url.URL{member.clientURL}
		embedCfg.ListenPeerUrls = []url.URL{member.peerURL}
		embedCfg.AdvertisePeerUrls = []url.URL{member.peerURL}
		embedCfg.InitialCluster = cfg.initialCluster
		embedCfg.InitialClusterToken = cfg.initialClusterToken
		embedCfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.NewNop())
		if cfg.tls.enabled() {
			embedCfg.ClientTLSInfo = transport.TLSInfo{
				CertFile: cfg.tls.certFile, KeyFile: cfg.tls.keyFile, TrustedCAFile: cfg.tls.caFile, ClientCertAuth: true,
			}
		}
		restored[index], err = embed.StartEtcd(embedCfg)
		if err != nil {
			return fmt.Errorf("start officially restored etcd member %q: %w", member.name, err)
		}
	}
	for index, server := range restored {
		select {
		case <-server.Server.ReadyNotify():
		case serveErr, ok := <-server.Err():
			if !ok || serveErr == nil {
				return fmt.Errorf("officially restored etcd member %q stopped before becoming ready", cfg.members[index].name)
			}
			return fmt.Errorf("officially restored etcd member %q stopped before becoming ready: %w", cfg.members[index].name, serveErr)
		case <-verifyCtx.Done():
			return fmt.Errorf("wait for officially restored etcd member %q readiness: %w", cfg.members[index].name, context.Cause(verifyCtx))
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
		return verifyRestoredClusterReplicationAndQuorum(verifyCtx, adminClient, adminConfig, cfg, topology, restored, revision)
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
