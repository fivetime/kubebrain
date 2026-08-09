// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	mvcc "go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
)

type failingMaintenanceSnapshotServer struct {
	*maintenanceSnapshotServer
	err error
}

func TestSnapshotPassesBboltConsistencyCheck(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	var revisions []int64
	for index := range 5 {
		response, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(fmt.Sprintf("/snapshot/bbolt-check/%d", index)),
			Value: []byte(fmt.Sprintf("value-%d", index)),
		})
		require.NoError(t, err)
		revisions = append(revisions, response.Header.Revision)
	}

	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, server.buildSnapshot(ctx, path))
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, PreLoadFreelist: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for checkErr := range tx.Check() {
			return checkErr
		}
		return nil
	}))
	require.NoError(t, db.Close())

	etcdutl := os.Getenv("KUBEBRAIN_ETCDUTL_BIN")
	if etcdutl == "" {
		// Keep the A4171 variable working while callers migrate to the
		// command-wide name used by both bbolt and hashkv checks.
		etcdutl = os.Getenv("KUBEBRAIN_ETCDUTL_BBOLT_BIN")
	}
	if etcdutl != "" {
		command := exec.CommandContext(ctx, etcdutl, "bbolt", "check", path)
		output, commandErr := command.CombinedOutput()
		require.NoError(t, commandErr, string(output))
		require.Equal(t, "OK", strings.TrimSpace(string(output)))

		type hashKVResult struct {
			Hash            uint32 `json:"hash"`
			HashRevision    int64  `json:"hashRevision"`
			CompactRevision int64  `json:"compactRevision"`
		}
		runHashKV := func(arguments ...string) hashKVResult {
			command = exec.CommandContext(ctx, etcdutl, append([]string{"--write-out=json", "hashkv", path}, arguments...)...)
			output, commandErr = command.CombinedOutput()
			require.NoError(t, commandErr, string(output))
			var result hashKVResult
			require.NoError(t, json.Unmarshal(output, &result), string(output))
			return result
		}
		latest := runHashKV()
		require.Equal(t, revisions[len(revisions)-1], latest.HashRevision)
		require.Zero(t, latest.CompactRevision)
		historical := runHashKV("--rev", fmt.Sprint(revisions[1]))
		require.Equal(t, revisions[1], historical.HashRevision)
		require.Zero(t, historical.CompactRevision)

		stream := &maintenanceSnapshotServer{ctx: ctx}
		require.NoError(t, server.Snapshot(&etcdserverpb.SnapshotRequest{}, stream))
		require.GreaterOrEqual(t, len(stream.responses), 2)
		var artifact []byte
		for _, response := range stream.responses {
			artifact = append(artifact, response.Blob...)
		}
		require.Equal(t, int64(sha256.Size), int64(len(artifact))%512,
			"official tools recognize the final 32 bytes as the snapshot checksum")
		artifactPath := filepath.Join(t.TempDir(), "snapshot-with-checksum.db")
		require.NoError(t, os.WriteFile(artifactPath, artifact, 0o600))
		command = exec.CommandContext(ctx, etcdutl, "--write-out=json", "snapshot", "status", artifactPath)
		output, commandErr = command.CombinedOutput()
		require.NoError(t, commandErr, string(output))
		var snapshotStatus struct {
			Revision int64  `json:"revision"`
			TotalKey int    `json:"totalKey"`
			Version  string `json:"version"`
		}
		require.NoError(t, json.Unmarshal(output, &snapshotStatus), string(output))
		require.Equal(t, revisions[len(revisions)-1], snapshotStatus.Revision)
		require.Positive(t, snapshotStatus.TotalKey)
		require.Equal(t, Version, snapshotStatus.Version)

		restoredDir := filepath.Join(t.TempDir(), "restored.etcd")
		command = exec.CommandContext(ctx, etcdutl, "snapshot", "restore", artifactPath,
			"--data-dir", restoredDir, "--name", "snapshot-checksum",
			"--initial-cluster", "snapshot-checksum=http://127.0.0.1:42380",
			"--initial-advertise-peer-urls", "http://127.0.0.1:42380")
		output, commandErr = command.CombinedOutput()
		require.NoError(t, commandErr, string(output))
		require.FileExists(t, filepath.Join(restoredDir, "member", "snap", "db"))
	}
}

type unlinkCheckingSnapshotServer struct {
	*maintenanceSnapshotServer
	path    string
	checked bool
}

func (s *unlinkCheckingSnapshotServer) Send(response *etcdserverpb.SnapshotResponse) error {
	if !s.checked {
		s.checked = true
		if _, err := os.Stat(s.path); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("snapshot path still exists at first response: %v", err)
		}
	}
	return s.maintenanceSnapshotServer.Send(response)
}

func (s *failingMaintenanceSnapshotServer) Send(*etcdserverpb.SnapshotResponse) error {
	return s.err
}

func TestMaintenanceSnapshotDurationObservedForSuccessAndSendFailure(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	server.metricCli = rec

	require.NoError(t, server.Snapshot(&etcdserverpb.SnapshotRequest{},
		&maintenanceSnapshotServer{ctx: context.Background()}))
	sendErr := fmt.Errorf("injected snapshot send failure")
	err := server.Snapshot(&etcdserverpb.SnapshotRequest{}, &failingMaintenanceSnapshotServer{
		maintenanceSnapshotServer: &maintenanceSnapshotServer{ctx: context.Background()},
		err:                       sendErr,
	})
	require.ErrorIs(t, err, sendErr)

	var samples []recordedHistogram
	for _, histogram := range rec.histograms {
		if histogram.name == etcdBackendSnapshotDurationMetric {
			samples = append(samples, histogram)
		}
	}
	require.Len(t, samples, 2)
	for _, sample := range samples {
		require.GreaterOrEqual(t, sample.value.(float64), float64(0))
		require.Empty(t, sample.tags)
	}
}

func TestSnapshotFileIsUnlinkedBeforeFirstStreamResponse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sensitive-snapshot.db")
	want := bytes.Repeat([]byte("snapshot-secret-"), 4096)
	require.NoError(t, os.WriteFile(path, want, 0o600))
	stream := &unlinkCheckingSnapshotServer{
		maintenanceSnapshotServer: &maintenanceSnapshotServer{ctx: context.Background()},
		path:                      path,
	}
	require.NoError(t, streamSnapshotFile(path, stream))
	require.True(t, stream.checked)
	require.GreaterOrEqual(t, len(stream.responses), 2)

	var got []byte
	for _, response := range stream.responses[:len(stream.responses)-1] {
		got = append(got, response.Blob...)
	}
	require.Equal(t, want, got)
	wantHash := sha256.Sum256(want)
	require.Equal(t, wantHash[:], stream.responses[len(stream.responses)-1].Blob)
}

func TestMaintenanceSnapshotRemainsAvailableUnderNoSpaceAndCorruptAlarmsLikeEtcd(t *testing.T) {
	server := newQuotaRPCServer(t, 0)
	ctx := context.Background()
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("snapshot-alarm-key"), Value: []byte("value")})
	require.NoError(t, err)
	for memberID, alarm := range map[uint64]etcdserverpb.AlarmType{
		41640: etcdserverpb.AlarmType_NOSPACE,
		41641: etcdserverpb.AlarmType_CORRUPT,
	} {
		_, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: alarm, MemberID: memberID,
		})
		require.NoError(t, err)
	}

	stream := &maintenanceSnapshotServer{ctx: ctx}
	require.NoError(t, server.Snapshot(&etcdserverpb.SnapshotRequest{}, stream))
	require.GreaterOrEqual(t, len(stream.responses), 2)
	var artifact []byte
	for _, response := range stream.responses[:len(stream.responses)-1] {
		artifact = append(artifact, response.Blob...)
	}
	checksum := stream.responses[len(stream.responses)-1]
	want := sha256.Sum256(artifact)
	require.Equal(t, want[:], checksum.Blob)
	require.Zero(t, checksum.RemainingBytes)
	require.Equal(t, Version, checksum.Version)
	dbPath := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, os.WriteFile(dbPath, artifact, 0o600))
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		keyBucket := tx.Bucket(schema.Key.Name())
		require.NotNil(t, keyBucket)
		require.Positive(t, keyBucket.Stats().KeyN)
		var alarms []string
		require.NoError(t, tx.Bucket(schema.Alarm.Name()).ForEach(func(key, _ []byte) error {
			alarm := new(etcdserverpb.AlarmMember)
			if err := proto.Unmarshal(key, alarm); err != nil {
				return err
			}
			alarms = append(alarms, fmt.Sprintf("%d/%d", alarm.MemberID, alarm.Alarm))
			return nil
		}))
		require.ElementsMatch(t, []string{"41640/1", "41641/2"}, alarms)
		return nil
	}))
	require.NoError(t, db.Close())
}

type streamingSnapshotBackend struct {
	BackendShim
	fullRangeLists int
	streams        int
	dataChunks     int
	maxChunkKeys   int
}

type pausedSnapshotBackend struct {
	BackendShim
	paused  chan struct{}
	release chan struct{}
	once    sync.Once
}

type metadataBarrierSnapshotBackend struct {
	*pausedSnapshotBackend
	barrierAcquired chan struct{}
	proceedMetadata chan struct{}
}

type localSnapshotTrapBackend struct {
	BackendShim
	called bool
}

type fixedSnapshotRevisionBackend struct {
	BackendShim
	revision uint64
}

func (b *fixedSnapshotRevisionBackend) GetCurrentRevision() uint64 { return b.revision }

func TestMaintenanceSnapshotRejectsRevisionWithoutSuccessorBeforeCapture(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.backend = &fixedSnapshotRevisionBackend{BackendShim: server.backend, revision: math.MaxInt64}

	path := filepath.Join(t.TempDir(), "snapshot.db")
	err := server.buildSnapshotOnce(context.Background(), path)
	require.ErrorContains(t, err, "snapshot revision leaves no room for next etcd write")
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestMaintenanceSnapshotRejectsUnremovablePriorCaptureBeforeReadingState(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	trap := &localSnapshotTrapBackend{BackendShim: server.backend}
	server.backend = trap

	path := filepath.Join(t.TempDir(), "occupied")
	require.NoError(t, os.Mkdir(path, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "stale-page"), []byte("stale"), 0o600))
	err := server.buildSnapshot(context.Background(), path)
	require.ErrorContains(t, err, "reset etcd snapshot capture path")
	require.False(t, trap.called, "an unreset artifact path must fail before snapshot state capture")
}

type stalledSnapshotBackend struct {
	BackendShim
	afterFirst bool
	started    chan struct{}
	release    chan struct{}
}

type corruptCurrentLeaseSnapshotBackend struct {
	BackendShim
	key, value []byte
	lease      int64
}

func (b *corruptCurrentLeaseSnapshotBackend) SnapshotHistoryStreamChan(_ context.Context, revision uint64) (<-chan backend.SnapshotHistoryChunk, error) {
	out := make(chan backend.SnapshotHistoryChunk, 2)
	out <- backend.SnapshotHistoryChunk{Revision: revision, Records: []backend.SnapshotHistoryRecord{{
		Key: b.key, Value: b.value, CreateRevision: revision, ModRevision: revision,
		Version: 1, Lease: b.lease, LeaseKnown: true, Current: true,
	}}}
	out <- backend.SnapshotHistoryChunk{Revision: revision, Done: true}
	close(out)
	return out, nil
}

func (b *stalledSnapshotBackend) SnapshotHistoryStreamChan(ctx context.Context, revision uint64) (<-chan backend.SnapshotHistoryChunk, error) {
	out := make(chan backend.SnapshotHistoryChunk)
	if !b.afterFirst {
		go func() {
			close(b.started)
			<-b.release
			close(out)
		}()
		return out, nil
	}
	source, err := b.BackendShim.SnapshotHistoryStreamChan(ctx, revision)
	if err != nil {
		return nil, err
	}
	go func() {
		first, ok := <-source
		if ok {
			out <- first
		}
		close(b.started)
		<-b.release
		close(out)
	}()
	return out, nil
}

func (b *localSnapshotTrapBackend) BeginRangeTxn(ctx context.Context) (context.Context, func()) {
	b.called = true
	return b.BackendShim.BeginRangeTxn(ctx)
}

func TestMaintenanceSnapshotFollowerForwardsCompleteStreamToLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	trap := &localSnapshotTrapBackend{BackendShim: server.backend}
	server.backend = trap
	digest := sha256.Sum256([]byte("abc"))
	want := []*etcdserverpb.SnapshotResponse{
		{RemainingBytes: 2, Blob: []byte("a"), Version: Version},
		{RemainingBytes: 0, Blob: []byte("bc"), Version: Version},
		{RemainingBytes: 0, Blob: digest[:], Version: Version},
	}
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		snapshotFn: func(_ context.Context, request *etcdserverpb.SnapshotRequest) (<-chan etcdproxy.SnapshotResult, error) {
			require.NotNil(t, request)
			results := make(chan etcdproxy.SnapshotResult, len(want))
			for _, response := range want {
				results <- etcdproxy.SnapshotResult{Response: response}
			}
			close(results)
			return results, nil
		},
	}
	stream := &maintenanceSnapshotServer{ctx: context.Background()}
	require.NoError(t, server.Snapshot(&etcdserverpb.SnapshotRequest{}, stream))
	require.False(t, trap.called, "a follower must not capture independently from concurrent leader metadata mutations")
	require.Equal(t, want, stream.responses)
}

func TestMaintenanceSnapshotFollowerRejectsProxyStreamWithoutChecksum(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		snapshotFn: func(_ context.Context, _ *etcdserverpb.SnapshotRequest) (<-chan etcdproxy.SnapshotResult, error) {
			results := make(chan etcdproxy.SnapshotResult, 1)
			results <- etcdproxy.SnapshotResult{Response: &etcdserverpb.SnapshotResponse{
				RemainingBytes: 0,
				Blob:           []byte("truncated-database"),
				Version:        Version,
			}}
			close(results)
			return results, nil
		},
	}
	stream := &maintenanceSnapshotServer{ctx: context.Background()}
	err := server.Snapshot(&etcdserverpb.SnapshotRequest{}, stream)
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.ErrorContains(t, err, "ended before checksum")
	require.Len(t, stream.responses, 1)
}

func TestMaintenanceSnapshotFollowerRejectsNilProxyResultChannel(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		snapshotFn: func(context.Context, *etcdserverpb.SnapshotRequest) (<-chan etcdproxy.SnapshotResult, error) {
			return nil, nil
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: context.Background()})
	}()
	select {
	case err := <-done:
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.ErrorContains(t, err, "nil result channel")
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "nil leader snapshot result channel blocked the follower RPC")
	}
}

func TestMaintenanceSnapshotFollowerCancelsLeaderStreamOnDownstreamSendFailure(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	callerCtx, cancelCaller := context.WithCancel(context.Background())
	defer cancelCaller()
	leaderCanceled := make(chan struct{})
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		snapshotFn: func(ctx context.Context, _ *etcdserverpb.SnapshotRequest) (<-chan etcdproxy.SnapshotResult, error) {
			results := make(chan etcdproxy.SnapshotResult, 1)
			results <- etcdproxy.SnapshotResult{Response: &etcdserverpb.SnapshotResponse{
				RemainingBytes: 0, Blob: []byte("db"), Version: Version,
			}}
			go func() {
				<-ctx.Done()
				close(leaderCanceled)
			}()
			return results, nil
		},
	}
	err := server.Snapshot(&etcdserverpb.SnapshotRequest{}, &failingMaintenanceSnapshotServer{
		maintenanceSnapshotServer: &maintenanceSnapshotServer{ctx: callerCtx},
		err:                       errors.New("snapshot send failed"),
	})
	require.ErrorContains(t, err, "snapshot send failed")
	select {
	case <-leaderCanceled:
	case <-time.After(100 * time.Millisecond):
		cancelCaller()
		require.Fail(t, "leader snapshot context was not canceled when downstream send failed")
	}
}

func TestMaintenanceSnapshotFollowerRejectsMalformedProxyTermination(t *testing.T) {
	data := &etcdserverpb.SnapshotResponse{RemainingBytes: 0, Blob: []byte("db"), Version: Version}
	digest := sha256.Sum256(data.Blob)
	checksum := &etcdserverpb.SnapshotResponse{RemainingBytes: 0, Blob: digest[:], Version: Version}
	for _, tc := range []struct {
		name          string
		responses     []*etcdserverpb.SnapshotResponse
		wantError     string
		wantForwarded int
	}{
		{
			name:          "nil response",
			responses:     []*etcdserverpb.SnapshotResponse{nil},
			wantError:     "empty response",
			wantForwarded: 0,
		},
		{
			name: "invalid checksum",
			responses: []*etcdserverpb.SnapshotResponse{
				data, {RemainingBytes: 0, Blob: []byte("short"), Version: Version},
			},
			wantError: "invalid checksum frame", wantForwarded: 1,
		},
		{
			name: "checksum mismatch",
			responses: []*etcdserverpb.SnapshotResponse{
				data, {RemainingBytes: 0, Blob: bytes.Repeat([]byte{1}, sha256.Size), Version: Version},
			},
			wantError: "checksum mismatch", wantForwarded: 1,
		},
		{
			name: "remaining bytes discontinuity",
			responses: func() []*etcdserverpb.SnapshotResponse {
				fullDigest := sha256.Sum256([]byte("abc"))
				return []*etcdserverpb.SnapshotResponse{
					{RemainingBytes: 2, Blob: []byte("ab"), Version: Version},
					{RemainingBytes: 0, Blob: []byte("c"), Version: Version},
					{RemainingBytes: 0, Blob: fullDigest[:], Version: Version},
				}
			}(),
			wantError: "remaining bytes", wantForwarded: 1,
		},
		{
			name: "data after checksum",
			responses: []*etcdserverpb.SnapshotResponse{
				data, checksum, {RemainingBytes: 0, Blob: []byte("extra"), Version: Version},
			},
			wantError: "data after checksum", wantForwarded: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				snapshotFn: func(_ context.Context, _ *etcdserverpb.SnapshotRequest) (<-chan etcdproxy.SnapshotResult, error) {
					results := make(chan etcdproxy.SnapshotResult, len(tc.responses))
					for _, response := range tc.responses {
						results <- etcdproxy.SnapshotResult{Response: response}
					}
					close(results)
					return results, nil
				},
			}
			stream := &maintenanceSnapshotServer{ctx: context.Background()}
			err := server.Snapshot(&etcdserverpb.SnapshotRequest{}, stream)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.ErrorContains(t, err, tc.wantError)
			require.Len(t, stream.responses, tc.wantForwarded)
		})
	}
}

func TestMaintenanceSnapshotFollowerWithoutProxyRejectsLocalCapture(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	trap := &localSnapshotTrapBackend{BackendShim: server.backend}
	server.backend = trap
	server.peers = testPeerService{isLeader: false, proxyEnabled: false}

	err := server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: context.Background()})
	require.ErrorIs(t, err, rpctypes.ErrGRPCNotLeader)
	require.False(t, trap.called,
		"a follower's process-local barrier cannot freeze concurrent leader metadata mutations")
}

func TestMaintenanceSnapshotFollowerForwardsRootCredential(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_ = setupAuthKVUser(t, server)
	rootToken, err := server.tokens.authenticate(context.Background(), "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken))
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		snapshotFn: func(ctx context.Context, _ *etcdserverpb.SnapshotRequest) (<-chan etcdproxy.SnapshotResult, error) {
			outgoing, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			require.Equal(t, []string{rootToken}, outgoing.Get(rpctypes.TokenFieldNameGRPC))
			digest := sha256.Sum256([]byte("db"))
			results := make(chan etcdproxy.SnapshotResult, 2)
			results <- etcdproxy.SnapshotResult{Response: &etcdserverpb.SnapshotResponse{
				RemainingBytes: 0, Blob: []byte("db"), Version: Version,
			}}
			results <- etcdproxy.SnapshotResult{Response: &etcdserverpb.SnapshotResponse{
				RemainingBytes: 0, Blob: digest[:], Version: Version,
			}}
			close(results)
			return results, nil
		},
	}
	require.NoError(t, server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: rootCtx}))
}

func (b *pausedSnapshotBackend) SnapshotHistoryStreamChan(ctx context.Context, revision uint64) (<-chan backend.SnapshotHistoryChunk, error) {
	source, err := b.BackendShim.SnapshotHistoryStreamChan(ctx, revision)
	if err != nil {
		return nil, err
	}
	out := make(chan backend.SnapshotHistoryChunk)
	go func() {
		defer close(out)
		for chunk := range source {
			select {
			case out <- chunk:
			case <-ctx.Done():
				return
			}
			if len(chunk.Records) > 0 {
				b.once.Do(func() {
					close(b.paused)
					select {
					case <-b.release:
					case <-ctx.Done():
					}
				})
			}
		}
	}()
	return out, nil
}

func (b *metadataBarrierSnapshotBackend) BeginRangeTxn(ctx context.Context) (context.Context, func()) {
	rangeCtx, unlock := b.BackendShim.BeginRangeTxn(ctx)
	close(b.barrierAcquired)
	select {
	case <-b.proceedMetadata:
	case <-ctx.Done():
	}
	return rangeCtx, unlock
}

func TestMaintenanceSnapshotDoesNotBlockWritesAndKeepsPinnedRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	seed, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("snapshot-seed"), Value: []byte("before")})
	require.NoError(t, err)
	wrapped := &pausedSnapshotBackend{
		BackendShim: server.backend,
		paused:      make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = wrapped

	path := filepath.Join(t.TempDir(), "snapshot.db")
	snapshotDone := make(chan error, 1)
	go func() { snapshotDone <- server.buildSnapshot(context.Background(), path) }()
	select {
	case <-wrapped.paused:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not reach its first data chunk")
	}
	putDone := make(chan *etcdserverpb.PutResponse, 1)
	putErr := make(chan error, 1)
	go func() {
		response, putError := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("after-snapshot-pin"), Value: []byte("after")})
		putDone <- response
		putErr <- putError
	}()
	var concurrentPut *etcdserverpb.PutResponse
	writeCompleted := false
	select {
	case concurrentPut = <-putDone:
		writeCompleted = true
	case <-time.After(250 * time.Millisecond):
	}
	close(wrapped.release)
	if !writeCompleted {
		concurrentPut = <-putDone
	}
	require.NoError(t, <-putErr)
	require.True(t, writeCompleted, "a pinned snapshot must not hold the logical-write barrier for its full scan")
	require.NoError(t, <-snapshotDone)
	require.Greater(t, concurrentPut.Header.Revision, seed.Header.Revision)

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	var keys [][]byte
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(schema.Key.Name()).ForEach(func(_, value []byte) error {
			kv := new(mvccpb.KeyValue)
			if err := proto.Unmarshal(value, kv); err != nil {
				return err
			}
			keys = append(keys, kv.Key)
			return nil
		})
	}))
	// The snapshot linearizes before the concurrent Put: it retains the seed
	// but excludes the later key despite allowing that write to complete.
	require.Contains(t, keys, []byte("snapshot-seed"))
	require.NotContains(t, keys, []byte("after-snapshot-pin"))
}

func TestMaintenanceSnapshotPinsAuthMetadataBeforeReleasingWriteBarrier(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("snapshot-auth-barrier-seed"), Value: []byte("seed"),
	})
	require.NoError(t, err)

	wrapped := &metadataBarrierSnapshotBackend{
		pausedSnapshotBackend: &pausedSnapshotBackend{
			BackendShim: server.backend,
			paused:      make(chan struct{}),
			release:     make(chan struct{}),
		},
		barrierAcquired: make(chan struct{}),
		proceedMetadata: make(chan struct{}),
	}
	server.backend = wrapped

	path := filepath.Join(t.TempDir(), "snapshot.db")
	snapshotDone := make(chan error, 1)
	go func() { snapshotDone <- server.buildSnapshot(context.Background(), path) }()
	select {
	case <-wrapped.barrierAcquired:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not acquire its metadata barrier")
	}

	roleDone := make(chan error, 1)
	go func() {
		_, roleErr := server.RoleAdd(context.Background(), &etcdserverpb.AuthRoleAddRequest{Name: "after-snapshot-pin"})
		roleDone <- roleErr
	}()
	select {
	case roleErr := <-roleDone:
		require.FailNow(t, "auth mutation crossed snapshot metadata barrier", "err=%v", roleErr)
	case <-time.After(50 * time.Millisecond):
	}

	close(wrapped.proceedMetadata)
	select {
	case <-wrapped.paused:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not establish its pinned history stream")
	}
	select {
	case roleErr := <-roleDone:
		require.NoError(t, roleErr)
	case <-time.After(5 * time.Second):
		t.Fatal("auth mutation did not resume after snapshot released its barrier")
	}
	close(wrapped.release)
	require.NoError(t, <-snapshotDone)

	current, err := server.auth.repo.load(context.Background())
	require.NoError(t, err)
	require.Contains(t, current.Roles, "after-snapshot-pin")
	require.Equal(t, uint64(2), current.Config.Revision)

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		require.Nil(t, tx.Bucket(schema.AuthRoles.Name()).Get([]byte("after-snapshot-pin")),
			"the artifact must retain the auth state captured before its pinned user revision")
		require.Equal(t, uint64(1), binary.BigEndian.Uint64(
			tx.Bucket(schema.Auth.Name()).Get(schema.AuthRevisionKeyName)))
		return nil
	}))
}

func TestMaintenanceSnapshotRejectsLeadershipTermChangeBeforePin(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	var epoch atomic.Uint64
	epoch.Store(7)
	server.peers = testPeerService{
		isLeader: true,
		epochFn: func() (uint64, bool) {
			return epoch.Load(), true
		},
	}
	wrapped := &metadataBarrierSnapshotBackend{
		pausedSnapshotBackend: &pausedSnapshotBackend{
			BackendShim: server.backend,
			paused:      make(chan struct{}),
			release:     make(chan struct{}),
		},
		barrierAcquired: make(chan struct{}),
		proceedMetadata: make(chan struct{}),
	}
	server.backend = wrapped

	done := make(chan error, 1)
	path := filepath.Join(t.TempDir(), "snapshot.db")
	go func() { done <- server.buildSnapshotOnce(context.Background(), path) }()
	select {
	case <-wrapped.barrierAcquired:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not acquire its metadata barrier")
	}
	epoch.Store(8)
	close(wrapped.proceedMetadata)

	require.ErrorIs(t, <-done, errSnapshotLeaderChanged)
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist,
		"a term-spanning capture must be rejected before creating its bbolt artifact")
}

func TestMaintenanceSnapshotRejectsLeadershipTermChangeDuringStream(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("snapshot-term-stream"), Value: []byte("value"),
	})
	require.NoError(t, err)
	var epoch atomic.Uint64
	epoch.Store(7)
	server.peers = testPeerService{
		isLeader: true,
		epochFn: func() (uint64, bool) {
			return epoch.Load(), true
		},
	}
	wrapped := &pausedSnapshotBackend{
		BackendShim: server.backend,
		paused:      make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = wrapped

	done := make(chan error, 1)
	go func() {
		done <- server.buildSnapshotOnce(context.Background(), filepath.Join(t.TempDir(), "snapshot.db"))
	}()
	select {
	case <-wrapped.paused:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not stream its first data chunk")
	}
	epoch.Store(8)
	close(wrapped.release)
	require.ErrorIs(t, <-done, errSnapshotLeaderChanged)
}

func TestMaintenanceSnapshotPollsLeadershipWhileScannerIsStalled(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	var epoch atomic.Uint64
	epoch.Store(7)
	server.peers = testPeerService{
		isLeader: true,
		epochFn: func() (uint64, bool) {
			return epoch.Load(), true
		},
	}
	stalled := &stalledSnapshotBackend{
		BackendShim: server.backend,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = stalled

	done := make(chan error, 1)
	go func() {
		done <- server.buildSnapshotOnce(context.Background(), filepath.Join(t.TempDir(), "snapshot.db"))
	}()
	select {
	case <-stalled.started:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot scanner did not reach the injected pre-chunk stall")
	}
	epoch.Store(8)
	var snapshotErr error
	returnedOnFence := false
	select {
	case snapshotErr = <-done:
		returnedOnFence = true
	case <-time.After(250 * time.Millisecond):
	}
	close(stalled.release)
	if !returnedOnFence {
		snapshotErr = <-done
	}
	require.True(t, returnedOnFence, "term polling must interrupt a stalled scanner without client cancellation")
	require.ErrorIs(t, snapshotErr, errSnapshotLeaderChanged)
}

func TestMaintenanceSnapshotClassifiesRepeatedTermChanges(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	var calls atomic.Uint64
	server.peers = testPeerService{
		isLeader: true,
		epochFn: func() (uint64, bool) {
			return calls.Add(1), true
		},
	}

	err := server.buildSnapshot(context.Background(), filepath.Join(t.TempDir(), "snapshot.db"))
	require.ErrorIs(t, err, rpctypes.ErrGRPCLeaderChanged)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.GreaterOrEqual(t, calls.Load(), uint64(16), "all eight capture attempts must observe a changed term")
}

func TestMaintenanceSnapshotClassifiesFreshnessLossAfterAdmission(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	var fresh atomic.Bool
	fresh.Store(true)
	server.peers = testPeerService{
		isLeader: true,
		epochFn: func() (uint64, bool) {
			return 7, fresh.Load()
		},
	}
	stalled := &stalledSnapshotBackend{
		BackendShim: server.backend,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = stalled

	done := make(chan error, 1)
	go func() {
		done <- server.buildSnapshotOnce(context.Background(), filepath.Join(t.TempDir(), "snapshot.db"))
	}()
	select {
	case <-stalled.started:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot scanner did not reach the injected stall")
	}
	fresh.Store(false)
	var snapshotErr error
	select {
	case snapshotErr = <-done:
	case <-time.After(250 * time.Millisecond):
		close(stalled.release)
		t.Fatal("freshness loss did not interrupt capture")
	}
	close(stalled.release)
	require.ErrorIs(t, snapshotErr, errSnapshotLeaderChanged)
	require.NotErrorIs(t, snapshotErr, rpctypes.ErrGRPCNotLeader)
}

func TestMaintenanceSnapshotEmptyKeyspaceCompletesAfterTerminalHandshake(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	require.NoError(t, server.buildSnapshot(context.Background(), filepath.Join(t.TempDir(), "snapshot.db")))
}

func TestMaintenanceSnapshotPreservesUncompactedHistory(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	key := []byte("snapshot-history-key")
	create, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	update, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)
	deleted, err := server.DeleteRange(context.Background(), &etcdserverpb.DeleteRangeRequest{Key: key})
	require.NoError(t, err)
	recreated, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: key, Value: []byte("v3")})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, server.buildSnapshot(context.Background(), path))
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	type retainedVersion struct {
		revision  int64
		value     string
		tombstone bool
	}
	var got []retainedVersion
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(schema.Key.Name())
		return bucket.ForEach(func(encodedRevision, value []byte) error {
			kv := new(mvccpb.KeyValue)
			if err := proto.Unmarshal(value, kv); err != nil {
				return err
			}
			if !bytes.Equal(kv.Key, key) {
				return nil
			}
			got = append(got, retainedVersion{
				revision:  mvcc.BytesToRev(encodedRevision).Main,
				value:     string(kv.Value),
				tombstone: mvcc.IsTombstone(encodedRevision),
			})
			return nil
		})
	}))
	require.Equal(t, []retainedVersion{
		{revision: create.Header.Revision, value: "v1"},
		{revision: update.Header.Revision, value: "v2"},
		{revision: deleted.Header.Revision, tombstone: true},
		{revision: recreated.Header.Revision, value: "v3"},
	}, got)
}

func TestMaintenanceSnapshotPreservesActualCompactWatermark(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	key := []byte("snapshot-compact-history-key")
	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	updated, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)
	_, err = server.Compact(context.Background(), &etcdserverpb.CompactionRequest{Revision: updated.Header.Revision})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, server.buildSnapshot(context.Background(), path))
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(schema.Meta.Name())
		require.Equal(t, updated.Header.Revision, mvcc.BytesToRev(meta.Get(schema.FinishedCompactKeyName)).Main)
		require.Equal(t, updated.Header.Revision, mvcc.BytesToRev(meta.Get(schema.ScheduledCompactKeyName)).Main)
		return nil
	}))
	require.NoError(t, db.Close())

	etcdutl := os.Getenv("KUBEBRAIN_ETCDUTL_BIN")
	if etcdutl == "" {
		etcdutl = os.Getenv("KUBEBRAIN_ETCDUTL_BBOLT_BIN")
	}
	if etcdutl == "" {
		return
	}
	type hashKVResult struct {
		HashRevision    int64 `json:"hashRevision"`
		CompactRevision int64 `json:"compactRevision"`
	}
	runHashKV := func(revision int64) (hashKVResult, []byte, error) {
		arguments := []string{"--write-out=json", "hashkv", path}
		if revision != 0 {
			arguments = append(arguments, "--rev", fmt.Sprint(revision))
		}
		output, commandErr := exec.CommandContext(context.Background(), etcdutl, arguments...).CombinedOutput()
		var result hashKVResult
		if commandErr == nil {
			require.NoError(t, json.Unmarshal(output, &result), string(output))
		}
		return result, output, commandErr
	}
	for _, revision := range []int64{0, updated.Header.Revision} {
		result, output, commandErr := runHashKV(revision)
		require.NoError(t, commandErr, string(output))
		require.Equal(t, updated.Header.Revision, result.HashRevision)
		require.Equal(t, updated.Header.Revision, result.CompactRevision)
	}
	_, output, commandErr := runHashKV(updated.Header.Revision - 1)
	require.Error(t, commandErr)
	require.Contains(t, string(output), rpctypes.ErrCompacted.Error())
	_, output, commandErr = runHashKV(updated.Header.Revision + 1)
	require.Error(t, commandErr)
	require.Contains(t, string(output), rpctypes.ErrFutureRev.Error())
}

func TestMaintenanceSnapshotCancellationInterruptsStalledStream(t *testing.T) {
	for _, tc := range []struct {
		name       string
		afterFirst bool
	}{
		{name: "before first response"},
		{name: "after first response", afterFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			stalled := &stalledSnapshotBackend{
				BackendShim: server.backend,
				afterFirst:  tc.afterFirst,
				started:     make(chan struct{}),
				release:     make(chan struct{}),
			}
			server.backend = stalled
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- server.buildSnapshot(ctx, filepath.Join(t.TempDir(), "snapshot.db"))
			}()
			select {
			case <-stalled.started:
			case <-time.After(5 * time.Second):
				t.Fatal("snapshot stream did not reach the injected stall")
			}
			cancel()
			returnedOnCancel := false
			var snapshotErr error
			select {
			case snapshotErr = <-done:
				returnedOnCancel = true
			case <-time.After(250 * time.Millisecond):
			}
			close(stalled.release)
			if !returnedOnCancel {
				snapshotErr = <-done
			}
			require.True(t, returnedOnCancel, "client cancellation must interrupt a stalled snapshot stream")
			require.ErrorIs(t, snapshotErr, context.Canceled)
		})
	}
}

func (b *streamingSnapshotBackend) List(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if bytes.Equal(req.Key, []byte{0}) && bytes.Equal(req.RangeEnd, []byte{0}) {
		b.fullRangeLists++
		return nil, fmt.Errorf("snapshot must not materialize the complete keyspace with List")
	}
	return b.BackendShim.List(ctx, req)
}

func (b *streamingSnapshotBackend) SnapshotHistoryStreamChan(ctx context.Context, revision uint64) (<-chan backend.SnapshotHistoryChunk, error) {
	b.streams++
	source, err := b.BackendShim.SnapshotHistoryStreamChan(ctx, revision)
	if err != nil {
		return nil, err
	}
	out := make(chan backend.SnapshotHistoryChunk)
	go func() {
		defer close(out)
		for chunk := range source {
			if len(chunk.Records) > 0 {
				b.dataChunks++
				if len(chunk.Records) > b.maxChunkKeys {
					b.maxChunkKeys = len(chunk.Records)
				}
			}
			out <- chunk
		}
	}()
	return out, nil
}

func TestMaintenanceSnapshotStreamsCurrentKVAndPreservesMetadata(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	var lastRevision int64
	const recordCount = 605
	for i := 0; i < recordCount; i++ {
		put, err := server.Put(aliceCtx, &etcdserverpb.PutRequest{
			Key: []byte(fmt.Sprintf("/allowed/snapshot/%03d", i)), Value: []byte(fmt.Sprintf("value-%03d", i)),
		})
		require.NoError(t, err)
		lastRevision = put.Header.Revision
	}
	_, err := server.backend.ArmNoSpace(context.Background(), 29)
	require.NoError(t, err)
	require.NoError(t, server.backend.ArmCorrupt(context.Background(), 31))
	_, err = server.mutateGenericAlarm(context.Background(), etcdserverpb.AlarmType(127), 37, true)
	require.NoError(t, err)

	wrapped := &streamingSnapshotBackend{BackendShim: server.backend}
	server.backend = wrapped
	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, server.buildSnapshot(context.Background(), path))
	require.Zero(t, wrapped.fullRangeLists)
	require.Equal(t, 1, wrapped.streams)
	require.Greater(t, wrapped.dataChunks, 1)
	require.Less(t, wrapped.maxChunkKeys, recordCount)

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var records []*mvccpb.KeyValue
		require.NoError(t, tx.Bucket(schema.Key.Name()).ForEach(func(_, value []byte) error {
			kv := new(mvccpb.KeyValue)
			if err := proto.Unmarshal(value, kv); err != nil {
				return err
			}
			if len(kv.Key) != 0 {
				records = append(records, kv)
			}
			return nil
		}))
		require.Len(t, records, recordCount)
		require.Equal(t, lastRevision, records[len(records)-1].ModRevision)
		require.Equal(t, []byte("value-604"), records[len(records)-1].Value)
		require.NotNil(t, tx.Bucket(schema.AuthUsers.Name()).Get([]byte("alice")))
		var alarms []string
		require.NoError(t, tx.Bucket(schema.Alarm.Name()).ForEach(func(key, _ []byte) error {
			alarm := new(etcdserverpb.AlarmMember)
			if err := proto.Unmarshal(key, alarm); err != nil {
				return err
			}
			alarms = append(alarms, fmt.Sprintf("%d/%d", alarm.MemberID, alarm.Alarm))
			return nil
		}))
		require.ElementsMatch(t, []string{"29/1", "31/2", "37/127"}, alarms)
		return nil
	}))
}

func TestMaintenanceSnapshotRecoversCurrentLegacyLeaseFromAttachment(t *testing.T) {
	// Pre-inline KubeBrain values carry create/version in the separate etcdmeta
	// keyspace, but no per-version lease. The durable current-key attachment is
	// therefore the only authoritative source for the lease of the live row.
	server, closeFn := newTestRPCServerWithCompatibility(t, false)
	defer closeFn()
	ctx := context.Background()
	grant, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: 1701, TTL: 300})
	require.NoError(t, err)
	key := []byte("/snapshot/legacy-leased")
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: grant.ID})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, server.buildSnapshot(ctx, path))
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var restored *mvccpb.KeyValue
		require.NoError(t, tx.Bucket(schema.Key.Name()).ForEach(func(_, value []byte) error {
			kv := new(mvccpb.KeyValue)
			if err := proto.Unmarshal(value, kv); err != nil {
				return err
			}
			if bytes.Equal(kv.Key, key) && kv.ModRevision == put.Header.Revision {
				restored = kv
			}
			return nil
		}))
		require.NotNil(t, restored)
		require.Equal(t, grant.ID, restored.Lease,
			"a restorable current legacy lease must not be flattened to no-lease")
		return nil
	}))
}

func TestMaintenanceSnapshotRejectsUnknownHistoricalLegacyLease(t *testing.T) {
	// A pre-v2 value stores create/version metadata but not its per-version
	// lease. Once the key is rebound, the current attachment can recover only
	// the live version; it cannot prove whether the retained old version was
	// leased. Flattening that unknown state to lease=0 creates a valid-looking
	// bbolt snapshot with false MVCC history.
	server, closeFn := newTestRPCServerWithCompatibility(t, false)
	defer closeFn()
	ctx := context.Background()
	grant, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: 1751, TTL: 300})
	require.NoError(t, err)
	key := []byte("/snapshot/legacy-historical-lease")
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("leased-v1"), Lease: grant.ID})
	require.NoError(t, err)
	current, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("unleased-v2")})
	require.NoError(t, err)

	err = server.buildSnapshot(ctx, filepath.Join(t.TempDir(), "snapshot.db"))
	require.ErrorContains(t, err, "snapshot cannot determine lease for retained legacy version")

	// The limitation is bounded by retained history, not by the lifetime of the
	// database. Once a physical compaction removes the ambiguous old version,
	// the recoverable current legacy row can be snapshotted normally.
	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: current.Header.Revision, Physical: true})
	require.NoError(t, err)
	require.NoError(t, server.buildSnapshot(ctx, filepath.Join(t.TempDir(), "after-compact.db")))
}

func TestMaintenanceSnapshotRejectsCurrentLeaseDisagreeingWithPinnedAttachment(t *testing.T) {
	server, closeFn := newTestRPCServerWithCompatibility(t, false)
	defer closeFn()
	ctx := context.Background()
	attached, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: 1801, TTL: 300})
	require.NoError(t, err)
	other, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: 1802, TTL: 300})
	require.NoError(t, err)
	key := []byte("/snapshot/pinned-lease")
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: attached.ID})
	require.NoError(t, err)

	// Model a late chunk whose live in-memory lookup raced ahead of the pinned
	// history/metadata point. Merely checking that the other lease exists would
	// accept a restorable but semantically false artifact.
	server.backend = &corruptCurrentLeaseSnapshotBackend{
		BackendShim: server.backend, key: key, value: []byte("value"), lease: other.ID,
	}
	err = server.buildSnapshot(ctx, filepath.Join(t.TempDir(), "snapshot.db"))
	require.ErrorIs(t, err, errSnapshotChanged)
	require.Positive(t, put.Header.Revision)
}
