package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/hack/backup/internal/pitrinventory"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/kubewharf/kubebrain/hack/backup/internal/semanticverify"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestNativeFullRestoreRealBR is an opt-in destructive integration test for
// two disposable, dedicated PD/TiKV clusters. The caller owns their lifecycle.
func TestNativeFullRestoreRealBR(t *testing.T) {
	testNativeRestoreRealBR(t, false)
}

func TestNativeLogReplayRealBR(t *testing.T) {
	testNativeRestoreRealBR(t, true)
}

// TestNativeQuotaRealTiKV verifies etcd's logical quota/NOSPACE contract on a
// disposable real TiKV keyspace. It deliberately does not simulate physical
// ENOSPC: filling a store filesystem is a separate infrastructure failure mode.
func TestNativeQuotaRealTiKV(t *testing.T) {
	targetPD := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD")
	serverBinary := os.Getenv("KUBEBRAIN_NATIVE_PITR_SERVER")
	if targetPD == "" || serverBinary == "" {
		t.Skip("set KUBEBRAIN_NATIVE_PITR_TARGET_PD and KUBEBRAIN_NATIVE_PITR_SERVER")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	root := t.TempDir()
	const endpoint = "127.0.0.1:45379"

	server := startKubeBrainForKeyspace(t, ctx, serverBinary, targetPD, root, "quota-initial", "quota-integration", "--quota-backend-bytes=10")
	client := waitForEndpoint(t, ctx, endpoint, server)

	_, err := client.Put(ctx, "a", "1234") // 5 logical bytes.
	require.NoError(t, err)
	_, err = client.Put(ctx, "b", "123") // 9 logical bytes total.
	require.NoError(t, err)
	_, err = client.Put(ctx, "c", "x") // 11 bytes would exceed the quota.
	require.ErrorIs(t, err, rpctypes.ErrNoSpace)

	alarms, err := client.AlarmList(ctx)
	require.NoError(t, err)
	require.Len(t, alarms.Alarms, 1)
	require.Equal(t, etcdserverpb.AlarmType_NOSPACE, alarms.Alarms[0].Alarm)
	status, err := client.Status(ctx, endpoint)
	require.NoError(t, err)
	require.EqualValues(t, 9, status.DbSize)
	require.EqualValues(t, 9, status.DbSizeInUse)
	require.EqualValues(t, 10, status.DbSizeQuota)
	require.Contains(t, strings.Join(status.Errors, " "), "NOSPACE")
	got, err := client.Get(ctx, "a")
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, []byte("1234"), got.Kvs[0].Value)

	require.NoError(t, client.Close())
	server.stop(t)

	server = startKubeBrainForKeyspace(t, ctx, serverBinary, targetPD, root, "quota-restart", "quota-integration", "--quota-backend-bytes=10")
	defer server.stop(t)
	client = waitForEndpoint(t, ctx, endpoint, server)
	defer client.Close()

	alarms, err = client.AlarmList(ctx)
	require.NoError(t, err)
	require.Len(t, alarms.Alarms, 1, "NOSPACE must survive a KubeBrain restart")
	require.Equal(t, etcdserverpb.AlarmType_NOSPACE, alarms.Alarms[0].Alarm)
	for key, value := range map[string]string{"a": "1234", "b": "123"} {
		got, getErr := client.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, got.Kvs, 1)
		require.Equal(t, []byte(value), got.Kvs[0].Value)
	}
	_, err = client.Put(ctx, "a", "1")
	require.ErrorIs(t, err, rpctypes.ErrNoSpace, "sticky NOSPACE must cap even a shrinking Put")
	_, err = client.Delete(ctx, "b")
	require.NoError(t, err, "Delete must remain available for capacity recovery")
	stillActive, err := client.AlarmList(ctx)
	require.NoError(t, err)
	require.Len(t, stillActive.Alarms, 1, "capacity recovery must not implicitly disarm NOSPACE")

	_, err = client.AlarmDisarm(ctx, (*clientv3.AlarmMember)(alarms.Alarms[0]))
	require.NoError(t, err)
	cleared, err := client.AlarmList(ctx)
	require.NoError(t, err)
	require.Empty(t, cleared.Alarms)
	_, err = client.Put(ctx, "c", "x")
	require.NoError(t, err)
	status, err = client.Status(ctx, endpoint)
	require.NoError(t, err)
	require.EqualValues(t, 7, status.DbSize)
	require.EqualValues(t, 7, status.DbSizeInUse)
	require.EqualValues(t, 10, status.DbSizeQuota)
	require.Empty(t, status.Errors)
}

// TestNativePhysicalENOSPCRealTiKV fills one bounded target store filesystem,
// verifies that a three-replica data plane still commits through the remaining
// quorum, then restores the same store data directory and identity.
func TestNativePhysicalENOSPCRealTiKV(t *testing.T) {
	targetPD := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD")
	serverBinary := os.Getenv("KUBEBRAIN_NATIVE_PITR_SERVER")
	storeContainer := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_CONTAINER")
	storeDataDir := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_DATA_DIR")
	storeStatus := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_STATUS")
	if targetPD == "" || serverBinary == "" || storeContainer == "" || storeDataDir == "" || storeStatus == "" {
		t.Skip("set the native PITR target and ENOSPC integration environment")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pdc, err := pd.NewClientWithContext(ctx, strings.Split(targetPD, ","), pd.SecurityOption{})
	require.NoError(t, err)
	defer pdc.Close()
	storeID := requireStoreIDAtAddress(t, ctx, pdc, "127.0.0.1:43160")

	root := t.TempDir()
	const endpoint = "127.0.0.1:45379"
	server := startKubeBrainForKeyspace(t, ctx, serverBinary, targetPD, root, "physical-enospc", "physical-enospc-integration")
	defer server.stop(t)
	client := waitForEndpoint(t, ctx, endpoint, server)
	defer client.Close()
	_, err = client.Put(ctx, "before", "durable-before-pressure")
	require.NoError(t, err)

	pressurePath := filepath.Join(storeDataDir, "kubebrain-enospc-pressure")
	defer os.Remove(pressurePath)
	fill := exec.CommandContext(ctx, "dd", "if=/dev/zero", "of="+pressurePath, "bs=1M", "status=none", "conv=fsync")
	fillOutput, fillErr := fill.CombinedOutput()
	require.Error(t, fillErr, "unbounded dd must stop at the bounded test filesystem")
	require.Contains(t, strings.ToLower(string(fillOutput)), "no space left on device")
	var stat syscall.Statfs_t
	require.NoError(t, syscall.Statfs(storeDataDir, &stat))
	require.Less(t, stat.Bavail*uint64(stat.Bsize), uint64(1024*1024), "test store filesystem was not filled")

	largeValue := strings.Repeat("x", 256*1024)
	lastPressureKey := ""
	observedENOSPC := false
	for index := 0; index < 512; index++ {
		lastPressureKey = fmt.Sprintf("during-pressure-%03d", index)
		faultWriteCtx, faultWriteCancel := context.WithTimeout(ctx, 45*time.Second)
		_, err = client.Put(faultWriteCtx, lastPressureKey, largeValue)
		faultWriteCancel()
		require.NoError(t, err, "the remaining two stores must preserve write quorum")
		if containerLogContains(ctx, storeContainer, "no space left on device", "os error 28") {
			observedENOSPC = true
			break
		}
	}
	require.True(t, observedENOSPC, "TiKV did not surface physical ENOSPC after exhausting preallocated files")

	require.NoError(t, os.Remove(pressurePath))
	restartOutput, err := exec.CommandContext(ctx, "docker", "restart", storeContainer).CombinedOutput()
	require.NoError(t, err, "%s", restartOutput)
	waitForAddresses(t, ctx, storeStatus)
	require.Eventually(t, func() bool {
		return requireStoreIDAtAddressNoFail(ctx, pdc, "127.0.0.1:43160") == storeID
	}, 45*time.Second, 500*time.Millisecond, "the recovered address must retain its original PD store identity")

	_, err = client.Put(ctx, "after", "durable-after-recovery")
	require.NoError(t, err)
	for key, value := range map[string]string{
		"before":        "durable-before-pressure",
		lastPressureKey: largeValue,
		"after":         "durable-after-recovery",
	} {
		response, getErr := client.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, response.Kvs, 1)
		require.Equal(t, []byte(value), response.Kvs[0].Value)
	}
}

// TestNativeTwoStoreENOSPCRealTiKV verifies fail-closed behavior after physical
// ENOSPC removes a TiKV majority. Failed client calls are reconciled after
// recovery instead of being incorrectly classified as definitely uncommitted.
func TestNativeTwoStoreENOSPCRealTiKV(t *testing.T) {
	targetPD := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD")
	serverBinary := os.Getenv("KUBEBRAIN_NATIVE_PITR_SERVER")
	containers := strings.Split(os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_CONTAINERS"), ",")
	dataDirs := strings.Split(os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_DATA_DIRS"), ",")
	statuses := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_STATUSES")
	if targetPD == "" || serverBinary == "" || len(containers) != 2 || len(dataDirs) != 2 || statuses == "" || containers[0] == "" || dataDirs[0] == "" {
		t.Skip("set the native PITR two-store ENOSPC integration environment")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pdc, err := pd.NewClientWithContext(ctx, strings.Split(targetPD, ","), pd.SecurityOption{})
	require.NoError(t, err)
	defer pdc.Close()
	storeIDs := []uint64{
		requireStoreIDAtAddress(t, ctx, pdc, "127.0.0.1:43160"),
		requireStoreIDAtAddress(t, ctx, pdc, "127.0.0.1:43161"),
	}

	root := t.TempDir()
	const endpoint = "127.0.0.1:45379"
	server := startKubeBrainForKeyspace(t, ctx, serverBinary, targetPD, root, "two-store-enospc", "two-store-enospc-integration")
	defer server.stop(t)
	client := waitForEndpoint(t, ctx, endpoint, server)
	defer client.Close()
	_, err = client.Put(ctx, "before", "durable-before-quorum-loss")
	require.NoError(t, err)

	largeValue := strings.Repeat("q", 768*1024)
	acknowledged := make([]string, 0, 256)
	pressurePaths := make([]string, 2)
	for storeIndex := 0; storeIndex < 2; storeIndex++ {
		pressurePaths[storeIndex] = filepath.Join(dataDirs[storeIndex], "kubebrain-enospc-pressure")
		defer os.Remove(pressurePaths[storeIndex])
		fillBoundedFilesystem(t, ctx, dataDirs[storeIndex], pressurePaths[storeIndex])
		observedENOSPC := false
		for writeIndex := 0; writeIndex < 256; writeIndex++ {
			key := fmt.Sprintf("acknowledged-%d-%03d", storeIndex, writeIndex)
			attempt, attemptCancel := context.WithTimeout(ctx, 15*time.Second)
			_, putErr := client.Put(attempt, key, largeValue)
			attemptCancel()
			if putErr == nil {
				acknowledged = append(acknowledged, key)
			}
			if containerLogContains(ctx, containers[storeIndex], "no space left on device", "os error 28") {
				observedENOSPC = true
				break
			}
		}
		require.True(t, observedENOSPC, "TiKV store %d did not surface physical ENOSPC", storeIndex)
	}
	require.NotEmpty(t, acknowledged, "the first single-store fault window must retain acknowledged progress")

	ambiguousCtx, ambiguousCancel := context.WithTimeout(ctx, 5*time.Second)
	_, ambiguousErr := client.Put(ambiguousCtx, "ambiguous-at-quorum-loss", "reconcile-after-recovery")
	ambiguousCancel()
	require.Error(t, ambiguousErr, "a two-store ENOSPC quorum loss must not return write success")

	for index, pressurePath := range pressurePaths {
		require.NoError(t, os.Remove(pressurePath))
		restartOutput, restartErr := exec.CommandContext(ctx, "docker", "restart", containers[index]).CombinedOutput()
		require.NoError(t, restartErr, "%s", restartOutput)
	}
	waitForAddresses(t, ctx, statuses)
	for index, address := range []string{"127.0.0.1:43160", "127.0.0.1:43161"} {
		wantID := storeIDs[index]
		require.Eventually(t, func() bool {
			return requireStoreIDAtAddressNoFail(ctx, pdc, address) == wantID
		}, 45*time.Second, 500*time.Millisecond, "recovered store %d must retain its original identity", index)
	}

	for _, key := range acknowledged {
		response, getErr := client.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, response.Kvs, 1, "every acknowledged write must survive quorum recovery")
		require.Equal(t, []byte(largeValue), response.Kvs[0].Value)
	}
	// A timeout/error is not proof of non-commit. A linearizable read reconciles
	// the operation to either legal state before subsequent progress.
	ambiguous, err := client.Get(ctx, "ambiguous-at-quorum-loss")
	require.NoError(t, err)
	require.LessOrEqual(t, len(ambiguous.Kvs), 1)
	if len(ambiguous.Kvs) == 1 {
		require.Equal(t, []byte("reconcile-after-recovery"), ambiguous.Kvs[0].Value)
	}
	_, err = client.Put(ctx, "after", "durable-after-quorum-recovery")
	require.NoError(t, err)
	before, err := client.Get(ctx, "before")
	require.NoError(t, err)
	require.Len(t, before.Kvs, 1)
	require.Equal(t, []byte("durable-before-quorum-loss"), before.Kvs[0].Value)
}

func fillBoundedFilesystem(t *testing.T, ctx context.Context, dataDir, pressurePath string) {
	t.Helper()
	fill := exec.CommandContext(ctx, "dd", "if=/dev/zero", "of="+pressurePath, "bs=1M", "status=none", "conv=fsync")
	fillOutput, fillErr := fill.CombinedOutput()
	require.Error(t, fillErr, "unbounded dd must stop at the bounded test filesystem")
	require.Contains(t, strings.ToLower(string(fillOutput)), "no space left on device")
	var stat syscall.Statfs_t
	require.NoError(t, syscall.Statfs(dataDir, &stat))
	require.Less(t, stat.Bavail*uint64(stat.Bsize), uint64(1024*1024), "test filesystem was not filled")
}

// TestNativePDLeaderENOSPCRealCluster verifies that physical disk exhaustion
// of the live PD leader is handled as a single control-plane member failure.
func TestNativePDLeaderENOSPCRealCluster(t *testing.T) {
	targetPD := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD")
	serverBinary := os.Getenv("KUBEBRAIN_NATIVE_PITR_SERVER")
	pdContainer := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_CONTAINER")
	pdDataDir := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_DATA_DIR")
	pdEndpoint := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_ENDPOINT")
	pdMemberName := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_MEMBER")
	if targetPD == "" || serverBinary == "" || pdContainer == "" || pdDataDir == "" || pdEndpoint == "" || pdMemberName == "" {
		t.Skip("set the native PITR target PD ENOSPC integration environment")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pdEndpoints := strings.Split(targetPD, ",")
	pdEtcd, err := clientv3.New(clientv3.Config{Endpoints: pdEndpoints, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	defer pdEtcd.Close()
	members, err := pdEtcd.MemberList(ctx)
	require.NoError(t, err)
	oldLeaderID := uint64(0)
	for _, member := range members.Members {
		if member.Name == pdMemberName {
			oldLeaderID = member.ID
			break
		}
	}
	require.NotZero(t, oldLeaderID, "live leader %s is absent from embedded etcd membership", pdMemberName)

	root := t.TempDir()
	const endpoint = "127.0.0.1:45379"
	server := startKubeBrainForKeyspace(t, ctx, serverBinary, targetPD, root, "pd-enospc", "pd-enospc-integration")
	defer server.stop(t)
	client := waitForEndpoint(t, ctx, endpoint, server)
	defer client.Close()
	_, err = client.Put(ctx, "before", "durable-before-pd-pressure")
	require.NoError(t, err)

	pressurePath := filepath.Join(pdDataDir, "kubebrain-pd-enospc-pressure")
	defer os.Remove(pressurePath)
	fill := exec.CommandContext(ctx, "dd", "if=/dev/zero", "of="+pressurePath, "bs=1M", "status=none", "conv=fsync")
	fillOutput, fillErr := fill.CombinedOutput()
	require.Error(t, fillErr)
	require.Contains(t, strings.ToLower(string(fillOutput)), "no space left on device")
	var stat syscall.Statfs_t
	require.NoError(t, syscall.Statfs(pdDataDir, &stat))
	require.Less(t, stat.Bavail*uint64(stat.Bsize), uint64(1024*1024))

	probeValue := strings.Repeat("p", 256*1024)
	observedENOSPC := false
	for index := 0; index < 512; index++ {
		putCtx, putCancel := context.WithTimeout(ctx, 10*time.Second)
		_, putErr := pdEtcd.Put(putCtx, fmt.Sprintf("/kubebrain-integration/pd-enospc/%04d", index), probeValue)
		putCancel()
		if putErr != nil && !containerLogContains(ctx, pdContainer, "no space left on device", "os error 28") {
			continue
		}
		if containerLogContains(ctx, pdContainer, "no space left on device", "os error 28") {
			observedENOSPC = true
			break
		}
	}
	require.True(t, observedENOSPC, "PD did not surface physical ENOSPC after exhausting its preallocated WAL")
	newLeaderID := waitForDifferentEtcdLeader(t, ctx, pdEtcd, pdEndpoints, oldLeaderID)
	require.NotEqual(t, oldLeaderID, newLeaderID)
	require.Eventually(t, func() bool {
		attempt, attemptCancel := context.WithTimeout(ctx, 3*time.Second)
		_, putErr := client.Put(attempt, "during-pressure", "durable-through-pd-quorum")
		attemptCancel()
		return putErr == nil
	}, 45*time.Second, 500*time.Millisecond, "KubeBrain must recover its PD session/election through the remaining quorum")

	require.NoError(t, os.Remove(pressurePath))
	restartOutput, err := exec.CommandContext(ctx, "docker", "restart", pdContainer).CombinedOutput()
	require.NoError(t, err, "%s", restartOutput)
	waitForAddresses(t, ctx, pdEndpoint)
	require.Eventually(t, func() bool {
		response, listErr := pdEtcd.MemberList(ctx)
		if listErr != nil {
			return false
		}
		for _, member := range response.Members {
			if member.Name == pdMemberName && member.ID == oldLeaderID {
				return true
			}
		}
		return false
	}, 45*time.Second, 500*time.Millisecond, "recovered PD must retain its original member identity")
	_, err = pdEtcd.Delete(ctx, "/kubebrain-integration/pd-enospc/", clientv3.WithPrefix())
	require.NoError(t, err)
	_, err = client.Put(ctx, "after", "durable-after-pd-recovery")
	require.NoError(t, err)
	for key, value := range map[string]string{
		"before":          "durable-before-pd-pressure",
		"during-pressure": "durable-through-pd-quorum",
		"after":           "durable-after-pd-recovery",
	} {
		response, getErr := client.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, response.Kvs, 1)
		require.Equal(t, []byte(value), response.Kvs[0].Value)
	}
}

func waitForDifferentEtcdLeader(t *testing.T, ctx context.Context, client *clientv3.Client, endpoints []string, oldLeaderID uint64) uint64 {
	t.Helper()
	var leaderID uint64
	require.Eventually(t, func() bool {
		for _, endpoint := range endpoints {
			attempt, cancel := context.WithTimeout(ctx, time.Second)
			status, err := client.Status(attempt, endpoint)
			cancel()
			if err == nil && status.Leader != 0 && status.Leader != oldLeaderID {
				leaderID = status.Leader
				return true
			}
		}
		return false
	}, 45*time.Second, 500*time.Millisecond)
	return leaderID
}

func requireStoreIDAtAddress(t *testing.T, ctx context.Context, client pd.Client, address string) uint64 {
	t.Helper()
	storeID := requireStoreIDAtAddressNoFail(ctx, client, address)
	require.NotZero(t, storeID, "PD has no store at %s", address)
	return storeID
}

func requireStoreIDAtAddressNoFail(ctx context.Context, client pd.Client, address string) uint64 {
	stores, err := client.GetAllStores(ctx)
	if err != nil {
		return 0
	}
	for _, store := range stores {
		if store.GetAddress() == address {
			return store.GetId()
		}
	}
	return 0
}

func containerLogContains(ctx context.Context, container string, needles ...string) bool {
	output, err := exec.CommandContext(ctx, "docker", "logs", "--tail", "300", container).CombinedOutput()
	if err != nil {
		return false
	}
	lower := strings.ToLower(string(output))
	for _, needle := range needles {
		if strings.Contains(lower, strings.ToLower(needle)) {
			return true
		}
	}
	return false
}

func TestPDHTTPEndpointsPreserveEveryAddress(t *testing.T) {
	require.Equal(t, []string{"http://127.0.0.1:42379", "http://127.0.0.1:42389", "http://127.0.0.1:42399"}, pdHTTPEndpoints([]string{"127.0.0.1:42379", "127.0.0.1:42389", "127.0.0.1:42399"}))
}

func TestFaultContainersRejectMalformedInput(t *testing.T) {
	require.Equal(t, []string{"pd-0", "tikv-0"}, faultContainers("pd-0,tikv-0"))
	require.Nil(t, faultContainers(""))
	require.Nil(t, faultContainers("pd-0,,tikv-0"))
	require.Nil(t, faultContainers("pd/0"))
}

func TestFaultMarkerWriterMatchesAcrossWritesOnce(t *testing.T) {
	var output bytes.Buffer
	triggers := 0
	w := &faultMarkerWriter{dst: &output, marker: []byte("import mode"), trigger: func() error {
		triggers++
		return nil
	}}
	for _, part := range []string{"switch to im", "port mode at beginning\n", "import mode again"} {
		_, err := w.Write([]byte(part))
		require.NoError(t, err)
	}
	require.Equal(t, "switch to import mode at beginning\nimport mode again", output.String())
	require.Equal(t, 1, triggers)
}

func TestFaultMarkerWriterPropagatesTriggerError(t *testing.T) {
	w := &faultMarkerWriter{dst: io.Discard, marker: []byte("marker"), trigger: func() error {
		return errors.New("pause failed")
	}}
	_, err := w.Write([]byte("marker"))
	require.ErrorContains(t, err, "pause failed")
}

func pdHTTPEndpoints(addresses []string) []string {
	endpoints := make([]string, len(addresses))
	for i, address := range addresses {
		endpoints[i] = "http://" + address
	}
	return endpoints
}

func faultContainers(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		if part == "" || strings.Trim(part, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-") != "" {
			return nil
		}
		if _, ok := seen[part]; ok {
			return nil
		}
		seen[part] = struct{}{}
	}
	return parts
}

func injectContainerLoss(t *testing.T, ctx context.Context, raw string) bool {
	t.Helper()
	containers := faultContainers(raw)
	if raw == "" {
		return false
	}
	require.NotEmpty(t, containers, "invalid fault container list")
	args := append([]string{"pause"}, containers...)
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	require.NoError(t, err, "%s", output)
	for _, container := range containers {
		state, inspectErr := exec.CommandContext(ctx, "docker", "inspect", "--format={{.State.Paused}}", container).CombinedOutput()
		require.NoError(t, inspectErr, "%s", state)
		require.Equal(t, "true", strings.TrimSpace(string(state)), "fault target %s is not paused", container)
	}
	return true
}

type brFaultRunner struct {
	commandRunner
	ctx           context.Context
	raw           string
	recoveryDelay time.Duration
	mu            sync.Mutex
	injected      bool
	recovered     bool
	err           error
	recoveryDone  chan error
}

func (r *brFaultRunner) Run(ctx context.Context, name string, args []string, stdout, stderr io.Writer) error {
	marker := &faultMarkerWriter{dst: stderr, marker: []byte("switch to import mode at beginning"), trigger: func() error {
		err := pauseContainers(r.ctx, r.raw)
		r.mu.Lock()
		r.injected, r.err = err == nil, err
		if err == nil {
			r.recoveryDone = make(chan error, 1)
			go func() {
				timer := time.NewTimer(r.recoveryDelay)
				defer timer.Stop()
				select {
				case <-r.ctx.Done():
					r.recoveryDone <- r.ctx.Err()
				case <-timer.C:
					recoveryErr := unpauseContainers(r.ctx, r.raw)
					r.mu.Lock()
					r.recovered = recoveryErr == nil
					r.mu.Unlock()
					r.recoveryDone <- recoveryErr
				}
			}()
		}
		r.mu.Unlock()
		return err
	}}
	err := r.commandRunner.Run(ctx, name, args, stdout, marker)
	r.mu.Lock()
	injectionErr, injected, recoveryDone := r.err, r.injected, r.recoveryDone
	r.mu.Unlock()
	if injectionErr != nil {
		return fmt.Errorf("inject target fault during BR import: %w", injectionErr)
	}
	if err == nil && !injected {
		return errors.New("BR import-mode marker was not observed before restore completed")
	}
	if recoveryDone != nil {
		if recoveryErr := <-recoveryDone; recoveryErr != nil {
			return fmt.Errorf("recover target fault during BR import: %w", recoveryErr)
		}
	}
	return err
}

func (r *brFaultRunner) faultInjected() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.injected
}

func (r *brFaultRunner) faultRecovered() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recovered
}

type faultMarkerWriter struct {
	dst       io.Writer
	marker    []byte
	triggered bool
	buffer    []byte
	trigger   func() error
}

func (w *faultMarkerWriter) Write(p []byte) (int, error) {
	n, err := w.dst.Write(p)
	if err != nil || w.triggered {
		return n, err
	}
	w.buffer = append(w.buffer, p[:n]...)
	if bytes.Contains(w.buffer, w.marker) {
		w.triggered = true
		if triggerErr := w.trigger(); triggerErr != nil {
			return n, triggerErr
		}
	}
	if keep := len(w.marker) - 1; len(w.buffer) > keep {
		w.buffer = append(w.buffer[:0], w.buffer[len(w.buffer)-keep:]...)
	}
	return n, nil
}

func pauseContainers(ctx context.Context, raw string) error {
	containers := faultContainers(raw)
	if len(containers) == 0 {
		return errors.New("invalid or empty fault container list")
	}
	args := append([]string{"pause"}, containers...)
	if output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("pause fault containers: %w: %s", err, output)
	}
	for _, container := range containers {
		state, err := exec.CommandContext(ctx, "docker", "inspect", "--format={{.State.Paused}}", container).CombinedOutput()
		if err != nil {
			return fmt.Errorf("inspect fault container %s: %w: %s", container, err, state)
		}
		if strings.TrimSpace(string(state)) != "true" {
			return fmt.Errorf("fault target %s is not paused", container)
		}
	}
	return nil
}

func unpauseContainers(ctx context.Context, raw string) error {
	containers := faultContainers(raw)
	if len(containers) == 0 {
		return errors.New("invalid or empty fault container list")
	}
	args := append([]string{"unpause"}, containers...)
	if output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("unpause fault containers: %w: %s", err, output)
	}
	for _, container := range containers {
		state, err := exec.CommandContext(ctx, "docker", "inspect", "--format={{.State.Paused}}", container).CombinedOutput()
		if err != nil {
			return fmt.Errorf("inspect recovered container %s: %w: %s", container, err, state)
		}
		if strings.TrimSpace(string(state)) != "false" {
			return fmt.Errorf("fault target %s remains paused", container)
		}
	}
	return nil
}

func recoverContainer(t *testing.T, ctx context.Context, container, address string) {
	t.Helper()
	if container == "" && address == "" {
		return
	}
	require.Len(t, faultContainers(container), 1, "invalid recovery container")
	require.NotEmpty(t, address, "recovery address is required")
	output, err := exec.CommandContext(ctx, "docker", "unpause", container).CombinedOutput()
	require.NoError(t, err, "%s", output)
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := net.DialTimeout("tcp", address, 250*time.Millisecond)
		if dialErr == nil {
			require.NoError(t, conn.Close())
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("recovered fault target %s did not listen on %s", container, address)
}

func testNativeRestoreRealBR(t *testing.T, withLogs bool) {
	sourcePD := os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_PD")
	targetPD := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD")
	br := os.Getenv("KUBEBRAIN_NATIVE_PITR_BR")
	server := os.Getenv("KUBEBRAIN_NATIVE_PITR_SERVER")
	admissionCommand := os.Getenv("KUBEBRAIN_NATIVE_PITR_ADMISSION")
	restorePlanCommand := os.Getenv("KUBEBRAIN_NATIVE_PITR_RESTORE_PLAN")
	if sourcePD == "" || targetPD == "" || br == "" || server == "" || admissionCommand == "" || restorePlanCommand == "" {
		t.Skip("set the native PITR source/target PD, BR, server, admission, and restore-plan binaries")
	}
	preflight, taskCreate, mc := os.Getenv("KUBEBRAIN_NATIVE_PITR_PREFLIGHT"), os.Getenv("KUBEBRAIN_NATIVE_PITR_TASK_CREATE"), os.Getenv("KUBEBRAIN_NATIVE_PITR_MC")
	fenceCommand, replayCommand, semanticCommand := os.Getenv("KUBEBRAIN_NATIVE_PITR_FENCE"), os.Getenv("KUBEBRAIN_NATIVE_PITR_LOG_REPLAY"), os.Getenv("KUBEBRAIN_NATIVE_PITR_SEMANTIC_VERIFY")
	sourceCaptureCommand := os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_CAPTURE")
	s3Endpoint, s3Bucket, s3Prefix := os.Getenv("KUBEBRAIN_NATIVE_PITR_S3_ENDPOINT"), os.Getenv("KUBEBRAIN_NATIVE_PITR_S3_BUCKET"), os.Getenv("KUBEBRAIN_NATIVE_PITR_S3_PREFIX")
	if sourceCaptureCommand == "" {
		t.Skip("set KUBEBRAIN_NATIVE_PITR_SOURCE_CAPTURE")
	}
	if withLogs && (preflight == "" || taskCreate == "" || mc == "" || fenceCommand == "" || replayCommand == "" || semanticCommand == "" || s3Endpoint == "" || s3Bucket == "" || s3Prefix == "") {
		t.Skip("set native PITR preflight/task-create/mc and S3 integration variables")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	sourceAddrs, err := parseAddrs(sourcePD)
	require.NoError(t, err)
	targetAddrs, err := parseAddrs(targetPD)
	require.NoError(t, err)
	ks, err := coder.NewKeyspace("restore-integration")
	require.NoError(t, err)

	root := t.TempDir()
	const endpoint = "http://127.0.0.1:45379"
	sourceServer := startKubeBrain(t, ctx, server, sourcePD, root, "source")
	cli := waitForEndpoint(t, ctx, endpoint, sourceServer)
	lease, err := cli.Grant(ctx, 600)
	require.NoError(t, err)
	_, err = cli.Put(ctx, "/native-full", "kubebrain-native-full-restore-v1", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	_, err = cli.Put(ctx, "/native-unleased", "persistent")
	require.NoError(t, err)
	sourceKV, err := storagetikv.NewKvStorage(sourceAddrs, 1, storagetikv.Security{})
	require.NoError(t, err)
	pdc, err := pd.NewClientWithContext(ctx, sourceAddrs, pd.SecurityOption{})
	require.NoError(t, err)
	clusterID := pdc.GetClusterID(ctx)
	pdc.Close()
	require.NotZero(t, clusterID)
	const d = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	witnessPath := filepath.Join(root, "source.logical.v2")
	var task nativepitr.TaskCreateReceipt
	var taskBytes []byte
	var advancer *runningServer
	if withLogs {
		preflightPath := filepath.Join(root, "preflight.json")
		preflightBytes := runOutput(t, ctx, preflight, "--pd-addrs="+sourcePD, "--keyspace=restore-integration", "--task-name=restore-integration", "--timeout=30s")
		require.NoError(t, os.WriteFile(preflightPath, preflightBytes, 0o600))
		taskBytes = runOutput(t, ctx, taskCreate, "--preflight="+preflightPath, "--s3-endpoint="+s3Endpoint, "--s3-region=us-east-1", "--s3-bucket="+s3Bucket, "--s3-prefix="+s3Prefix, "--s3-provider=aws", "--s3-force-path-style=true", "--timeout=30s")
		var decodeErr error
		task, decodeErr = nativepitr.DecodeTaskCreate(bytes.NewReader(taskBytes))
		require.NoError(t, decodeErr)
		advancer = startProcess(t, ctx, filepath.Join(root, "advancer.log"), br, "log", "advancer", "--pd", sourcePD, "--task-name", task.TaskName, "--tick-interval=1s", "--try-advance-threshold=1s", "--log-file=/dev/stderr")
	} else {
		startTS, tsoErr := sourceKV.GetTimestampOracle(ctx)
		require.NoError(t, tsoErr)
		endTS := startTS + (uint64((10*time.Minute)/time.Millisecond) << 18)
		task = nativepitr.TaskCreateReceipt{Format: nativepitr.TaskCreateFormat, ClusterID: clusterID, Keyspace: ks.Name(), TaskName: "restore-integration", StartTS: startTS, CommittedAtTS: startTS, EndTS: endTS, StartKeyHex: hex.EncodeToString(ks.ObjectKeyspaceStart()), EndKeyHex: hex.EncodeToString(ks.ObjectKeyspaceEnd()), LogStoragePrefix: "s3://integration/log/task", LogStorageSHA256: d, PreflightSHA256: d, OwnerKey: nativepitr.TaskOwnerKey, BootstrapSafePointID: "kubebrain-native-pitr-bootstrap-restore-integration", BootstrapSafePointTTL: 7200, AtomicMetadataCreated: true}
		taskBytes = canonicalJSON(t, task)
	}
	var witnessStatus backupfile.Status
	sourceTaskPath := filepath.Join(root, "source-task.json")
	require.NoError(t, os.WriteFile(sourceTaskPath, taskBytes, 0o600))
	var sourceCaptureFencePath, sourceCapturePath string
	if !withLogs {
		injectContainerLoss(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_FAULT_CONTAINERS"))
		witnessStatus = writeWitness(t, ctx, cli, witnessPath)
		recoverContainer(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_RECOVERY_CONTAINER"), os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_RECOVERY_ADDRESS"))
		recoverContainer(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_PD_RECOVERY_CONTAINER"), os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_PD_RECOVERY_ADDRESS"))
	}
	if !withLogs {
		require.NoError(t, cli.Close())
		sourceServer.stop(t)
	}
	if !withLogs {
		sourceCaptureFenceBytes := runReceiptOutput(t, ctx, sourceCaptureCommand, "--action=acquire", "--task-create="+sourceTaskPath, "--source-witness="+witnessPath, "--operation-id=restore-integration-source", "--source-pd-addrs="+sourcePD, "--timeout=1m")
		sourceCaptureFence, decodeErr := nativepitr.DecodeSourceCaptureFenceReceipt(bytes.NewReader(sourceCaptureFenceBytes))
		require.NoError(t, decodeErr)
		sourceCaptureFencePath = filepath.Join(root, "source-capture-fence.json")
		require.NoError(t, os.WriteFile(sourceCaptureFencePath, sourceCaptureFenceBytes, 0o600))
		require.LessOrEqual(t, sourceCaptureFence.FenceSnapshotTS, task.EndTS)
	}
	backupTS, err := sourceKV.GetTimestampOracle(ctx)
	require.NoError(t, err)

	artifactRoot := filepath.Join(root, "br")
	require.NoError(t, os.Mkdir(artifactRoot, 0o700))
	backup := exec.CommandContext(ctx, br, "backup", "txn", "--pd", strings.Join(sourceAddrs, ","), "--storage", "local://"+artifactRoot, "--backupts", fmt.Sprint(backupTS), "--checksum=false", "--log-file", "/dev/stderr")
	backup.Stdout, backup.Stderr = os.Stderr, os.Stderr
	require.NoError(t, backup.Run())
	if withLogs {
		injectContainerLoss(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_FAULT_CONTAINERS"))
	}

	restoreTS := backupTS
	var ready nativepitr.TaskReadyReceipt
	var readyBytes []byte
	var logs nativepitr.LogArtifactReceipt
	var logRoot string
	if withLogs {
		if coldRestart := os.Getenv("KUBEBRAIN_NATIVE_PITR_COLD_RESTART_DURING_FAULT"); coldRestart != "" {
			require.Equal(t, "true", coldRestart, "invalid cold restart setting")
			require.NoError(t, cli.Close())
			sourceServer.stop(t)
			sourceServer = startKubeBrain(t, ctx, server, sourcePD, root, "source-log-window")
			cli = waitForEndpoint(t, ctx, endpoint, sourceServer)
			waitForLeader(t, ctx, cli, endpoint, sourceServer)
		}
		if os.Getenv("KUBEBRAIN_NATIVE_PITR_COLD_RESTART_DURING_FAULT") == "true" {
			require.NoError(t, putEventually(ctx, cli, "/native-full", "kubebrain-after-full", 45*time.Second, clientv3.WithLease(lease.ID)))
		} else {
			_, err = cli.Put(ctx, "/native-full", "kubebrain-after-full", clientv3.WithLease(lease.ID))
			require.NoError(t, err)
		}
		_, err = cli.Delete(ctx, "/native-unleased")
		require.NoError(t, err)
		_, err = cli.Put(ctx, "/native-after-full", strings.Repeat("log-value-", 64))
		require.NoError(t, err)
		witnessStatus = writeWitness(t, ctx, cli, witnessPath)
		recoverContainer(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_RECOVERY_CONTAINER"), os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_RECOVERY_ADDRESS"))
		recoverContainer(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_PD_RECOVERY_CONTAINER"), os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_PD_RECOVERY_ADDRESS"))
		require.NoError(t, cli.Close())
		sourceServer.stop(t)
		sourceCaptureFenceBytes := runReceiptOutput(t, ctx, sourceCaptureCommand, "--action=acquire", "--task-create="+sourceTaskPath, "--source-witness="+witnessPath, "--operation-id=restore-integration-source", "--source-pd-addrs="+sourcePD, "--timeout=1m")
		sourceCaptureFence, decodeErr := nativepitr.DecodeSourceCaptureFenceReceipt(bytes.NewReader(sourceCaptureFenceBytes))
		require.NoError(t, decodeErr)
		restoreTS = sourceCaptureFence.FenceSnapshotTS
		sourceCaptureFencePath = filepath.Join(root, "source-capture-fence.json")
		require.NoError(t, os.WriteFile(sourceCaptureFencePath, sourceCaptureFenceBytes, 0o600))
		metadataClient, err := clientv3.New(clientv3.Config{Endpoints: pdHTTPEndpoints(sourceAddrs), DialTimeout: 5 * time.Second})
		require.NoError(t, err)
		defer metadataClient.Close()
		pdcReady, err := pd.NewClientWithContext(ctx, sourceAddrs, pd.SecurityOption{})
		require.NoError(t, err)
		defer pdcReady.Close()
		deadline := time.Now().Add(45 * time.Second)
		var lastSnapshot nativepitr.TaskStatusSnapshot
		for {
			snapshot, statusErr := (nativepitr.EtcdMetadata{KV: metadataClient.KV}).ReadTaskStatus(ctx, task)
			lastSnapshot = snapshot
			if statusErr == nil && len(snapshot.GlobalCheckpoint) == 8 && binary.BigEndian.Uint64(snapshot.GlobalCheckpoint) >= restoreTS {
				break
			}
			if time.Now().After(deadline) {
				advancerLog, _ := os.ReadFile(filepath.Join(root, "advancer.log"))
				t.Fatalf("log checkpoint did not reach restore TSO %d: status=%v owner=%q checkpoint=%x advancer-log=%s", restoreTS, statusErr, lastSnapshot.AdvancerOwner, lastSnapshot.GlobalCheckpoint, advancerLog)
			}
			time.Sleep(time.Second)
		}
		ready, err = nativepitr.CheckTaskReady(ctx, pdcReady, nativepitr.EtcdMetadata{KV: metadataClient.KV}, task.ClusterID, task)
		require.NoError(t, err)
		readyBytes, err = json.Marshal(ready)
		require.NoError(t, err)
		stop := exec.CommandContext(ctx, br, "log", "stop", "--pd", sourcePD, "--task-name", task.TaskName, "--log-file=/dev/stderr")
		stop.Stdout, stop.Stderr = os.Stderr, os.Stderr
		require.NoError(t, stop.Run())
		advancer.stop(t)
		logRoot = filepath.Join(root, "log-mirror")
		require.NoError(t, os.Mkdir(logRoot, 0o700))
		copyCommand := exec.CommandContext(ctx, mc, "cp", "--recursive", "drill/"+s3Bucket+"/"+strings.Trim(s3Prefix, "/")+"/", logRoot)
		copyCommand.Stdout, copyCommand.Stderr = os.Stderr, os.Stderr
		require.NoError(t, copyCommand.Run())
		logInventory := inventoryForRoot(t, logRoot, s3Bucket, strings.Trim(s3Prefix, "/"))
		logs, err = nativepitr.VerifyLogArtifacts(task, digest(taskBytes), ready, digest(readyBytes), logInventory, digest(canonicalJSON(t, logInventory)), logRoot)
		require.NoError(t, err)
	}
	require.NoError(t, sourceKV.Close())
	full, err := nativepitr.BuildFullSnapshot(task, digest(taskBytes), "s3://integration/full/snapshot", mustRead(t, filepath.Join(artifactRoot, "backupmeta")))
	require.NoError(t, err)
	fullPath := filepath.Join(t.TempDir(), "full.json")
	fullBytes := canonicalFile(t, fullPath, full)
	sourceCaptureBytes := runReceiptOutput(t, ctx, sourceCaptureCommand, "--action=finalize", "--task-create="+sourceTaskPath, "--capture-fence="+sourceCaptureFencePath, "--full-snapshot="+fullPath, "--source-pd-addrs="+sourcePD, "--timeout=1m")
	sourceCapture, decodeErr := nativepitr.DecodeSourceCaptureReceipt(bytes.NewReader(sourceCaptureBytes))
	require.NoError(t, decodeErr)
	restoreTS = sourceCapture.CaptureTS
	require.True(t, sourceCapture.ContinuousSourceExclusion)
	sourceCapturePath = filepath.Join(root, "source-capture.json")
	require.NoError(t, os.WriteFile(sourceCapturePath, sourceCaptureBytes, 0o600))

	inventory := inventoryForRoot(t, artifactRoot, "integration", "full/snapshot")
	inventoryPath := filepath.Join(t.TempDir(), "inventory.json")
	inventoryBytes := canonicalFile(t, inventoryPath, inventory)
	artifact, err := nativepitr.VerifyFullArtifacts(full, digest(fullBytes), inventory, digest(inventoryBytes), artifactRoot)
	require.NoError(t, err)
	artifactPath := filepath.Join(t.TempDir(), "artifact.json")
	canonicalFile(t, artifactPath, artifact)

	sourceEvidence, err := nativepitr.InspectLiveSourceRangeExclusive(ctx, full, digest(fullBytes), sourceAddrs, "", "", "", time.Now().Unix())
	require.NoError(t, err)
	sourcePath := filepath.Join(t.TempDir(), "source.json")
	canonicalFile(t, sourcePath, sourceEvidence)
	targetEvidence, err := nativepitr.InspectLiveTargetSnapshotEmpty(ctx, targetAddrs, "", "", "", time.Now().Unix())
	require.NoError(t, err)
	targetPath := filepath.Join(t.TempDir(), "target.json")
	canonicalFile(t, targetPath, targetEvidence)

	var logPath string
	if !withLogs {
		ready = nativepitr.TaskReadyReceipt{Format: nativepitr.TaskReadyFormat, ClusterID: clusterID, Keyspace: task.Keyspace, TaskName: task.TaskName, StartTS: task.StartTS, CommittedAtTS: task.CommittedAtTS, EndTS: task.EndTS, GlobalCheckpointTS: full.BackupTS + 1, AdvancerOwner: "integration-owner", PreflightSHA256: d, BootstrapSafePointID: task.BootstrapSafePointID, BootstrapReleased: true, MetadataSnapshotValid: true}
		readyBytes, err = json.Marshal(ready)
		require.NoError(t, err)
		emptyObjects := []nativepitr.LogArtifactObject{}
		emptyManifest, marshalErr := json.Marshal(emptyObjects)
		require.NoError(t, marshalErr)
		logs = nativepitr.LogArtifactReceipt{Format: nativepitr.LogArtifactReceiptFormat, ClusterID: clusterID, Keyspace: task.Keyspace, TaskName: task.TaskName, TaskCreateSHA256: digest(taskBytes), TaskReadySHA256: digest(readyBytes), StartTS: task.StartTS, GlobalCheckpointTS: ready.GlobalCheckpointTS, StoragePrefix: task.LogStoragePrefix, StorageSHA256: task.LogStorageSHA256, RemoteInventorySHA256: d, ObjectStoreID: "integration", Bucket: "integration", ObjectPrefix: "log/task", MinRetainUntilUnix: 2_100_000_000, InventoryCheckedAtUnix: 2_000_000_000, Objects: emptyObjects, ManifestSHA256: digest(emptyManifest), ExactMirror: true, RemoteVersionsVerified: true, AllSegmentsVerified: true}
	} else {
		logPath = filepath.Join(t.TempDir(), "logs.json")
		canonicalFile(t, logPath, logs)
	}
	taskPath := filepath.Join(t.TempDir(), "task.json")
	require.NoError(t, os.WriteFile(taskPath, taskBytes, 0o600))
	readyPath := filepath.Join(t.TempDir(), "ready.json")
	require.NoError(t, os.WriteFile(readyPath, readyBytes, 0o600))
	if logPath == "" {
		logPath = filepath.Join(t.TempDir(), "logs.json")
		require.NoError(t, os.WriteFile(logPath, canonicalJSON(t, logs), 0o600))
	}
	planBytes := runReceiptOutput(t, ctx, restorePlanCommand, "--task-create="+taskPath, "--full-snapshot="+fullPath, "--full-artifacts="+artifactPath, "--task-ready="+readyPath, "--log-artifacts="+logPath, "--source-range-exclusive="+sourcePath, "--target-snapshot-empty="+targetPath, "--source-witness="+witnessPath, "--source-capture="+sourceCapturePath)
	plan, err := nativepitr.DecodePlan(bytes.NewReader(planBytes))
	require.NoError(t, err)
	planPath := filepath.Join(t.TempDir(), "plan.json")
	require.NoError(t, os.WriteFile(planPath, planBytes, 0o600))
	admissionBytes := runReceiptOutput(t, ctx, admissionCommand, "--action=acquire", "--plan="+planPath, "--operation-id=restore-integration", "--target-pd-addrs="+targetPD, "--approve-plan-sha256="+digest(planBytes), "--timeout=30s")
	_, err = nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(admissionBytes))
	require.NoError(t, err)
	admissionPath := filepath.Join(t.TempDir(), "admission.json")
	require.NoError(t, os.WriteFile(admissionPath, admissionBytes, 0o600))

	var receiptOut strings.Builder
	runner := commandRunner(osRunner{})
	var importFault *brFaultRunner
	if setting := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_FAULT_DURING_BR"); setting != "" {
		require.Equal(t, "true", setting, "invalid target BR fault setting")
		importFault = &brFaultRunner{commandRunner: runner, ctx: ctx, raw: os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_FAULT_CONTAINERS"), recoveryDelay: 10 * time.Second}
		runner = importFault
	}
	err = execute(ctx, options{plan: planPath, full: fullPath, artifacts: artifactPath, inventory: inventoryPath, artifactRoot: artifactRoot, sourceExclusive: sourcePath, target: targetPath, admission: admissionPath, pdAddrs: strings.Join(targetAddrs, ","), brBinary: br, approve: digest(planBytes), timeout: 3 * time.Minute}, runner, nativepitr.InspectLiveTargetSnapshotEmpty, &receiptOut, os.Stderr, time.Now)
	require.NoError(t, err)
	restore, err := nativepitr.DecodeFullRestoreExecution(strings.NewReader(receiptOut.String()))
	require.NoError(t, err)
	targetFaultInjected := false
	if importFault != nil {
		require.True(t, importFault.faultInjected(), "target fault was not injected during BR import")
		require.True(t, importFault.faultRecovered(), "target fault was not recovered during BR import")
	} else {
		targetFaultInjected = injectContainerLoss(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_FAULT_CONTAINERS"))
	}
	var fenceReceipt nativepitr.RestorationFenceReceipt
	var fenceToken restorationfence.Token
	var fenceBytes []byte
	var fencePath, restorePath, replayPath string
	if withLogs {
		fenceBytes = runReceiptOutput(t, ctx, fenceCommand, "--action=acquire", "--plan="+planPath, "--operation-id=restore-integration", "--target-pd-addrs="+targetPD, "--approve-plan-sha256="+digest(planBytes), "--timeout=30s")
		fenceReceipt, err = nativepitr.DecodeRestorationFenceReceipt(bytes.NewReader(fenceBytes))
		require.NoError(t, err)
		fenceToken, err = fenceReceipt.Token()
		require.NoError(t, err)
	} else {
		fenceReceipt, fenceToken, err = nativepitr.BuildRestorationFenceReceipt(plan, digest(planBytes), "restore-integration", time.Now().Unix(), false)
		require.NoError(t, err)
		fenceBytes = canonicalJSON(t, fenceReceipt)
	}
	targetKV, err := storagetikv.NewKvStorage(targetAddrs, 1, storagetikv.Security{})
	require.NoError(t, err)
	predecessorToken, err := restorationfence.NewToken(plan.SourceCapture.OperationID, plan.SourceCapture.TaskCreateSHA256, plan.Source.ClusterID, plan.Source.Keyspace)
	require.NoError(t, err)
	resumed, err := restorationfence.AcquireFrom(ctx, targetKV, fenceReceipt.CoordinationPrefix, fenceToken, &predecessorToken)
	require.NoError(t, err)
	require.Equal(t, withLogs, resumed)
	require.NoError(t, restorationfence.Verify(ctx, targetKV, fenceReceipt.CoordinationPrefix, fenceToken))
	fencePath = filepath.Join(t.TempDir(), "fence.json")
	require.NoError(t, os.WriteFile(fencePath, fenceBytes, 0o600))
	restorePath = filepath.Join(t.TempDir(), "restore.json")
	require.NoError(t, os.WriteFile(restorePath, []byte(receiptOut.String()), 0o600))
	admissionHandoffBytes := runReceiptOutput(t, ctx, admissionCommand, "--action=release", "--plan="+planPath, "--admission-receipt="+admissionPath, "--full-restore="+restorePath, "--restoration-fence="+fencePath, "--target-pd-addrs="+targetPD, "--approve-plan-sha256="+digest(planBytes), "--timeout=30s")
	admissionHandoff, handoffErr := nativepitr.DecodeAdmissionHandoff(bytes.NewReader(admissionHandoffBytes))
	require.NoError(t, handoffErr)
	require.True(t, admissionHandoff.ContinuousWriterExclusion)
	admissionHandoffPath := filepath.Join(t.TempDir(), "admission-handoff.json")
	require.NoError(t, os.WriteFile(admissionHandoffPath, admissionHandoffBytes, 0o600))
	if withLogs {
		replayBytes := runReceiptOutput(t, ctx, replayCommand, "--plan="+planPath, "--full-restore="+restorePath, "--log-artifacts="+logPath, "--log-root="+logRoot, "--restoration-fence="+fencePath, "--admission-handoff="+admissionHandoffPath, "--target-pd-addrs="+targetPD, "--approve-plan-sha256="+digest(planBytes), "--timeout=1m")
		replayPath = filepath.Join(t.TempDir(), "replay.json")
		require.NoError(t, os.WriteFile(replayPath, replayBytes, 0o600))
		replayReceipt, receiptErr := nativepitr.DecodeLogReplayExecution(bytes.NewReader(replayBytes))
		require.NoError(t, receiptErr)
		require.Positive(t, replayReceipt.AppliedMutations)
		require.NoError(t, restorationfence.Verify(ctx, targetKV, fenceReceipt.CoordinationPrefix, fenceToken))
		require.True(t, replayReceipt.LogReplayCompleted)
		require.True(t, replayReceipt.ReplayWriteFenceProven)
		require.True(t, replayReceipt.ContinuousWriterExclusion)
		require.True(t, replayReceipt.TargetWriteFenceProven)
		require.False(t, replayReceipt.PITRComplete)
	}

	// A real target process can start its listeners, but the imported keyspace
	// cannot elect a leader or accept writes while the plan-bound token is held.
	fencedTarget := startKubeBrain(t, ctx, server, targetPD, root, "target-fenced")
	waitForTCP(t, ctx, "127.0.0.1:45379", fencedTarget)
	fencedClient, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	require.NoError(t, err)
	writeCtx, writeCancel := context.WithTimeout(ctx, 3*time.Second)
	_, fencedWriteErr := fencedClient.Put(writeCtx, "/must-not-commit-during-restore", "blocked")
	writeCancel()
	require.Error(t, fencedWriteErr)
	require.NoError(t, fencedClient.Close())
	fencedTarget.stop(t)
	require.NoError(t, restorationfence.Verify(ctx, targetKV, fenceReceipt.CoordinationPrefix, fenceToken))
	var handoffPath string
	if withLogs {
		handoffBytes := runReceiptOutput(t, ctx, fenceCommand, "--action=release", "--plan="+planPath, "--fence-receipt="+fencePath, "--log-replay-receipt="+replayPath, "--target-pd-addrs="+targetPD, "--approve-plan-sha256="+digest(planBytes), "--timeout=30s")
		handoff, handoffErr := nativepitr.DecodeRestorationFenceHandoff(bytes.NewReader(handoffBytes))
		require.NoError(t, handoffErr)
		require.True(t, handoff.AllKeysReopened)
		handoffPath = filepath.Join(t.TempDir(), "handoff.json")
		require.NoError(t, os.WriteFile(handoffPath, handoffBytes, 0o600))
	} else {
		require.NoError(t, restorationfence.Release(ctx, targetKV, fenceReceipt.CoordinationPrefix, fenceToken))
	}
	require.NoError(t, targetKV.Close())

	targetQuorumLoss := injectContainerLoss(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_QUORUM_LOSS_CONTAINERS"))
	targetServer := startKubeBrain(t, ctx, server, targetPD, root, "target")
	defer targetServer.stop(t)
	var targetClient *clientv3.Client
	if targetQuorumLoss {
		unavailableClient, clientErr := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
		require.NoError(t, clientErr)
		writeCtx, writeCancel := context.WithTimeout(ctx, 3*time.Second)
		_, writeErr := unavailableClient.Put(writeCtx, "/native-pitr-quorum-loss-probe", "must-not-commit")
		writeCancel()
		require.Error(t, writeErr, "target write unexpectedly committed without a TiKV quorum")
		require.NoError(t, unavailableClient.Close())
		require.NoError(t, unpauseContainers(ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_QUORUM_LOSS_CONTAINERS")))
		waitForAddresses(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_QUORUM_RECOVERY_ADDRESSES"))
		targetClient = waitForEndpoint(t, ctx, endpoint, targetServer)
		waitForLeader(t, ctx, targetClient, endpoint, targetServer)
		failedWrite, getErr := targetClient.Get(ctx, "/native-pitr-quorum-loss-probe")
		require.NoError(t, getErr)
		require.Empty(t, failedWrite.Kvs, "write attempted without quorum became visible after recovery")
		require.NoError(t, putEventually(ctx, targetClient, "/native-pitr-quorum-recovery-probe", "recovered", 45*time.Second))
		_, deleteErr := targetClient.Delete(ctx, "/native-pitr-quorum-recovery-probe")
		require.NoError(t, deleteErr)
	} else {
		targetClient = waitForEndpoint(t, ctx, endpoint, targetServer)
	}
	defer targetClient.Close()
	if withLogs {
		semanticBytes := runReceiptOutputEnv(t, ctx, []string{"ENDPOINT=" + endpoint}, semanticCommand, "--plan="+planPath, "--full-snapshot="+fullPath, "--full-restore="+restorePath, "--log-replay="+replayPath, "--admission-handoff="+admissionHandoffPath, "--fence-handoff="+handoffPath, "--witness="+witnessPath, "--probe-prefix=/native-pitr-integration-probe", "--target-pd-addrs="+targetPD, "--timeout=1m")
		semanticReceipt, semanticErr := nativepitr.DecodePITRSemanticVerification(bytes.NewReader(semanticBytes))
		require.NoError(t, semanticErr)
		require.True(t, semanticReceipt.PostRestoreSemanticValidated)
		require.True(t, semanticReceipt.FenceHandoffProven)
		require.True(t, semanticReceipt.TargetFullImportFenceProven)
		require.True(t, semanticReceipt.ContinuousWriterExclusion)
		require.True(t, semanticReceipt.PITRComplete)
	} else {
		verified, openErr := backupfile.OpenVerified(witnessPath)
		require.NoError(t, openErr)
		defer verified.Close()
		observation, verifyErr := semanticverify.Verify(ctx, targetClient, verified, "/native-pitr-integration-probe")
		require.NoError(t, verifyErr)
		require.NoError(t, semanticverify.VerifyTargetProbeHistory(ctx, targetAddrs, storagetikv.Security{}, task.Keyspace, observation))
		semanticReceipt, buildErr := nativepitr.BuildFullSemanticVerification(plan, full, restore, witnessStatus, nativepitr.FullSemanticVerificationInput{PlanSHA256: digest(planBytes), FullSnapshotSHA256: digest(fullBytes), FullRestoreSHA256: digest([]byte(receiptOut.String())), WitnessFileSHA256: digest(mustRead(t, witnessPath)), HistoricalHeaderRevision: observation.HistoricalHeaderRevision, CurrentHeaderRevision: observation.CurrentHeaderRevision, ProbePutRevision: observation.ProbePutRevision, ProbeDeleteRevision: observation.ProbeDeleteRevision, HistoricalExact: observation.HistoricalExact, CurrentExact: observation.CurrentExact, LeaseIdentityExact: observation.LeaseIdentityExact, WatchProbeSucceeded: observation.WatchProbeSucceeded, TargetProbeHistoryExact: true, VerifiedAtUnix: time.Now().UTC().Unix()})
		require.NoError(t, buildErr)
		require.True(t, semanticReceipt.FullRestoreSemanticValidated)
		require.False(t, semanticReceipt.PITRComplete)
	}
	if targetFaultInjected {
		recoverContainer(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_RECOVERY_CONTAINER"), os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_RECOVERY_ADDRESS"))
		recoverContainer(t, ctx, os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD_RECOVERY_CONTAINER"), os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD_RECOVERY_ADDRESS"))
	}
}

func waitForTCP(t *testing.T, ctx context.Context, address string, server *runningServer) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := (&net.Dialer{Timeout: 250 * time.Millisecond}).DialContext(ctx, "tcp", address)
		if err == nil {
			require.NoError(t, conn.Close())
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	logBytes, logErr := os.ReadFile(server.logPath)
	require.NoError(t, logErr)
	t.Fatalf("KubeBrain TCP listener did not become ready: %s", logBytes)
}

func waitForAddresses(t *testing.T, ctx context.Context, raw string) {
	t.Helper()
	addresses, err := parseAddrs(raw)
	require.NoError(t, err)
	require.NotEmpty(t, addresses)
	for _, address := range addresses {
		deadline := time.Now().Add(45 * time.Second)
		for time.Now().Before(deadline) {
			conn, dialErr := (&net.Dialer{Timeout: 250 * time.Millisecond}).DialContext(ctx, "tcp", address)
			if dialErr == nil {
				require.NoError(t, conn.Close())
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		conn, dialErr := (&net.Dialer{Timeout: 250 * time.Millisecond}).DialContext(ctx, "tcp", address)
		require.NoError(t, dialErr, "recovered target did not listen on %s", address)
		require.NoError(t, conn.Close())
	}
}

type runningServer struct {
	cmd     *exec.Cmd
	done    chan error
	logPath string
}

func startProcess(t *testing.T, ctx context.Context, logPath, binary string, args ...string) *runningServer {
	t.Helper()
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoError(t, cmd.Start())
	require.NoError(t, logFile.Close())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return &runningServer{cmd: cmd, done: done, logPath: logPath}
}

func runOutput(t *testing.T, ctx context.Context, binary string, args ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(ctx, binary, args...)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	return output
}

func runReceiptOutput(t *testing.T, ctx context.Context, binary string, args ...string) []byte {
	t.Helper()
	var stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout, cmd.Stderr = &stdout, os.Stderr
	require.NoError(t, cmd.Run())
	return stdout.Bytes()
}

func runReceiptOutputEnv(t *testing.T, ctx context.Context, env []string, binary string, args ...string) []byte {
	t.Helper()
	var stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = &stdout, os.Stderr
	require.NoError(t, cmd.Run())
	return stdout.Bytes()
}

func startKubeBrain(t *testing.T, ctx context.Context, binary, pdAddrs, root, label string) *runningServer {
	t.Helper()
	return startKubeBrainForKeyspace(t, ctx, binary, pdAddrs, root, label, "restore-integration")
}

func startKubeBrainForKeyspace(t *testing.T, ctx context.Context, binary, pdAddrs, root, label, keyspace string, extraArgs ...string) *runningServer {
	t.Helper()
	peerPort, infoPort := freeTCPPort(t), freeTCPPort(t)
	logFile, err := os.Create(filepath.Join(root, label+"-kubebrain.log"))
	require.NoError(t, err)
	args := []string{
		"--port=45379", fmt.Sprintf("--peer-port=%d", peerPort), fmt.Sprintf("--info-port=%d", infoPort),
		"--advertise-host=127.0.0.1", "--advertise-client-urls=http://127.0.0.1:45379",
		fmt.Sprintf("--initial-cluster=integration=http://127.0.0.1:%d", peerPort),
		"--pd-addrs=" + pdAddrs, "--keyspace=" + keyspace, "--compatible-with-etcd=true",
	}
	cmd := exec.CommandContext(ctx, binary, append(args, extraArgs...)...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoError(t, cmd.Start())
	require.NoError(t, logFile.Close())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return &runningServer{cmd: cmd, done: done, logPath: filepath.Join(root, label+"-kubebrain.log")}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func (s *runningServer) stop(t *testing.T) {
	t.Helper()
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return
	}
	_ = s.cmd.Process.Signal(os.Interrupt)
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		_ = s.cmd.Process.Kill()
		<-s.done
	}
	s.cmd = nil
}

func waitForEndpoint(t *testing.T, ctx context.Context, endpoint string, server *runningServer) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	require.NoError(t, err)
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		_, err = cli.Status(attempt, endpoint)
		cancel()
		if err == nil {
			return cli
		}
		time.Sleep(250 * time.Millisecond)
	}
	cli.Close()
	logBytes, logErr := os.ReadFile(server.logPath)
	require.NoError(t, err, "KubeBrain endpoint did not become ready; log-read-error=%v log=%s", logErr, logBytes)
	return nil
}

func waitForLeader(t *testing.T, ctx context.Context, cli *clientv3.Client, endpoint string, server *runningServer) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		status, err := cli.Status(attempt, endpoint)
		cancel()
		if err == nil && status.Leader != 0 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	logBytes, logErr := os.ReadFile(server.logPath)
	require.NoError(t, logErr)
	t.Fatalf("KubeBrain did not publish a leader: %s", logBytes)
}

func putEventually(ctx context.Context, cli *clientv3.Client, key, value string, timeout time.Duration, opts ...clientv3.OpOption) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, lastErr = cli.Put(attempt, key, value, opts...)
		cancel()
		if lastErr == nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("KubeBrain did not become writable: %w", lastErr)
}

func writeWitness(t *testing.T, ctx context.Context, cli *clientv3.Client, path string) backupfile.Status {
	t.Helper()
	response, err := cli.Get(ctx, "/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.NotNil(t, response.Header)
	require.NotEmpty(t, response.Kvs)
	writer, err := backupfile.NewAtomicWriter(path, "/", response.Header.Revision)
	require.NoError(t, err)
	defer writer.Abort()
	leases := make(map[int64]struct{})
	for _, kv := range response.Kvs {
		if kv.Lease != 0 {
			leases[kv.Lease] = struct{}{}
		}
	}
	for id := range leases {
		ttl, err := cli.TimeToLive(ctx, clientv3.LeaseID(id), clientv3.WithAttachedKeys())
		require.NoError(t, err)
		require.NoError(t, writer.AddLease(record.Lease{ID: id, TTL: ttl.TTL, GrantedTTL: ttl.GrantedTTL}))
	}
	for _, kv := range response.Kvs {
		require.NoError(t, writer.Add(record.Record{Key: base64.StdEncoding.EncodeToString(kv.Key), Value: base64.StdEncoding.EncodeToString(kv.Value), ModRevision: kv.ModRevision, CreateRevision: kv.CreateRevision, Version: kv.Version, Lease: kv.Lease}))
	}
	status, err := writer.Commit()
	require.NoError(t, err)
	return status
}

func canonicalFile(t *testing.T, path string, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	b = append(b, '\n')
	require.NoError(t, os.WriteFile(path, b, 0o600))
	return b
}
func canonicalJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return append(b, '\n')
}
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}
func inventoryForRoot(t *testing.T, root, bucket, prefix string) pitrinventory.Receipt {
	t.Helper()
	var entries []pitrinventory.Entry
	err := filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root || e.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		entries = append(entries, pitrinventory.Entry{Name: name, ObjectKey: prefix + "/" + name, VersionID: "integration-version-" + fmt.Sprint(len(entries)+1), Bytes: int64(len(b)), SHA256: digest(b), RetentionMode: "COMPLIANCE", RetainUntilUnix: 2_100_000_000})
		return nil
	})
	require.NoError(t, err)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	var total uint64
	for _, e := range entries {
		total += uint64(e.Bytes)
	}
	return pitrinventory.Receipt{Format: pitrinventory.Format, ObjectStoreID: "integration", Bucket: bucket, Prefix: prefix, Entries: entries, ObjectCount: len(entries), TotalBytes: total, Pages: 1, PaginationExhausted: true, ExactVersionsVerified: true, MinRetainUntilUnix: 2_100_000_000, CheckedAtUnix: 2_000_000_000}
}
