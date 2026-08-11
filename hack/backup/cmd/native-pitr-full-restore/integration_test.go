package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
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

func testNativeRestoreRealBR(t *testing.T, withLogs bool) {
	sourcePD := os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_PD")
	targetPD := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD")
	br := os.Getenv("KUBEBRAIN_NATIVE_PITR_BR")
	server := os.Getenv("KUBEBRAIN_NATIVE_PITR_SERVER")
	admissionCommand := os.Getenv("KUBEBRAIN_NATIVE_PITR_ADMISSION")
	if sourcePD == "" || targetPD == "" || br == "" || server == "" || admissionCommand == "" {
		t.Skip("set KUBEBRAIN_NATIVE_PITR_SOURCE_PD, KUBEBRAIN_NATIVE_PITR_TARGET_PD, KUBEBRAIN_NATIVE_PITR_BR, and KUBEBRAIN_NATIVE_PITR_SERVER")
	}
	preflight, taskCreate, mc := os.Getenv("KUBEBRAIN_NATIVE_PITR_PREFLIGHT"), os.Getenv("KUBEBRAIN_NATIVE_PITR_TASK_CREATE"), os.Getenv("KUBEBRAIN_NATIVE_PITR_MC")
	fenceCommand, replayCommand, semanticCommand := os.Getenv("KUBEBRAIN_NATIVE_PITR_FENCE"), os.Getenv("KUBEBRAIN_NATIVE_PITR_LOG_REPLAY"), os.Getenv("KUBEBRAIN_NATIVE_PITR_SEMANTIC_VERIFY")
	s3Endpoint, s3Bucket, s3Prefix := os.Getenv("KUBEBRAIN_NATIVE_PITR_S3_ENDPOINT"), os.Getenv("KUBEBRAIN_NATIVE_PITR_S3_BUCKET"), os.Getenv("KUBEBRAIN_NATIVE_PITR_S3_PREFIX")
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
	}
	var witnessStatus backupfile.Status
	if !withLogs {
		witnessStatus = writeWitness(t, ctx, cli, witnessPath)
	}
	require.NoError(t, cli.Close())
	sourceServer.stop(t)

	sourceKV, err := storagetikv.NewKvStorage(sourceAddrs, 1, storagetikv.Security{})
	require.NoError(t, err)
	backupTS, err := sourceKV.GetTimestampOracle(ctx)
	require.NoError(t, err)

	artifactRoot := filepath.Join(root, "br")
	require.NoError(t, os.Mkdir(artifactRoot, 0o700))
	backup := exec.CommandContext(ctx, br, "backup", "txn", "--pd", strings.Join(sourceAddrs, ","), "--storage", "local://"+artifactRoot, "--backupts", fmt.Sprint(backupTS), "--checksum=false", "--log-file", "/dev/stderr")
	backup.Stdout, backup.Stderr = os.Stderr, os.Stderr
	require.NoError(t, backup.Run())

	restoreTS := backupTS
	var ready nativepitr.TaskReadyReceipt
	var readyBytes []byte
	var logs nativepitr.LogArtifactReceipt
	var logRoot string
	if withLogs {
		sourceServer = startKubeBrain(t, ctx, server, sourcePD, root, "source-log-window")
		cli = waitForEndpoint(t, ctx, endpoint, sourceServer)
		_, err = cli.Put(ctx, "/native-full", "kubebrain-after-full", clientv3.WithLease(lease.ID))
		require.NoError(t, err)
		_, err = cli.Delete(ctx, "/native-unleased")
		require.NoError(t, err)
		_, err = cli.Put(ctx, "/native-after-full", strings.Repeat("log-value-", 64))
		require.NoError(t, err)
		witnessStatus = writeWitness(t, ctx, cli, witnessPath)
		require.NoError(t, cli.Close())
		sourceServer.stop(t)
		restoreTS, err = sourceKV.GetTimestampOracle(ctx)
		require.NoError(t, err)
		metadataClient, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://" + sourcePD}, DialTimeout: 5 * time.Second})
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

	pdc, err := pd.NewClientWithContext(ctx, sourceAddrs, pd.SecurityOption{})
	require.NoError(t, err)
	clusterID := pdc.GetClusterID(ctx)
	pdc.Close()
	require.NotZero(t, clusterID)
	const d = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if !withLogs {
		endTS := backupTS + (uint64((10*time.Minute)/time.Millisecond) << 18)
		task = nativepitr.TaskCreateReceipt{Format: nativepitr.TaskCreateFormat, ClusterID: clusterID, Keyspace: ks.Name(), TaskName: "restore-integration", StartTS: backupTS - 2, CommittedAtTS: backupTS - 1, EndTS: endTS, StartKeyHex: hex.EncodeToString(ks.ObjectKeyspaceStart()), EndKeyHex: hex.EncodeToString(ks.ObjectKeyspaceEnd()), LogStoragePrefix: "s3://integration/log/task", LogStorageSHA256: d, PreflightSHA256: d, OwnerKey: nativepitr.TaskOwnerKey, BootstrapSafePointID: "kubebrain-native-pitr-bootstrap-restore-integration", BootstrapSafePointTTL: 7200, AtomicMetadataCreated: true}
		taskBytes = canonicalFile(t, filepath.Join(t.TempDir(), "task.json"), task)
	}
	full, err := nativepitr.BuildFullSnapshot(task, digest(taskBytes), "s3://integration/full/snapshot", mustRead(t, filepath.Join(artifactRoot, "backupmeta")))
	require.NoError(t, err)
	if !withLogs {
		restoreTS = full.BackupTS
	}
	fullPath := filepath.Join(t.TempDir(), "full.json")
	fullBytes := canonicalFile(t, fullPath, full)

	inventory := inventoryForRoot(t, artifactRoot, "integration", "full/snapshot")
	inventoryPath := filepath.Join(t.TempDir(), "inventory.json")
	inventoryBytes := canonicalFile(t, inventoryPath, inventory)
	artifact, err := nativepitr.VerifyFullArtifacts(full, digest(fullBytes), inventory, digest(inventoryBytes), artifactRoot)
	require.NoError(t, err)
	artifactPath := filepath.Join(t.TempDir(), "artifact.json")
	artifactBytes := canonicalFile(t, artifactPath, artifact)

	sourceEvidence, err := nativepitr.InspectLiveSourceRangeExclusive(ctx, full, digest(fullBytes), sourceAddrs, "", "", "", time.Now().Unix())
	require.NoError(t, err)
	sourcePath := filepath.Join(t.TempDir(), "source.json")
	sourceBytes := canonicalFile(t, sourcePath, sourceEvidence)
	targetEvidence, err := nativepitr.InspectLiveTargetSnapshotEmpty(ctx, targetAddrs, "", "", "", time.Now().Unix())
	require.NoError(t, err)
	targetPath := filepath.Join(t.TempDir(), "target.json")
	targetBytes := canonicalFile(t, targetPath, targetEvidence)

	logReceiptSHA := d
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
		logReceiptSHA = digest(canonicalFile(t, logPath, logs))
	}
	plan, err := nativepitr.BuildFromReceipts(task, full, artifact, ready, logs, sourceEvidence, targetEvidence, nativepitr.ReceiptPlanInputs{TaskCreateSHA256: digest(taskBytes), FullSnapshotSHA256: digest(fullBytes), ArtifactReceiptSHA256: digest(artifactBytes), TaskReadySHA256: digest(readyBytes), LogArtifactSHA256: logReceiptSHA, SourceExclusiveSHA256: digest(sourceBytes), TargetReceiptSHA256: digest(targetBytes), RestoreTS: restoreTS})
	require.NoError(t, err)
	planPath := filepath.Join(t.TempDir(), "plan.json")
	planBytes := canonicalFile(t, planPath, plan)
	admissionBytes := runReceiptOutput(t, ctx, admissionCommand, "--action=acquire", "--plan="+planPath, "--operation-id=restore-integration", "--target-pd-addrs="+targetPD, "--approve-plan-sha256="+digest(planBytes), "--timeout=30s")
	_, err = nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(admissionBytes))
	require.NoError(t, err)
	admissionPath := filepath.Join(t.TempDir(), "admission.json")
	require.NoError(t, os.WriteFile(admissionPath, admissionBytes, 0o600))

	var receiptOut strings.Builder
	err = execute(ctx, options{plan: planPath, full: fullPath, artifacts: artifactPath, inventory: inventoryPath, artifactRoot: artifactRoot, sourceExclusive: sourcePath, target: targetPath, admission: admissionPath, pdAddrs: strings.Join(targetAddrs, ","), brBinary: br, approve: digest(planBytes), timeout: 3 * time.Minute}, osRunner{}, nativepitr.InspectLiveTargetSnapshotEmpty, &receiptOut, os.Stderr, time.Now)
	require.NoError(t, err)
	restore, err := nativepitr.DecodeFullRestoreExecution(strings.NewReader(receiptOut.String()))
	require.NoError(t, err)
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
	resumed, err := restorationfence.Acquire(ctx, targetKV, fenceReceipt.CoordinationPrefix, fenceToken)
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

	targetServer := startKubeBrain(t, ctx, server, targetPD, root, "target")
	defer targetServer.stop(t)
	targetClient := waitForEndpoint(t, ctx, endpoint, targetServer)
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
	peerPort, infoPort := freeTCPPort(t), freeTCPPort(t)
	logFile, err := os.Create(filepath.Join(root, label+"-kubebrain.log"))
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, binary,
		"--port=45379", fmt.Sprintf("--peer-port=%d", peerPort), fmt.Sprintf("--info-port=%d", infoPort),
		"--advertise-host=127.0.0.1", "--advertise-client-urls=http://127.0.0.1:45379",
		fmt.Sprintf("--initial-cluster=integration=http://127.0.0.1:%d", peerPort),
		"--pd-addrs="+pdAddrs, "--keyspace=restore-integration", "--compatible-with-etcd=true",
	)
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
