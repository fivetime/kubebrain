// Command native-pitr-full-restore executes BR's whole-cluster transactional
// full import for a plan that proves visible source keys are range-exclusive.
// It never claims that logs or post-restore etcd semantics were verified.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/hack/backup/internal/pitrinventory"
	"github.com/kubewharf/kubebrain/pkg/backend/admissionfence"
	pingcaplog "github.com/pingcap/log"
	"github.com/tikv/pd/client/tlsutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap/zapcore"
)

const maxReceiptBytes = 4 << 20

type options struct {
	plan, full, artifacts, inventory, artifactRoot, admission string
	sourceExclusive, target, targetProvisioning               string
	targetQualification, writerExclusion                      string
	pdAddrs, ca, cert, key, brBinary, approve                 string
	encryptionKeyID, encryptionKeyFile                        string
	admissionCheckInterval                                    time.Duration
	timeout                                                   time.Duration
}

type commandRunner interface {
	Output(context.Context, string, ...string) ([]byte, error)
	Run(context.Context, string, []string, io.Writer, io.Writer) error
}
type inspectTargetFn func(context.Context, []string, string, string, string, int64) (nativepitr.TargetSnapshotEmptyReceipt, error)
type osRunner struct{}

func (osRunner) Output(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}
func (osRunner) Run(ctx context.Context, name string, args []string, stdout, stderr io.Writer) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	return cmd.Run()
}

func main() {
	pingcaplog.SetLevel(zapcore.ErrorLevel)
	var o options
	flag.StringVar(&o.plan, "plan", "", "exact native-pitr-restore-plan receipt")
	flag.StringVar(&o.full, "full-snapshot", "", "exact native-pitr-full-snapshot.v3 receipt")
	flag.StringVar(&o.artifacts, "full-artifacts", "", "exact native-pitr-full-artifacts receipt")
	flag.StringVar(&o.inventory, "remote-inventory", "", "exact native-pitr-object-inventory.v1 receipt")
	flag.StringVar(&o.artifactRoot, "artifact-root", "", "absolute local exact-version full backup mirror")
	flag.StringVar(&o.sourceExclusive, "source-range-exclusive", "", "exact source range-exclusive receipt bound by plan")
	flag.StringVar(&o.target, "target-snapshot-empty", "", "exact target snapshot-empty receipt bound by plan")
	flag.StringVar(&o.targetProvisioning, "target-provisioning", "", "exact target physical provisioning receipt")
	flag.StringVar(&o.targetQualification, "target-qualification", "", "exact restore-safe target qualification receipt")
	flag.StringVar(&o.writerExclusion, "target-writer-exclusion", "", "exact KubeBrain writer exclusion evidence bound by qualification")
	flag.StringVar(&o.admission, "restore-admission", "", "exact PD-backed restore admission receipt")
	flag.StringVar(&o.pdAddrs, "pd-addrs", "", "comma-separated target PD addresses")
	flag.StringVar(&o.ca, "ca", "", "target CA file")
	flag.StringVar(&o.cert, "cert", "", "target client certificate")
	flag.StringVar(&o.key, "key", "", "target client private key")
	flag.StringVar(&o.brBinary, "br-binary", "br", "BR v7.5.1 executable")
	flag.StringVar(&o.encryptionKeyID, "encryption-key-id", "", "immutable non-secret key version ID required by encrypted artifacts")
	flag.StringVar(&o.encryptionKeyFile, "encryption-key-file", "", "file containing the exact AES-256 key required by encrypted artifacts")
	flag.StringVar(&o.approve, "approve-plan-sha256", "", "explicit approval equal to exact plan SHA-256")
	flag.DurationVar(&o.admissionCheckInterval, "admission-check-interval", 5*time.Second, "PD-backed restore admission verification interval during BR import")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Hour, "full restore deadline")
	flag.Parse()
	if err := execute(context.Background(), o, osRunner{}, nativepitr.InspectLiveTargetSnapshotEmpty, os.Stdout, os.Stderr, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR full restore:", err)
		os.Exit(1)
	}
}

func execute(parent context.Context, o options, runner commandRunner, inspectTarget inspectTargetFn, out, logs io.Writer, now func() time.Time) error {
	if err := validateOptions(o); err != nil {
		return err
	}
	planBytes, err := readSmall(o.plan)
	if err != nil {
		return err
	}
	plan, err := nativepitr.DecodePlan(bytes.NewReader(planBytes))
	if err != nil {
		return err
	}
	planSHA := digest(planBytes)
	if o.approve != planSHA {
		return errors.New("approve-plan-sha256 must equal the exact plan file SHA-256")
	}
	fullBytes, err := readSmall(o.full)
	if err != nil {
		return err
	}
	if digest(fullBytes) != plan.Full.ReceiptSHA256 {
		return errors.New("full-snapshot receipt does not match plan")
	}
	full, err := nativepitr.DecodeFullSnapshot(bytes.NewReader(fullBytes))
	if err != nil {
		return err
	}

	sourceBytes, err := readSmall(o.sourceExclusive)
	if err != nil {
		return err
	}
	if digest(sourceBytes) != plan.Source.RangeExclusiveReceiptSHA256 {
		return errors.New("source range-exclusive receipt does not match plan")
	}
	source, err := nativepitr.DecodeSourceRangeExclusive(bytes.NewReader(sourceBytes))
	if err != nil {
		return err
	}
	if source.FullSnapshotReceiptSHA256 != digest(fullBytes) || source.ClusterID != plan.Source.ClusterID || source.SnapshotTS != plan.Full.BackupTS {
		return errors.New("source range-exclusive receipt identity does not match plan")
	}

	artifactBytes, err := readSmall(o.artifacts)
	if err != nil {
		return err
	}
	if digest(artifactBytes) != plan.Full.ArtifactReceiptSHA256 {
		return errors.New("full-artifact receipt does not match plan")
	}
	artifact, err := nativepitr.DecodeArtifactReceipt(bytes.NewReader(artifactBytes))
	if err != nil {
		return err
	}
	planMethod := plan.Full.Encryption
	if planMethod == "" {
		planMethod = nativepitr.CipherMethodPlaintext
	}
	if planMethod != artifact.Encryption || plan.Full.EncryptionKeyID != artifact.EncryptionKeyID {
		return errors.New("restore plan encryption identity differs from full artifacts")
	}
	encryption := nativepitr.EncryptionIdentity{Method: artifact.Encryption, KeyID: artifact.EncryptionKeyID}
	if err := encryption.Validate(); err != nil {
		return err
	}
	var encryptionKey []byte
	runtimeEncryptionKeyFile := ""
	if encryption.Method == nativepitr.CipherMethodPlaintext {
		if o.encryptionKeyID != "" || o.encryptionKeyFile != "" {
			return errors.New("plaintext restore must not receive encryption key inputs")
		}
	} else {
		if o.encryptionKeyID != encryption.KeyID {
			return errors.New("encryption-key-id must equal the plan-bound immutable key version ID")
		}
		encryptionKey, err = nativepitr.ReadAES256KeyFile(o.encryptionKeyFile)
		if err != nil {
			return err
		}
		var cleanup func()
		runtimeEncryptionKeyFile, cleanup, err = nativepitr.StageAES256KeyFile(encryptionKey)
		if err != nil {
			return err
		}
		defer cleanup()
	}
	inventory, inventoryBytes, err := pitrinventory.ReadCanonical(o.inventory)
	if err != nil {
		return err
	}
	if err := verifyMirror(full, fullBytes, artifact, inventory, inventoryBytes, o.artifactRoot, encryptionKey); err != nil {
		return err
	}

	targetBytes, err := readSmall(o.target)
	if err != nil {
		return err
	}
	if digest(targetBytes) != plan.Target.SnapshotEmptyReceiptSHA256 {
		return errors.New("target snapshot-empty receipt does not match plan")
	}
	target, err := nativepitr.DecodeTargetSnapshotEmpty(bytes.NewReader(targetBytes))
	if err != nil {
		return err
	}
	addrs, err := parseAddrs(o.pdAddrs)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(addrs, target.PDAddrs) {
		return errors.New("target PD addresses differ from plan-bound receipt")
	}
	provisioningBytes, err := readSmall(o.targetProvisioning)
	if err != nil {
		return err
	}
	provisioning, err := nativepitr.DecodeTargetProvisioningReceipt(bytes.NewReader(provisioningBytes))
	if err != nil {
		return err
	}
	writerBytes, err := readSmall(o.writerExclusion)
	if err != nil {
		return err
	}
	writers, err := nativepitr.DecodeTargetWriterExclusionEvidence(bytes.NewReader(writerBytes))
	if err != nil {
		return err
	}
	qualificationBytes, err := readSmall(o.targetQualification)
	if err != nil {
		return err
	}
	qualification, err := nativepitr.DecodeTargetQualificationReceipt(bytes.NewReader(qualificationBytes))
	if err != nil {
		return err
	}
	if err := nativepitr.VerifyTargetQualificationBinding(qualification, provisioning, target, writers, digest(provisioningBytes), digest(targetBytes), digest(writerBytes), addrs); err != nil {
		return fmt.Errorf("target qualification: %w", err)
	}
	admissionBytes, err := readSmall(o.admission)
	if err != nil {
		return err
	}
	admission, err := nativepitr.DecodeRestoreAdmissionReceipt(bytes.NewReader(admissionBytes))
	if err != nil {
		return err
	}
	if admission.PlanSHA256 != planSHA || admission.TargetClusterID != plan.Target.ClusterID || admission.Keyspace != plan.Source.Keyspace {
		return errors.New("restore admission receipt does not match plan")
	}
	admissionToken, err := admission.Token()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	admissionClient, err := newAdmissionClient(addrs, o.ca, o.cert, o.key)
	if err != nil {
		return err
	}
	defer admissionClient.Close()
	if err := admissionfence.Verify(ctx, admissionClient, plan.Source.Keyspace, admissionToken); err != nil {
		return fmt.Errorf("pre-BR restore admission: %w", err)
	}
	fresh, err := inspectTarget(ctx, addrs, o.ca, o.cert, o.key, now().Unix())
	if err != nil {
		return fmt.Errorf("pre-write target recheck: %w", err)
	}
	if fresh.ClusterID != plan.Target.ClusterID || fresh.ClusterID != target.ClusterID || fresh.SnapshotTS < target.SnapshotTS || !reflect.DeepEqual(fresh.Stores, target.Stores) {
		return errors.New("pre-write target identity differs from approved plan")
	}
	for _, evidence := range []struct {
		path string
		data []byte
		name string
	}{
		{o.targetProvisioning, provisioningBytes, "target provisioning"},
		{o.writerExclusion, writerBytes, "target writer exclusion"},
		{o.targetQualification, qualificationBytes, "target qualification"},
	} {
		stable, err := readSmall(evidence.path)
		if err != nil || !bytes.Equal(stable, evidence.data) {
			return fmt.Errorf("%s evidence changed before restore", evidence.name)
		}
	}

	resolved, err := exec.LookPath(o.brBinary)
	if err != nil {
		return fmt.Errorf("resolve BR binary: %w", err)
	}
	versionBytes, err := runner.Output(ctx, resolved, "--version")
	if err != nil {
		return fmt.Errorf("read BR version: %w: %s", err, strings.TrimSpace(string(versionBytes)))
	}
	version := strings.TrimSpace(string(versionBytes))
	if !pinnedBR(version) {
		return fmt.Errorf("BR must be exact v7.5.1 build, got %q", version)
	}
	brSHA, err := fileDigest(resolved)
	if err != nil {
		return err
	}

	started := now().Unix()
	if admission.AcquiredAtUnix > started {
		return errors.New("restore admission was acquired after BR start")
	}
	verifyAdmission := func(checkCtx context.Context) error {
		return admissionfence.Verify(checkCtx, admissionClient, plan.Source.Keyspace, admissionToken)
	}
	if err := runWithAdmissionMonitor(ctx, o.admissionCheckInterval, verifyAdmission, func(runCtx context.Context) error {
		return runner.Run(runCtx, resolved, buildBRArgs(addrs, o.artifactRoot, o.ca, o.cert, o.key, encryption, runtimeEncryptionKeyFile), logs, logs)
	}); err != nil {
		return fmt.Errorf("BR transactional full restore failed: %w", err)
	}
	if encryption.Method == nativepitr.CipherMethodAES256CTR {
		current, err := nativepitr.ReadAES256KeyFile(o.encryptionKeyFile)
		if err != nil || !bytes.Equal(current, encryptionKey) {
			return errors.New("encryption key changed during BR restore")
		}
	}
	if err := admissionfence.Verify(ctx, admissionClient, plan.Source.Keyspace, admissionToken); err != nil {
		return fmt.Errorf("post-BR restore admission: %w", err)
	}
	if err := verifyMirror(full, fullBytes, artifact, inventory, inventoryBytes, o.artifactRoot, encryptionKey); err != nil {
		return errors.New("local full mirror changed during restore")
	}
	postBRHash, err := fileDigest(resolved)
	if err != nil || postBRHash != brSHA {
		return errors.New("BR binary changed during restore")
	}

	stableAdmission, err := readSmall(o.admission)
	if err != nil || !bytes.Equal(stableAdmission, admissionBytes) {
		return errors.New("restore admission receipt changed during restore")
	}
	if err := admissionfence.Verify(ctx, admissionClient, plan.Source.Keyspace, admissionToken); err != nil {
		return fmt.Errorf("final restore admission: %w", err)
	}
	receipt := nativepitr.FullRestoreExecutionReceipt{Format: nativepitr.FullRestoreExecutionFormat, PlanSHA256: planSHA, SourceExclusiveSHA256: digest(sourceBytes), FullArtifactSHA256: digest(artifactBytes), ArtifactManifestSHA256: artifact.ManifestSHA256, RestoreAdmissionSHA256: digest(admissionBytes), PreWriteTarget: fresh, BRVersion: version, BRBinarySHA256: brSHA, Encryption: encryption.Method, EncryptionKeyID: encryption.KeyID, StartedAtUnix: started, CompletedAtUnix: now().Unix(), WholeClusterTxnImport: true, SourceVisibleRangeExclusive: true, TargetWriteFenceProven: true, FullImportAdmissionProven: true, FullSnapshotRestored: true}
	if err := receipt.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = out.Write(b)
	return err
}

func verifyMirror(full nativepitr.FullSnapshotReceipt, fullBytes []byte, artifact nativepitr.ArtifactReceipt, inventory pitrinventory.Receipt, inventoryBytes []byte, root string, encryptionKey []byte) error {
	got, err := nativepitr.VerifyFullArtifactsWithEncryption(full, digest(fullBytes), artifact.BackupAttestation, artifact.BackupAttestationSHA256, inventory, digest(inventoryBytes), root, encryptionKey)
	if err != nil {
		return fmt.Errorf("reverify local full mirror: %w", err)
	}
	if !reflect.DeepEqual(got, artifact) {
		return errors.New("reverified local mirror differs from plan-bound artifact receipt")
	}
	return nil
}

func validateOptions(o options) error {
	if o.plan == "" || o.full == "" || o.artifacts == "" || o.inventory == "" || o.artifactRoot == "" || o.sourceExclusive == "" || o.target == "" || o.targetProvisioning == "" || o.targetQualification == "" || o.writerExclusion == "" || o.admission == "" || o.pdAddrs == "" || o.brBinary == "" {
		return errors.New("all receipt, artifact, target, BR, and positive timeout options are required")
	}
	if o.timeout <= 0 || o.admissionCheckInterval <= 0 || o.admissionCheckInterval >= o.timeout {
		return errors.New("timeout must be positive and admission-check-interval must be positive and shorter than timeout")
	}
	if !filepath.IsAbs(o.artifactRoot) || filepath.Clean(o.artifactRoot) != o.artifactRoot {
		return errors.New("artifact-root must be an absolute clean path")
	}
	if (o.cert == "") != (o.key == "") || ((o.cert != "" || o.key != "") && o.ca == "") {
		return errors.New("cert and key must be set together and require ca")
	}
	return nil
}

func runWithAdmissionMonitor(ctx context.Context, interval time.Duration, verify func(context.Context) error, run func(context.Context) error) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var finished atomic.Bool
	monitorDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				monitorDone <- nil
				return
			case <-ticker.C:
				if err := verify(runCtx); err != nil {
					if finished.Load() {
						monitorDone <- nil
					} else {
						monitorDone <- fmt.Errorf("restore admission lost during BR import: %w", err)
						cancel()
					}
					return
				}
			}
		}
	}()
	runErr := run(runCtx)
	finished.Store(true)
	cancel()
	monitorErr := <-monitorDone
	if monitorErr != nil {
		return monitorErr
	}
	return runErr
}

func newAdmissionClient(addrs []string, ca, cert, key string) (*clientv3.Client, error) {
	tlsConfig, err := (tlsutil.TLSConfig{CAPath: ca, CertPath: cert, KeyPath: key}).ToTLSConfig()
	if err != nil {
		return nil, err
	}
	return clientv3.New(clientv3.Config{Endpoints: addrs, DialTimeout: 5 * time.Second, TLS: tlsConfig})
}
func readSmall(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxReceiptBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxReceiptBytes {
		return nil, fmt.Errorf("receipt %s exceeds %d bytes", path, maxReceiptBytes)
	}
	return b, nil
}
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func fileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func parseAddrs(raw string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, p := range strings.Split(raw, ",") {
		a := strings.TrimSpace(p)
		if a == "" || strings.Contains(a, "://") || seen[a] {
			return nil, errors.New("invalid or duplicate target PD address")
		}
		seen[a] = true
		out = append(out, a)
	}
	sort.Strings(out)
	return out, nil
}
func pinnedBR(v string) bool {
	return nativepitr.PinnedBRVersion(v)
}
func buildBRArgs(addrs []string, root, ca, cert, key string, encryption nativepitr.EncryptionIdentity, encryptionKeyFile string) []string {
	args := []string{"restore", "txn", "--pd", strings.Join(addrs, ","), "--storage", "local://" + root, "--send-credentials-to-tikv=false", "--check-requirements=true", "--checksum=false", "--log-file", "/dev/stderr"}
	args = append(args, "--crypter.method="+encryption.Method)
	if encryption.Method == nativepitr.CipherMethodAES256CTR {
		args = append(args, "--crypter.key-file="+encryptionKeyFile)
	}
	if ca != "" {
		args = append(args, "--ca", ca)
	}
	if cert != "" {
		args = append(args, "--cert", cert, "--key", key)
	}
	return args
}
