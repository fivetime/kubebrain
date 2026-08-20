package legacyremediation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const LegacyDiagnostic = "snapshot cannot determine lease for retained legacy version"

var positiveDecimal = regexp.MustCompile(`^[1-9][0-9]*$`)

var compactRevisionDiagnostic = regexp.MustCompile(`minimum physical compact revision ([1-9][0-9]*)`)

type Config struct {
	Action, Endpoint, Output            string
	ExpectedClusterID, ExpectedRevision string
	ConfirmEndpoint                     string
	AllowIrreversible                   bool
	CACert, Cert, Key, User, Password   string
	Timeout                             time.Duration
}

type Result struct {
	ClusterID, Revision, CompactRevision, SnapshotStatus string
}

type etcdClient interface {
	Status(context.Context, string) (*clientv3.StatusResponse, error)
	SnapshotWithVersion(context.Context) (*clientv3.SnapshotResponse, error)
	Compact(context.Context, int64, ...clientv3.CompactOption) (*clientv3.CompactResponse, error)
}

func ConfigFromEnv() (Config, error) {
	c := Config{
		Action: os.Getenv("ACTION"), Endpoint: os.Getenv("ENDPOINT"), Output: os.Getenv("OUTPUT"),
		ExpectedClusterID: os.Getenv("EXPECTED_CLUSTER_ID"), ExpectedRevision: os.Getenv("EXPECTED_REVISION"),
		ConfirmEndpoint: os.Getenv("CONFIRM_ENDPOINT"), AllowIrreversible: os.Getenv("ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION") == "true",
		CACert: os.Getenv("ETCDCTL_CACERT"), Cert: os.Getenv("ETCDCTL_CERT"), Key: os.Getenv("ETCDCTL_KEY"),
		Timeout: 30 * time.Minute,
	}
	if c.Action == "" {
		c.Action = "diagnose"
	}
	if raw := os.Getenv("TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return Config{}, errors.New("TIMEOUT must be a positive Go duration")
		}
		c.Timeout = d
	}
	if raw := os.Getenv("ETCDCTL_USER"); raw != "" {
		parts := strings.SplitN(raw, ":", 2)
		c.User = parts[0]
		if len(parts) == 2 {
			c.Password = parts[1]
		}
	}
	if c.Action != "diagnose" && c.Action != "compact" {
		return Config{}, errors.New("ACTION must be diagnose or compact")
	}
	if c.Endpoint == "" || strings.ContainsAny(c.Endpoint, ",\r\n\t\"\\ ") {
		return Config{}, errors.New("ENDPOINT must identify exactly one endpoint")
	}
	return c, nil
}

func (c Config) clientConfig() (clientv3.Config, error) {
	var tlsConfig *tls.Config
	if c.CACert != "" || c.Cert != "" || c.Key != "" {
		if (c.Cert == "") != (c.Key == "") {
			return clientv3.Config{}, errors.New("ETCDCTL_CERT and ETCDCTL_KEY must be set together")
		}
		info := transport.TLSInfo{TrustedCAFile: c.CACert, CertFile: c.Cert, KeyFile: c.Key}
		configured, err := info.ClientConfig()
		if err != nil {
			return clientv3.Config{}, fmt.Errorf("configure client TLS: %w", err)
		}
		tlsConfig = configured
	}
	return clientv3.Config{Endpoints: []string{c.Endpoint}, DialTimeout: min(c.Timeout, 30*time.Second), TLS: tlsConfig, Username: c.User, Password: c.Password, Logger: zap.NewNop()}, nil
}

// Run returns exitCode=3 only for a successful read-only diagnosis of the
// upgrade-era legacy lease limitation. All other errors use exitCode=1/2.
func Run(ctx context.Context, c Config, out io.Writer) (result Result, exitCode int, err error) {
	clientConfig, err := c.clientConfig()
	if err != nil {
		return result, 2, err
	}
	cli, err := clientv3.New(clientConfig)
	if err != nil {
		return result, 1, err
	}
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close etcd client: %w", closeErr))
			exitCode = 1
		}
	}()
	return runWithClient(ctx, c, out, cli)
}

func runWithClient(ctx context.Context, c Config, out io.Writer, cli etcdClient) (result Result, exitCode int, err error) {
	status, err := cli.Status(ctx, c.Endpoint)
	if err != nil {
		return result, 1, fmt.Errorf("endpoint status: %w", err)
	}
	if err := validateStatusResponse(status); err != nil {
		return result, 1, err
	}
	result.ClusterID = fmt.Sprint(status.Header.ClusterId)
	result.Revision = fmt.Sprint(status.Header.Revision)
	if !positiveDecimal.MatchString(result.ClusterID) || !positiveDecimal.MatchString(result.Revision) {
		return result, 1, errors.New("endpoint returned a non-positive cluster ID or revision")
	}
	if _, err = fmt.Fprintf(out, "cluster_id=%s\nrevision=%s\nendpoint=%s\n", result.ClusterID, result.Revision, c.Endpoint); err != nil {
		return result, 1, fmt.Errorf("write endpoint status: %w", err)
	}

	probeDir, err := os.MkdirTemp("", "kubebrain-legacy-snapshot-preflight.")
	if err != nil {
		return result, 1, err
	}
	defer func() {
		if removeErr := os.RemoveAll(probeDir); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("remove snapshot preflight directory: %w", removeErr))
			exitCode = 1
		}
	}()
	probe := filepath.Join(probeDir, "preflight.db")
	err = downloadAndValidate(ctx, cli, probe, status.StorageVersion)
	if err == nil {
		if _, statusErr := continuationStatus(ctx, cli, c.Endpoint, status, status.Header.Revision); statusErr != nil {
			return result, 1, statusErr
		}
		result.SnapshotStatus = "healthy"
		if _, err = fmt.Fprintln(out, "snapshot_status=healthy"); err != nil {
			return result, 1, fmt.Errorf("write snapshot status: %w", err)
		}
		if c.Action == "compact" {
			return result, 1, errors.New("refusing compaction: Maintenance Snapshot already succeeds")
		}
		return result, 0, nil
	}
	if !strings.Contains(err.Error(), LegacyDiagnostic) {
		return result, 1, fmt.Errorf("refusing legacy-history remediation: Snapshot failed for a different reason: %w", err)
	}
	diagnosticErr := err
	continuedStatus, err := continuationStatus(ctx, cli, c.Endpoint, status, status.Header.Revision)
	if err != nil {
		return result, 1, err
	}
	match := compactRevisionDiagnostic.FindStringSubmatch(diagnosticErr.Error())
	if len(match) != 2 {
		return result, 1, errors.New("refusing legacy-history remediation: Snapshot diagnostic has no safe compact revision")
	}
	result.CompactRevision = match[1]
	compactRevision, parseErr := strconv.ParseInt(result.CompactRevision, 10, 64)
	if parseErr != nil || compactRevision <= 0 || compactRevision > status.Header.Revision {
		return result, 1, errors.New("refusing legacy-history remediation: Snapshot returned an invalid compact revision")
	}
	result.SnapshotStatus = "legacy_lease_history_ambiguous"
	if _, err = fmt.Fprintf(out, "snapshot_status=%s\nminimum_compact_revision=%s\n", result.SnapshotStatus, result.CompactRevision); err != nil {
		return result, 1, fmt.Errorf("write legacy snapshot status: %w", err)
	}
	if c.Action == "diagnose" {
		return result, 3, nil
	}
	if !c.AllowIrreversible || c.ConfirmEndpoint != c.Endpoint || c.ExpectedClusterID != result.ClusterID || c.ExpectedRevision != result.Revision {
		return result, 2, errors.New("refusing irreversible compaction: confirmation fields do not match current endpoint identity")
	}
	if c.Output == "" {
		return result, 2, errors.New("OUTPUT is required for ACTION=compact")
	}
	if _, statErr := os.Lstat(c.Output); !errors.Is(statErr, os.ErrNotExist) {
		if statErr == nil {
			return result, 2, errors.New("refusing to overwrite existing OUTPUT")
		}
		return result, 1, statErr
	}
	parent := filepath.Dir(c.Output)
	if info, statErr := os.Stat(parent); statErr != nil || !info.IsDir() {
		return result, 2, errors.New("OUTPUT parent directory does not exist")
	}
	revision := compactRevision
	compactResponse, compactErr := cli.Compact(ctx, revision, clientv3.WithCompactPhysical())
	if compactErr != nil {
		err = compactErr
		return result, 1, fmt.Errorf("physical compact: %w", err)
	}
	if err = validateCompactResponse(compactResponse, status.Header.ClusterId, continuedStatus.Header.Revision); err != nil {
		return result, 1, err
	}
	candidate, err := os.CreateTemp(parent, ".kubebrain-post-remediation.*.db")
	if err != nil {
		return result, 1, err
	}
	candidatePath := candidate.Name()
	if err = errors.Join(candidate.Close(), os.Remove(candidatePath)); err != nil {
		return result, 1, fmt.Errorf("prepare post-remediation snapshot path: %w", err)
	}
	defer func() {
		if removeErr := os.Remove(candidatePath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove post-remediation snapshot temporary file: %w", removeErr))
			exitCode = 1
		}
	}()
	if err = downloadAndValidate(ctx, cli, candidatePath, status.StorageVersion); err != nil {
		return result, 1, fmt.Errorf("post-compaction snapshot: %w", err)
	}
	if _, err = continuationStatus(ctx, cli, c.Endpoint, status, compactResponse.Header.Revision); err != nil {
		return result, 1, fmt.Errorf("post-compaction endpoint identity: %w", err)
	}
	if err = os.Link(candidatePath, c.Output); err != nil {
		return result, 1, fmt.Errorf("publish snapshot without overwrite: %w", err)
	}
	if err = os.Remove(candidatePath); err != nil {
		return result, 1, fmt.Errorf("remove published remediation snapshot temporary link: %w", err)
	}
	directory, err := os.Open(parent)
	if err != nil {
		return result, 1, fmt.Errorf("open remediation snapshot directory: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		syncErr = fmt.Errorf("sync remediation snapshot directory: %w", syncErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close remediation snapshot directory: %w", closeErr)
	}
	if err = errors.Join(syncErr, closeErr); err != nil {
		return result, 1, err
	}
	result.SnapshotStatus = "remediated"
	if _, err = fmt.Fprintf(out, "snapshot_status=remediated\ncompacted_revision=%d\nsnapshot_output=%s\n", revision, c.Output); err != nil {
		return result, 1, fmt.Errorf("write remediation status: %w", err)
	}
	return result, 0, nil
}

func validateStatusResponse(status *clientv3.StatusResponse) error {
	if status == nil || status.Header == nil || status.Header.ClusterId == 0 || status.Header.MemberId == 0 || status.Header.Revision <= 0 {
		return errors.New("endpoint returned an invalid status response header")
	}
	if status.Version == "" {
		return errors.New("endpoint returned an empty server version")
	}
	if _, err := semver.StrictNewVersion(status.Version); err != nil {
		return errors.New("endpoint returned an invalid server version")
	}
	if status.StorageVersion == "" {
		return errors.New("endpoint returned an empty storage version")
	}
	if _, err := semver.StrictNewVersion(status.StorageVersion); err != nil {
		return errors.New("endpoint returned an invalid storage version")
	}
	if status.DbSize < 0 || status.DbSizeInUse < 0 {
		return errors.New("endpoint returned invalid database sizes")
	}
	if status.Leader == 0 || len(status.Errors) != 0 {
		return errors.New("endpoint status reports an unhealthy leader")
	}
	return nil
}

func continuationStatus(ctx context.Context, cli etcdClient, endpoint string, initial *clientv3.StatusResponse, minRevision int64) (*clientv3.StatusResponse, error) {
	status, err := cli.Status(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("recheck endpoint status: %w", err)
	}
	if err := validateStatusResponse(status); err != nil {
		return nil, err
	}
	if status.Header.ClusterId != initial.Header.ClusterId || status.Header.Revision < minRevision {
		return nil, errors.New("endpoint identity changed or revision regressed during snapshot remediation")
	}
	if status.Version != initial.Version || status.StorageVersion != initial.StorageVersion {
		return nil, errors.New("endpoint version changed during snapshot remediation")
	}
	return status, nil
}

func validateCompactResponse(response *clientv3.CompactResponse, clusterID uint64, compactRevision int64) error {
	if response == nil || response.Header == nil || response.Header.ClusterId != clusterID || response.Header.MemberId == 0 || response.Header.Revision < compactRevision {
		return errors.New("physical compact returned an invalid acknowledgement")
	}
	return nil
}

func downloadAndValidate(ctx context.Context, cli etcdClient, path, expectedStorageVersion string) (retErr error) {
	response, err := cli.SnapshotWithVersion(ctx)
	if err != nil {
		if response != nil && response.Snapshot != nil {
			return errors.Join(err, response.Snapshot.Close())
		}
		return err
	}
	if response == nil {
		return errors.New("snapshot returned an empty response")
	}
	if response.Snapshot == nil {
		return errors.New("snapshot returned an empty reader")
	}
	reader := response.Snapshot
	if response.Version != expectedStorageVersion {
		return errors.Join(fmt.Errorf("snapshot storage version %q does not match endpoint %q", response.Version, expectedStorageVersion), reader.Close())
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			retErr = errors.Join(retErr, closeErr)
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				retErr = errors.Join(retErr, removeErr)
			}
		}
	}()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = io.Copy(f, reader); err != nil {
		return errors.Join(err, f.Close(), os.Remove(path))
	}
	if err = f.Sync(); err != nil {
		return errors.Join(err, f.Close(), os.Remove(path))
	}
	if err = f.Close(); err != nil {
		return errors.Join(err, os.Remove(path))
	}
	if err = verifySnapshot(path); err != nil {
		return errors.Join(err, os.Remove(path))
	}
	return nil
}

func verifySnapshot(path string) (retErr error) {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() <= sha256.Size {
		return errors.New("snapshot is too short")
	}
	h := sha256.New()
	if _, err = io.CopyN(h, f, info.Size()-sha256.Size); err != nil {
		return err
	}
	want := make([]byte, sha256.Size)
	if _, err = io.ReadFull(f, want); err != nil {
		return err
	}
	if !bytes.Equal(h.Sum(nil), want) {
		return errors.New("snapshot SHA-256 mismatch")
	}
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("open snapshot bbolt: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()
	return db.View(func(tx *bolt.Tx) error {
		for checkErr := range tx.Check() {
			if checkErr != nil {
				return fmt.Errorf("snapshot bbolt consistency: %w", checkErr)
			}
		}
		return nil
	})
}
