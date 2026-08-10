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
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const LegacyDiagnostic = "snapshot cannot determine lease for retained legacy version"

var positiveDecimal = regexp.MustCompile(`^[1-9][0-9]*$`)

type Config struct {
	Action, Endpoint, Output            string
	ExpectedClusterID, ExpectedRevision string
	ConfirmEndpoint                     string
	AllowIrreversible                   bool
	CACert, Cert, Key, User, Password   string
	Timeout                             time.Duration
}

type Result struct {
	ClusterID, Revision, SnapshotStatus string
}

type etcdClient interface {
	Status(context.Context, string) (*clientv3.StatusResponse, error)
	Snapshot(context.Context) (io.ReadCloser, error)
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
	defer cli.Close()
	return runWithClient(ctx, c, out, cli)
}

func runWithClient(ctx context.Context, c Config, out io.Writer, cli etcdClient) (result Result, exitCode int, err error) {
	status, err := cli.Status(ctx, c.Endpoint)
	if err != nil {
		return result, 1, fmt.Errorf("endpoint status: %w", err)
	}
	result.ClusterID = fmt.Sprint(status.Header.ClusterId)
	result.Revision = fmt.Sprint(status.Header.Revision)
	if !positiveDecimal.MatchString(result.ClusterID) || !positiveDecimal.MatchString(result.Revision) {
		return result, 1, errors.New("endpoint returned a non-positive cluster ID or revision")
	}
	fmt.Fprintf(out, "cluster_id=%s\nrevision=%s\nendpoint=%s\n", result.ClusterID, result.Revision, c.Endpoint)

	probeDir, err := os.MkdirTemp("", "kubebrain-legacy-snapshot-preflight.")
	if err != nil {
		return result, 1, err
	}
	defer os.RemoveAll(probeDir)
	probe := filepath.Join(probeDir, "preflight.db")
	err = downloadAndValidate(ctx, cli, probe)
	if err == nil {
		result.SnapshotStatus = "healthy"
		fmt.Fprintln(out, "snapshot_status=healthy")
		if c.Action == "compact" {
			return result, 1, errors.New("refusing compaction: Maintenance Snapshot already succeeds")
		}
		return result, 0, nil
	}
	if !strings.Contains(err.Error(), LegacyDiagnostic) {
		return result, 1, fmt.Errorf("refusing legacy-history remediation: Snapshot failed for a different reason: %w", err)
	}
	result.SnapshotStatus = "legacy_lease_history_ambiguous"
	fmt.Fprintln(out, "snapshot_status="+result.SnapshotStatus)
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
	revision := status.Header.Revision
	if _, err = cli.Compact(ctx, revision, clientv3.WithCompactPhysical()); err != nil {
		return result, 1, fmt.Errorf("physical compact: %w", err)
	}
	candidate, err := os.CreateTemp(parent, ".kubebrain-post-remediation.*.db")
	if err != nil {
		return result, 1, err
	}
	candidatePath := candidate.Name()
	candidate.Close()
	os.Remove(candidatePath)
	defer os.Remove(candidatePath)
	if err = downloadAndValidate(ctx, cli, candidatePath); err != nil {
		return result, 1, fmt.Errorf("post-compaction snapshot: %w", err)
	}
	if err = os.Link(candidatePath, c.Output); err != nil {
		return result, 1, fmt.Errorf("publish snapshot without overwrite: %w", err)
	}
	result.SnapshotStatus = "remediated"
	fmt.Fprintf(out, "snapshot_status=remediated\ncompacted_revision=%d\nsnapshot_output=%s\n", revision, c.Output)
	return result, 0, nil
}

func downloadAndValidate(ctx context.Context, cli etcdClient, path string) error {
	reader, err := cli.Snapshot(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = io.Copy(f, reader); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	if err = f.Close(); err != nil {
		os.Remove(path)
		return err
	}
	if err = verifySnapshot(path); err != nil {
		os.Remove(path)
		return err
	}
	return nil
}

func verifySnapshot(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
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
	defer db.Close()
	return db.View(func(tx *bolt.Tx) error {
		for checkErr := range tx.Check() {
			if checkErr != nil {
				return fmt.Errorf("snapshot bbolt consistency: %w", checkErr)
			}
		}
		return nil
	})
}
