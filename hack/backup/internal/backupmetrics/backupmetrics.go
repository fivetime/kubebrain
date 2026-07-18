package backupmetrics

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
)

// WriteSuccess atomically publishes the last completed logical backup for a
// Prometheus textfile collector. Failed exports intentionally leave the last
// success intact so staleness alerts remain meaningful.
func WriteSuccess(path, instance string, status backupfile.Status, artifactBytes int64, completedAt time.Time) error {
	if path == "" {
		return errors.New("metrics output path is empty")
	}
	if instance == "" {
		return errors.New("backup metrics instance is empty")
	}
	if artifactBytes < 0 {
		return fmt.Errorf("invalid backup artifact size %d", artifactBytes)
	}
	if completedAt.IsZero() {
		return errors.New("backup completion time is empty")
	}

	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	cleanup := func() {
		_ = temp.Close()
		_ = os.Remove(temp.Name())
	}
	if err := temp.Chmod(0o644); err != nil {
		cleanup()
		return err
	}

	writer := bufio.NewWriter(temp)
	label := `{instance="` + escapeLabel(instance) + `"}`
	metrics := []struct {
		name  string
		help  string
		kind  string
		value string
	}{
		{"kubebrain_logical_backup_last_success_timestamp_seconds", "Unix timestamp of the last completed KubeBrain logical backup.", "gauge", strconv.FormatInt(completedAt.Unix(), 10)},
		{"kubebrain_logical_backup_artifact_bytes", "Size in bytes of the last completed KubeBrain logical backup artifact.", "gauge", strconv.FormatInt(artifactBytes, 10)},
		{"kubebrain_logical_backup_records", "Number of records in the last completed KubeBrain logical backup.", "gauge", strconv.Itoa(status.Records)},
		{"kubebrain_logical_backup_leases", "Number of leases in the last completed KubeBrain logical backup.", "gauge", strconv.Itoa(status.Leases)},
		{"kubebrain_logical_backup_snapshot_revision", "Snapshot revision of the last completed KubeBrain logical backup.", "gauge", strconv.FormatInt(status.Revision, 10)},
	}
	for _, metric := range metrics {
		if _, err := fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s %s\n%s%s %s\n",
			metric.name, metric.help, metric.name, metric.kind, metric.name, label, metric.value); err != nil {
			cleanup()
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		cleanup()
		return err
	}
	if err := temp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(temp.Name())
		return err
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		_ = os.Remove(temp.Name())
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func escapeLabel(value string) string {
	value = strings.ToValidUTF8(value, "\uFFFD")
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}
