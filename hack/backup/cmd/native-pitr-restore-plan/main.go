// Command native-pitr-restore-plan binds full snapshot, log checkpoint and
// empty target evidence into a canonical, read-only restore plan.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
)

func main() {
	var source string
	var in nativepitr.Inputs
	flag.StringVar(&source, "source-preflight", "", "native-pitr-preflight.v1 receipt")
	flag.Uint64Var(&in.TaskStartTS, "task-start-ts", 0, "registered log task start TSO")
	flag.Uint64Var(&in.FullBackupTS, "full-backup-ts", 0, "br txn backupmeta end TSO")
	flag.StringVar(&in.BackupMetaSHA256, "backupmeta-sha256", "", "SHA-256 of the exact backupmeta object")
	flag.StringVar(&in.StoragePrefix, "storage-prefix", "", "immutable full/log artifact prefix")
	flag.Uint64Var(&in.GlobalCheckpointTS, "global-checkpoint-ts", 0, "durable backup-stream global checkpoint")
	flag.StringVar(&in.AdvancerOwner, "advancer-owner", "", "observed checkpoint-advancer owner identity")
	flag.Uint64Var(&in.TargetClusterID, "target-cluster-id", 0, "fresh isolated target PD cluster ID")
	flag.StringVar(&in.EmptyWitnessSHA256, "target-empty-witness-sha256", "", "SHA-256 of target emptiness evidence")
	flag.Uint64Var(&in.RestoreTS, "restore-ts", 0, "requested point-in-time TSO")
	flag.Parse()
	if err := run(source, in, os.Stdout); err != nil {
		fail(err)
	}
}

func run(source string, in nativepitr.Inputs, out io.Writer) error {
	if source == "" {
		return fmt.Errorf("source-preflight is required")
	}
	f, err := os.Open(source)
	if err != nil {
		return err
	}
	preflight, err := nativepitr.DecodePreflight(f)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	plan, err := nativepitr.Build(preflight, in)
	if err != nil {
		return err
	}
	b, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = out.Write(b)
	return err
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "native PITR restore plan:", err)
	os.Exit(1)
}
