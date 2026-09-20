package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
)

func TestRunNativeCommandRefusesBeforeClusterRequests(t *testing.T) {
	for _, mode := range []string{"missing-gate", "pre-admit", "preclaim-admit", "changed-plan", "changed-plan-after-setup", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			p := serializedCommandFixture(t)
			require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
			data, err := json.Marshal(p)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "plan.json")
			require.NoError(t, os.WriteFile(path, data, 0600))
			calls := 0
			refused := errors.New("independent admission refused")
			check := func(context.Context) error { return nil }
			a := CommandAdmission{Own: check, Original: check, Network: check, Successor: check, Metrics: check, Outcome: check, Stack: func(context.Context, string) error { return nil }}
			a.Tools = func(context.Context) error {
				calls++
				if mode == "changed-plan" || (mode == "changed-plan-after-setup" && calls == 2) {
					require.NoError(t, os.WriteFile(path, append(data, '\n'), 0600))
					return nil
				}
				if mode == "pre-admit" || calls == 2 {
					return refused
				}
				return nil
			}
			if mode == "missing-gate" {
				a.Outcome = nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			result, err := RunNativeCommand(ctx, path, planinput.SHA256(data), a)
			require.Error(t, err)
			require.Nil(t, result.Owner)
			require.False(t, result.Lifecycle.RecoveryAttempted)
			if mode == "pre-admit" || mode == "preclaim-admit" {
				require.ErrorIs(t, err, refused)
			}
			if mode == "changed-plan" || mode == "changed-plan-after-setup" {
				require.Contains(t, err.Error(), "command plan changed")
			}
			if mode == "preclaim-admit" || mode == "changed-plan-after-setup" {
				require.Equal(t, 2, calls, "real RunVerified reaches preclaim tools gate")
				require.FileExists(t, filepath.Join(p.OwnerDirectory, "probe.stderr"))
				require.DirExists(t, p.BeforeDirectory)
			} else {
				require.NoFileExists(t, filepath.Join(p.OwnerDirectory, "probe.stderr"))
				require.NoDirExists(t, p.BeforeDirectory)
			}
			for _, name := range []string{ownerIntentFile, ownerReceiptFile, "deployment-claimed", faultOriginFile} {
				_, statErr := os.Lstat(filepath.Join(p.OwnerDirectory, name))
				require.ErrorIs(t, statErr, os.ErrNotExist)
			}
		})
	}
}
