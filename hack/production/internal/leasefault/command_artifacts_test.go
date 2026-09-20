package leasefault

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareCommandArtifacts(t *testing.T) {
	for _, mode := range []string{"valid", "existing-log", "existing-directory", "held", "public", "bad-port"} {
		t.Run(mode, func(t *testing.T) {
			p := commandPlan(t)
			require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
			p.ProbeLog, p.BeforeLog, p.AfterLog = nil, nil, nil
			targets := []MetricCommandTarget{{PodName: p.Bindings.Network.PodName, PodUID: p.Bindings.Network.PodUID, InfoPort: 18600, AnonymousPort: 18601}}
			switch mode {
			case "existing-log":
				require.NoError(t, os.WriteFile(filepath.Join(p.OwnerDirectory, "probe.stderr"), []byte("retain me"), 0600))
			case "existing-directory":
				require.NoError(t, os.Mkdir(p.BeforeDirectory, 0700))
			case "held":
				require.NoError(t, os.WriteFile(filepath.Join(p.OwnerDirectory, "HOLD"), nil, 0600))
			case "public":
				require.NoError(t, os.Chmod(p.OwnerDirectory, 0755))
			case "bad-port":
				targets[0].InfoPort = 80
			}
			a, err := p.PrepareArtifacts("/approved/protected-metrics-worker.sh", targets)
			if mode != "valid" {
				require.Error(t, err)
				require.Nil(t, a)
				if mode == "existing-log" {
					data, err := os.ReadFile(filepath.Join(p.OwnerDirectory, "probe.stderr"))
					require.NoError(t, err)
					require.Equal(t, "retain me", string(data))
				}
				if mode == "bad-port" {
					require.FileExists(t, filepath.Join(p.OwnerDirectory, "probe.stderr"), "partial artifacts are not deleted")
					_, retryErr := p.PrepareArtifacts("/approved/protected-metrics-worker.sh", targets)
					require.Error(t, retryErr)
				}
				return
			}
			require.NoError(t, err)
			defer a.Close()
			require.Nil(t, p.ProbeLog)
			require.Nil(t, targets[0].Stderr)
			for _, dir := range []string{p.BeforeDirectory, p.AfterDirectory} {
				st, err := os.Lstat(dir)
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0700), st.Mode().Perm())
				entries, err := os.ReadDir(dir)
				require.NoError(t, err)
				require.Empty(t, entries)
			}
			for _, f := range a.files {
				st, err := f.Stat()
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0600), st.Mode().Perm())
				_, err = f.WriteString("evidence\n")
				require.NoError(t, err)
			}
			_, err = a.Plan.MetricCommands("/approved/protected-metrics-worker.sh", a.Targets)
			require.NoError(t, err)
			require.NoError(t, a.Close())
			require.NoError(t, a.Close())
			for _, f := range a.files {
				_, err := f.WriteString("closed")
				require.Error(t, err)
			}
			data, err := os.ReadFile(filepath.Join(p.OwnerDirectory, "probe.stderr"))
			require.NoError(t, err)
			require.Equal(t, "evidence\n", string(data))
			_, err = p.PrepareArtifacts("/approved/protected-metrics-worker.sh", targets)
			require.Error(t, err)
		})
	}
}
