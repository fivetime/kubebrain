package leasefault

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
)

func TestIsolatedJoinPinnedInput(t *testing.T) {
	for _, mode := range []string{"valid", "missing-pin", "wrong-hash", "public", "symlink", "missing-file", "extra-field", "unterminated", "extra-line", "oversized", "zero-start", "bad-boot", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, IsolatedJoinIdentity)
			data := "v1\t4:12345\t11111111-2222-3333-4444-555555555555\t12345\t6:789\n"
			switch mode {
			case "extra-field":
				data = strings.TrimSuffix(data, "\n") + "\textra\n"
			case "unterminated":
				data = strings.TrimSuffix(data, "\n")
			case "extra-line":
				data += "\n"
			case "oversized":
				data += strings.Repeat("x", 257)
			case "zero-start":
				data = strings.Replace(data, "\t12345\t", "\t0\t", 1)
			case "bad-boot":
				data = strings.Replace(data, "11111111-", "bad-", 1)
			}
			require.NoError(t, os.WriteFile(path, []byte(data), 0600))
			files := map[string]string{path: planinput.SHA256([]byte(data))}
			switch mode {
			case "missing-pin":
				delete(files, path)
			case "wrong-hash":
				files[path] = strings.Repeat("a", 64)
			case "public":
				require.NoError(t, os.Chmod(path, 0644))
			case "symlink":
				require.NoError(t, os.Rename(path, path+".saved"))
				require.NoError(t, os.Symlink(path+".saved", path))
			case "missing-file":
				require.NoError(t, os.Rename(path, path+".saved"))
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			err := VerifyJoinInputs(ctx, dir, filepath.Join(t.TempDir(), IsolatedJoinScript), files)
			if mode == "valid" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestCommandPlanRequiresIsolatedJoinIdentity(t *testing.T) {
	p := serializedCommandFixture(t)
	join := filepath.Join(filepath.Dir(p.JoinScript), IsolatedJoinScript)
	require.NoError(t, os.Rename(p.JoinScript, join))
	p.Files[join] = p.Files[p.JoinScript]
	delete(p.Files, p.JoinScript)
	p.JoinScript = join
	require.ErrorContains(t, p.CheckLocal(), "requires pinned namespace identity")
	path := filepath.Join(p.OwnerDirectory, IsolatedJoinIdentity)
	data := []byte("v1\t4:12345\t11111111-2222-3333-4444-555555555555\t12345\t6:789\n")
	require.NoError(t, os.WriteFile(path, data, 0600))
	p.Files[path] = planinput.SHA256(data)
	// Local checks authenticate syntax and bytes, NOT these synthetic live IDs.
	require.NoError(t, p.CheckLocal())
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0600))
	require.ErrorContains(t, p.VerifyFiles(context.Background()), "identity differs")
}
