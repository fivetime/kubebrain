package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunFaultJoin(t *testing.T) {
	for _, mode := range []string{"success", "pending-exit", "failed", "pre-admission", "post-admission", "cancelled", "unbounded", "directory-replaced"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			script := filepath.Join(t.TempDir(), "join.sh")
			body := "printf 'joined\\n'\nprintf 'run\\n' >> \"$1/runs\"\n"
			if mode == "pending-exit" {
				body += "exit 75\n"
			}
			if mode == "failed" {
				body += "exit 7\n"
			}
			require.NoError(t, os.WriteFile(script, []byte(body), 0600))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			if mode == "unbounded" {
				ctx = context.Background()
			}
			calls := 0
			admit := func(context.Context) error {
				calls++
				if mode == "pre-admission" || (mode == "post-admission" && calls == 2) {
					return errors.New("input changed")
				}
				if mode == "directory-replaced" {
					require.NoError(t, os.Rename(dir, filepath.Join(t.TempDir(), "moved")))
					require.NoError(t, os.Mkdir(dir, 0700))
				}
				return nil
			}
			err := RunFaultJoin(ctx, dir, script, admit)
			if mode == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			files, globErr := filepath.Glob(filepath.Join(dir, "recovery-join.*.json"))
			require.NoError(t, globErr)
			if mode == "success" || mode == "pending-exit" || mode == "failed" || mode == "post-admission" {
				require.Len(t, files, 1)
				runs, readErr := os.ReadFile(filepath.Join(dir, "runs"))
				require.NoError(t, readErr)
				require.Equal(t, "run\n", string(runs), "cleanup must never retry")
				data, readErr := os.ReadFile(files[0])
				require.NoError(t, readErr)
				var record struct {
					Output []byte
					Error  string
				}
				require.NoError(t, json.Unmarshal(data, &record))
				require.Equal(t, "joined\n", string(record.Output))
				if mode == "pending-exit" || mode == "failed" {
					require.NotEmpty(t, record.Error)
				} else {
					require.Empty(t, record.Error)
					require.Equal(t, 2, calls)
				}
			} else {
				require.Empty(t, files)
				require.NoFileExists(t, filepath.Join(dir, "runs"))
			}
		})
	}
}
