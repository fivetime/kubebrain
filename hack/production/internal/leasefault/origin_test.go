package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
)

func TestDurableFaultOrigin(t *testing.T) {
	for _, mode := range []string{"success", "existing-partial", "symlink", "public-directory", "cancelled", "pre-owner-lost", "post-owner-lost", "post-cancelled", "directory-replaced"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			require.NoError(t, os.Chmod(directory, 0700))
			bindings := commandPlan(t).Bindings
			raw, err := json.Marshal(bindings)
			require.NoError(t, err)
			owner := bindings.Network.Owner
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			calls := 0
			var finalDeadline time.Time
			hook, err := NewDurableFaultOrigin(directory, bindings, func(ctx context.Context) error {
				calls++
				if (mode == "pre-owner-lost" && calls == 1) || (mode == "post-owner-lost" && calls == 2) {
					return errors.New("ownership lost")
				}
				if calls == 2 {
					finalDeadline, _ = ctx.Deadline()
					if mode == "post-cancelled" {
						cancel()
					}
					if mode == "directory-replaced" {
						require.NoError(t, os.Rename(directory, directory+"-retained"))
						t.Cleanup(func() { require.NoError(t, os.RemoveAll(directory+"-retained")) })
						require.NoError(t, os.Mkdir(directory, 0700))
					}
				}
				return nil
			})
			require.NoError(t, err)
			require.Zero(t, calls)
			path := filepath.Join(directory, faultOriginFile)
			require.NoFileExists(t, path)
			// Construction freezes identity even if the caller mutates slices later.
			bindings.StackPorts[0]++
			switch mode {
			case "existing-partial":
				require.NoError(t, os.WriteFile(path, []byte("partial"), 0600))
			case "symlink":
				require.NoError(t, os.Symlink(filepath.Join(directory, "missing"), path))
			case "public-directory":
				require.NoError(t, os.Chmod(directory, 0755))
			case "cancelled":
				cancel()
			}
			before := time.Now()
			origin, err := hook(ctx)
			if mode != "success" {
				require.Error(t, err)
				require.True(t, origin.IsZero())
				if mode == "post-owner-lost" || mode == "post-cancelled" {
					require.FileExists(t, path)
				} else if mode == "existing-partial" {
					data, err := os.ReadFile(path)
					require.NoError(t, err)
					require.Equal(t, "partial", string(data))
				} else if mode == "directory-replaced" {
					require.NoFileExists(t, path)
					require.FileExists(t, filepath.Join(directory+"-retained", faultOriginFile))
				}
				return
			}
			require.NoError(t, err)
			require.False(t, origin.Before(before))
			require.False(t, origin.After(time.Now()))
			require.Equal(t, origin.Add(30*time.Second), finalDeadline)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			var record faultOriginRecord
			require.NoError(t, json.Unmarshal(data, &record))
			require.Equal(t, faultOriginRecord{Version: 1, Owner: owner, BindingsSHA256: planinput.SHA256(raw), OriginNS: strconv.FormatInt(origin.UnixNano(), 10), DeadlineNS: strconv.FormatInt(origin.Add(30*time.Second).UnixNano(), 10)}, record)
			st, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0600), st.Mode().Perm())
			_, err = hook(ctx)
			require.Error(t, err, "never restart the fault clock")
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, data, after)
		})
	}
}

func TestDurableFaultOriginConcurrentHooks(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.Chmod(directory, 0700))
	bindings := commandPlan(t).Bindings
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	results := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		// Independent constructors must still share the persistent one-shot fence.
		hook, err := NewDurableFaultOrigin(directory, bindings, func(context.Context) error { return nil })
		require.NoError(t, err)
		wg.Add(1)
		go func() { defer wg.Done(); _, err := hook(ctx); results <- err }()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
}

func TestDurableFaultOriginRejectsUnboundedContext(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.Chmod(directory, 0700))
	calls := 0
	hook, err := NewDurableFaultOrigin(directory, commandPlan(t).Bindings, func(context.Context) error { calls++; return nil })
	require.NoError(t, err)
	long, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	for _, ctx := range []context.Context{nil, context.Background(), long} {
		origin, err := hook(ctx)
		require.Error(t, err)
		require.True(t, origin.IsZero())
	}
	require.Zero(t, calls)
	require.NoFileExists(t, filepath.Join(directory, faultOriginFile))
}
