package leasefault

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommandJournalPreservesAttemptAndAmbiguity(t *testing.T) {
	for _, mode := range []string{"returned", "replaced-directory", "existing-return", "existing-claim"} {
		t.Run(mode, func(t *testing.T) {
			p := NativeCommandPlan{OwnerDirectory: t.TempDir()}
			p.Bindings.Network.Owner = "test-owner"
			require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
			if mode == "existing-return" || mode == "existing-claim" {
				name := commandReturnFile
				if mode == "existing-claim" {
					name = ownerIntentFile
				}
				require.NoError(t, os.WriteFile(filepath.Join(p.OwnerDirectory, name), []byte("existing"), 0600))
			}
			digest := strings.Repeat("a", 64)
			finish, err := beginCommandJournal(p, digest)
			if strings.HasPrefix(mode, "existing-") {
				require.Error(t, err)
				require.Nil(t, finish)
				require.NoFileExists(t, filepath.Join(p.OwnerDirectory, commandAttemptFile))
				return
			}
			require.NoError(t, err)
			before, err := os.ReadFile(filepath.Join(p.OwnerDirectory, commandAttemptFile))
			require.NoError(t, err)
			_, err = beginCommandJournal(p, digest)
			require.ErrorContains(t, err, "fresh attempt")
			if mode == "replaced-directory" {
				require.NoError(t, os.Rename(p.OwnerDirectory, p.OwnerDirectory+".old"))
				require.NoError(t, os.Mkdir(p.OwnerDirectory, 0700))
				require.ErrorContains(t, finish(ClaimedCommandResult{}, nil), "directory changed")
				require.NoFileExists(t, filepath.Join(p.OwnerDirectory, commandReturnFile))
				require.FileExists(t, filepath.Join(p.OwnerDirectory+".old", commandAttemptFile))
				return
			}
			require.NoError(t, finish(ClaimedCommandResult{}, errors.New("connection refused")))
			raw, err := os.ReadFile(filepath.Join(p.OwnerDirectory, commandReturnFile))
			require.NoError(t, err)
			var record map[string]any
			require.NoError(t, json.Unmarshal(raw, &record))
			require.Equal(t, digest, record["plan_sha256"])
			require.Equal(t, "connection refused", record["error"])
			require.Equal(t, "", record["known_claim_uid"])
			require.Equal(t, false, record["recovery_attempted"])
			require.Contains(t, record["scope"], "not proof of no mutation")
			after, err := os.ReadFile(filepath.Join(p.OwnerDirectory, commandAttemptFile))
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
