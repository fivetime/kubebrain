package imageprepull

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func githubRunFixture(t *testing.T) ([]byte, ReleaseIdentity) {
	t.Helper()
	wanted := ReleaseIdentity{Source: strings.Repeat("a", 40), RunID: "123", RunAttempt: "2"}
	repo := map[string]any{"id": 1285006877, "full_name": "fivetime/kubebrain"}
	raw, err := json.Marshal(map[string]any{"id": 123, "run_attempt": 2, "head_sha": wanted.Source, "head_branch": "dbaas", "event": "push", "path": ".github/workflows/image.yml", "status": "completed", "conclusion": "success", "repository": repo, "head_repository": repo})
	require.NoError(t, err)
	return raw, wanted
}

func TestGitHubReleaseRunMetadata(t *testing.T) {
	raw, wanted := githubRunFixture(t)
	require.NoError(t, checkGitHubReleaseRun(raw, wanted))
	for _, field := range []string{"id", "run_attempt", "head_sha", "head_branch", "event", "path", "status", "conclusion", "repository", "head_repository"} {
		t.Run(field, func(t *testing.T) {
			var data map[string]any
			require.NoError(t, json.Unmarshal(raw, &data))
			data[field] = "wrong"
			changed, err := json.Marshal(data)
			require.NoError(t, err)
			require.Error(t, checkGitHubReleaseRun(changed, wanted))
		})
	}
	for _, invalid := range [][]byte{[]byte("null"), append(raw, []byte(" {}")...), []byte(strings.Replace(string(raw), `"status"`, `"STATUS"`, 1)), append([]byte(`{"id":123,`), raw[1:]...)} {
		require.Error(t, checkGitHubReleaseRun(invalid, wanted))
	}
}

func TestVerifyGitHubReleaseRunCommand(t *testing.T) {
	for _, mode := range []string{"success", "process-error", "post-admission", "retention-error"} {
		t.Run(mode, func(t *testing.T) {
			raw, wanted := githubRunFixture(t)
			dir := t.TempDir()
			gh := filepath.Join(dir, "gh")
			script := "#!/bin/sh\nset -eu\ntest \"$*\" = 'api --hostname github.com --method GET repos/fivetime/kubebrain/actions/runs/123/attempts/2'\ntest -z \"${GH_TOKEN-}\"\nprintf '%s' '" + string(raw) + "'\n"
			script += "test \"$PWD\" = \"$GH_CONFIG_DIR\"\n"
			if mode == "process-error" {
				script += "exit 1\n"
			}
			require.NoError(t, os.WriteFile(gh, []byte(script), 0700))
			t.Setenv("GH_TOKEN", "must-not-be-inherited")
			ctx, cancel := context.WithTimeout(t.Context(), time.Second*10)
			defer cancel()
			admissions, retentions := 0, 0
			err := VerifyGitHubReleaseRun(ctx, gh, dir, wanted, func(context.Context) error {
				admissions++
				if mode == "post-admission" && admissions == 2 {
					return errors.New("changed tools")
				}
				return nil
			}, func(out []byte, observed error) error {
				retentions++
				require.Equal(t, raw, out)
				if mode == "process-error" {
					require.Error(t, observed)
				} else {
					require.NoError(t, observed)
				}
				if mode == "retention-error" {
					return errors.New("retention refused")
				}
				return nil
			})
			require.Equal(t, 1, retentions)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, 2, admissions)
			} else {
				require.Error(t, err)
			}
		})
	}
}
