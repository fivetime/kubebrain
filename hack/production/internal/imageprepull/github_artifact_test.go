package imageprepull

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFetchGitHubReleaseEvidence(t *testing.T) {
	for _, mode := range []string{"success", "run-before", "artifact-before", "archive", "artifact-after", "run-after", "retention", "post-admission"} {
		t.Run(mode, func(t *testing.T) {
			run, wanted := githubRunFixture(t)
			object, platforms := releaseFixture(t)
			image, index := releaseBytes(t, object)
			wanted.Image = image
			receipt, err := json.Marshal(ReleaseEvidence{SchemaVersion: 1, Repository: "fivetime/kubebrain", Source: wanted.Source, RunID: wanted.RunID, RunAttempt: wanted.RunAttempt, Workflow: ".github/workflows/image.yml", Image: image, IndexSHA256: strings.Split(image, "sha256:")[1], Platforms: platforms})
			require.NoError(t, err)
			var buffer bytes.Buffer
			zipWriter := zip.NewWriter(&buffer)
			for name, data := range map[string][]byte{"index.json": index, "release.json": receipt} {
				file, err := zipWriter.Create(name)
				require.NoError(t, err)
				_, err = file.Write(data)
				require.NoError(t, err)
			}
			require.NoError(t, zipWriter.Close())
			archive := buffer.Bytes()
			metadata := map[string]any{"id": 7, "name": "dbaas-release-123-2", "size_in_bytes": len(archive), "digest": fmt.Sprintf("sha256:%x", sha256.Sum256(archive)), "expired": false, "workflow_run": map[string]any{"id": 123, "repository_id": 1285006877, "head_repository_id": 1285006877, "head_branch": "dbaas", "head_sha": wanted.Source}}
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			writeMeta := func() {
				data, err := json.Marshal(metadata)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(dir, "artifact.json"), data, 0600))
			}
			if mode == "artifact-before" {
				metadata["expired"] = true
			}
			writeMeta()
			if mode == "run-before" {
				run = bytes.Replace(run, []byte(`"success"`), []byte(`"failure"`), 1)
			}
			require.NoError(t, os.WriteFile(filepath.Join(dir, "run.json"), run, 0600))
			if mode == "archive" {
				archive = append(append([]byte(nil), archive...), byte('!'))
			}
			require.NoError(t, os.WriteFile(filepath.Join(dir, "archive.zip"), archive, 0600))
			gh := filepath.Join(dir, "gh")
			script := `#!/bin/sh
set -eu
test "$1 $2 $3 $4 $5" = 'api --hostname github.com --method GET'
printf '%s\n' "$6" >> "$GH_CONFIG_DIR/requests"
case "$6" in
repos/fivetime/kubebrain/actions/runs/123/attempts/2) cat "$GH_CONFIG_DIR/run.json" ;;
repos/fivetime/kubebrain/actions/artifacts/7) cat "$GH_CONFIG_DIR/artifact.json" ;;
repos/fivetime/kubebrain/actions/artifacts/7/zip) cat "$GH_CONFIG_DIR/archive.zip" ;;
*) exit 2 ;;
esac
`
			require.NoError(t, os.WriteFile(gh, []byte(script), 0700))
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var stages []string
			admissions := 0
			got, gotIndex, err := FetchGitHubReleaseEvidence(ctx, gh, dir, "7", wanted, func(context.Context) error {
				admissions++
				if mode == "post-admission" && admissions == 6 {
					return errors.New("changed inputs")
				}
				return nil
			}, func(stage string, data []byte, observed error) error {
				stages = append(stages, stage)
				require.NotEmpty(t, data)
				if mode == "artifact-after" && stage == "archive" {
					metadata["digest"] = "sha256:" + strings.Repeat("b", 64)
					writeMeta()
				}
				if mode == "run-after" && stage == "artifact-after" {
					require.NoError(t, os.WriteFile(filepath.Join(dir, "run.json"), bytes.Replace(run, []byte(`"success"`), []byte(`"failure"`), 1), 0600))
				}
				if mode == "retention" && stage == "archive" {
					return errors.New("disk failed")
				}
				return RetainReleaseResponse(dir, stage, data, observed)
			})
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, index, gotIndex)
				require.Equal(t, platforms, got.Platforms)
				require.Equal(t, 10, admissions)
			} else {
				require.Error(t, err)
				require.Nil(t, gotIndex)
				require.Equal(t, ReleaseEvidence{}, got)
			}
			count := map[string]int{"success": 5, "run-before": 1, "artifact-before": 2, "archive": 3, "artifact-after": 4, "run-after": 5, "retention": 3, "post-admission": 3}[mode]
			require.Equal(t, []string{"run-before", "artifact-before", "archive", "artifact-after", "run-after"}[:count], stages)
			requests, readErr := os.ReadFile(filepath.Join(dir, "requests"))
			require.NoError(t, readErr)
			require.Equal(t, count, strings.Count(string(requests), "\n"), "one request per phase, without retries")
			for _, stage := range stages {
				if mode == "retention" && stage == "archive" {
					continue
				}
				require.FileExists(t, filepath.Join(dir, "github-"+stage+".json"))
			}
		})
	}
}

func TestReleaseArtifactOwnershipMetadata(t *testing.T) {
	_, wanted := githubRunFixture(t)
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, field := range []string{"id", "name", "size_in_bytes", "digest", "expired", "workflow_run.id", "workflow_run.repository_id", "workflow_run.head_repository_id", "workflow_run.head_branch", "workflow_run.head_sha"} {
		t.Run(field, func(t *testing.T) {
			object := map[string]any{"id": 7, "name": "dbaas-release-123-2", "size_in_bytes": 123, "digest": digest, "expired": false, "workflow_run": map[string]any{"id": 123, "repository_id": 1285006877, "head_repository_id": 1285006877, "head_branch": "dbaas", "head_sha": wanted.Source}}
			raw, err := json.Marshal(object)
			require.NoError(t, err)
			_, err = checkReleaseArtifact(raw, "7", wanted)
			require.NoError(t, err)
			parent, key := object, field
			if strings.HasPrefix(field, "workflow_run.") {
				parent, key = object["workflow_run"].(map[string]any), strings.TrimPrefix(field, "workflow_run.")
			}
			// Missing and case-aliased identity fields must not default into a
			// successful decision (especially expired:false).
			value := parent[key]
			delete(parent, key)
			for _, alias := range []bool{false, true} {
				if alias {
					parent[strings.ToUpper(key)] = value
				}
				raw, err = json.Marshal(object)
				require.NoError(t, err)
				result, err := checkReleaseArtifact(raw, "7", wanted)
				require.Error(t, err)
				require.Equal(t, releaseArtifactBinding{}, result)
			}
		})
	}
}
