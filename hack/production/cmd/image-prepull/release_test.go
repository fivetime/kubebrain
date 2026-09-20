package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/imageprepull"
	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
)

func TestFetchReleaseCommand(t *testing.T) {
	for _, mode := range []string{"success", "library", "library-failure", "wrong-plan-hash", "workflow-changed", "public-credentials", "regression-failed", "reuse"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			config, evidence := filepath.Join(dir, "config"), filepath.Join(dir, "evidence")
			require.NoError(t, os.Mkdir(config, 0700))
			require.NoError(t, os.Mkdir(evidence, 0700))
			write := func(path string, data []byte) {
				t.Helper()
				require.NoError(t, os.WriteFile(path, data, 0600))
			}
			encode := func(value any) []byte {
				t.Helper()
				data, err := json.Marshal(value)
				require.NoError(t, err)
				return data
			}
			platforms := map[string]string{"linux/amd64": "sha256:" + strings.Repeat("a", 64), "linux/arm64": "sha256:" + strings.Repeat("b", 64)}
			var manifests []any
			for _, arch := range []string{"amd64", "arm64"} {
				manifests = append(manifests, map[string]any{"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": platforms["linux/"+arch], "size": 123, "platform": map[string]string{"os": "linux", "architecture": arch}})
			}
			index := encode(map[string]any{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": manifests})
			p := releaseDownloadPlan{Source: strings.Repeat("a", 40), Image: "ghcr.io/fivetime/kubebrain@sha256:" + planinput.SHA256(index), RunID: "123", RunAttempt: "1", RegressionRunID: "456", RegressionAttempt: "1", ArtifactID: "7", GH: filepath.Join(dir, "gh"), ConfigDirectory: config, EvidenceDirectory: evidence, ImageWorkflow: filepath.Join(dir, "image.yml"), RegressionWorkflow: filepath.Join(dir, "probe-regression.yml"), Files: map[string]string{}}
			receipt := encode(imageprepull.ReleaseEvidence{SchemaVersion: 1, Repository: "fivetime/kubebrain", Source: p.Source, RunID: p.RunID, RunAttempt: p.RunAttempt, Workflow: ".github/workflows/image.yml", Image: p.Image, IndexSHA256: planinput.SHA256(index), Platforms: platforms})
			var archive bytes.Buffer
			zw := zip.NewWriter(&archive)
			for name, data := range map[string][]byte{"index.json": index, "release.json": receipt} {
				w, err := zw.Create(name)
				require.NoError(t, err)
				_, err = w.Write(data)
				require.NoError(t, err)
			}
			require.NoError(t, zw.Close())
			write(filepath.Join(config, "archive.zip"), archive.Bytes())
			repo := map[string]any{"id": 1285006877, "full_name": "fivetime/kubebrain"}
			for id, workflow := range map[int]string{123: "image.yml", 456: "probe-regression.yml"} {
				conclusion := "success"
				if (mode == "regression-failed" || mode == "library-failure") && id == 456 {
					conclusion = "failure"
				}
				write(filepath.Join(config, fmt.Sprintf("%d.json", id)), encode(map[string]any{"id": id, "run_attempt": 1, "head_sha": p.Source, "head_branch": "dbaas", "event": "push", "path": ".github/workflows/" + workflow, "status": "completed", "conclusion": conclusion, "repository": repo, "head_repository": repo}))
			}
			write(filepath.Join(config, "artifact.json"), encode(map[string]any{"id": 7, "name": "dbaas-release-123-1", "size_in_bytes": archive.Len(), "digest": "sha256:" + planinput.SHA256(archive.Bytes()), "expired": false, "workflow_run": map[string]any{"id": 123, "repository_id": 1285006877, "head_repository_id": 1285006877, "head_sha": p.Source, "head_branch": "dbaas"}}))
			write(p.GH, []byte(`#!/bin/sh
set -eu
test "$1 $2 $3 $4 $5" = 'api --hostname github.com --method GET'
case "$6" in
repos/fivetime/kubebrain/actions/runs/123/attempts/1) cat "$GH_CONFIG_DIR/123.json" ;;
repos/fivetime/kubebrain/actions/runs/456/attempts/1) cat "$GH_CONFIG_DIR/456.json" ;;
repos/fivetime/kubebrain/actions/artifacts/7) cat "$GH_CONFIG_DIR/artifact.json" ;;
repos/fivetime/kubebrain/actions/artifacts/7/zip) cat "$GH_CONFIG_DIR/archive.zip" ;;
*) exit 2 ;;
esac
`))
			require.NoError(t, os.Chmod(p.GH, 0700))
			for _, file := range []string{p.ImageWorkflow, p.RegressionWorkflow, filepath.Join(config, "hosts.yml"), filepath.Join(config, "config.yml")} {
				write(file, []byte("fixture"))
			}
			for _, file := range []string{p.GH, p.ImageWorkflow, p.RegressionWorkflow, filepath.Join(config, "hosts.yml"), filepath.Join(config, "config.yml")} {
				data, err := os.ReadFile(file)
				require.NoError(t, err)
				p.Files[file] = planinput.SHA256(data)
			}
			plan := filepath.Join(dir, "plan.json")
			data := encode(p)
			write(plan, data)
			digest := planinput.SHA256(data)
			switch mode {
			case "wrong-plan-hash":
				digest = strings.Repeat("0", 64)
			case "workflow-changed":
				write(p.ImageWorkflow, []byte("changed"))
			case "public-credentials":
				require.NoError(t, os.Chmod(filepath.Join(config, "hosts.yml"), 0644))
			}
			args := []string{"--mode=fetch-release", "--release-plan=" + plan, "--release-plan-sha256=" + digest}
			if mode == "library" || mode == "library-failure" {
				got, rawIndex, err := imageprepull.FetchReleasePlan(t.Context(), plan, digest)
				if mode == "library-failure" {
					require.Error(t, err)
					require.Equal(t, imageprepull.ReleaseEvidence{}, got)
					require.Nil(t, rawIndex, "no partially authenticated index may escape")
					return
				}
				require.NoError(t, err)
				require.Equal(t, index, rawIndex, "preserve verified raw OCI bytes, not reserialized JSON")
				require.Equal(t, p.Source, got.Source)
				require.Equal(t, p.Image, got.Image)
				require.Equal(t, platforms, got.Platforms)
				entries, err := os.ReadDir(evidence)
				require.NoError(t, err)
				require.Len(t, entries, 7)
				return
			}
			var output bytes.Buffer
			err := run(t.Context(), args, &output)
			if mode == "success" || mode == "reuse" {
				require.NoError(t, err)
				require.Contains(t, output.String(), p.Image)
				require.Contains(t, output.String(), "not deployment or fault acceptance")
				entries, err := os.ReadDir(evidence)
				require.NoError(t, err)
				require.Len(t, entries, 7)
				if mode == "reuse" {
					output.Reset()
					require.Error(t, run(t.Context(), args, &output))
					require.Empty(t, output.String())
				}
			} else {
				require.Error(t, err)
				require.Empty(t, output.String())
			}
		})
	}
}
