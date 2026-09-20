package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type faultToolsWorkflow struct {
	On   map[string]any `yaml:"on"`
	Jobs map[string]struct {
		If     string `yaml:"if"`
		RunsOn string `yaml:"runs-on"`
		Steps  []struct {
			Name string `yaml:"name"`
			Run  string `yaml:"run"`
			Uses string `yaml:"uses"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

func readFaultToolsWorkflow(t *testing.T) faultToolsWorkflow {
	t.Helper()
	data, err := os.ReadFile("../../.github/workflows/lease-fault-tools.yml")
	require.NoError(t, err)
	var workflow faultToolsWorkflow
	require.NoError(t, yaml.Unmarshal(data, &workflow))
	return workflow
}

func TestLeaseFaultToolsWorkflowBoundaries(t *testing.T) {
	w := readFaultToolsWorkflow(t)
	require.Len(t, w.On, 2)
	require.Contains(t, w.On, "workflow_dispatch")
	require.Contains(t, w.On, "push")
	require.Len(t, w.Jobs, 2)
	job := w.Jobs["build-tools"]
	require.Contains(t, job.If, "github.event_name == 'workflow_dispatch'")
	require.Equal(t, "github.event_name == 'push'", w.Jobs["register"].If)
	require.Equal(t, "self-hosted", job.RunsOn)
	order := map[string]int{}
	for i, step := range job.Steps {
		order[step.Name] = i
		if step.Uses != "" {
			require.Regexp(t, regexp.MustCompile(`@[a-f0-9]{40}$`), step.Uses)
		}
		if step.Run != "" {
			require.NotContains(t, step.Run, "${{", "untrusted inputs must travel through env, not shell interpolation")
			cmd := exec.Command("bash", "-n")
			cmd.Stdin = strings.NewReader(step.Run)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s: %s", step.Name, out)
		}
	}
	for _, name := range []string{"Validate independently reviewed inputs", "Smoke test before publishing", "Publish separate immutable-run tag and evidence"} {
		require.Contains(t, order, name)
	}
	require.Less(t, order["Smoke test before publishing"], order["Publish separate immutable-run tag and evidence"])
}

func TestLeaseFaultToolsWorkflowInputAdmission(t *testing.T) {
	w := readFaultToolsWorkflow(t)
	var script string
	for _, step := range w.Jobs["build-tools"].Steps {
		if step.Name == "Validate independently reviewed inputs" {
			script = step.Run
		}
	}
	require.NotEmpty(t, script)
	for _, mode := range []string{"valid", "image-tag", "image-shell", "bad-digest", "wrong-gh", "runner-arch", "wrong-source"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "bin")
			require.NoError(t, os.Mkdir(bin, 0700))
			sha := strings.Repeat("a", 40)
			require.NoError(t, os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\nprintf '%s\\n' "+sha+"\n"), 0700))
			require.NoError(t, os.WriteFile(filepath.Join(bin, "uname"), []byte("#!/bin/sh\necho x86_64\n"), 0700))
			gh := []byte("#!/bin/sh\nexit 99\n")
			require.NoError(t, os.WriteFile(filepath.Join(bin, "gh"), gh, 0700))
			digest := fmt.Sprintf("%x", sha256.Sum256(gh))
			base, arch := "ghcr.io/fivetime/kubebrain@sha256:"+strings.Repeat("b", 64), "X64"
			switch mode {
			case "image-tag":
				base = "ghcr.io/fivetime/kubebrain:dbaas"
			case "image-shell":
				base += "; touch injected"
			case "bad-digest":
				digest = "not-a-digest"
			case "wrong-gh":
				digest = strings.Repeat("0", 64)
			case "runner-arch":
				arch = "ARM64"
			case "wrong-source":
				sha = strings.Repeat("c", 40)
			}
			output := filepath.Join(dir, "output")
			require.NoError(t, os.WriteFile(output, nil, 0600))
			cmd := exec.Command("bash", "-c", script)
			cmd.Dir = "../.."
			cmd.Env = []string{"PATH=" + bin + ":/usr/bin:/bin", "BASE_IMAGE=" + base, "GH_SHA256=" + digest, "GITHUB_SHA=" + sha, "RUNNER_ARCH=" + arch, "RUNNER_TEMP=" + dir, "GITHUB_OUTPUT=" + output, "GITHUB_RUN_ID=123", "GITHUB_RUN_ATTEMPT=1"}
			out, err := cmd.CombinedOutput()
			data, readErr := os.ReadFile(output)
			require.NoError(t, readErr)
			contexts, globErr := filepath.Glob(filepath.Join(dir, "lease-fault-tools.*"))
			require.NoError(t, globErr)
			if mode == "valid" {
				require.NoError(t, err, string(out))
				require.Len(t, contexts, 1)
				require.Contains(t, string(data), "image=ghcr.io/fivetime/kubebrain:fault-tools-"+sha+"-123-1")
				copied, err := os.ReadFile(filepath.Join(contexts[0], "gh"))
				require.NoError(t, err)
				require.Equal(t, gh, copied)
				require.FileExists(t, filepath.Join(contexts[0], ".dockerignore"))
			} else {
				require.Error(t, err, string(out))
				require.Empty(t, data)
				require.Empty(t, contexts, "invalid input must fail before creating build context")
			}
		})
	}
}
