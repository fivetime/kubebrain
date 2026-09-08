package main

import (
	"testing"

	"k8s.io/kubectl/pkg/cmd"
)

func TestUpstreamCommandSurface(t *testing.T) {
	root := cmd.NewDefaultKubectlCommand()
	for _, name := range []string{
		"api-resources", "api-versions", "apply", "auth", "certificate", "config",
		"cp", "create", "delete", "diff", "exec", "get", "kustomize", "patch",
		"port-forward", "replace", "rollout", "version", "wait",
	} {
		found, _, err := root.Find([]string{name})
		if err != nil || found == root || found.Name() != name {
			t.Errorf("upstream command %q missing: command=%v error=%v", name, found, err)
		}
	}
}
