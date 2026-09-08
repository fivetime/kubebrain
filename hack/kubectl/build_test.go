package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildRequiresProvenanceAndSupportedArchitecture(t *testing.T) {
	const sha = "0123456789abcdef0123456789abcdef01234567"
	for _, tc := range []struct{ name, sha, date, arch, want string }{
		{"missing SHA", "", "2026-09-08T18:05:00Z", "amd64", "full commit SHA"},
		{"short SHA", "abcd", "2026-09-08T18:05:00Z", "amd64", "full commit SHA"},
		{"missing date", sha, "", "amd64", "UTC RFC3339"},
		{"unsafe date", sha, "2026-09-08T18:05:00Z -X injected", "amd64", "UTC RFC3339"},
		{"unknown arch", sha, "2026-09-08T18:05:00Z", "s390x", "unsupported TARGETARCH=s390x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "bash", "build.sh", filepath.Join(t.TempDir(), "kubectl"))
			command.Env = append(os.Environ(), "KUBEBRAIN_GIT_SHA="+tc.sha, "KUBEBRAIN_BUILD_DATE="+tc.date, "TARGETARCH="+tc.arch)
			output, err := command.CombinedOutput()
			if err == nil || !strings.Contains(string(output), tc.want) {
				t.Fatalf("expected rejection %q: error=%v output=%s", tc.want, err, output)
			}
		})
	}
}

func TestBuildRecordsDownstreamIdentityForBothArchitectures(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte("#!/bin/sh\nprintf '%s\\n' \"$GOOS/$GOARCH/$CGO_ENABLED\" \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, arch := range []string{"amd64", "arm64"} {
		t.Run(arch, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "bash", "build.sh", filepath.Join(dir, "kubectl-"+arch))
			command.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
				"TARGETARCH="+arch, "KUBEBRAIN_GIT_SHA=0123456789abcdef0123456789abcdef01234567", "KUBEBRAIN_BUILD_DATE=2026-09-08T18:05:00Z")
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("build invocation failed: %v: %s", err, output)
			}
			for _, want := range []string{"linux/" + arch + "/0", "-mod=readonly", "-buildvcs=false", "-trimpath",
				"k8s.io/component-base/version.gitVersion=v1.36.4+kubebrain", "k8s.io/client-go/pkg/version.gitVersion=v1.36.4+kubebrain",
				".gitTreeState=dirty", ".gitCommit=0123456789abcdef0123456789abcdef01234567", ".buildDate=2026-09-08T18:05:00Z"} {
				if !strings.Contains(string(output), want) {
					t.Errorf("missing build contract %q: %s", want, output)
				}
			}
		})
	}
}
