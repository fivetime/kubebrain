package main

import (
	"strings"
	"testing"
)

func TestValidateAuditPrefix(t *testing.T) {
	for _, prefix := range []string{
		"/__kubebrain/audit",
		"/tmp/kubebrain-audit",
		"/registry-safe",
	} {
		t.Run("valid "+prefix, func(t *testing.T) {
			if err := validateAuditPrefix(prefix); err != nil {
				t.Fatalf("validateAuditPrefix(%q) returned %v", prefix, err)
			}
		})
	}

	for _, tc := range []struct {
		name    string
		prefix  string
		message string
	}{
		{name: "empty", prefix: "", message: "absolute key prefix"},
		{name: "relative", prefix: "relative", message: "absolute key prefix"},
		{name: "root", prefix: "/", message: "must not target"},
		{name: "slashes", prefix: "////", message: "must not target"},
		{name: "registry", prefix: "/registry", message: "must not target"},
		{name: "registry trailing", prefix: "/registry/", message: "must not target"},
		{name: "registry child", prefix: "/registry/pods", message: "must not target"},
		{name: "control", prefix: "/__kubebrain/audit\nprobe", message: "control characters"},
		{name: "del", prefix: "/__kubebrain/audit\x7fprobe", message: "control characters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAuditPrefix(tc.prefix)
			if err == nil {
				t.Fatalf("validateAuditPrefix(%q) succeeded, want error containing %q", tc.prefix, tc.message)
			}
			if got := err.Error(); !strings.Contains(got, tc.message) {
				t.Fatalf("validateAuditPrefix(%q) error %q, want substring %q", tc.prefix, got, tc.message)
			}
		})
	}
}
