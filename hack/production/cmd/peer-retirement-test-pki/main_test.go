package main

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/endpoint"
	"github.com/stretchr/testify/require"
)

func testOptions(t *testing.T) options {
	t.Helper()
	return options{dir: filepath.Join(t.TempDir(), "bundle"), service: "peer.test.svc", members: "member-0,member-1,member-2", cluster: 42, prefix: "/test", keyspace: "tenant", port: 3380}
}

func TestGenerateDistinctPeerBundles(t *testing.T) {
	o := testOptions(t)
	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, generate(o, now))
	caPEM, err := os.ReadFile(filepath.Join(o.dir, "ca.crt"))
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM))
	scope, err := election.ComputeRetirementScope(o.cluster, o.keyspace, o.prefix)
	require.NoError(t, err)
	seen := make(map[string]bool)
	for _, member := range []string{"member-0", "member-1", "member-2"} {
		dir := filepath.Join(o.dir, member)
		certificate, err := tls.LoadX509KeyPair(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"))
		require.NoError(t, err)
		leaf, err := x509.ParseCertificate(certificate.Certificate[0])
		require.NoError(t, err)
		require.False(t, leaf.IsCA)
		require.Equal(t, now.Add(24*time.Hour), leaf.NotAfter)
		for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
			_, err := leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: member + "." + o.service, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{usage}})
			require.NoError(t, err)
		}
		require.NoError(t, leaf.VerifyHostname(o.service))
		require.Error(t, leaf.VerifyHostname("other.test.svc"))
		pin := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
		encoded := hex.EncodeToString(pin[:])
		require.False(t, seen[encoded], "each member must have a distinct key")
		seen[encoded] = true
		policy, err := endpoint.LoadPeerRetirementOptions(filepath.Join(dir, "policy.json"))
		require.NoError(t, err)
		holder := member + "." + o.service + ":3380"
		require.Equal(t, []string{encoded}, policy.HolderPins[holder])
		require.Equal(t, scope, policy.Scope)
		require.Len(t, policy.HolderPins, 3)
		require.Len(t, policy.EndpointHolders, 2)
		require.NotContains(t, policy.EndpointHolders, "https://"+holder)
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 4, "only this member's key, leaf, CA and policy belong in its mount")
	}
	require.NoError(t, filepath.WalkDir(o.dir, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)
		info, err := entry.Info()
		require.NoError(t, err)
		if entry.IsDir() {
			require.Equal(t, os.FileMode(0700), info.Mode().Perm())
		} else {
			require.Equal(t, os.FileMode(0600), info.Mode().Perm())
		}
		return nil
	}))
	_, err = os.Stat(filepath.Join(o.dir, "COMPLETE"))
	require.NoError(t, err)
	before, err := os.ReadFile(filepath.Join(o.dir, "ca.key"))
	require.NoError(t, err)
	require.Error(t, generate(o, now), "existing bundles must never be overwritten")
	after, err := os.ReadFile(filepath.Join(o.dir, "ca.key"))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestGenerateRejectsInvalidInputsBeforeWriting(t *testing.T) {
	for name, mutate := range map[string]func(*options){
		"shared member": func(o *options) { o.members = "member-0,member-0" },
		"path member":   func(o *options) { o.members = "member-0,../other" },
		"one member":    func(o *options) { o.members = "member-0" },
		"bad service":   func(o *options) { o.service = "https://peer.test" },
		"no cluster":    func(o *options) { o.cluster = 0 },
		"no prefix":     func(o *options) { o.prefix = "" },
		"bad port":      func(o *options) { o.port = 65536 },
	} {
		t.Run(name, func(t *testing.T) {
			o := testOptions(t)
			mutate(&o)
			require.Error(t, generate(o, time.Now()))
			_, err := os.Stat(o.dir)
			require.True(t, os.IsNotExist(err))
		})
	}
	o := testOptions(t)
	require.NoError(t, os.Symlink(t.TempDir(), o.dir))
	require.Error(t, generate(o, time.Now()))
	require.Error(t, run(nil))
}
