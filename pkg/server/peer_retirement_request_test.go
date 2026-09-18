package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type retirementCountingBody struct {
	io.Reader
	consumed int
}

func (b *retirementCountingBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.consumed += n
	return n, err
}
func (*retirementCountingBody) Close() error { return nil }

func TestPeerRetirementRequestAuthenticatesBeforeReading(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("instance", map[string][]string{"old": {retirementTestPin(certs[0])}})
	require.NoError(t, err)
	chains, err := certs[0].Leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	require.NoError(t, err)
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	lock := election.NewResourceLockManager(election.Config{Prefix: "/request", Identity: "old", Timeout: time.Second}, kv).GetResourceLock()
	record := resourcelock.LeaderElectionRecord{HolderIdentity: "old", LeaseDurationSeconds: 30, LeaderTransitions: 1}
	require.NoError(t, lock.Create(context.Background(), record))
	expected, ok := lock.(election.OwnershipConditionProvider).OwnershipConditionFor(record)
	require.True(t, ok)
	payload, err := expected.MarshalBinary()
	require.NoError(t, err)
	for _, name := range []string{"valid", "plaintext", "client key", "instance", "holder", "duplicate identity", "method", "compressed", "declared oversize", "chunked oversize", "exact limit", "canceled", "wrong body holder", "disabled"} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "https://peer.invalid/retire", nil)
			r.Header.Set(retirementInstanceHeader, "instance")
			r.Header.Set(retirementHolderHeader, "old")
			r.Header.Set("Content-Type", "application/json")
			r.TLS = &tls.ConnectionState{Version: tls.VersionTLS13, HandshakeComplete: true,
				PeerCertificates: []*x509.Certificate{certs[0].Leaf}, VerifiedChains: chains}
			body := &retirementCountingBody{Reader: bytes.NewReader(payload)}
			r.Body = body
			r.ContentLength = -1
			a := auth
			wantRead := name == "valid" || name == "exact limit" || name == "chunked oversize" || name == "wrong body holder"
			switch name {
			case "plaintext":
				r.TLS = nil
			case "client key":
				chain, err := certs[1].Leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
				require.NoError(t, err)
				r.TLS.PeerCertificates, r.TLS.VerifiedChains = []*x509.Certificate{certs[1].Leaf}, chain
			case "instance":
				r.Header.Set(retirementInstanceHeader, "other")
			case "holder":
				r.Header.Set(retirementHolderHeader, "other")
			case "duplicate identity":
				r.Header.Add(retirementHolderHeader, "old")
			case "method":
				r.Method = "GET"
			case "compressed":
				r.Header.Set("Content-Encoding", "gzip")
			case "declared oversize":
				r.ContentLength = election.MaxOwnershipConditionBytes + 1
			case "chunked oversize":
				body.Reader = bytes.NewReader(bytes.Repeat([]byte(" "), election.MaxOwnershipConditionBytes*2))
			case "exact limit":
				body.Reader = bytes.NewReader(append(append([]byte(nil), payload...), bytes.Repeat([]byte(" "), election.MaxOwnershipConditionBytes-len(payload))...))
			case "canceled":
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r = r.WithContext(ctx)
			case "wrong body holder":
				body.Reader = bytes.NewReader(bytes.Replace(payload, []byte(`"holder":"old"`), []byte(`"holder":"other"`), 1))
			case "disabled":
				a = nil
			}
			condition, err := readPeerRetirementCondition(r, a)
			if name == "valid" || name == "exact limit" {
				require.NoError(t, err)
				require.Equal(t, expected, condition)
			} else {
				require.Error(t, err)
				require.Equal(t, election.OwnershipCondition{}, condition)
			}
			if wantRead {
				require.Positive(t, body.consumed)
			} else {
				require.Zero(t, body.consumed, "refuse before consuming body")
			}
			require.LessOrEqual(t, body.consumed, election.MaxOwnershipConditionBytes+1)
		})
	}
	current, _, err := lock.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, record, *current, "request parsing must never release or mutate storage")
}
