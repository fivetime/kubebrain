package endpoint

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestThreePeerTrustTransitionAndRollback(t *testing.T) {
	oldCA, newCA := newRotationCA(t), newRotationCA(t)
	shared := t.TempDir()
	oldCert, oldKey := writeRotationCertificate(t, shared, "old-shared-peer", oldCA, 10, "peer.test")
	var links, caFiles, newDirs [3]string
	var servers, clients [3]*tls.Config
	serials := [3]int64{10, 10, 10}
	for i := range servers {
		dir := t.TempDir()
		links[i] = filepath.Join(dir, "active")
		require.NoError(t, os.Symlink(shared, links[i]))
		caFiles[i] = filepath.Join(dir, "ca.pem")
		writeRotationCA(t, caFiles[i], oldCA)
		newDirs[i] = t.TempDir()
		writeRotationCertificate(t, newDirs[i], fmt.Sprintf("new-peer-%d", i), newCA, int64(20+i), "peer.test")
		sc := &SecurityConfig{CertFile: filepath.Join(links[i], "tls.crt"), KeyFile: filepath.Join(links[i], "tls.key"),
			CA: caFiles[i], ClientAuth: true, ServerName: "peer.test"}
		require.NoError(t, sc.validate())
		// Retain these exact configs throughout all stages: callbacks must read
		// current material, not a fresh constructor's cached startup snapshot.
		servers[i], clients[i] = sc.getServerTLSConfig(), sc.getClientTLSConfig()
	}
	switchLeaf := func(i int, modern bool) {
		dir, serial := shared, int64(10)
		if modern {
			dir, serial = newDirs[i], int64(20+i)
		}
		require.NoError(t, os.Symlink(dir, links[i]+".next"))
		require.NoError(t, os.Rename(links[i]+".next", links[i]))
		serials[i] = serial
	}
	checkAll := func(stage string) {
		t.Helper()
		for server := range servers {
			for client := range clients {
				if server == client {
					continue
				}
				state, peer := handshakeTLS(t, servers[server], clients[client])
				require.Equal(t, serials[client], state.PeerCertificates[0].SerialNumber.Int64(), stage)
				require.Equal(t, serials[server], peer.PeerCertificates[0].SerialNumber.Int64(), stage)
				require.NotEmpty(t, state.VerifiedChains, stage)
			}
		}
	}
	checkBrokenPair := func(stage string) {
		t.Helper()
		for _, pair := range [][2]int{{0, 1}, {1, 0}} {
			_, _, serverErr, clientErr := tryHandshakeTLS(t, servers[pair[0]], clients[pair[1]])
			require.True(t, serverErr != nil || clientErr != nil, stage)
		}
	}
	checkAll("initial old shared leaf")
	switchLeaf(0, true)
	checkBrokenPair("new leaf before dual trust must fail")
	switchLeaf(0, false)
	checkAll("restore failed premature leaf change")
	for i := range servers {
		writeRotationCA(t, caFiles[i], oldCA, newCA)
		checkAll("expand trust one member at a time")
	}
	for i := range servers {
		switchLeaf(i, true)
		checkAll("migrate distinct leaves one member at a time")
	}
	for i := range servers {
		writeRotationCA(t, caFiles[i], newCA)
		checkAll("remove old trust after all leaves changed")
	}
	// An old client still trusting the new server must be rejected at the
	// server, proving old client CA removal rather than a client-side failure.
	oldCertificate, err := tls.LoadX509KeyPair(oldCert, oldKey)
	require.NoError(t, err)
	newRoots := x509.NewCertPool()
	newRoots.AddCert(newCA.cert)
	_, _, serverErr, _ := tryHandshakeTLS(t, servers[0], &tls.Config{RootCAs: newRoots, ServerName: "peer.test", Certificates: []tls.Certificate{oldCertificate}})
	require.Error(t, serverErr)
	switchLeaf(0, false)
	writeRotationCA(t, caFiles[0], oldCA)
	checkBrokenPair("jumping straight back to the original single-CA material must fail")
	writeRotationCA(t, caFiles[0], newCA)
	switchLeaf(0, true)
	checkAll("restore failed premature rollback")
	for i := range servers {
		writeRotationCA(t, caFiles[i], oldCA, newCA)
		checkAll("rollback first restores dual trust")
	}
	for i := range servers {
		switchLeaf(i, false)
		checkAll("rollback restores old leaves under dual trust")
	}
	for i := range servers {
		writeRotationCA(t, caFiles[i], oldCA)
		checkAll("rollback completes with original trust")
	}
	newCertificate, err := tls.LoadX509KeyPair(filepath.Join(newDirs[0], "tls.crt"), filepath.Join(newDirs[0], "tls.key"))
	require.NoError(t, err)
	oldRoots := x509.NewCertPool()
	oldRoots.AddCert(oldCA.cert)
	_, _, serverErr, _ = tryHandshakeTLS(t, servers[1], &tls.Config{RootCAs: oldRoots, ServerName: "peer.test", Certificates: []tls.Certificate{newCertificate}})
	require.Error(t, serverErr, "completed rollback must remove the new client CA, not retain dual trust")
}
