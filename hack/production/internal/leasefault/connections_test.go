package leasefault

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd"
	api "k8s.io/client-go/tools/clientcmd/api"
)

func connectionFixture(t *testing.T) (ConnectionPlan, *api.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture"}, DNSNames: []string{"test.example"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	dir := t.TempDir()
	p := ConnectionPlan{Context: "test", APIServer: "https://127.0.0.1:6443", Endpoint: "127.0.0.1:2379", ServerName: "test.example", Files: map[string]string{}}
	write := func(name string, data []byte) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, data, 0600))
		p.Files[path] = planinput.SHA256(data)
		return path
	}
	p.CA, p.Certificate, p.Key = write("ca.pem", cert), write("cert.pem", cert), write("key.pem", keyPEM)
	raw := api.NewConfig()
	raw.Contexts["test"] = &api.Context{Cluster: "cluster", AuthInfo: "user"}
	raw.Clusters["cluster"] = &api.Cluster{Server: p.APIServer, CertificateAuthorityData: cert}
	raw.AuthInfos["user"] = &api.AuthInfo{ClientCertificateData: cert, ClientKeyData: keyPEM}
	data, err := clientcmd.Write(*raw)
	require.NoError(t, err)
	p.Kubeconfig = write("config", data)
	return p, raw
}

func TestFaultConnections(t *testing.T) {
	for _, mode := range []string{"valid", "changed-ca", "changed-key", "changed-cert", "changed-config", "missing-pin", "public-key", "key-symlink", "wrong-api", "missing-context", "endpoint", "server-name", "proxy", "insecure", "token-file", "exec", "impersonate", "external-ca", "external-key"} {
		t.Run(mode, func(t *testing.T) {
			p, raw := connectionFixture(t)
			a, c := raw.AuthInfos["user"], raw.Clusters["cluster"]
			switch mode {
			case "changed-ca":
				require.NoError(t, os.WriteFile(p.CA, []byte("changed"), 0600))
			case "changed-key":
				require.NoError(t, os.WriteFile(p.Key, []byte("changed"), 0600))
			case "changed-cert":
				require.NoError(t, os.WriteFile(p.Certificate, []byte("changed"), 0600))
			case "missing-pin":
				delete(p.Files, p.CA)
			case "public-key":
				require.NoError(t, os.Chmod(p.Key, 0644))
			case "key-symlink":
				link := filepath.Join(t.TempDir(), "link")
				require.NoError(t, os.Symlink(p.Key, link))
				p.Files[link] = p.Files[p.Key]
				p.Key = link
			case "wrong-api":
				p.APIServer = "https://127.0.0.2:6443"
			case "missing-context":
				p.Context = "other"
			case "endpoint":
				p.Endpoint = "dns:///test.example:2379"
			case "server-name":
				p.ServerName = ""
			case "proxy":
				c.ProxyURL = "http://127.0.0.1:3128"
			case "insecure":
				c.InsecureSkipTLSVerify = true
			case "token-file":
				a.TokenFile = "/must-not-read/token"
			case "exec":
				a.Exec = &api.ExecConfig{Command: "/must-not-execute/plugin"}
			case "impersonate":
				a.Impersonate = "other"
			case "external-ca":
				c.CertificateAuthority = "/must-not-read/ca"
			case "external-key":
				a.ClientKey = "/must-not-read/key"
			}
			data, err := clientcmd.Write(*raw)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(p.Kubeconfig, data, 0600))
			p.Files[p.Kubeconfig] = planinput.SHA256(data)
			if mode == "changed-config" {
				require.NoError(t, os.WriteFile(p.Kubeconfig, []byte("changed"), 0600))
			}
			client, conn, err := NewFaultConnections(p)
			if mode == "valid" {
				require.NoError(t, err)
				require.NotNil(t, client)
				require.NotNil(t, conn)
				require.NoError(t, conn.Close())
			} else {
				require.Error(t, err)
				require.Nil(t, client)
				require.Nil(t, conn)
			}
		})
	}
}

func TestFaultTLSUsesPinnedBytesAtHandshake(t *testing.T) {
	p, _ := connectionFixture(t)
	cfg, err := p.tlsConfig()
	require.NoError(t, err)
	require.Nil(t, cfg.GetClientCertificate)
	require.False(t, cfg.InsecureSkipVerify)
	require.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	for _, path := range []string{p.CA, p.Certificate, p.Key} {
		require.NoError(t, os.WriteFile(path, []byte("replaced after admission"), 0600))
	}
	_, err = p.tlsConfig()
	require.Error(t, err, "a fresh attempt must reject changed credentials")
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	require.NoError(t, left.SetDeadline(time.Now().Add(3*time.Second)))
	require.NoError(t, right.SetDeadline(time.Now().Add(3*time.Second)))
	server := tls.Server(left, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: cfg.Certificates, ClientCAs: cfg.RootCAs, ClientAuth: tls.RequireAndVerifyClientCert})
	client := tls.Client(right, cfg)
	done := make(chan error, 1)
	go func() { done <- server.Handshake() }()
	clientErr := client.Handshake()
	serverErr := <-done
	require.NoError(t, clientErr)
	require.NoError(t, serverErr)
	require.NotEmpty(t, client.ConnectionState().VerifiedChains)
	require.NotEmpty(t, server.ConnectionState().VerifiedChains)
}
