package leasefault

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

// ConnectionPlan must come from independent plan admission. Files pins the
// actual credential bytes, not just paths checked at some earlier instant.
// NewFaultConnections creates lazy clients; it does not prove live identities,
// acquire ownership or authorize an experiment.
type ConnectionPlan struct {
	Kubeconfig, Context, APIServer             string
	Endpoint, ServerName, CA, Certificate, Key string
	Files                                      map[string]string
}

func (p ConnectionPlan) read(path string, private bool) ([]byte, error) {
	wanted := p.Files[path]
	if !planinput.ValidSHA256(wanted) {
		return nil, errors.New("missing independently pinned connection input")
	}
	data, err := planinput.ReadFile(path, private, 1<<20)
	if err != nil {
		return nil, err
	}
	if planinput.SHA256(data) != wanted {
		return nil, fmt.Errorf("connection input differs from admission: %s", path)
	}
	return data, nil
}

func (p ConnectionPlan) tlsConfig() (*tls.Config, error) {
	ca, err := p.read(p.CA, false)
	if err != nil {
		return nil, err
	}
	cert, err := p.read(p.Certificate, false)
	if err != nil {
		return nil, err
	}
	key, err := p.read(p.Key, true)
	if err != nil {
		return nil, err
	}
	identity, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid pinned CA bundle")
	}
	// No GetClientCertificate callback: transport must never reload unverified
	// credential paths after admission, including on a later handshake.
	return &tls.Config{MinVersion: tls.VersionTLS12, ServerName: p.ServerName, RootCAs: roots, Certificates: []tls.Certificate{identity}}, nil
}

func NewFaultConnections(p ConnectionPlan) (dynamic.Interface, *grpc.ClientConn, error) {
	u, err := url.Parse(p.APIServer)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || p.Context == "" {
		return nil, nil, errors.New("invalid admitted Kubernetes connection")
	}
	host, port, err := net.SplitHostPort(p.Endpoint)
	n, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || host == "" || n < 1 || n > 65535 || p.ServerName == "" {
		return nil, nil, errors.New("require direct RPC endpoint and TLS identity")
	}
	data, err := p.read(p.Kubeconfig, true)
	if err != nil {
		return nil, nil, err
	}
	raw, err := clientcmd.Load(data)
	if err != nil {
		return nil, nil, err
	}
	selected := raw.Contexts[p.Context]
	if selected == nil {
		return nil, nil, errors.New("missing admitted Kubernetes context")
	}
	cluster, auth := raw.Clusters[selected.Cluster], raw.AuthInfos[selected.AuthInfo]
	// Reject external credential sources BEFORE ClientConfig can read a token
	// file or construct a plugin-backed configuration. Only embedded mTLS.
	if cluster == nil || auth == nil || cluster.Server != p.APIServer || cluster.InsecureSkipTLSVerify || cluster.ProxyURL != "" || cluster.CertificateAuthority != "" || len(cluster.CertificateAuthorityData) == 0 || auth.ClientCertificate != "" || auth.ClientKey != "" || len(auth.ClientCertificateData) == 0 || len(auth.ClientKeyData) == 0 || auth.Token != "" || auth.TokenFile != "" || auth.Username != "" || auth.Password != "" || auth.Exec != nil || auth.AuthProvider != nil || auth.Impersonate != "" || auth.ImpersonateUID != "" || len(auth.ImpersonateGroups) != 0 || len(auth.ImpersonateUserExtra) != 0 {
		return nil, nil, errors.New("require pinned embedded mTLS without plugins, proxy or impersonation")
	}
	cfg, err := clientcmd.NewNonInteractiveClientConfig(*raw, p.Context, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		return nil, nil, err
	}
	cfg.Timeout = 15 * time.Second
	cfg.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	cfg.QPS, cfg.Burst = 20, 40
	tlsConfig, err := p.tlsConfig()
	if err != nil {
		return nil, nil, err
	}
	client, err := NewDynamicClient(cfg)
	if err != nil {
		return nil, nil, err
	}
	conn, err := grpc.NewClient("passthrough:///"+p.Endpoint, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithDisableRetry(), grpc.WithNoProxy())
	if err != nil {
		return nil, nil, err
	}
	return client, conn, nil
}
