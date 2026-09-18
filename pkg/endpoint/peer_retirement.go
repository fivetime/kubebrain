package endpoint

import (
	"context"
	"crypto/tls"
	"errors"
	"sort"
	"time"

	"github.com/kubewharf/kubebrain/pkg/server"
)

// PeerRetirementOptions deliberately contains no TLS callbacks or credential
// objects. The experimental transports use the peer listener's file policy.
// No CLI enables this mode yet; nil remains the ordinary endpoint behavior.
type PeerRetirementOptions struct {
	Scope                                   string
	HolderPins                              map[string][]string
	EndpointHolders                         map[string]string
	ReadBudget, OperationBudget, SendBudget time.Duration
	Concurrency                             int
	RequestsPerSecond                       float64
}

func (c *Config) preparePeerRetirement(ctx context.Context) (server.PeerRetirementConfig, *tls.Config, error) {
	invalid := errors.New("invalid experimental peer retirement endpoint configuration")
	if c == nil || c.ExperimentalPeerRetirement == nil || ctx == nil || ctx.Err() != nil {
		return server.PeerRetirementConfig{}, nil, invalid
	}
	options := c.ExperimentalPeerRetirement
	if len(options.EndpointHolders) == 0 || len(options.EndpointHolders) > 16 {
		return server.PeerRetirementConfig{}, nil, invalid
	}
	source, err := newPeerCredentialSource(c.PeerSecurityConfig)
	if err != nil {
		return server.PeerRetirementConfig{}, nil, invalid
	}
	loadCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	material, err := source.LoadClientCredentialMaterial(loadCtx)
	if err != nil || loadCtx.Err() != nil {
		return server.PeerRetirementConfig{}, nil, invalid
	}
	// This snapshot is for existing startup identity validation only. Both
	// outgoing runtime transports use the source, including its ServerName and
	// CRL policy, rather than inheriting/removing normal endpoint TLS callbacks.
	bootstrap := &tls.Config{Certificates: []tls.Certificate{material.Certificate}, RootCAs: material.Roots,
		MinVersion: material.MinVersion, MaxVersion: material.MaxVersion, CipherSuites: append([]uint16(nil), material.CipherSuites...)}
	config := server.PeerRetirementConfig{Scope: options.Scope, TLS: bootstrap,
		ControlCredentialSource: source, ProxyCredentialSource: source,
		ReadBudget: options.ReadBudget, OperationBudget: options.OperationBudget, SendBudget: options.SendBudget,
		Concurrency: options.Concurrency, RequestsPerSecond: options.RequestsPerSecond,
		HolderPins: make(map[string][]string, len(options.HolderPins)), SuccessorHolders: make(map[string]string, len(options.EndpointHolders))}
	for holder, pins := range options.HolderPins {
		config.HolderPins[holder] = append([]string(nil), pins...)
	}
	for endpoint, holder := range options.EndpointHolders {
		config.PeerURLs = append(config.PeerURLs, endpoint)
		config.SuccessorHolders[endpoint] = holder
	}
	sort.Strings(config.PeerURLs)
	return config, bootstrap.Clone(), nil
}

func (e *Endpoint) newDataServer(ctx context.Context) (server.Server, error) {
	if e.config == nil {
		return nil, errors.New("missing endpoint configuration")
	}
	config := e.config.getServerConfig()
	if e.config.ExperimentalPeerRetirement == nil {
		return server.NewServer(ctx, e.backend, e.metrics, config), nil
	}
	retirement, bootstrap, err := e.config.preparePeerRetirement(ctx)
	if err != nil {
		return nil, err
	}
	config.ProxyTLS, config.ProxyAllowInsecure = bootstrap, false
	return server.NewServerWithPeerRetirement(ctx, e.backend, e.metrics, config, retirement)
}
