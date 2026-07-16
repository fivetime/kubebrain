// Copyright 2022 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package endpoint

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/server"
)

type secureMode int

func (s secureMode) String() string {
	if s < 0 || int(s) >= len(secureModeStrings) {
		return "INVALID_SECURE_MODE"
	}
	return secureModeStrings[s]
}

const (
	modeOnlyInsecure secureMode = iota
	modeOnlySecure
	modeBothInsecureAndSecure
)

var secureModeStrings = []string{
	"ONLY_INSECURE",
	"ONLY_SECURE",
	"BOTH_INSECURE_AND_SECURE",
}

var tlsNextProtos = []string{"http/1.1", "h2"}

type Config struct {
	// Port is the listened port for client server
	Port int

	// PeerPort is the listened port for peer server
	PeerPort int

	// InfoPort is the listened port for info server
	InfoPort int

	// ClientSecurityConfig is the security config for client server
	ClientSecurityConfig *SecurityConfig

	// PeerSecurityConfig is the security config for peer server
	PeerSecurityConfig *SecurityConfig

	// InfoSecurityConfig is the security config for the info/metrics server. Empty
	// (the default) keeps the info port plaintext; setting cert/key enables TLS so
	// metrics/pprof are not served in cleartext (#32).
	InfoSecurityConfig *SecurityConfig

	// EnablePprof exposes the net/http/pprof debug handlers on the info port. It is
	// off by default because pprof is an unauthenticated CPU/heap DoS and
	// info-disclosure surface; it is never exposed on the client data port (#32).
	EnablePprof bool

	// EnableEtcdCompatibility is the flag if KubeWharf should try to be compatible with etcd3
	EnableEtcdCompatibility bool

	// Leader-election durations (0 = default 8/5/1s). Tunable failover speed vs
	// spurious-failover resistance; see server.Config / leader.Config.
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration

	// GRPCMaxConnectionAge bounds how long one HTTP/2 transport can retain a
	// pre-rotation TLS identity. Zero disables aging. Grace is the drain window
	// after GOAWAY before active streams are forcibly closed.
	GRPCMaxConnectionAge      time.Duration
	GRPCMaxConnectionAgeGrace time.Duration

	// ClusterMembers is the control-plane supplied KubeBrain service
	// membership returned by etcd MemberList.
	ClusterMembers []*etcdserverpb.Member
}

func (c *Config) getServerConfig() server.Config {
	var clientTLS *tls.Config
	clientCertAuth := false
	clientAllowInsecure := false
	if c.ClientSecurityConfig != nil {
		clientTLS = c.ClientSecurityConfig.getClientTLSConfig()
		clientCertAuth = c.ClientSecurityConfig.ClientAuth
		clientAllowInsecure = c.ClientSecurityConfig.AllowInsecure
	}
	return server.Config{
		EnableEtcdProxy: c.EnableEtcdCompatibility,
		ClientPort:      c.Port,
		// The proxy dials the leader's CLIENT endpoint (0492c84), so the dial-side
		// TLS must mirror the client server's security config — the peer config
		// would fail the handshake whenever the two listeners differ (e.g. TLS
		// client port + plaintext peer port), leaving the proxy permanently
		// not-ready: every follower historical read Unavailable, every count a
		// full-scan fallback (review #51).
		ClientTLS:           clientTLS,
		ClientCertAuth:      clientCertAuth,
		ClientAllowInsecure: clientAllowInsecure,
		LeaseDuration:       c.LeaseDuration,
		RenewDeadline:       c.RenewDeadline,
		RetryPeriod:         c.RetryPeriod,
		ClusterMembers:      c.ClusterMembers,
	}
}

// SecurityConfig is the configuration of server tls
type SecurityConfig struct {

	// CertFile is the file path of server's cert
	CertFile string

	// KeyFile is the file path of server's private key
	KeyFile string

	// CA is the file path of ca's cert
	CA string

	// ServerName is used by TLS clients to verify the server certificate.
	ServerName string

	// ClientAuth indicate if client certs should be verified
	ClientAuth bool

	// AllowInsecure indicates if server can be access in insecure mode without tls
	AllowInsecure bool

	serverTlsConfig *tls.Config
	clientTlsConfig *tls.Config
	once            sync.Once
	err             error
}

// Validate checks if config is valid
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("invalid config")
	}

	if c.Port == 0 {
		return fmt.Errorf("invalid port %d", c.Port)
	}

	if c.PeerPort == 0 || c.PeerPort == c.Port {
		return fmt.Errorf("invalid peer port %d", c.PeerPort)
	}

	if c.InfoPort != 0 && (c.InfoPort == c.Port || c.InfoPort == c.PeerPort) {
		return fmt.Errorf("invalid info port %d", c.InfoPort)
	}
	if c.GRPCMaxConnectionAge < 0 {
		return fmt.Errorf("grpc max connection age must not be negative: %s", c.GRPCMaxConnectionAge)
	}
	if c.GRPCMaxConnectionAgeGrace < 0 {
		return fmt.Errorf("grpc max connection age grace must not be negative: %s", c.GRPCMaxConnectionAgeGrace)
	}
	if c.GRPCMaxConnectionAge > 0 && c.GRPCMaxConnectionAgeGrace <= 0 {
		return fmt.Errorf("grpc max connection age grace must be positive when connection aging is enabled: %s", c.GRPCMaxConnectionAgeGrace)
	}

	klog.InfoS("validate client security config", c.ClientSecurityConfig.ToKvs()...)
	err := c.ClientSecurityConfig.validate()
	if err != nil {
		klog.ErrorS(err, "invalid client security config")
		return err
	}

	klog.InfoS("validate peer security config", c.PeerSecurityConfig.ToKvs()...)
	err = c.PeerSecurityConfig.validate()
	if err != nil {
		klog.ErrorS(err, "invalid peer security config")
		return err
	}

	if c.InfoSecurityConfig != nil {
		klog.InfoS("validate info security config", c.InfoSecurityConfig.ToKvs()...)
		if err = c.InfoSecurityConfig.validate(); err != nil {
			klog.ErrorS(err, "invalid info security config")
			return err
		}
	}
	return nil
}

// ToKvs make config to kvs for klog
func (sc *SecurityConfig) ToKvs() []interface{} {
	if sc == nil {
		return []interface{}{}
	}

	return []interface{}{
		"cert", sc.CertFile,
		"key", sc.KeyFile,
		"ca", sc.CA,
		"serverName", sc.ServerName,
		"clientAuth", strconv.FormatBool(sc.ClientAuth),
	}
}

func (sc *SecurityConfig) validate() error {
	if sc.isInsecure() {
		return nil
	}

	return sc.init()
}

func (sc *SecurityConfig) mode() secureMode {
	if sc.isInsecure() {
		return modeOnlyInsecure
	} else if sc.AllowInsecure {
		return modeBothInsecureAndSecure
	}
	return modeOnlySecure
}

func (sc *SecurityConfig) isInsecure() bool {
	if sc == nil {
		return true
	}

	return sc.CertFile == "" &&
		sc.KeyFile == "" &&
		sc.CA == "" &&
		sc.ClientAuth == false
}

func (sc *SecurityConfig) init() (err error) {
	if sc == nil {
		return nil
	}
	sc.once.Do(func() {
		// Validate the initial key pair before accepting traffic. Runtime TLS
		// configs deliberately keep Certificates empty so every handshake invokes
		// the reload callbacks, matching etcd's mounted-secret rotation behavior.
		_, err := tls.LoadX509KeyPair(sc.CertFile, sc.KeyFile)
		if err != nil {
			klog.ErrorS(err, "can not load key pair", "cert", sc.CertFile, "key", sc.KeyFile)
			sc.err = errors.Wrapf(err, "can not load key pair")
			return
		}

		loadCertificate := func() (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(sc.CertFile, sc.KeyFile)
			if err != nil {
				klog.ErrorS(err, "can not reload key pair", "cert", sc.CertFile, "key", sc.KeyFile)
				return nil, errors.Wrap(err, "can not reload key pair")
			}
			return &cert, nil
		}
		loadCertPool := func() (*x509.CertPool, error) {
			caFileBytes, err := os.ReadFile(sc.CA)
			if err != nil {
				return nil, errors.Wrap(err, "can not reload CA cert")
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caFileBytes) {
				return nil, fmt.Errorf("can not reload CA cert: no certificates found in %s", sc.CA)
			}
			return certPool, nil
		}

		sc.serverTlsConfig = &tls.Config{NextProtos: tlsNextProtos}
		sc.serverTlsConfig.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return loadCertificate()
		}

		sc.clientTlsConfig = &tls.Config{
			ServerName: sc.ServerName,
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return loadCertificate()
			},
		}

		// load ca file
		if sc.CA != "" {
			certPool, err := loadCertPool()
			if err != nil {
				klog.ErrorS(err, "can not load ca cert", "ca", sc.CA)
				sc.err = errors.Wrapf(err, "can not load ca cert")
				return
			}

			sc.serverTlsConfig.ClientAuth = tls.NoClientCert
			sc.serverTlsConfig.ClientCAs = certPool
			sc.serverTlsConfig.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
				pool, err := loadCertPool()
				if err != nil {
					klog.ErrorS(err, "can not reload client CA cert", "ca", sc.CA)
					return nil, err
				}
				config := sc.serverTlsConfig.Clone()
				config.GetConfigForClient = nil
				config.ClientCAs = pool
				return config, nil
			}

			// crypto/tls has no dynamic RootCAs callback. Skip its static verifier
			// and reproduce the same chain and hostname checks with roots loaded for
			// this handshake. VerifyConnection still runs after normal certificate
			// parsing and before the connection is made available to gRPC.
			sc.clientTlsConfig.InsecureSkipVerify = true
			sc.clientTlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
				pool, err := loadCertPool()
				if err != nil {
					klog.ErrorS(err, "can not reload server CA cert", "ca", sc.CA)
					return err
				}
				if len(state.PeerCertificates) == 0 {
					return fmt.Errorf("server did not provide a certificate")
				}
				intermediates := x509.NewCertPool()
				for _, cert := range state.PeerCertificates[1:] {
					intermediates.AddCert(cert)
				}
				_, err = state.PeerCertificates[0].Verify(x509.VerifyOptions{
					Roots:         pool,
					Intermediates: intermediates,
					DNSName:       state.ServerName,
				})
				return err
			}
		}

		if sc.ClientAuth && sc.CA == "" {
			// RequireAndVerifyClientCert with a nil ClientCAs makes Go verify client
			// certs against the SYSTEM root pool, accepting any cert signed by a
			// publicly-trusted CA -- defeating the purpose of client cert auth.
			// Client cert auth is meaningless without an explicit trusted CA, so
			// refuse to start (matching etcd's --client-cert-auth requirement) (#50).
			sc.err = fmt.Errorf("client cert auth is enabled but no trusted CA file is set; " +
				"refusing to verify client certs against the system root pool")
			return
		}
		if sc.CA != "" || sc.ClientAuth {
			sc.serverTlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
		return
	})
	return sc.err
}

func (sc *SecurityConfig) getServerTLSConfig() (ret *tls.Config) {
	_ = sc.init()
	return sc.serverTlsConfig
}

func (sc *SecurityConfig) getClientTLSConfig() (ret *tls.Config) {
	_ = sc.init()
	return sc.clientTlsConfig
}
