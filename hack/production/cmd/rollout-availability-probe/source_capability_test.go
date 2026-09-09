// Copyright 2026 ByteDance and/or its affiliates
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

package main

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/server/capability"
)

func TestValidateSourceCapabilityConfig(t *testing.T) {
	valid := config{
		directEndpoints:     []string{"http://kb-0:3379", "http://kb-1:3379", "http://kb-2:3379"},
		sourceInfoEndpoints: []string{"http://kb-0:8080", "http://kb-1:8080", "http://kb-2:8080"},
		requiredSourceCaps:  []string{capability.SnapshotDrainPinned},
	}
	require.NoError(t, validateSourceCapabilityConfig(valid))
	require.NoError(t, validateSourceCapabilityConfig(config{}))

	for name, mutate := range map[string]func(*config){
		"missing capability": func(cfg *config) { cfg.requiredSourceCaps = nil },
		"wrong count":        func(cfg *config) { cfg.sourceInfoEndpoints = cfg.sourceInfoEndpoints[:2] },
		"duplicate":          func(cfg *config) { cfg.sourceInfoEndpoints[1] = cfg.sourceInfoEndpoints[0] },
		"path":               func(cfg *config) { cfg.sourceInfoEndpoints[0] += "/status" },
		"query":              func(cfg *config) { cfg.sourceInfoEndpoints[0] += "?x=1" },
		"empty query":        func(cfg *config) { cfg.sourceInfoEndpoints[0] += "?" },
		"missing port":       func(cfg *config) { cfg.sourceInfoEndpoints[0] = "http://kb-0" },
		"zero port":          func(cfg *config) { cfg.sourceInfoEndpoints[0] = "http://kb-0:0" },
		"oversized port":     func(cfg *config) { cfg.sourceInfoEndpoints[0] = "http://kb-0:65536" },
		"TLS mismatch":       func(cfg *config) { cfg.sourceInfoEndpoints[0] = "https://kb-0:8080" },
		"unsorted required":  func(cfg *config) { cfg.requiredSourceCaps = []string{"z", "a"} },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			candidate.sourceInfoEndpoints = append([]string(nil), valid.sourceInfoEndpoints...)
			candidate.requiredSourceCaps = append([]string(nil), valid.requiredSourceCaps...)
			mutate(&candidate)
			require.Error(t, validateSourceCapabilityConfig(candidate))
		})
	}
}

func TestVerifySourceCapabilities(t *testing.T) {
	canonical := fmt.Sprintf(`{"format":%q,"capabilities":[%q]}`,
		capability.DocumentFormat, capability.SnapshotDrainPinned)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, "/capabilities", request.URL.Path)
		require.Equal(t, "application/json", request.Header.Get("Accept"))
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write([]byte(canonical))
	}))
	t.Cleanup(server.Close)

	require.NoError(t, verifySourceCapabilities(t.Context(), []string{server.URL},
		[]string{capability.SnapshotDrainPinned}, nil, time.Second, time.Second))
}

func TestSourceInfoTransportIsIndependentOfPublicClient(t *testing.T) {
	identity := newClientOnlySnapshotTLSFixture(t)
	for _, infoHTTPS := range []bool{false, true} {
		for _, publicHTTPS := range []bool{false, true} {
			t.Run(fmt.Sprintf("infoTLS=%t/publicTLS=%t", infoHTTPS, publicHTTPS), func(t *testing.T) {
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if infoHTTPS && (r.TLS == nil || len(r.TLS.PeerCertificates) != 0) {
						t.Error("info preflight must not disclose the public client certificate")
						w.WriteHeader(http.StatusForbidden)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w, `{"format":%q,"capabilities":[%q]}`, capability.DocumentFormat, capability.SnapshotDrainPinned)
				}))
				if infoHTTPS {
					// Actively ask for an identity to catch accidental mTLS reuse.
					server.TLS = &tls.Config{ClientAuth: tls.RequestClientCert, MinVersion: tls.VersionTLS12}
					server.StartTLS()
				} else {
					server.Start()
				}
				defer server.Close()
				cfg := config{
					directEndpoints: []string{"http://unused:3379"}, sourceInfoEndpoints: []string{server.URL},
					requiredSourceCaps: []string{capability.SnapshotDrainPinned},
					pdEndpoints:        []string{"http://127.0.0.1:1"}, dialTimeout: time.Second, commandTimeout: time.Second,
				}
				if publicHTTPS {
					cfg.directEndpoints[0] = "https://unused:3379"
					cfg.caFile, cfg.certFile, cfg.keyFile, cfg.tlsServerName = identity.caFile, identity.certFile, identity.keyFile, identity.serverName
				}
				if infoHTTPS {
					cfg.sourceInfoCAFile = filepath.Join(t.TempDir(), "info-ca.crt")
					require.NoError(t, os.WriteFile(cfg.sourceInfoCAFile,
						pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600))
				}
				require.NoError(t, validateSourceCapabilityConfig(cfg))
				infoTLS, err := cfg.sourceInfoTLSConfig()
				require.NoError(t, err)
				if infoHTTPS {
					require.False(t, infoTLS.InsecureSkipVerify)
					require.Empty(t, infoTLS.Certificates)
					require.Nil(t, infoTLS.GetClientCertificate)
					require.Empty(t, infoTLS.ServerName, "verify each endpoint hostname, not the public service name")
				}
				require.NoError(t, verifySourceCapabilities(t.Context(), cfg.sourceInfoEndpoints, cfg.requiredSourceCaps, infoTLS, time.Second, time.Second))
				// Exercise run's actual wiring: only the deliberately unavailable
				// backend may fail, after the HTTPS capability preflight succeeds.
				require.ErrorContains(t, run(t.Context(), cfg), "backend preflight: read PD leader")
			})
		}
	}
}

func TestSourceInfoTLSFailsClosed(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"format":%q,"capabilities":[%q]}`, capability.DocumentFormat, capability.SnapshotDrainPinned)
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	caFile := filepath.Join(t.TempDir(), "info-ca.crt")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600))
	valid := config{directEndpoints: []string{"http://unused:3379"}, sourceInfoEndpoints: []string{server.URL},
		requiredSourceCaps: []string{capability.SnapshotDrainPinned}, sourceInfoCAFile: caFile}
	unknown := newClientOnlySnapshotTLSFixture(t)
	for name, mutate := range map[string]func(*config){
		"missing CA":        func(c *config) { c.sourceInfoCAFile = "" },
		"missing CA file":   func(c *config) { c.sourceInfoCAFile += ".missing" },
		"not a certificate": func(c *config) { c.sourceInfoCAFile = unknown.keyFile },
		"unknown CA":        func(c *config) { c.sourceInfoCAFile = unknown.caFile },
		"wrong hostname":    func(c *config) { c.sourceInfoServerName = "wrong.example" },
		"HTTP downgrade":    func(c *config) { c.sourceInfoEndpoints = []string{strings.Replace(server.URL, "https:", "http:", 1)} },
		"orphan trust":      func(c *config) { c.sourceInfoEndpoints = nil; c.requiredSourceCaps = nil },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			mutate(&cfg)
			infoTLS, err := cfg.sourceInfoTLSConfig()
			if err == nil {
				err = verifySourceCapabilities(t.Context(), cfg.sourceInfoEndpoints, cfg.requiredSourceCaps, infoTLS, time.Second, time.Second)
			}
			require.Error(t, err)
		})
	}
	infoTLS, err := valid.sourceInfoTLSConfig()
	require.NoError(t, err)
	require.NotEmpty(t, server.Certificate().DNSNames)
	withName := valid
	withName.sourceInfoServerName = server.Certificate().DNSNames[0]
	namedTLS, err := withName.sourceInfoTLSConfig()
	require.NoError(t, err)
	require.NoError(t, verifySourceCapabilities(t.Context(), valid.sourceInfoEndpoints, valid.requiredSourceCaps, namedTLS, time.Second, time.Second))
	infoTLS.Time = func() time.Time { return server.Certificate().NotAfter.Add(time.Hour) }
	require.Error(t, verifySourceCapabilities(t.Context(), valid.sourceInfoEndpoints, valid.requiredSourceCaps, infoTLS, time.Second, time.Second))
}

func TestVerifySourceCapabilitiesFailsClosed(t *testing.T) {
	valid := fmt.Sprintf(`{"format":%q,"capabilities":[%q]}`,
		capability.DocumentFormat, capability.SnapshotDrainPinned)
	for name, handler := range map[string]http.HandlerFunc{
		"missing": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"format":%q,"capabilities":[]}`, capability.DocumentFormat)
		},
		"not found": func(w http.ResponseWriter, _ *http.Request) {
			http.NotFound(w, nil)
		},
		"wrong content type": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(valid))
		},
		"unknown field": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(strings.TrimSuffix(valid, "}") + `,"extra":true}`))
		},
		"trailing JSON": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(valid + `{}`))
		},
		"oversized": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(strings.Repeat(" ", maxSourceCapabilityDocumentBytes+1)))
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(handler)
			defer server.Close()
			require.Error(t, verifySourceCapabilities(context.Background(), []string{server.URL},
				[]string{capability.SnapshotDrainPinned}, nil, time.Second, time.Second))
		})
	}
}

func TestVerifySourceCapabilitiesDoesNotFollowRedirects(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"format":%q,"capabilities":[%q]}`,
			capability.DocumentFormat, capability.SnapshotDrainPinned)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.RedirectHandler(target.URL+"/capabilities", http.StatusTemporaryRedirect))
	defer redirect.Close()

	err := verifySourceCapabilities(t.Context(), []string{redirect.URL},
		[]string{capability.SnapshotDrainPinned}, nil, time.Second, time.Second)
	require.ErrorContains(t, err, "HTTP status 307")
}

func TestRunChecksSourceCapabilitiesBeforeBackendOrFixtureWork(t *testing.T) {
	source, err := os.ReadFile("main.go")
	require.NoError(t, err)
	text := string(source)
	capabilityCheck := strings.Index(text, "verifySourceCapabilities(ctx")
	backendCheck := strings.Index(text, "initialPDLeader, err := readPDLeader(ctx")
	fixtureWrite := strings.Index(text, "claimExternalFixtureOwnership(")
	require.Positive(t, capabilityCheck)
	require.Greater(t, backendCheck, capabilityCheck)
	require.Greater(t, fixtureWrite, capabilityCheck)
}
