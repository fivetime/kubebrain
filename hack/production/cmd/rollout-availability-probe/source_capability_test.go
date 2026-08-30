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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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
	require.NoError(t, validateSourceCapabilityConfig(valid, false))
	require.NoError(t, validateSourceCapabilityConfig(config{}, false))

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
			require.Error(t, validateSourceCapabilityConfig(candidate, false))
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
