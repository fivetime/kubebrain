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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/pkg/server/capability"
)

const maxSourceCapabilityDocumentBytes = 16 << 10

func validateSourceCapabilityConfig(cfg config) error {
	if len(cfg.sourceInfoEndpoints) == 0 && len(cfg.requiredSourceCaps) == 0 {
		if cfg.sourceInfoCAFile != "" || cfg.sourceInfoServerName != "" {
			return fmt.Errorf("source info TLS options require source capability preflight")
		}
		return nil
	}
	if cfg.sourceInfoServerName != "" && cfg.sourceInfoCAFile == "" {
		return fmt.Errorf("source info TLS server name requires source-info-cacert")
	}
	if len(cfg.sourceInfoEndpoints) != len(cfg.directEndpoints) || len(cfg.requiredSourceCaps) == 0 {
		return fmt.Errorf("source capability preflight requires one info endpoint per direct endpoint and at least one capability")
	}
	required := capability.Document{Format: capability.DocumentFormat, Capabilities: cfg.requiredSourceCaps}
	if err := required.Validate(); err != nil {
		return fmt.Errorf("required source capabilities are not canonical: %w", err)
	}
	seen := make(map[string]struct{}, len(cfg.sourceInfoEndpoints))
	for _, endpoint := range cfg.sourceInfoEndpoints {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Opaque != "" || parsed.User != nil || parsed.Hostname() == "" || parsed.Port() == "" ||
			(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" ||
			parsed.ForceQuery || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("source info endpoint must be an HTTP(S) origin with an explicit port: %q", endpoint)
		}
		port, portErr := strconv.Atoi(parsed.Port())
		if portErr != nil || port < 1 || port > 65535 {
			return fmt.Errorf("source info endpoint must use a port between 1 and 65535: %q", endpoint)
		}
		if (parsed.Scheme == "https") != (cfg.sourceInfoCAFile != "") {
			return fmt.Errorf("source info endpoint scheme and source-info-cacert must match: %q", endpoint)
		}
		canonical := strings.TrimSuffix(endpoint, "/")
		if _, duplicate := seen[canonical]; duplicate {
			return fmt.Errorf("source info endpoints must be unique: %q", endpoint)
		}
		seen[canonical] = struct{}{}
	}
	return nil
}

// Info endpoints have their own server trust domain. Never copy public-client
// roots, its server-name override, or its privileged mTLS identity here.
func (cfg config) sourceInfoTLSConfig() (*tls.Config, error) {
	if err := validateSourceCapabilityConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.sourceInfoCAFile == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(cfg.sourceInfoCAFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("source-info-cacert contains no valid certificates")
	}
	return &tls.Config{RootCAs: roots, ServerName: cfg.sourceInfoServerName, MinVersion: tls.VersionTLS12}, nil
}

func verifySourceCapabilities(parent context.Context, endpoints, required []string, tlsConfig *tls.Config,
	dialTimeout, requestTimeout time.Duration,
) error {
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: dialTimeout}).DialContext,
		TLSClientConfig:       tlsConfig,
		ForceAttemptHTTP2:     true,
		DisableKeepAlives:     true,
		ResponseHeaderTimeout: requestTimeout,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for _, endpoint := range endpoints {
		requestCtx, cancel := context.WithTimeout(parent, requestTimeout)
		request, err := http.NewRequestWithContext(requestCtx, http.MethodGet,
			strings.TrimSuffix(endpoint, "/")+"/capabilities", nil)
		if err != nil {
			cancel()
			return fmt.Errorf("build request for %s: %w", endpoint, err)
		}
		request.Header.Set("Accept", "application/json")
		response, err := client.Do(request)
		if err != nil {
			cancel()
			return fmt.Errorf("read %s: %w", endpoint, err)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, maxSourceCapabilityDocumentBytes+1))
		closeErr := response.Body.Close()
		cancel()
		if readErr != nil {
			return fmt.Errorf("read %s response: %w", endpoint, readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close %s response: %w", endpoint, closeErr)
		}
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("%s returned HTTP status %d", endpoint, response.StatusCode)
		}
		mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if mediaErr != nil || mediaType != "application/json" {
			return fmt.Errorf("%s returned invalid content type %q", endpoint, response.Header.Get("Content-Type"))
		}
		if len(body) > maxSourceCapabilityDocumentBytes {
			return fmt.Errorf("%s capability document exceeds %d bytes", endpoint, maxSourceCapabilityDocumentBytes)
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		var document capability.Document
		if err := decoder.Decode(&document); err != nil {
			return fmt.Errorf("decode %s capability document: %w", endpoint, err)
		}
		if err := document.Validate(); err != nil {
			return fmt.Errorf("validate %s capability document: %w", endpoint, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return fmt.Errorf("decode %s capability document: trailing JSON value", endpoint)
		}
		for _, requiredCapability := range required {
			if !document.Has(requiredCapability) {
				return fmt.Errorf("%s does not advertise required capability %q", endpoint, requiredCapability)
			}
		}
	}
	return nil
}
