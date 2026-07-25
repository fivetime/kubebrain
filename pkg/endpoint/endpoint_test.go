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
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	"github.com/kubewharf/kubebrain/pkg/backend"
	mockmetrics "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/server"
	"github.com/kubewharf/kubebrain/pkg/storage"
	ibadger "github.com/kubewharf/kubebrain/pkg/storage/badger"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func newBadgerStorage(t *testing.T, ast *assert.Assertions) storage.KvStorage {
	p := path.Join(t.TempDir(), "badger")
	st, err := ibadger.NewKvStorage(ibadger.Config{Dir: p})
	if err != nil {
		ast.FailNow(err.Error())
	}
	return st
}

func getAuthPath(filename string) string {
	return filepath.Join("../util/auth/testdata", filename)
}

func reserveEndpointTestPorts(t *testing.T) (int, int) {
	t.Helper()
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer clientListener.Close()
	peerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer peerListener.Close()
	return clientListener.Addr().(*net.TCPAddr).Port, peerListener.Addr().(*net.TCPAddr).Port
}

func waitForEndpointHealth(t *testing.T, client *http.Client, url string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			last = err.Error()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			last = readErr.Error()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if string(body) == server.HealthResponse {
			return
		}
		last = string(body)
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("endpoint %s did not become healthy: %s", url, last)
}

func waitForEndpointHealthProto(t *testing.T, client *http.Client, url string, protoMajor int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			last = err.Error()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			last = readErr.Error()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if string(body) == server.HealthResponse && resp.ProtoMajor == protoMajor {
			return
		}
		last = fmt.Sprintf("proto=%s body=%q", resp.Proto, body)
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("endpoint %s did not become healthy over HTTP/%d: %s", url, protoMajor, last)
}

func TestRunEndpoint(t *testing.T) {
	if raceDetectorEnabled {
		// This trips a known data race INSIDE vendored github.com/soheilhy/cmux
		// v0.1.5 (its cMux.serve/Match on the connection matcher), not in
		// KubeBrain code. It passes without -race. Skip under -race to keep the
		// race build signal clean.
		t.Skip("skipping under -race: vendored cmux data race, not KubeBrain code")
	}
	ast := assert.New(t)
	clientPort, peerPort := reserveEndpointTestPorts(t)
	conf := Config{
		Port:     clientPort,
		PeerPort: peerPort,
		ClientSecurityConfig: &SecurityConfig{
			CertFile:      getAuthPath("server.crt"),
			KeyFile:       getAuthPath("server.key"),
			CA:            getAuthPath("ca.crt"),
			ClientAuth:    true,
			AllowInsecure: true,
		},
		PeerSecurityConfig: &SecurityConfig{
			CertFile:      getAuthPath("server.crt"),
			KeyFile:       getAuthPath("server.key"),
			CA:            getAuthPath("ca.crt"),
			ClientAuth:    true,
			AllowInsecure: true,
		},
	}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockMetrics := mockmetrics.NewMinimalMetrics(mockCtrl)
	backendConf := backend.Config{
		Prefix:   "/test",
		Identity: net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", peerPort)),
	}
	testBackend := backend.NewBackend(newBadgerStorage(t, ast), backendConf, mockMetrics)
	ep := NewEndpoint(testBackend, mockMetrics, &conf)

	ctx, cancel := context.WithCancel(context.Background())
	eg, _ := errgroup.WithContext(context.Background())
	eg.Go(func() error {
		return ep.Run(ctx)
	})
	defer func() {
		cancel()
		ast.NoError(eg.Wait())
	}()

	client := &http.Client{
		Timeout: 1 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: conf.PeerSecurityConfig.getClientTLSConfig(),
		},
	}
	urls := []string{
		fmt.Sprintf("http://127.0.0.1:%d/health", clientPort),
		fmt.Sprintf("https://127.0.0.1:%d/health", clientPort),
	}
	for _, url := range urls {
		t.Logf("testing url %s", url)
		waitForEndpointHealth(t, client, url)
	}

	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	h2Client := &http.Client{
		Timeout: 1 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: conf.PeerSecurityConfig.getClientTLSConfig(),
			Protocols:       protocols,
		},
	}
	waitForEndpointHealthProto(t, h2Client, fmt.Sprintf("https://127.0.0.1:%d/health", clientPort), 2)
}

func TestRunEndpointBindFailureStopsBackgroundWorkBeforeBackendClose(t *testing.T) {
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer clientListener.Close()
	peerListener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer peerListener.Close()

	ctrl := gomock.NewController(t)
	m := mockmetrics.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity: "bind-failure", EnableEtcdCompatibility: true,
	}, m)
	conf := &Config{
		Port:                 clientListener.Addr().(*net.TCPAddr).Port,
		PeerPort:             peerListener.Addr().(*net.TCPAddr).Port,
		ClientSecurityConfig: &SecurityConfig{},
		PeerSecurityConfig:   &SecurityConfig{},
	}

	runErr := NewEndpoint(b, m, conf).Run(context.Background())
	assert.Error(t, runErr)
	assert.Contains(t, runErr.Error(), "address already in use")
	ctrl.Finish()
}
