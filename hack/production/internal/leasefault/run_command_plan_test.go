package leasefault

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/clientcmd"
)

// Exercise the public serialized command entry point through its real dynamic
// client and canonical jq predicate. Successful fixture hooks are NOT production
// admission; they let the test isolate the mandatory built-in process checks.
func TestRunNativeCommandLiveProcessRefusal(t *testing.T) {
	for _, mode := range []string{"original-restarted", "observer-replaced", "api-forbidden"} {
		t.Run(mode, func(t *testing.T) {
			p := serializedCommandFixture(t)
			require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
			// The serialized fixture's snapshots intentionally omit fields unused
			// by static checks. Real process admission requires restart counters.
			complete := func(raw []byte) json.RawMessage {
				return json.RawMessage(strings.Replace(string(raw), `"containerID":`, `"restartCount":0,"containerID":`, 1))
			}
			p.Bindings.Network.PodBefore = complete(p.Bindings.Network.PodBefore)
			p.Processes.Metrics[0] = p.Bindings.Network.PodBefore
			p.Processes.Observer = complete(p.Processes.Observer)
			p.Processes.JQ = "/usr/bin/jq"
			var err error
			p.Processes.Predicate, err = filepath.Abs("../../same-pod-process.jq")
			require.NoError(t, err)
			for _, path := range []string{p.Processes.JQ, p.Processes.Predicate} {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				p.Files[path] = planinput.SHA256(data)
			}
			var mu sync.Mutex
			var requests []string
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests = append(requests, r.Method+" "+r.URL.Path)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				if r.Method != http.MethodGet || (r.URL.Path != "/api/v1/namespaces/test-ns/pods/brain-0" && r.URL.Path != "/api/v1/namespaces/test-ns/pods/brain-1") {
					http.Error(w, "unexpected API operation", http.StatusInternalServerError)
					return
				}
				if mode == "api-forbidden" {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Forbidden","code":403}`))
					return
				}
				raw := string(p.Bindings.Network.PodBefore)
				if strings.HasSuffix(r.URL.Path, "/brain-1") {
					raw = strings.Replace(string(p.Processes.Observer), `"uid":"observer-uid"`, `"uid":"replacement-uid"`, 1)
				} else if mode == "original-restarted" {
					raw = strings.Replace(raw, `"restartCount":0`, `"restartCount":1`, 1)
				}
				_, _ = w.Write([]byte(raw))
			}))
			ca, err := os.ReadFile(p.CA)
			require.NoError(t, err)
			pool := x509.NewCertPool()
			require.True(t, pool.AppendCertsFromPEM(ca))
			server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
			server.StartTLS()
			defer server.Close()
			config, err := clientcmd.LoadFromFile(p.Kubeconfig)
			require.NoError(t, err)
			p.APIServer = server.URL
			config.Clusters["cluster"].Server = server.URL
			config.Clusters["cluster"].CertificateAuthorityData = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			configData, err := clientcmd.Write(*config)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(p.Kubeconfig, configData, 0600))
			p.Files[p.Kubeconfig] = planinput.SHA256(configData)
			data, err := json.Marshal(p)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "plan.json")
			require.NoError(t, os.WriteFile(path, data, 0600))
			check := func(context.Context) error { return nil }
			a := CommandAdmission{Tools: check, Own: check, Original: check, Network: check, Successor: check, Metrics: check, Outcome: check, Stack: func(context.Context, string) error { return nil }}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result, err := RunNativeCommand(ctx, path, planinput.SHA256(data), a)
			require.Error(t, err)
			require.Nil(t, result.Owner)
			require.False(t, result.Lifecycle.RecoveryAttempted)
			if mode == "api-forbidden" {
				require.True(t, apierrors.IsForbidden(err), "preserve Kubernetes status reason")
			} else {
				require.ErrorContains(t, err, "exit status 1")
			}
			want := []string{"GET /api/v1/namespaces/test-ns/pods/brain-0"}
			if mode == "observer-replaced" {
				want = append(want, "GET /api/v1/namespaces/test-ns/pods/brain-1")
			}
			mu.Lock()
			got := append([]string(nil), requests...)
			mu.Unlock()
			require.Equal(t, want, got, "no claim, fault mutation or retry")
			proof, globErr := filepath.Glob(filepath.Join(p.OwnerDirectory, "experiment-process-*.json"))
			require.NoError(t, globErr)
			require.Len(t, proof, len(want))
			raw, readErr := os.ReadFile(filepath.Join(p.OwnerDirectory, commandReturnFile))
			require.NoError(t, readErr)
			var record map[string]any
			require.NoError(t, json.Unmarshal(raw, &record))
			require.Equal(t, err.Error(), record["error"])
			for _, name := range []string{ownerIntentFile, ownerReceiptFile, "deployment-claimed", faultOriginFile} {
				require.NoFileExists(t, filepath.Join(p.OwnerDirectory, name))
			}
		})
	}
}

func TestRunNativeCommandRefusesBeforeClusterRequests(t *testing.T) {
	for _, mode := range []string{"missing-gate", "pre-admit", "preclaim-admit", "changed-plan", "changed-plan-after-setup", "changed-tool", "changed-tool-after-setup", "public-key", "non-executable", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			p := serializedCommandFixture(t)
			require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
			data, err := json.Marshal(p)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "plan.json")
			require.NoError(t, os.WriteFile(path, data, 0600))
			calls := 0
			refused := errors.New("independent admission refused")
			check := func(context.Context) error { return nil }
			a := CommandAdmission{Own: check, Original: check, Network: check, Successor: check, Metrics: check, Outcome: check, Stack: func(context.Context, string) error { return nil }}
			a.Tools = func(context.Context) error {
				calls++
				if mode == "changed-tool" || (mode == "changed-tool-after-setup" && calls == 2) {
					require.NoError(t, os.WriteFile(p.ProbeExecutable, []byte("changed executable"), 0700))
					return nil
				}
				if mode == "public-key" {
					require.NoError(t, os.Chmod(p.Key, 0644))
					return nil
				}
				if mode == "non-executable" {
					require.NoError(t, os.Chmod(p.ProbeExecutable, 0600))
					return nil
				}
				if mode == "changed-plan" || (mode == "changed-plan-after-setup" && calls == 2) {
					require.NoError(t, os.WriteFile(path, append(data, '\n'), 0600))
					return nil
				}
				if mode == "pre-admit" || calls == 2 {
					return refused
				}
				return nil
			}
			if mode == "missing-gate" {
				a.Outcome = nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			result, err := RunNativeCommand(ctx, path, planinput.SHA256(data), a)
			require.Error(t, err)
			require.Nil(t, result.Owner)
			require.False(t, result.Lifecycle.RecoveryAttempted)
			if mode == "pre-admit" || mode == "preclaim-admit" {
				require.ErrorIs(t, err, refused)
			}
			if mode == "changed-plan" || mode == "changed-plan-after-setup" {
				require.Contains(t, err.Error(), "command plan changed")
			}
			if mode == "changed-tool" || mode == "changed-tool-after-setup" {
				require.ErrorContains(t, err, "command input differs from admission")
			}
			if mode == "preclaim-admit" || mode == "changed-plan-after-setup" || mode == "changed-tool-after-setup" {
				require.Equal(t, 2, calls, "real RunVerified reaches preclaim tools gate")
				require.FileExists(t, filepath.Join(p.OwnerDirectory, "probe.stderr"))
				require.DirExists(t, p.BeforeDirectory)
				raw, readErr := os.ReadFile(filepath.Join(p.OwnerDirectory, commandReturnFile))
				require.NoError(t, readErr)
				var record map[string]any
				require.NoError(t, json.Unmarshal(raw, &record))
				require.Equal(t, err.Error(), record["error"])
				require.Equal(t, planinput.SHA256(data), record["plan_sha256"])
			} else {
				require.NoFileExists(t, filepath.Join(p.OwnerDirectory, commandAttemptFile))
				require.NoFileExists(t, filepath.Join(p.OwnerDirectory, "probe.stderr"))
				require.NoDirExists(t, p.BeforeDirectory)
			}
			for _, name := range []string{ownerIntentFile, ownerReceiptFile, "deployment-claimed", faultOriginFile} {
				_, statErr := os.Lstat(filepath.Join(p.OwnerDirectory, name))
				require.ErrorIs(t, statErr, os.ErrNotExist)
			}
		})
	}
}
