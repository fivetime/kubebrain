package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/imageprepull"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestVerifyReleaseCommand(t *testing.T) {
	amd64, arm64 := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	data := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":123,"platform":{"os":"linux","architecture":"amd64"}},{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":124,"platform":{"os":"linux","architecture":"arm64"}}]}`, amd64, arm64))
	file := filepath.Join(t.TempDir(), "index.json")
	require.NoError(t, os.WriteFile(file, data, 0600))
	image := fmt.Sprintf("ghcr.io/fivetime/kubebrain@sha256:%x", sha256.Sum256(data))
	args := []string{"--mode=verify-release", "--index-file=" + file, "--image=" + image, "--amd64-digest=" + amd64, "--arm64-digest=" + arm64}
	var output bytes.Buffer
	require.NoError(t, run(context.Background(), args, &output))
	var result struct {
		Image          string
		RuntimeDigests map[string][]string
		Scope          string
	}
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, image, result.Image)
	require.Equal(t, amd64, result.RuntimeDigests["linux/amd64"][0])
	require.Contains(t, result.Scope, "not CI authorization")
	require.NoError(t, os.WriteFile(file, append(data, '\n'), 0600))
	output.Reset()
	require.ErrorContains(t, run(context.Background(), args, &output), "bytes do not match")
	require.Empty(t, output.String())
}

func TestRecoveryCommandRequiresExplicitScopeAndVerifiedHTTPS(t *testing.T) {
	for _, mode := range []string{"success", "scope mismatch", "namespace replaced", "unknown context", "insecure kubeconfig permissions"} {
		t.Run(mode, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, "/api/v1/namespaces/kubebrain-test", r.URL.Path)
				uid := "namespace-uid"
				if mode == "namespace replaced" {
					uid = "different-namespace"
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"kubebrain-test","uid":%q}}`, uid)
			}))
			defer server.Close()
			directory := t.TempDir()
			require.NoError(t, os.Chmod(directory, 0700))
			scope := imageprepull.RecoveryScope{Namespace: "kubebrain-test", NamespaceUID: "namespace-uid", SourceName: "kubebrain", SourceUID: "source-uid"}
			journal, err := imageprepull.CreateRecoveryJournal(directory, "attempt", scope)
			require.NoError(t, err)
			require.NoError(t, journal.Close())
			config := clientcmdapi.Config{
				Clusters:       map[string]*clientcmdapi.Cluster{"cluster": {Server: server.URL, CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}},
				AuthInfos:      map[string]*clientcmdapi.AuthInfo{"identity": {}},
				Contexts:       map[string]*clientcmdapi.Context{"explicit": {Cluster: "cluster", AuthInfo: "identity"}},
				CurrentContext: "must-not-use-implicit-context",
			}
			data, err := clientcmd.Write(config)
			require.NoError(t, err)
			kubeconfig := filepath.Join(directory, "kubeconfig")
			require.NoError(t, os.WriteFile(kubeconfig, data, 0600))
			contextName, sourceUID := "explicit", "source-uid"
			if mode == "scope mismatch" {
				sourceUID = "other-source"
			}
			if mode == "unknown context" {
				contextName = "missing"
			}
			if mode == "insecure kubeconfig permissions" {
				require.NoError(t, os.Chmod(kubeconfig, 0644))
			}
			args := []string{"--mode=recover-cleanup", "--receipt-directory=" + directory, "--receipt-name=attempt",
				"--namespace=kubebrain-test", "--namespace-uid=namespace-uid", "--statefulset=kubebrain", "--statefulset-uid=" + sourceUID,
				"--kubeconfig=" + kubeconfig, "--context=" + contextName}
			var output bytes.Buffer
			err = run(context.Background(), args, &output)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, "PREPULL_CLEANUP_CONFIRMED\n", output.String())
				require.EqualValues(t, 1, requests.Load())
			} else {
				require.Error(t, err)
				require.Empty(t, output.String())
				if mode != "namespace replaced" {
					require.Zero(t, requests.Load())
				}
			}
			// Recovery command must release the journal lock on every exit path.
			reopened, err := imageprepull.OpenRecoveryJournal(directory, "attempt")
			require.NoError(t, err)
			require.NoError(t, reopened.Close())
		})
	}
}

func TestCommandInputsFailWithoutMutation(t *testing.T) {
	var output bytes.Buffer
	for _, args := range [][]string{nil, {"--mode=prepare"}, {"--mode=recover-cleanup"}, {"--mode=verify-release", "unexpected"}, {"--unknown"}} {
		require.Error(t, run(context.Background(), args, &output))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, run(ctx, []string{"--mode=verify-release"}, &output), context.Canceled)
	require.Empty(t, output.String())
	dir := t.TempDir()
	path := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(path, []byte("1234"), 0600))
	_, err := readRegularFile(path, 3, false)
	require.Error(t, err)
	require.NoError(t, os.Symlink("file", filepath.Join(dir, "symlink")))
	_, err = readRegularFile(filepath.Join(dir, "symlink"), 4, false)
	require.Error(t, err)
	require.NoError(t, unix.Mkfifo(filepath.Join(dir, "fifo"), 0600))
	_, err = readRegularFile(filepath.Join(dir, "fifo"), 4, false)
	require.Error(t, err)
	_, err = readRegularFile("relative", 4, false)
	require.Error(t, err)
}
