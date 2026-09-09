package imageprepull

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const emptyNodeList = `{"apiVersion":"v1","kind":"NodeList","metadata":{"resourceVersion":"1"},"items":[]}`

func httpsClientFixture(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *rest.Config) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	config := &rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{
		CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
	}}
	return server, config
}

func TestBoundedClientAcceptsExactLimitAndPreservesConfig(t *testing.T) {
	_, config := httpsClientFixture(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/nodes", r.URL.Path)
		require.Equal(t, "Bearer fixture-token", r.Header.Get("Authorization"))
		require.Empty(t, r.Header.Get("Accept-Encoding"))
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, emptyNodeList)
	})
	config.BearerToken = "fixture-token"
	client, err := NewBoundedClient(config, time.Second, int64(len(emptyNodeList)))
	require.NoError(t, err)
	list, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, list.Items)
	require.Equal(t, "1", list.ResourceVersion)
	require.Zero(t, config.Timeout)
	require.False(t, config.DisableCompression)
	require.Nil(t, config.Transport)
}

func TestBoundedClientRejectsOversizedAndEncodedResponses(t *testing.T) {
	for _, mode := range []string{"known length", "chunked valid prefix", "encoded"} {
		t.Run(mode, func(t *testing.T) {
			_, config := httpsClientFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if mode == "encoded" {
					w.Header().Set("Content-Encoding", "gzip")
				}
				if mode == "chunked valid prefix" {
					fmt.Fprint(w, emptyNodeList)
					w.(http.Flusher).Flush()
					fmt.Fprint(w, strings.Repeat(" ", 512))
				} else {
					fmt.Fprint(w, strings.Repeat("x", 512))
				}
			})
			client, err := NewBoundedClient(config, time.Second, 256)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if mode == "encoded" {
				require.ErrorContains(t, err, "content encoding")
			} else {
				require.ErrorIs(t, err, ErrResponseTooLarge)
			}
		})
	}
}

func TestBoundedClientTimeoutCoversHeadersAndBody(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			aborted := make(chan struct{})
			_, config := httpsClientFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if phase == "body" {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"apiVersion":`)
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
				close(aborted)
			})
			client, err := NewBoundedClient(config, 150*time.Millisecond, 1024)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			started := time.Now()
			_, err = client.CoreV1().RESTClient().Get().Resource("nodes").MaxRetries(0).DoRaw(ctx)
			require.Error(t, err)
			require.True(t, errors.Is(err, context.DeadlineExceeded), err)
			require.Less(t, time.Since(started), 1500*time.Millisecond)
			select {
			case <-aborted:
			case <-ctx.Done():
				t.Fatal("transport did not cancel the server-side request")
			}
		})
	}
}

func TestBoundedClientRefusesRedirectBeforeForwardingCredentials(t *testing.T) {
	var forwarded atomic.Int32
	target, _ := httpsClientFixture(t, func(w http.ResponseWriter, r *http.Request) { forwarded.Add(1); fmt.Fprint(w, emptyNodeList) })
	_, config := httpsClientFixture(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/api/v1/nodes", http.StatusTemporaryRedirect)
	})
	config.BearerToken = "fixture-token"
	client, err := NewBoundedClient(config, time.Second, 1024)
	require.NoError(t, err)
	_, err = client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	require.ErrorContains(t, err, "redirects are forbidden")
	require.Zero(t, forwarded.Load())
}

func TestBoundedClientRejectsUnverifiedTLSAndUnboundedExtensions(t *testing.T) {
	for name, mutate := range map[string]func(*rest.Config){
		"plaintext":        func(c *rest.Config) { c.Host = "http://127.0.0.1" },
		"skip verify":      func(c *rest.Config) { c.Insecure = true },
		"URL password":     func(c *rest.Config) { c.Host = "https://user:password@example.invalid" },
		"URL query":        func(c *rest.Config) { c.Host = "https://example.invalid?token=value" },
		"URL fragment":     func(c *rest.Config) { c.Host = "https://example.invalid#fragment" },
		"exec plugin":      func(c *rest.Config) { c.ExecProvider = &clientcmdapi.ExecConfig{} },
		"auth plugin":      func(c *rest.Config) { c.AuthProvider = &clientcmdapi.AuthProviderConfig{} },
		"custom transport": func(c *rest.Config) { c.Transport = http.DefaultTransport },
	} {
		t.Run(name, func(t *testing.T) {
			config := &rest.Config{Host: "https://example.invalid"}
			mutate(config)
			_, err := NewBoundedClient(config, time.Second, 1024)
			require.Error(t, err)
		})
	}
	for _, limit := range []int64{0, -1, 32<<20 + 1} {
		_, err := NewBoundedClient(&rest.Config{Host: "https://example.invalid"}, time.Second, limit)
		require.Error(t, err)
	}
	for _, timeout := range []time.Duration{0, -1, 31 * time.Second} {
		_, err := NewBoundedClient(&rest.Config{Host: "https://example.invalid"}, timeout, 1024)
		require.Error(t, err)
	}
	_, err := NewBoundedClient(nil, time.Second, 1024)
	require.Error(t, err)
	_, config := httpsClientFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, emptyNodeList) })
	config.CAData = nil
	client, err := NewBoundedClient(config, time.Second, 1024)
	require.NoError(t, err)
	_, err = client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	require.ErrorContains(t, err, "certificate")
}
