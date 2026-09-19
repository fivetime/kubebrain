package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

func TestProtectedMetricsCapture(t *testing.T) {
	for _, scenario := range []string{"success", "tls12", "unprotected", "wrong-pin", "wrong-name", "redirect", "empty", "truncated", "oversize", "existing-file", "same-tunnel", "html", "comments-only", "type-only", "bad-value", "bad-label", "conflicting-type"} {
		t.Run(scenario, func(t *testing.T) {
			auth, version := tls.RequireAndVerifyClientCert, uint16(tls.VersionTLS13)
			if scenario == "unprotected" {
				auth = tls.NoClientCert
			}
			if scenario == "tls12" {
				version = tls.VersionTLS12
			}
			body := "# TYPE leader_retirement_peer_result counter\nleader_retirement_peer_result{outcome=\"confirmed\"} 1\n"
			c := fixture(t, auth, version, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/ping":
					_, _ = io.WriteString(w, "ok")
				case "/metrics":
					switch scenario {
					case "redirect":
						http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
					case "empty":
						w.WriteHeader(http.StatusOK)
					case "truncated":
						_, _ = io.WriteString(w, strings.TrimSuffix(body, "\n"))
					case "oversize":
						_, _ = io.WriteString(w, strings.Repeat("x", stackLimit+1)+"\n")
					case "html":
						_, _ = io.WriteString(w, "<html>not metrics</html>\n")
					case "comments-only":
						_, _ = io.WriteString(w, "# no samples\n")
					case "type-only":
						_, _ = io.WriteString(w, "# TYPE x counter\n")
					case "bad-value":
						_, _ = io.WriteString(w, "private_name private_value\n")
					case "bad-label":
						_, _ = io.WriteString(w, "x{label=unquoted} 1\n")
					case "conflicting-type":
						_, _ = io.WriteString(w, "# TYPE x counter\n# TYPE x gauge\nx 1\n")
					default:
						_, _ = io.WriteString(w, body)
					}
				default:
					t.Errorf("unexpected readiness/profile/redirect request: %s", r.URL.Path)
					w.WriteHeader(503)
				}
			}))
			c.mode, c.stackOutput = "protected-metrics", ""
			c.metricsOutput = filepath.Join(t.TempDir(), "metrics.txt")
			if scenario == "wrong-pin" {
				c.serverPin = strings.Repeat("0", 64)
			}
			if scenario == "wrong-name" {
				c.serverName = "not-the-server.invalid"
			}
			if scenario == "same-tunnel" {
				c.anonymousEndpoint = c.endpoint
			}
			if scenario == "existing-file" {
				require.NoError(t, os.WriteFile(c.metricsOutput, []byte("preserved"), 0600))
			}
			var output bytes.Buffer
			err := run(c, &output)
			if scenario != "success" && scenario != "tls12" {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "private_value")
				require.Empty(t, output.String())
				if scenario == "existing-file" {
					data, err := os.ReadFile(c.metricsOutput)
					require.NoError(t, err)
					require.Equal(t, "preserved", string(data))
				} else {
					_, err := os.Stat(c.metricsOutput)
					require.True(t, os.IsNotExist(err))
				}
				return
			}
			require.NoError(t, err)
			data, err := os.ReadFile(c.metricsOutput)
			require.NoError(t, err)
			require.Equal(t, body, string(data))
			info, err := os.Stat(c.metricsOutput)
			require.NoError(t, err)
			require.EqualValues(t, 0600, info.Mode().Perm())
			var summary map[string]any
			require.NoError(t, json.Unmarshal(output.Bytes(), &summary))
			for _, key := range []string{"readiness_checked", "pod_identity_proven", "fault_acceptance_proven", "metric_semantics_proven"} {
				require.Equal(t, false, summary[key])
			}
			require.NotContains(t, summary, "stack_bytes")
			require.Equal(t, true, summary["metrics_text_syntax_validated"])
			require.EqualValues(t, len(body), summary["metrics_bytes"])
			// Exercise the actual probe summary contract, not just a handcrafted
			// JSON fixture. Process identity here is synthetic, not Kubernetes proof.
			process := retirementmetrics.Process{PodUID: "fixture-pod", ContainerID: "fixture-container", StartedAt: "fixture-start"}
			before, err := retirementmetrics.NewSample(data, output.Bytes(), process)
			require.NoError(t, err)
			c.metricsOutput = filepath.Join(t.TempDir(), "metrics.txt")
			output.Reset()
			require.NoError(t, run(c, &output))
			data, err = os.ReadFile(c.metricsOutput)
			require.NoError(t, err)
			after, err := retirementmetrics.NewSample(data, output.Bytes(), process)
			require.NoError(t, err)
			delta, err := retirementmetrics.SampleDelta(before, after, retirementmetrics.Key{Stage: "peer", Outcome: "confirmed"})
			require.NoError(t, err)
			require.Zero(t, delta)
		})
	}
}

func TestMetricsOutputCannotChangeOtherModes(t *testing.T) {
	for _, mode := range []string{"protected", "protected-stack", "disabled", "protected-metrics"} {
		c := fixture(t, tls.RequireAndVerifyClientCert, tls.VersionTLS13, http.NotFoundHandler())
		c.mode = mode
		c.metricsOutput = "unexpected"
		if mode == "disabled" {
			c.stackOutput, c.anonymousEndpoint = "", ""
		}
		// protected-metrics must reject a simultaneous stack output too.
		require.Error(t, c.validate())
	}
}

func TestProtectedMetricsFailureDoesNotProduceSuccess(t *testing.T) {
	for _, scenario := range []string{"cancel-in-body", "deadline-in-body", "trace-failure", "missing-directory", "summary-failure"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			bodyStarted := make(chan struct{}, 1)
			c := fixture(t, tls.RequireAndVerifyClientCert, tls.VersionTLS13, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/ping" {
					_, _ = io.WriteString(w, "ok")
					return
				}
				if r.URL.Path != "/metrics" {
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(404)
					return
				}
				if scenario == "cancel-in-body" || scenario == "deadline-in-body" {
					bodyStarted <- struct{}{}
					_, _ = io.WriteString(w, "x ")
					w.(http.Flusher).Flush()
					if scenario == "cancel-in-body" {
						cancel()
					}
					<-r.Context().Done()
					return
				}
				_, _ = io.WriteString(w, "x 1\n")
			}))
			c.mode, c.stackOutput = "protected-metrics", ""
			c.metricsOutput = filepath.Join(t.TempDir(), "metrics.txt")
			var summary bytes.Buffer
			var err error
			switch scenario {
			case "cancel-in-body", "deadline-in-body":
				if scenario == "deadline-in-body" {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, time.Second)
					defer stop()
				}
				var body []byte
				body, err = probeWithDiagnostics(ctx, c, io.Discard)
				require.Empty(t, body)
				require.NotEmpty(t, bodyStarted, "must reach metrics response, not fail during TLS setup")
				if scenario == "cancel-in-body" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
			case "trace-failure":
				err = runWithDiagnostics(c, &summary, &timingFailureWriter{})
			case "missing-directory":
				c.metricsOutput = filepath.Join(t.TempDir(), "absent", "metrics.txt")
				err = run(c, &summary)
			case "summary-failure":
				err = run(c, &timingFailureWriter{})
			}
			require.Error(t, err)
			require.Empty(t, summary.String())
			if scenario == "summary-failure" {
				// Raw bytes may remain after final summary output fails. The
				// caller must require both exit success and a bound summary.
				body, readErr := os.ReadFile(c.metricsOutput)
				require.NoError(t, readErr)
				require.Equal(t, "x 1\n", string(body))
			} else {
				_, statErr := os.Stat(c.metricsOutput)
				require.True(t, os.IsNotExist(statErr))
			}
		})
	}
}
