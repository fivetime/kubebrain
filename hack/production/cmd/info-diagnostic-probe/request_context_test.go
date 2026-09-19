package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type requestTransportFunc func(*http.Request) (*http.Response, error)

func (f requestTransportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type requestReaderFunc func([]byte) (int, error)

func (f requestReaderFunc) Read(p []byte) (int, error) { return f(p) }

// A transport may deliver EOF concurrently with caller cancellation. EOF is not
// proof that response consumption finished within the caller's original budget.
func TestRequestRejectsBodyCompletedAfterCancellation(t *testing.T) {
	for _, mode := range []string{"cancel-complete", "cancel-partial", "deadline-complete", "deadline-partial", "cancel-read-error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wanted := context.Canceled
			if strings.HasPrefix(mode, "deadline-") {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 10*time.Millisecond)
				defer stop()
				wanted = context.DeadlineExceeded
			}
			client := &http.Client{Transport: requestTransportFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Request: r, Body: io.NopCloser(requestReaderFunc(func(p []byte) (int, error) {
					if wanted == context.DeadlineExceeded {
						<-ctx.Done()
					} else {
						cancel()
					}
					if mode == "cancel-read-error" {
						return 0, io.ErrUnexpectedEOF
					}
					data := "x 1\n"
					if strings.HasSuffix(mode, "partial") {
						data = "x "
					}
					return copy(p, data), io.EOF
				}))}, nil
			})}
			body, err := request(ctx, client, "http://fixture.invalid", "/metrics", http.StatusOK, 1024)
			require.ErrorIs(t, err, wanted)
			require.Empty(t, body)
			if mode == "cancel-read-error" {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			}
		})
	}
}

type requestWriterFunc func([]byte) (int, error)

func (f requestWriterFunc) Write(p []byte) (int, error) { return f(p) }

func TestStackProbeCancellationDuringFinalDiagnostics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := fixture(t, tls.RequireAndVerifyClientCert, tls.VersionTLS13, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			_, _ = io.WriteString(w, "ok")
			return
		}
		_, _ = io.WriteString(w, sampleStack)
	}))
	c.mode = "protected-stack"
	diagnostics := requestWriterFunc(func(p []byte) (int, error) {
		if bytes.Contains(p, []byte(`"phase":"authenticated-profile"`)) && bytes.Contains(p, []byte(`"event":"end"`)) {
			cancel()
		}
		return len(p), nil
	})
	body, err := probeWithDiagnostics(ctx, c, diagnostics)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, body)
}

// Model a deadline whose timer notification has not yet run. Acceptance must
// still be bounded by the deadline itself, not just the Done channel's timing.
type delayedDeadlineContext struct{ context.Context }

func (delayedDeadlineContext) Deadline() (time.Time, bool) {
	return time.Now().Add(-time.Second), true
}

func TestProbeContextChecksAbsoluteDeadline(t *testing.T) {
	body := []byte("valid response")
	err := error(io.ErrUnexpectedEOF)
	finishProbeContext(delayedDeadlineContext{context.Background()}, &body, &err)
	require.Empty(t, body)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
