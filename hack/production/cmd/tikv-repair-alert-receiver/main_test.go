package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakePolicyRunner struct {
	mu      sync.Mutex
	bodies  [][]byte
	err     error
	started chan struct{}
	release chan struct{}
}

func (r *fakePolicyRunner) Run(ctx context.Context, body []byte) error {
	r.mu.Lock()
	r.bodies = append(r.bodies, append([]byte(nil), body...))
	r.mu.Unlock()
	if r.started != nil {
		close(r.started)
		select {
		case <-r.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.err
}

func (r *fakePolicyRunner) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

func alertRequest(method, target, token, contentType, body string) *http.Request {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	return request
}

func TestAlertHandlerAcceptsAuthenticatedJSON(t *testing.T) {
	runner := &fakePolicyRunner{}
	h := handler([]byte("secret"), runner, time.Second, 1)
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, alertRequest(http.MethodPost, "/api/v1/alerts", "secret", "application/json; charset=utf-8", `{"status":"firing"}`))

	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
	require.Equal(t, 1, runner.calls())
	require.JSONEq(t, `{"status":"firing"}`, string(runner.bodies[0]))
}

func TestAlertHandlerRejectsInvalidRequestsBeforePolicy(t *testing.T) {
	tests := []struct {
		name        string
		request     *http.Request
		wantStatus  int
		wantHeader  string
		wantMessage string
	}{
		{name: "method", request: alertRequest(http.MethodGet, "/api/v1/alerts", "secret", "application/json", ""), wantStatus: http.StatusMethodNotAllowed, wantHeader: http.MethodPost},
		{name: "query", request: alertRequest(http.MethodPost, "/api/v1/alerts?retry=1", "secret", "application/json", `{}`), wantStatus: http.StatusBadRequest},
		{name: "missing token", request: alertRequest(http.MethodPost, "/api/v1/alerts", "", "application/json", `{}`), wantStatus: http.StatusUnauthorized},
		{name: "wrong token", request: alertRequest(http.MethodPost, "/api/v1/alerts", "other", "application/json", `{}`), wantStatus: http.StatusUnauthorized},
		{name: "content type", request: alertRequest(http.MethodPost, "/api/v1/alerts", "secret", "text/plain", `{}`), wantStatus: http.StatusUnsupportedMediaType},
		{name: "malformed content type", request: alertRequest(http.MethodPost, "/api/v1/alerts", "secret", "application/json; =bad", `{}`), wantStatus: http.StatusUnsupportedMediaType},
		{name: "empty", request: alertRequest(http.MethodPost, "/api/v1/alerts", "secret", "application/json", ""), wantStatus: http.StatusBadRequest},
		{name: "oversize", request: alertRequest(http.MethodPost, "/api/v1/alerts", "secret", "application/json", strings.Repeat("x", maxAlertBody+1)), wantStatus: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &fakePolicyRunner{}
			recorder := httptest.NewRecorder()
			handler([]byte("secret"), runner, time.Second, 1).ServeHTTP(recorder, test.request)
			require.Equal(t, test.wantStatus, recorder.Code)
			require.Equal(t, test.wantHeader, recorder.Header().Get("Allow"))
			require.Equal(t, 0, runner.calls())
		})
	}
}

func TestAlertHandlerMapsPolicyRejection(t *testing.T) {
	runner := &fakePolicyRunner{err: errors.New("rejected")}
	recorder := httptest.NewRecorder()
	handler([]byte("secret"), runner, time.Second, 1).ServeHTTP(recorder, alertRequest(http.MethodPost, "/api/v1/alerts", "secret", "application/json", `{}`))
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
}

func TestAlertHandlerBoundsConcurrency(t *testing.T) {
	runner := &fakePolicyRunner{started: make(chan struct{}), release: make(chan struct{})}
	h := handler([]byte("secret"), runner, time.Second, 1)
	firstDone := make(chan int, 1)
	go func() {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, alertRequest(http.MethodPost, "/api/v1/alerts", "secret", "application/json", `{}`))
		firstDone <- recorder.Code
	}()
	<-runner.started

	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, alertRequest(http.MethodPost, "/api/v1/alerts", "secret", "application/json", `{}`))
	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.Equal(t, "1", recorder.Header().Get("Retry-After"))
	close(runner.release)
	require.Equal(t, http.StatusAccepted, <-firstDone)
	require.Equal(t, 1, runner.calls())
}

func TestReadinessContract(t *testing.T) {
	h := handler([]byte("secret"), &fakePolicyRunner{}, time.Second, 1)
	for _, test := range []struct {
		name   string
		method string
		target string
		body   string
		want   int
	}{
		{name: "ready", method: http.MethodGet, target: "/readyz", want: http.StatusNoContent},
		{name: "method", method: http.MethodPost, target: "/readyz", want: http.StatusBadRequest},
		{name: "query", method: http.MethodGet, target: "/readyz?a=b", want: http.StatusBadRequest},
		{name: "body", method: http.MethodGet, target: "/readyz", body: "x", want: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, httptest.NewRequest(test.method, test.target, strings.NewReader(test.body)))
			require.Equal(t, test.want, recorder.Code)
		})
	}
}
