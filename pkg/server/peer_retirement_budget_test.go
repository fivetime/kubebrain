package server

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type retirementBudgetTransport func(*http.Request) (*http.Response, error)

func (f retirementBudgetTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestPeerRetirementAttemptBudgetsNeverExtendParent(t *testing.T) {
	_, _, _, payload, condition := retirementHandlerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	parentDeadline, _ := ctx.Deadline()
	var deadlines []time.Time
	var bodies [][]byte
	sender := &peerRetirementSender{instance: "instance", holder: "old", budget: time.Second,
		endpoints: []string{"https://first", "https://second", "https://third"},
		client: &http.Client{Transport: retirementBudgetTransport(func(r *http.Request) (*http.Response, error) {
			deadline, ok := r.Context().Deadline()
			require.True(t, ok)
			require.False(t, deadline.After(parentDeadline), "attempt cannot extend the caller deadline")
			deadlines = append(deadlines, deadline)
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			bodies = append(bodies, body)
			<-r.Context().Done()
			if len(deadlines) < 3 {
				return nil, r.Context().Err()
			}
			// Even a transport returning a late 204 cannot confirm retirement.
			return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
		})}}
	require.ErrorIs(t, sender.send(ctx, condition), errPeerRetirementUnconfirmed)
	require.Len(t, deadlines, 3)
	require.True(t, deadlines[0].Before(deadlines[1]))
	require.True(t, deadlines[1].Before(deadlines[2]))
	for _, body := range bodies {
		require.Equal(t, payload, body, "every peer must receive the same frozen claim")
	}
}

func TestPeerRetirementParentCancellationStopsLaterAttempts(t *testing.T) {
	_, _, _, _, condition := retirementHandlerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	sender := &peerRetirementSender{instance: "instance", holder: "old", budget: time.Second,
		endpoints: []string{"https://first", "https://second"},
		client: &http.Client{Transport: retirementBudgetTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			cancel()
			return nil, r.Context().Err()
		})}}
	require.ErrorIs(t, sender.send(ctx, condition), errPeerRetirementUnconfirmed)
	require.Equal(t, 1, calls, "parent cancellation is not an attempt-local timeout")
}

func TestPeerRetirementLateAttemptAcknowledgementIsNotSuccess(t *testing.T) {
	_, _, _, _, condition := retirementHandlerFixture(t)
	calls := 0
	sender := &peerRetirementSender{instance: "instance", holder: "old", budget: 400 * time.Millisecond,
		endpoints: []string{"https://first", "https://second"},
		client: &http.Client{Transport: retirementBudgetTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				<-r.Context().Done()
			}
			return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
		})}}
	require.NoError(t, sender.send(context.Background(), condition))
	require.Equal(t, 2, calls, "first 204 arrived after its attempt deadline; only the second may confirm")
}
