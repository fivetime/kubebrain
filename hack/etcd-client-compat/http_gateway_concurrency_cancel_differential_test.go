package compat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type httpConcurrencyCancelOutcome struct {
	LockCanceled          bool
	LockWaiterRemoved     bool
	LockSuccessorAcquired bool
	CampaignCanceled      bool
	CampaignWaiterRemoved bool
	CampaignSuccessorWon  bool
}

func TestHTTPGatewayConcurrencyCancellationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}
	referenceOutcome := runHTTPConcurrencyCancelScenario(t, reference, 8563590)
	kubebrainOutcome := runHTTPConcurrencyCancelScenario(t, kubebrain, 8563600)
	require.Equal(t, referenceOutcome, kubebrainOutcome)
}

func runHTTPConcurrencyCancelScenario(t *testing.T, endpoint string, leaseBase int64) httpConcurrencyCancelOutcome {
	t.Helper()
	baseURL := strings.TrimRight(endpoint, "/")
	client := &http.Client{Timeout: 10 * time.Second}
	encode := func(value []byte) string { return base64.StdEncoding.EncodeToString(value) }
	post := func(ctx context.Context, path string, body any) (map[string]any, error) {
		t.Helper()
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		responseBody, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s returned %d: %s", path, response.StatusCode, responseBody)
		}
		decoded := make(map[string]any)
		if err := json.Unmarshal(responseBody, &decoded); err != nil {
			return nil, fmt.Errorf("decode %s response %q: %w", path, responseBody, err)
		}
		return decoded, nil
	}
	mustPost := func(path string, body any) map[string]any {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		response, err := post(ctx, path, body)
		require.NoError(t, err, path)
		return response
	}
	prefixEnd := func(prefix []byte) []byte {
		end := append([]byte(nil), prefix...)
		for i := len(end) - 1; i >= 0; i-- {
			if end[i] < 0xff {
				end[i]++
				return end[:i+1]
			}
		}
		return []byte{0}
	}
	prefixCount := func(prefix []byte) int64 {
		t.Helper()
		response := mustPost("/v3/kv/range", map[string]any{
			"key": encode(prefix), "range_end": encode(prefixEnd(prefix)), "count_only": true,
		})
		rawCount, found := response["count"]
		if !found {
			return 0
		}
		count, err := strconv.ParseInt(rawCount.(string), 10, 64)
		require.NoError(t, err)
		return count
	}
	waitForCount := func(prefix []byte, expected int64) {
		t.Helper()
		require.Eventually(t, func() bool { return prefixCount(prefix) == expected },
			5*time.Second, 10*time.Millisecond, "prefix %q did not reach count %d", prefix, expected)
	}

	leaseIDs := make([]string, 6)
	for i := range leaseIDs {
		leaseIDs[i] = strconv.FormatInt(leaseBase+int64(i), 10)
		mustPost("/v3/lease/grant", map[string]any{"ID": leaseIDs[i], "TTL": "30"})
	}
	t.Cleanup(func() {
		for _, leaseID := range leaseIDs {
			mustPost("/v3/lease/revoke", map[string]any{"ID": leaseID})
		}
	})

	lockName := []byte("/a359/http-cancel/lock")
	lockOwner := mustPost("/v3/lock/lock", map[string]any{"name": encode(lockName), "lease": leaseIDs[0]})
	lockWaitCtx, cancelLockWait := context.WithCancel(context.Background())
	lockWaitDone := make(chan error, 1)
	go func() {
		_, err := post(lockWaitCtx, "/v3/lock/lock", map[string]any{"name": encode(lockName), "lease": leaseIDs[1]})
		lockWaitDone <- err
	}()
	waitForCount(lockName, 2)
	cancelLockWait()
	lockWaitErr := <-lockWaitDone
	require.ErrorIs(t, lockWaitErr, context.Canceled)
	waitForCount(lockName, 1)
	mustPost("/v3/lock/unlock", map[string]any{"key": lockOwner["key"]})
	lockSuccessor := mustPost("/v3/lock/lock", map[string]any{"name": encode(lockName), "lease": leaseIDs[2]})
	mustPost("/v3/lock/unlock", map[string]any{"key": lockSuccessor["key"]})

	electionName := []byte("/a359/http-cancel/election")
	electionOwner := mustPost("/v3/election/campaign", map[string]any{
		"name": encode(electionName), "lease": leaseIDs[3], "value": encode([]byte("owner")),
	})
	electionWaitCtx, cancelElectionWait := context.WithCancel(context.Background())
	electionWaitDone := make(chan error, 1)
	go func() {
		_, err := post(electionWaitCtx, "/v3/election/campaign", map[string]any{
			"name": encode(electionName), "lease": leaseIDs[4], "value": encode([]byte("canceled")),
		})
		electionWaitDone <- err
	}()
	waitForCount(electionName, 2)
	cancelElectionWait()
	electionWaitErr := <-electionWaitDone
	require.ErrorIs(t, electionWaitErr, context.Canceled)
	waitForCount(electionName, 1)
	mustPost("/v3/election/resign", map[string]any{"leader": electionOwner["leader"]})
	electionSuccessor := mustPost("/v3/election/campaign", map[string]any{
		"name": encode(electionName), "lease": leaseIDs[5], "value": encode([]byte("successor")),
	})
	mustPost("/v3/election/resign", map[string]any{"leader": electionSuccessor["leader"]})

	return httpConcurrencyCancelOutcome{
		LockCanceled:          errors.Is(lockWaitErr, context.Canceled),
		LockWaiterRemoved:     prefixCount(lockName) == 0,
		LockSuccessorAcquired: lockSuccessor["key"] != nil,
		CampaignCanceled:      errors.Is(electionWaitErr, context.Canceled),
		CampaignWaiterRemoved: prefixCount(electionName) == 0,
		CampaignSuccessorWon:  electionSuccessor["leader"] != nil,
	}
}
