package compat

import (
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type httpHealthBoundaryOutcome struct {
	Name        string
	Status      int
	ContentType string
	Body        string
}

func TestHTTPHealthBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_GATEWAY_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_GATEWAY_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_GATEWAY_ENDPOINT and KUBEBRAIN_GATEWAY_ENDPOINT")
	}

	referenceOutcomes := runHTTPHealthBoundaryMatrix(t, reference)
	require.Equal(t, []httpHealthBoundaryOutcome{
		{Name: "livez", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "ok\n"},
		{Name: "livez-verbose-presence", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "[+]serializable_read ok\nok\n"},
		{Name: "livez-verbose-false-is-still-verbose", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "[+]serializable_read ok\nok\n"},
		{Name: "livez-exclude", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "ok\n"},
		{Name: "livez-empty-and-repeated-exclude", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "ok\n"},
		{Name: "livez-subcheck", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "ok\n"},
		{Name: "livez-subcheck-ignores-exclude", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "ok\n"},
		{Name: "readyz", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "ok\n"},
		{Name: "readyz-verbose", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "[+]data_corruption ok\n[+]linearizable_read ok\n[+]non_learner ok\n[+]serializable_read ok\nok\n"},
		{Name: "readyz-exclude-all", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "ok\n"},
		{Name: "readyz-unknown-exclude", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "ok\n"},
		{Name: "readyz-subcheck-verbose", Status: 200, ContentType: "text/plain; charset=utf-8", Body: "[+]non_learner ok\nok\n"},
		{Name: "readyz-post", Status: 405, ContentType: "text/plain; charset=utf-8", Body: "Method Not Allowed\n"},
		{Name: "readyz-options", Status: 200},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runHTTPHealthBoundaryMatrix(t, kubebrain))
}

func runHTTPHealthBoundaryMatrix(t *testing.T, endpoint string) []httpHealthBoundaryOutcome {
	t.Helper()
	baseURL := strings.TrimRight(endpoint, "/")
	client := &http.Client{Timeout: 5 * time.Second}
	cases := []struct {
		name      string
		method    string
		path      string
		sortLines bool
	}{
		{name: "livez", method: http.MethodGet, path: "/livez"},
		{name: "livez-verbose-presence", method: http.MethodGet, path: "/livez?verbose"},
		{name: "livez-verbose-false-is-still-verbose", method: http.MethodGet, path: "/livez?verbose=false"},
		{name: "livez-exclude", method: http.MethodGet, path: "/livez?exclude=serializable_read"},
		{name: "livez-empty-and-repeated-exclude", method: http.MethodGet, path: "/livez?exclude=&exclude=serializable_read&exclude=serializable_read"},
		{name: "livez-subcheck", method: http.MethodGet, path: "/livez/serializable_read"},
		{name: "livez-subcheck-ignores-exclude", method: http.MethodGet, path: "/livez/serializable_read?exclude=serializable_read"},
		{name: "readyz", method: http.MethodGet, path: "/readyz"},
		{name: "readyz-verbose", method: http.MethodGet, path: "/readyz?verbose", sortLines: true},
		{name: "readyz-exclude-all", method: http.MethodGet, path: "/readyz?exclude=data_corruption&exclude=serializable_read&exclude=linearizable_read&exclude=non_learner"},
		{name: "readyz-unknown-exclude", method: http.MethodGet, path: "/readyz?exclude=unknown"},
		{name: "readyz-subcheck-verbose", method: http.MethodGet, path: "/readyz/non_learner?verbose"},
		{name: "readyz-post", method: http.MethodPost, path: "/readyz"},
		{name: "readyz-options", method: http.MethodOptions, path: "/readyz"},
	}

	outcomes := make([]httpHealthBoundaryOutcome, 0, len(cases))
	for _, testCase := range cases {
		request, err := http.NewRequest(testCase.method, baseURL+testCase.path, nil)
		require.NoError(t, err)
		response, err := client.Do(request)
		require.NoError(t, err, testCase.name)
		body, err := io.ReadAll(response.Body)
		require.NoError(t, err, testCase.name)
		require.NoError(t, response.Body.Close())
		normalizedBody := string(body)
		if testCase.sortLines {
			lines := strings.Split(strings.TrimSuffix(normalizedBody, "\n"), "\n")
			require.GreaterOrEqual(t, len(lines), 2, "%s: %q", testCase.name, normalizedBody)
			require.Equal(t, "ok", lines[len(lines)-1], "%s: %q", testCase.name, normalizedBody)
			checkLines := lines[:len(lines)-1]
			sort.Strings(checkLines)
			normalizedBody = strings.Join(checkLines, "\n") + "\nok\n"
		}
		outcomes = append(outcomes, httpHealthBoundaryOutcome{
			Name: testCase.name, Status: response.StatusCode,
			ContentType: response.Header.Get("Content-Type"), Body: normalizedBody,
		})
	}
	return outcomes
}
