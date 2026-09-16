package compat

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPIServerWatchIntegrity(t *testing.T) {
	event := func(name, version, revision string) string {
		return fmt.Sprintf(`{
  "type": "MODIFIED",
  "object": {"kind":"ConfigMap", "metadata": {
    "namespace":"test-run", "name":%q, "uid":%q, "resourceVersion":%q
  }, "data":{"version":%q}}
}`, name, "uid-"+name, revision, version)
	}
	a := event("soak-1", "1", "9007199254740993")
	b := event("soak-2", "1", "9007199254740994")
	c := event("soak-1", "2", "9007199254740995")
	d := event("soak-2", "2", "9007199254740996")
	for _, tc := range []struct {
		name   string
		events []string
		valid  bool
	}{
		{"multiline-large-revisions", []string{a, b, c, d}, true},
		{"initial-added", []string{strings.ReplaceAll(event("soak-1", "0", "9007199254740992"), "MODIFIED", "ADDED"), a, b, c, d}, true},
		{"unexpected-added", []string{strings.ReplaceAll(event("soak-99", "0", "9007199254740992"), "MODIFIED", "ADDED"), a, b, c, d}, false},
		{"bookmark", []string{a, b, `{"type":"BOOKMARK","object":{}}`, c, d}, true},
		{"duplicate-compensates-missing", []string{a, b, b, d}, false},
		{"extra-duplicate", []string{a, b, c, d, d}, false},
		{"missing", []string{a, b, c}, false},
		{"wrong-namespace", []string{a, b, c, strings.ReplaceAll(d, "test-run", "another-run")}, false},
		{"uid-change", []string{a, b, c, strings.ReplaceAll(d, "uid-soak-2", "replacement")}, false},
		{"wrong-kind", []string{a, b, c, strings.ReplaceAll(d, "ConfigMap", "Secret")}, false},
		{"revision-regression", []string{b, a, c, d}, false},
		{"same-revision", []string{a, b, c, strings.ReplaceAll(d, "9007199254740996", "9007199254740995")}, false},
		{"invalid-revision", []string{a, b, c, strings.ReplaceAll(d, "9007199254740996", "1e17")}, false},
		{"relist-not-update", []string{a, b, c, strings.ReplaceAll(d, "MODIFIED", "ADDED")}, false},
		{"error-event", []string{a, b, c, d, `{"type":"ERROR","object":{"code":410}}`}, false},
		{"truncated-json", []string{a, b, c, d, `{"type":`}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "watch.json")
			require.NoError(t, os.WriteFile(path, []byte(strings.Join(tc.events, "\n")), 0600))
			out, err := runCompatCommandContext(t, context.Background(), "jq", []string{
				"-s", "-e", "--arg", "namespace", "test-run", "--argjson", "objects", "2", "--argjson", "updates", "2",
				"-f", filepath.Join("..", "dev", "verify-apiserver-watch.jq"), path,
			}, nil)
			if tc.valid {
				require.NoError(t, err, "%s", out)
			} else {
				require.Error(t, err, "%s", out)
			}
		})
	}
}

func TestAPIServerWatchRunnersVerifyStoppedStream(t *testing.T) {
	for _, name := range []string{"apiserver-watch-soak.sh", "incluster-apiserver-watch-soak.sh"} {
		data, err := os.ReadFile(filepath.Join("..", "dev", name))
		require.NoError(t, err)
		body := string(data)
		verify := strings.Index(body, `-f "$ROOT_DIR/hack/dev/verify-apiserver-watch.jq"`)
		require.Greater(t, verify, 0)
		require.Contains(t, body[:verify], "wait \"$watch_pid\" 2>/dev/null || true\nwatch_pid=\"\"")
		require.Contains(t, body[verify:], "delete namespace")
		require.NotContains(t, body, "json.loads(line)")
	}
}
