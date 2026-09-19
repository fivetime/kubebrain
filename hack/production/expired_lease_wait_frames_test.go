package production_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpiredLeaseWaitFramesRequireReviewedSourceAndExactTopWaitSite(t *testing.T) {
	const source = "02786d91ff406fe50d72e9baebdf2963b74b702c"
	const hash = "3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88"
	const frame = "goroutine 42 [select]:\ngithub.com/kubewharf/kubebrain/pkg/server/etcd.(*leaseManager).refreshLeaseHoldingLocks(0x1, 0x2)\n\t/src/pkg/server/etcd/lease.go:1645 +0xabc\n\n"
	for _, test := range []struct {
		name, stack, sha, digest string
		count                    int
		fail                     bool
	}{
		{"matching", frame, source, hash, 1, false},
		{"reviewed new source", frame, "da303fdbd460ee42f3aa158fac27c396faf6b58f", hash, 1, false},
		{"post join release source", frame, "2ad79751ebc35291ed8caac144a6e73442b927a6", hash, 1, false},
		{"nonce collector source", frame, "e3914449b57ab6e211ece94cb88fd3318acb2970", hash, 1, false},
		{"native lifecycle source", frame, "71bddd9a6de10723157ae5526797146e6a221407", hash, 1, false},
		{"native lifecycle wrong file", frame, "71bddd9a6de10723157ae5526797146e6a221407", strings.Repeat("b", 64), 0, true},
		{"native lifecycle short sha", frame, "71bddd9a", hash, 0, true},
		{"native lifecycle wrong line", strings.Replace(frame, ":1645", ":1646", 1), "71bddd9a6de10723157ae5526797146e6a221407", hash, 0, false},
		{"nonce source wrong file", frame, "e3914449b57ab6e211ece94cb88fd3318acb2970", strings.Repeat("b", 64), 0, true},
		{"nonce source short sha", frame, "e3914449", hash, 0, true},
		{"post join source wrong file", frame, "2ad79751ebc35291ed8caac144a6e73442b927a6", strings.Repeat("b", 64), 0, true},
		{"post join short source", frame, "2ad79751", hash, 0, true},
		{"new source wrong file", frame, "da303fdbd460ee42f3aa158fac27c396faf6b58f", strings.Repeat("b", 64), 0, true},
		{"waiting duration", strings.Replace(frame, "[select]", "[select, 1 minutes]", 1), source, hash, 1, false},
		{"unknown source", frame, strings.Repeat("a", 40), hash, 0, true},
		{"wrong file", frame, source, strings.Repeat("b", 64), 0, true},
		{"old line", strings.Replace(frame, ":1645", ":1637", 1), source, hash, 0, false},
		{"line prefix", strings.Replace(frame, ":1645", ":16450", 1), source, hash, 0, false},
		{"running", strings.Replace(frame, "[select]", "[running]", 1), source, hash, 0, false},
		{"other function", strings.Replace(frame, "refreshLeaseHoldingLocks(", "refreshLeaseHoldingLocksOther(", 1), source, hash, 0, false},
		{"deeper frame", strings.Replace(frame, "[select]:\n", "[select]:\nother.Wait()\n\t/src/other.go:1\n", 1), source, hash, 0, false},
		{"missing newline", strings.TrimRight(frame, "\n"), source, hash, 0, true},
		{"HTML", "<html>not a stack</html>\n", source, hash, 0, true},
		{"two waiters", frame + strings.Replace(frame, "goroutine 42", "goroutine 43", 1), source, hash, 2, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := exec.Command("jq", "-Rs", "--arg", "source", test.sha, "--arg", "source_file_sha256", test.digest, "-f", "expired-lease-wait-frames.jq")
			cmd.Stdin = strings.NewReader(test.stack)
			output, err := cmd.CombinedOutput()
			if test.fail {
				require.Error(t, err, string(output))
				return
			}
			require.NoError(t, err, string(output))
			var frames []map[string]any
			require.NoError(t, json.Unmarshal(output, &frames))
			require.Len(t, frames, test.count)
			for _, f := range frames {
				require.Equal(t, test.sha, f["source"])
				require.Equal(t, hash, f["source_file_sha256"])
			}
		})
	}
}

// A product change must consciously re-review this classifier; passing synthetic
// stack fixtures alone must not let the actual wait site silently drift.
func TestExpiredLeaseWaitBindingMatchesCheckedOutSource(t *testing.T) {
	data, err := os.ReadFile("../../pkg/server/etcd/lease.go")
	require.NoError(t, err)
	sum := sha256.Sum256(data)
	require.Equal(t, "3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88", hex.EncodeToString(sum[:]))
	lines := strings.Split(string(data), "\n")
	require.Greater(t, len(lines), 1645)
	require.Equal(t, "select {", strings.TrimSpace(lines[1644]))
	require.Equal(t, "case <-revoked:", strings.TrimSpace(lines[1645]))
}
