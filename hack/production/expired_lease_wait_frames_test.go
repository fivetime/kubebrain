package production_test

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os/exec"
	"strings"
	"testing"
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
				require.Equal(t, source, f["source"])
				require.Equal(t, hash, f["source_file_sha256"])
			}
		})
	}
}
