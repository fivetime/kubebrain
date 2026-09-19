package production_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackendPolicyDropEvidence(t *testing.T) {
	const startup = `time=2026-09-19T00:00:00Z level=info msg="Initializing dissection cache..."`
	event := func(ip, port string) string {
		return fmt.Sprintf(`{"type":"drop","source":42,"reason":"Policy denied","summary":{"tcp":"SYN","l3":{"src":"10.0.0.1","dst":%q},"l4":{"src":"40000","dst":%q}}}`, ip, port) + "\n"
	}
	pd, tikv := event("10.0.0.2", "2379"), event("10.0.0.3", "20160")
	for _, mode := range []string{"both", "startup", "denylist", "pd-only", "wrong-endpoint", "wrong-source", "wrong-backend", "wrong-port", "wrong-reason", "udp", "no-tcp", "unknown-log", "late-startup", "duplicate-startup", "truncated", "empty", "string-endpoint"} {
		t.Run(mode, func(t *testing.T) {
			input := pd + tikv
			switch mode {
			case "startup":
				input = startup + "\n" + input
			case "denylist":
				input = strings.ReplaceAll(input, "Policy denied", "Policy denied by denylist")
			case "pd-only":
				input = pd
			case "wrong-endpoint":
				input = strings.ReplaceAll(input, `"source":42`, `"source":43`)
			case "wrong-source":
				input = strings.ReplaceAll(input, "10.0.0.1", "10.0.0.9")
			case "wrong-backend":
				input = strings.ReplaceAll(input, "10.0.0.3", "10.0.0.9")
			case "wrong-port":
				input = strings.ReplaceAll(input, "20160", "20161")
			case "wrong-reason":
				input = strings.ReplaceAll(input, "Policy denied", "Invalid packet")
			case "udp":
				input = strings.ReplaceAll(input, `"tcp":"SYN"`, `"udp":"UDP"`)
			case "no-tcp":
				input = strings.ReplaceAll(input, `"tcp":"SYN"`, `"tcp":""`)
			case "unknown-log":
				input = "unknown startup\n" + input
			case "late-startup":
				input = pd + startup + "\n" + tikv
			case "duplicate-startup":
				input = startup + "\n" + startup + "\n" + input
			case "truncated":
				input += `{"type":`
			case "empty":
				input = ""
			case "string-endpoint":
				input = strings.ReplaceAll(input, `"source":42`, `"source":"42"`)
			}
			cmd := exec.Command("jq", "-Rn", "-f", "monitor-stream.jq")
			cmd.Stdin = strings.NewReader(input)
			out, err := cmd.Output()
			if mode == "unknown-log" || mode == "late-startup" || mode == "duplicate-startup" || mode == "truncated" || mode == "empty" || mode == "string-endpoint" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			var envelope struct {
				Events []json.RawMessage `json:"events"`
				Proven bool              `json:"packet_enforcement_proven"`
			}
			require.NoError(t, json.Unmarshal(out, &envelope))
			require.False(t, envelope.Proven)
			cmd = exec.Command("jq", "-c", "--argjson", "endpoint", "42", "--arg", "source_ip", "10.0.0.1", "--argjson", "backends", `[{"ip":"10.0.0.2","port":"2379"},{"ip":"10.0.0.3","port":"20160"}]`, "-f", "backend-drops.jq")
			var events bytes.Buffer
			for _, raw := range envelope.Events {
				events.Write(raw)
				events.WriteByte('\n')
			}
			cmd.Stdin = &events
			out, err = cmd.Output()
			require.NoError(t, err)
			// Preserve the old gate: at least one bound PD and one TiKV drop.
			both := bytes.Contains(out, []byte(`"destination_port":"2379"`)) && bytes.Contains(out, []byte(`"destination_port":"20160"`))
			require.Equal(t, mode == "both" || mode == "startup" || mode == "denylist", both)
		})
	}
}
