package main

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const evidence = `{"phase":"expired_preflight","at":"2026-09-19T00:00:00Z","lease_id":-9223372036854775807,"ttl":-1,"member_id":9007199254740993,"raft_term":9007199254740995,"cluster_id":18446744073709551615}
{"phase":"request_sent","at":"2026-09-19T00:00:01Z","lease_id":-9223372036854775807}
{"phase":"response","at":"2026-09-19T00:00:03Z","lease_id":-9223372036854775807,"ttl":3,"header":{"cluster_id":18446744073709551615,"member_id":9007199254740997,"revision":4,"raft_term":9007199254740996}}
`

func bindings() []string {
	return []string{"--lease-id", "-9223372036854775807", "--cluster-id", "18446744073709551615", "--initial-member-id", "9007199254740993", "--initial-term", "9007199254740995", "--successor-term", "9007199254740996", "--fault-origin-ns", "1789776002000000000"}
}

func TestResponseCLI(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, run(bindings(), strings.NewReader(evidence), &out))
	require.JSONEq(t, `{"response_at":"2026-09-19T00:00:03Z","fault_to_response_ns":"1000000000","ttl":"3","member_id":"9007199254740997","raft_term":"9007199254740996","fault_acceptance_proven":false}`, out.String())
}

func TestResponseCLIRejects(t *testing.T) {
	for _, mode := range []string{"missing", "duplicate", "unknown", "positional", "leading-zero", "overflow", "rounded", "bad-origin", "truncated", "oversize", "read-error"} {
		t.Run(mode, func(t *testing.T) {
			args := bindings()
			var input io.Reader = strings.NewReader(evidence)
			switch mode {
			case "missing":
				args = args[:10]
			case "duplicate":
				args[2] = args[0]
			case "unknown":
				args[2] = "--unknown"
			case "positional":
				args = append(args, "extra")
			case "leading-zero":
				args[5] = "09007199254740993"
			case "overflow":
				args[3] = "18446744073709551616"
			case "rounded":
				args[5] = "9007199254740992"
			case "bad-origin":
				args[11] = "0"
			case "truncated":
				input = strings.NewReader(strings.TrimSuffix(evidence, "\n"))
			case "oversize":
				input = strings.NewReader(strings.Repeat("x", 13<<10))
			case "read-error":
				input = brokenIO{}
			}
			var out bytes.Buffer
			require.Error(t, run(args, input, &out))
			require.Empty(t, out.String())
		})
	}
}

type brokenIO struct{}

func (brokenIO) Read([]byte) (int, error)  { return 0, errors.New("test read failure") }
func (brokenIO) Write([]byte) (int, error) { return 0, errors.New("test write failure") }

func TestResponseCLIOutputFailure(t *testing.T) {
	require.Error(t, run(bindings(), strings.NewReader(evidence), brokenIO{}))
}
