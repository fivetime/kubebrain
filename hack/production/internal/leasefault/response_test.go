package leasefault

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fixture() (Binding, []map[string]any) {
	origin := time.Unix(1789798688, 811007697).UTC()
	b := Binding{LeaseID: -9223372036854775807, ClusterID: math.MaxUint64,
		InitialMemberID: 9007199254740993, InitialTerm: 9007199254740995,
		SuccessorTerm: 9007199254740996, Origin: origin}
	return b, []map[string]any{
		{"phase": "expired_preflight", "at": origin.Add(-2 * time.Second), "lease_id": b.LeaseID, "ttl": int64(-1), "member_id": b.InitialMemberID, "raft_term": b.InitialTerm, "cluster_id": b.ClusterID},
		{"phase": "request_sent", "at": origin.Add(-time.Second), "lease_id": b.LeaseID},
		{"phase": "response", "at": origin.Add(29 * time.Second), "lease_id": b.LeaseID, "ttl": int64(0), "header": map[string]any{"cluster_id": b.ClusterID, "member_id": uint64(9007199254740997), "raft_term": b.SuccessorTerm, "revision": int64(4)}},
	}
}

func encode(t *testing.T, events []map[string]any) []byte {
	t.Helper()
	var out bytes.Buffer
	for _, event := range events {
		require.NoError(t, json.NewEncoder(&out).Encode(event))
	}
	return out.Bytes()
}

func TestOriginalResponseExactIntegersAndBoundaries(t *testing.T) {
	for _, offset := range []time.Duration{0, 29 * time.Second, 30 * time.Second} {
		b, events := fixture()
		events[2]["at"] = b.Origin.Add(offset)
		got, err := ValidateOriginalResponse(encode(t, events), b)
		require.NoError(t, err)
		require.Equal(t, offset, got.FaultToResponse)
		require.Equal(t, uint64(9007199254740997), got.MemberID)
		require.Equal(t, b.SuccessorTerm, got.Term)
		require.Zero(t, got.TTL)
	}
}

func TestOriginalPendingPrefix(t *testing.T) {
	b, events := fixture()
	b.SuccessorTerm = 0 // No successor exists at this observation stage.
	prefix := encode(t, events[:2])
	require.NoError(t, ValidateOriginalPendingPrefix(prefix, b))
	for _, data := range [][]byte{
		nil, encode(t, events), prefix[:len(prefix)-1], append(append([]byte{}, prefix...), '{'),
		bytes.Replace(prefix, []byte(`"ttl":-1`), []byte(`"ttl":null`), 1),
		bytes.Replace(prefix, []byte(`"ttl":-1`), []byte(`"ttl":0`), 1),
		bytes.Replace(prefix, []byte(`"member_id":9007199254740993`), []byte(`"member_id":9007199254740992`), 1),
		bytes.Replace(prefix, []byte(`"lease_id":-9223372036854775807`), []byte(`"lease_id":-9223372036854775808`), 1),
		bytes.Replace(prefix, []byte(`"ttl":-1`), []byte(`"ttl":-1,"ttl":-1`), 1),
		bytes.Replace(prefix, []byte(`"ttl":-1`), []byte(`"TTL":-1`), 1),
		bytes.Repeat([]byte{'x'}, 8193),
	} {
		require.Error(t, ValidateOriginalPendingPrefix(data, b))
	}
	b.Origin = events[1]["at"].(time.Time)
	require.Error(t, ValidateOriginalPendingPrefix(prefix, b), "request must predate the original clock")
}

func TestOriginalResponseRejectsInvalidEvidence(t *testing.T) {
	for _, mode := range []string{"early-response", "late-response", "send-at-origin", "backward-preflight", "wrong-cluster", "rounded-cluster", "wrong-lease", "wrong-initial-member", "wrong-initial-term", "zero-response-member", "stale-response-term", "negative-ttl", "missing-ttl", "null-ttl", "live-preflight", "missing-header", "wrong-phase", "extra-event", "missing-event", "unknown-field", "duplicate-field", "case-alias", "fractional-id", "string-id", "overflow-id", "truncated", "oversize", "no-successor", "zero-origin", "zero-lease"} {
		t.Run(mode, func(t *testing.T) {
			b, events := fixture()
			header := events[2]["header"].(map[string]any)
			switch mode {
			case "early-response":
				events[2]["at"] = b.Origin.Add(-time.Nanosecond)
			case "late-response":
				events[2]["at"] = b.Origin.Add(30*time.Second + time.Nanosecond)
			case "send-at-origin":
				events[1]["at"] = b.Origin
			case "backward-preflight":
				events[0]["at"] = b.Origin
			case "wrong-cluster":
				header["cluster_id"] = uint64(12)
			case "rounded-cluster":
				header["cluster_id"] = b.ClusterID - 1
			case "wrong-lease":
				events[2]["lease_id"] = b.LeaseID + 1
			case "wrong-initial-member":
				events[0]["member_id"] = b.InitialMemberID - 1
			case "wrong-initial-term":
				events[0]["raft_term"] = b.InitialTerm - 1
			case "zero-response-member":
				header["member_id"] = 0
			case "stale-response-term":
				header["raft_term"] = b.SuccessorTerm - 1
			case "negative-ttl":
				events[2]["ttl"] = -1
			case "missing-ttl":
				delete(events[2], "ttl")
			case "null-ttl":
				events[2]["ttl"] = nil
			case "live-preflight":
				events[0]["ttl"] = 0
			case "missing-header":
				delete(events[2], "header")
			case "wrong-phase":
				events[1]["phase"] = "response"
			case "extra-event":
				events = append(events, events[2])
			case "missing-event":
				events = events[:2]
			case "unknown-field":
				events[2]["retry"] = false
			case "case-alias":
				events[2]["TTL"] = 0
			case "fractional-id":
				header["member_id"] = json.Number("1.5")
			case "string-id":
				header["member_id"] = "9007199254740997"
			case "overflow-id":
				header["member_id"] = json.Number("18446744073709551616")
			case "no-successor":
				b.SuccessorTerm = b.InitialTerm
			case "zero-origin":
				b.Origin = time.Time{}
			case "zero-lease":
				b.LeaseID = 0
			}
			data := encode(t, events)
			if mode == "duplicate-field" {
				data = bytes.Replace(data, []byte(`"ttl":0`), []byte(`"ttl":0,"ttl":1`), 1)
			}
			if mode == "truncated" {
				data = data[:len(data)-1]
			}
			if mode == "oversize" {
				data = append(data, bytes.Repeat([]byte{' '}, 12<<10)...)
			}
			got, err := ValidateOriginalResponse(data, b)
			require.Error(t, err)
			require.Equal(t, Response{}, got)
		})
	}
}
