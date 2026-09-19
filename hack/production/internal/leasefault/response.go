// Package leasefault validates evidence from the original expired-lease stream.
// It does not authenticate logs, install faults, or prove successful recovery.
package leasefault

import (
	"bytes"
	"errors"
	"time"

	strictjson "sigs.k8s.io/json"
)

type Binding struct {
	LeaseID                                                int64
	ClusterID, InitialMemberID, InitialTerm, SuccessorTerm uint64
	Origin                                                 time.Time
}

type Response struct {
	At              time.Time
	FaultToResponse time.Duration
	TTL             int64
	MemberID, Term  uint64
}

// ValidateOriginalPendingPrefix checks a bounded, complete two-event snapshot
// before activation. It does not prove the process is alive, the snapshot is
// current, or the server is blocked: the live observer must independently check
// those conditions before and after reading it. SuccessorTerm is not yet known.
func ValidateOriginalPendingPrefix(data []byte, b Binding) error {
	bad := errors.New("invalid original pending prefix")
	ns := b.Origin.UnixNano()
	if b.LeaseID == 0 || b.ClusterID == 0 || b.InitialMemberID == 0 || b.InitialTerm == 0 ||
		ns < 1000000000000000000 || ns >= 9000000000000000000 || !time.Unix(0, ns).Equal(b.Origin) ||
		len(data) == 0 || len(data) > 8<<10 || data[len(data)-1] != '\n' {
		return bad
	}
	lines := bytes.Split(data[:len(data)-1], []byte{'\n'})
	if len(lines) != 2 {
		return bad
	}
	var before struct {
		Phase     string    `json:"phase"`
		At        time.Time `json:"at"`
		LeaseID   int64     `json:"lease_id"`
		TTL       *int64    `json:"ttl"`
		MemberID  uint64    `json:"member_id"`
		Term      uint64    `json:"raft_term"`
		ClusterID uint64    `json:"cluster_id"`
	}
	var sent struct {
		Phase   string    `json:"phase"`
		At      time.Time `json:"at"`
		LeaseID int64     `json:"lease_id"`
	}
	if !decodeEvent(lines[0], &before) || !decodeEvent(lines[1], &sent) ||
		before.Phase != "expired_preflight" || before.LeaseID != b.LeaseID || before.TTL == nil || *before.TTL >= 0 ||
		before.ClusterID != b.ClusterID || before.MemberID != b.InitialMemberID || before.Term != b.InitialTerm ||
		before.At.IsZero() || sent.At.IsZero() || before.At.After(sent.At) || !sent.At.Before(b.Origin) ||
		sent.Phase != "request_sent" || sent.LeaseID != b.LeaseID {
		return bad
	}
	return nil
}

func decodeEvent(line []byte, target any) bool {
	if len(line) == 0 || len(line) > 4<<10 {
		return false
	}
	strictErrors, err := strictjson.UnmarshalStrict(line, target, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	return err == nil && len(strictErrors) == 0
}

// ValidateOriginalResponse checks exactly the three JSONL events emitted by
// lease-term-probe, using integer decoding (never float64 identity comparisons).
// Binding must come from independent admission and healthy-member successor
// observation. A matching response term need not equal the initial member ID:
// the original stream may return a response forwarded from the successor.
//
// The inclusive 30-second response timestamp bound preserves the old driver's
// gate. The outer driver must separately finish ALL gates within its original
// budget, join the probe with exit 0, verify isolation and stack/lease/key state,
// then restore. Log timestamps alone prove none of those conditions.
func ValidateOriginalResponse(data []byte, b Binding) (Response, error) {
	bad := func() (Response, error) { return Response{}, errors.New("invalid original lease response evidence") }
	ns := b.Origin.UnixNano()
	if b.LeaseID == 0 || b.ClusterID == 0 || b.InitialMemberID == 0 || b.InitialTerm == 0 || b.SuccessorTerm <= b.InitialTerm ||
		ns < 1000000000000000000 || ns >= 9000000000000000000 || !time.Unix(0, ns).Equal(b.Origin) {
		return bad()
	}
	if len(data) == 0 || len(data) > 12<<10 || data[len(data)-1] != '\n' {
		return bad()
	}
	lines := bytes.Split(data[:len(data)-1], []byte{'\n'})
	if len(lines) != 3 {
		return bad()
	}
	if ValidateOriginalPendingPrefix(data[:len(lines[0])+len(lines[1])+2], b) != nil {
		return bad()
	}
	var response struct {
		Phase   string    `json:"phase"`
		At      time.Time `json:"at"`
		LeaseID int64     `json:"lease_id"`
		TTL     *int64    `json:"ttl"`
		Header  *struct {
			ClusterID uint64 `json:"cluster_id"`
			MemberID  uint64 `json:"member_id"`
			Revision  int64  `json:"revision"`
			Term      uint64 `json:"raft_term"`
		} `json:"header"`
	}
	if !decodeEvent(lines[2], &response) {
		return bad()
	}
	if response.Phase != "response" || response.LeaseID != b.LeaseID || response.TTL == nil || *response.TTL < 0 ||
		response.Header == nil || response.Header.ClusterID != b.ClusterID || response.Header.MemberID == 0 ||
		response.Header.Revision < 0 || response.Header.Term < b.SuccessorTerm ||
		response.At.Before(b.Origin) || response.At.After(b.Origin.Add(30*time.Second)) {
		return bad()
	}
	return Response{At: response.At, FaultToResponse: response.At.Sub(b.Origin), TTL: *response.TTL,
		MemberID: response.Header.MemberID, Term: response.Header.Term}, nil
}
