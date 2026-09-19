package retirementmetrics

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"
)

// Sample is an immutable parsed counter snapshot bound to the probe's summary.
// Construction alone is NOT authentication: the caller must first verify the
// capture manifest, COMPLETE, and Kubernetes process identity before and after
// the probe. Hashes detect mismatched artifacts, not a forged capture bundle.
type Sample struct {
	counters           Counters
	process            Process
	started, completed time.Time
	captureIdentity    string
}

type probeSummary struct {
	Mode       string    `json:"mode"`
	Started    time.Time `json:"started"`
	Completed  time.Time `json:"completed"`
	Bytes      int       `json:"metrics_bytes"`
	SHA256     string    `json:"metrics_sha256"`
	Syntax     *bool     `json:"metrics_text_syntax_validated"`
	Semantics  *bool     `json:"metric_semantics_proven"`
	Readiness  *bool     `json:"readiness_checked"`
	Identity   *bool     `json:"pod_identity_proven"`
	Acceptance *bool     `json:"fault_acceptance_proven"`
}

func NewSample(raw, summary []byte, process Process) (Sample, error) {
	bad := func() (Sample, error) { return Sample{}, errors.New("invalid metrics capture summary or binding") }
	if !process.valid() || len(summary) == 0 || len(summary) > 64<<10 || len(raw) == 0 || len(raw) > 8<<20 {
		return bad()
	}
	// Reject duplicate fields, which encoding/json otherwise silently accepts.
	decoder := json.NewDecoder(bytes.NewReader(summary))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return bad()
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return bad()
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return bad()
		}
		// encoding/json matches struct fields case-insensitively. Permit only
		// canonical wire names so aliases cannot override an earlier field.
		switch key {
		case "scope", "mode", "started", "completed", "metrics_bytes", "metrics_sha256",
			"metrics_text_syntax_validated", "metric_semantics_proven", "readiness_checked",
			"pod_identity_proven", "fault_acceptance_proven":
		default:
			return bad()
		}
		seen[key] = true
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return bad()
		}
	}
	if _, err = decoder.Token(); err != nil {
		return bad()
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return bad()
	}
	var s probeSummary
	if json.Unmarshal(summary, &s) != nil {
		return bad()
	}
	flag := func(p *bool, expected bool) bool { return p != nil && *p == expected }
	if s.Mode != "protected-metrics" || !flag(s.Syntax, true) || !flag(s.Semantics, false) || !flag(s.Readiness, false) || !flag(s.Identity, false) || !flag(s.Acceptance, false) {
		return bad()
	}
	if s.Started.IsZero() || !s.Completed.After(s.Started) || s.Completed.Sub(s.Started) > 25*time.Second {
		return bad()
	}
	sum := sha256.Sum256(raw)
	if s.Bytes != len(raw) || s.SHA256 != hex.EncodeToString(sum[:]) {
		return bad()
	}
	counters, err := Parse(raw)
	if err != nil {
		return Sample{}, err
	}
	return Sample{counters: counters, process: process, started: s.Started, completed: s.Completed}, nil
}

// SampleDelta requires strictly ordered, nonoverlapping capture intervals.
// It reports only an event-count change, never an event timestamp or latency.
func SampleDelta(before, after Sample, key Key) (float64, error) {
	if before.captureIdentity != after.captureIdentity {
		return 0, errors.New("changed capture identity or mixed verification levels")
	}
	if before.completed.IsZero() || after.started.IsZero() || !after.started.After(before.completed) {
		return 0, errors.New("unordered or overlapping metrics captures")
	}
	return Delta(before.counters, after.counters, key, before.process, after.process)
}
