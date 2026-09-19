package retirementmetrics

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func summaryFixture(t *testing.T, raw []byte, start time.Time) map[string]any {
	t.Helper()
	hash := sha256.Sum256(raw)
	return map[string]any{"mode": "protected-metrics", "started": start, "completed": start.Add(time.Second), "metrics_bytes": len(raw), "metrics_sha256": hex.EncodeToString(hash[:]), "metrics_text_syntax_validated": true, "metric_semantics_proven": false, "readiness_checked": false, "pod_identity_proven": false, "fault_acceptance_proven": false}
}

func TestSampleBinding(t *testing.T) {
	raw := []byte("# TYPE leader_retirement_local_result counter\nleader_retirement_local_result{outcome=\"confirmed\"} 1\n")
	start := time.Date(2026, 9, 19, 1, 0, 0, 0, time.UTC)
	process := Process{PodUID: "pod", ContainerID: "container", StartedAt: "start"}
	for _, field := range []string{"mode", "started", "completed", "metrics_bytes", "metrics_sha256", "metrics_text_syntax_validated", "metric_semantics_proven", "readiness_checked", "pod_identity_proven", "fault_acceptance_proven"} {
		t.Run("missing-"+field, func(t *testing.T) {
			fields := summaryFixture(t, raw, start)
			delete(fields, field)
			summary, err := json.Marshal(fields)
			require.NoError(t, err)
			_, err = NewSample(raw, summary, process)
			require.Error(t, err)
		})
	}
	for name, change := range map[string]func(map[string]any){
		"wrong-hash":      func(s map[string]any) { s["metrics_sha256"] = "bad" },
		"wrong-size":      func(s map[string]any) { s["metrics_bytes"] = 1 },
		"reversed":        func(s map[string]any) { s["completed"] = start.Add(-time.Second) },
		"too-long":        func(s map[string]any) { s["completed"] = start.Add(26 * time.Second) },
		"false-syntax":    func(s map[string]any) { s["metrics_text_syntax_validated"] = false },
		"claim-readiness": func(s map[string]any) { s["readiness_checked"] = true },
	} {
		t.Run(name, func(t *testing.T) {
			s := summaryFixture(t, raw, start)
			change(s)
			summary, err := json.Marshal(s)
			require.NoError(t, err)
			_, err = NewSample(raw, summary, process)
			require.Error(t, err)
		})
	}
	summary, err := json.Marshal(summaryFixture(t, raw, start))
	require.NoError(t, err)
	before, err := NewSample(raw, summary, process)
	require.NoError(t, err)
	_, err = NewSample(append(append([]byte{}, raw...), '\n'), summary, process)
	require.Error(t, err)
	_, err = NewSample(raw, append(summary, summary...), process)
	require.Error(t, err)
	duplicate := append([]byte(`{"mode":"other",`), summary[1:]...)
	_, err = NewSample(raw, duplicate, process)
	require.Error(t, err)
	alias := append([]byte(`{"Mode":"other",`), summary[1:]...)
	_, err = NewSample(raw, alias, process)
	require.Error(t, err)
	key := Key{"local", "confirmed"}
	for _, offset := range []time.Duration{-time.Second, 0, time.Second} {
		summary, err = json.Marshal(summaryFixture(t, raw, start.Add(offset)))
		require.NoError(t, err)
		after, err := NewSample(raw, summary, process)
		require.NoError(t, err)
		_, err = SampleDelta(before, after, key)
		require.Error(t, err)
	}
	summary, err = json.Marshal(summaryFixture(t, raw, start.Add(2*time.Second)))
	require.NoError(t, err)
	after, err := NewSample(raw, summary, process)
	require.NoError(t, err)
	delta, err := SampleDelta(before, after, key)
	require.NoError(t, err)
	require.Zero(t, delta)
	_, err = SampleDelta(Sample{}, after, key)
	require.Error(t, err)
}
