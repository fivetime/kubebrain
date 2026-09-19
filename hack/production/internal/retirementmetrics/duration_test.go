package retirementmetrics

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func durationFixture(count int, sum float64) string {
	return fmt.Sprintf(`# TYPE leader_retirement_peer_result counter
leader_retirement_peer_result{cluster="test",outcome="confirmed"} %d
# TYPE leader_retirement_peer_duration_seconds histogram
leader_retirement_peer_duration_seconds_bucket{cluster="test",outcome="confirmed",le="1"} %d
leader_retirement_peer_duration_seconds_bucket{cluster="test",outcome="confirmed",le="+Inf"} %d
leader_retirement_peer_duration_seconds_sum{cluster="test",outcome="confirmed"} %g
leader_retirement_peer_duration_seconds_count{cluster="test",outcome="confirmed"} %d
`, count, count, count, sum, count)
}

func TestDurationHistogramValidation(t *testing.T) {
	valid := durationFixture(1, .25)
	for name, raw := range map[string]string{
		"sum-duplicate":           valid + `leader_retirement_peer_duration_seconds_sum{cluster="test",outcome="confirmed"} 0.25` + "\n",
		"count-duplicate":         valid + `leader_retirement_peer_duration_seconds_count{cluster="test",outcome="confirmed"} 1` + "\n",
		"bucket-duplicate":        valid + `leader_retirement_peer_duration_seconds_bucket{cluster="test",outcome="confirmed",le="1.0"} 1` + "\n",
		"nan":                     strings.ReplaceAll(valid, " 0.25\n", " NaN\n"),
		"inf":                     strings.ReplaceAll(valid, " 0.25\n", " +Inf\n"),
		"negative":                strings.ReplaceAll(valid, " 0.25\n", " -1\n"),
		"fractional-count":        strings.ReplaceAll(valid, " 1\n", " 1.5\n"),
		"infinity-count-mismatch": strings.ReplaceAll(valid, `le="+Inf"} 1`, `le="+Inf"} 0`),
		"nonmonotonic-buckets":    strings.ReplaceAll(valid, `le="1"} 1`, `le="1"} 2`),
		"missing-sum":             strings.ReplaceAll(valid, `leader_retirement_peer_duration_seconds_sum{cluster="test",outcome="confirmed"} 0.25`+"\n", ""),
		"wrong-type":              strings.ReplaceAll(valid, " histogram", " gauge"),
		"wrong-cluster":           strings.ReplaceAll(valid, `cluster="test"`, `cluster="other"`),
		"extra-label":             strings.ReplaceAll(valid, `cluster="test"`, `cluster="test",private="value"`),
		"zero-count-with-sum":     durationFixture(0, 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseDurations([]byte(raw), "test")
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private")
		})
	}
	parsed, err := parseDurations([]byte(valid), "test")
	require.NoError(t, err)
	require.Len(t, parsed, 1)
}

func TestSampleDurationDelta(t *testing.T) {
	key := Key{"peer", "confirmed"}
	process := Process{PodUID: "pod", ContainerID: "container", StartedAt: "start", Cluster: "test"}
	makeSample := func(raw string, offset int) Sample {
		start := time.Unix(1800000000+int64(offset), 0).UTC()
		summary, err := json.Marshal(summaryFixture(t, []byte(raw), start))
		require.NoError(t, err)
		sample, err := NewSample([]byte(raw), summary, process)
		require.NoError(t, err)
		return sample
	}
	before := makeSample(durationFixture(0, 0), 0)
	after := makeSample(durationFixture(1, .25), 2)
	delta, err := SampleDurationDelta(before, after, key)
	require.NoError(t, err)
	require.Equal(t, DurationDelta{Count: 1, Seconds: .25}, delta)
	for name, raw := range map[string]string{
		"changed-bounds":           strings.ReplaceAll(durationFixture(1, .25), `le="1"`, `le="2"`),
		"count-mismatch":           strings.ReplaceAll(durationFixture(1, .25), `leader_retirement_peer_result{cluster="test",outcome="confirmed"} 1`, `leader_retirement_peer_result{cluster="test",outcome="confirmed"} 2`),
		"no-new-event-sum-changed": durationFixture(1, .5),
		"sum-reset":                durationFixture(2, .1),
		"count-reset":              durationFixture(0, 0),
	} {
		t.Run(name, func(t *testing.T) {
			next := makeSample(raw, 4)
			_, err := SampleDurationDelta(after, next, key)
			require.Error(t, err)
		})
	}
}
