package retirementmetrics

import (
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

type durationHistogram struct {
	count, sum *float64
	buckets    map[float64]float64
}

func parseDurations(raw []byte, cluster string) (map[Key]*durationHistogram, error) {
	bad := func() (map[Key]*durationHistogram, error) {
		return nil, errors.New("invalid retirement duration histogram")
	}
	if len(raw) == 0 || len(raw) > 8<<20 {
		return bad()
	}
	p := expfmt.NewTextParser(model.UTF8Validation)
	original, err := p.TextToMetricFamilies(strings.NewReader(string(raw)))
	if err != nil {
		return bad()
	}
	// Parse histogram components as untyped samples as well: the standard
	// histogram parser merges repeated sum/count lines, losing duplicates.
	lines := strings.Split(string(raw), "\n")
	for i, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 4 && fields[0] == "#" && fields[1] == "TYPE" && (fields[2] == "leader_retirement_local_duration_seconds" || fields[2] == "leader_retirement_peer_duration_seconds") {
			lines[i] = ""
		}
	}
	p = expfmt.NewTextParser(model.UTF8Validation)
	components, err := p.TextToMetricFamilies(strings.NewReader(strings.Join(lines, "\n")))
	if err != nil {
		return bad()
	}
	result := map[Key]*durationHistogram{}
	for _, stage := range []string{"local", "peer"} {
		base := "leader_retirement_" + stage + "_duration_seconds"
		family := original[base]
		if family != nil && family.GetType() != dto.MetricType_HISTOGRAM {
			return bad()
		}
		for _, suffix := range []string{"_sum", "_count", "_bucket"} {
			component := components[base+suffix]
			if component == nil {
				continue
			}
			if family == nil || component.GetType() != dto.MetricType_UNTYPED {
				return bad()
			}
			for _, metric := range component.Metric {
				if metric.Untyped == nil || metric.TimestampMs != nil {
					return bad()
				}
				labels := map[string]string{}
				for _, label := range metric.Label {
					name := label.GetName()
					if _, exists := labels[name]; exists {
						return bad()
					}
					labels[name] = label.GetValue()
				}
				outcome := labels["outcome"]
				delete(labels, "outcome")
				valid := outcome == "confirmed" || outcome == "unconfirmed"
				if stage == "local" {
					valid = valid || outcome == "deadline" || outcome == "canceled"
				} else {
					valid = valid || outcome == "missing_condition" || outcome == "canceled_before_send"
				}
				if !valid {
					return bad()
				}
				if cluster != "" {
					if labels["cluster"] != cluster {
						return bad()
					}
					delete(labels, "cluster")
				}
				bound := float64(0)
				if suffix == "_bucket" {
					text, ok := labels["le"]
					if !ok {
						return bad()
					}
					bound, err = strconv.ParseFloat(text, 64)
					if err != nil || math.IsNaN(bound) || bound < 0 {
						return bad()
					}
					delete(labels, "le")
				}
				key := Key{stage, outcome}
				h := result[key]
				if h == nil {
					h = &durationHistogram{buckets: map[float64]float64{}}
					result[key] = h
				}
				value := metric.Untyped.GetValue()
				switch suffix {
				case "_sum":
					if h.sum != nil || !finiteNonnegative(value) {
						return bad()
					}
					h.sum = &value
				case "_count":
					if h.count != nil || !validCount(value) {
						return bad()
					}
					h.count = &value
				case "_bucket":
					if _, ok := h.buckets[bound]; ok || !validCount(value) {
						return bad()
					}
					h.buckets[bound] = value
				}
				if len(labels) != 0 {
					return bad()
				}
			}
		}
	}
	for _, h := range result {
		if !validHistogram(h) {
			return bad()
		}
	}
	return result, nil
}

func finiteNonnegative(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validHistogram(h *durationHistogram) bool {
	if h.count == nil || h.sum == nil || (*h.count == 0 && *h.sum != 0) {
		return false
	}
	last, ok := h.buckets[math.Inf(1)]
	if !ok || last != *h.count {
		return false
	}
	keys := make([]float64, 0, len(h.buckets))
	for key := range h.buckets {
		keys = append(keys, key)
	}
	sort.Float64s(keys)
	previous := float64(0)
	for _, key := range keys {
		if h.buckets[key] < previous || h.buckets[key] > *h.count {
			return false
		}
		previous = h.buckets[key]
	}
	return true
}

// DurationDelta reports aggregate completed-operation seconds, not total
// failover latency, time spent in lifecycle join, or individual event timestamps.
type DurationDelta struct {
	Count   float64 `json:"count"`
	Seconds float64 `json:"seconds"`
}

func SampleDurationDelta(before, after Sample, key Key) (DurationDelta, error) {
	count, err := SampleDelta(before, after, key)
	if err != nil {
		return DurationDelta{}, err
	}
	a, aOK := before.durations[key]
	b, bOK := after.durations[key]
	bad := func() (DurationDelta, error) {
		return DurationDelta{}, errors.New("missing, reset, or inconsistent retirement duration delta")
	}
	if !aOK || !bOK || len(a.buckets) != len(b.buckets) || *a.count != before.counters[key] || *b.count != after.counters[key] || *b.count-*a.count != count || *b.sum < *a.sum {
		return bad()
	}
	sum := *b.sum - *a.sum
	delta := &durationHistogram{count: &count, sum: &sum, buckets: map[float64]float64{}}
	for bound, value := range a.buckets {
		next, ok := b.buckets[bound]
		if !ok || next < value {
			return bad()
		}
		delta.buckets[bound] = next - value
	}
	if !finiteNonnegative(sum) || !validHistogram(delta) {
		return bad()
	}
	return DurationDelta{Count: count, Seconds: sum}, nil
}
