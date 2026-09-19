// Package retirementmetrics validates diagnostic retirement result counters.
// It does not establish successor readiness or fault acceptance.
package retirementmetrics

import (
	"errors"
	"math"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

type Key struct{ Stage, Outcome string }

// Counters retains absence: a lazily registered, missing series is not zero.
type Counters map[Key]float64

// Process must come from the verified Kubernetes capture, not metric labels.
type Process struct {
	PodUID, ContainerID, StartedAt string
	Cluster                        string
	RestartCount                   int32
}

func (p Process) valid() bool {
	return p.PodUID != "" && p.ContainerID != "" && p.StartedAt != "" && p.RestartCount >= 0
}

func Parse(raw []byte) (Counters, error) {
	return ParseForCluster(raw, "")
}

// ParseForCluster binds the global cluster label used by the production
// exporter. Empty cluster accepts only the unlabeled unit-fixture format.
func ParseForCluster(raw []byte, cluster string) (Counters, error) {
	if len(raw) == 0 || len(raw) > 8<<20 {
		return nil, errors.New("invalid metrics size")
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(raw)))
	if err != nil {
		return nil, errors.New("invalid metrics syntax")
	}
	result := Counters{}
	for _, stage := range []string{"local", "peer"} {
		family := families["leader_retirement_"+stage+"_result"]
		if family == nil {
			continue
		}
		if family.GetType() != dto.MetricType_COUNTER {
			return nil, errors.New("retirement result must be a counter")
		}
		for _, sample := range family.Metric {
			expectedLabels := 1
			if cluster != "" {
				expectedLabels = 2
			}
			if len(sample.Label) != expectedLabels || sample.Counter == nil || sample.TimestampMs != nil {
				return nil, errors.New("invalid retirement counter sample")
			}
			outcome := ""
			seenLabels := map[string]bool{}
			for _, label := range sample.Label {
				name := label.GetName()
				if seenLabels[name] {
					return nil, errors.New("duplicate retirement label")
				}
				seenLabels[name] = true
				switch name {
				case "outcome":
					outcome = label.GetValue()
				case "cluster":
					if cluster == "" || label.GetValue() != cluster {
						return nil, errors.New("retirement cluster mismatch")
					}
				default:
					return nil, errors.New("unexpected retirement label")
				}
			}
			valid := outcome == "confirmed" || outcome == "unconfirmed"
			if stage == "local" {
				valid = valid || outcome == "deadline" || outcome == "canceled"
			} else {
				valid = valid || outcome == "missing_condition" || outcome == "canceled_before_send"
			}
			value := sample.Counter.GetValue()
			if !valid || !validCount(value) {
				return nil, errors.New("invalid retirement counter value or outcome")
			}
			key := Key{stage, outcome}
			if _, exists := result[key]; exists {
				return nil, errors.New("duplicate retirement counter")
			}
			result[key] = value
		}
	}
	return result, nil
}

func validCount(value float64) bool {
	// Above 2^53-1 consecutive event counts cannot be distinguished in float64.
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value <= 9007199254740991 && math.Trunc(value) == value
}

// Delta requires an externally verified same-process pair. Callers must bind
// both captures to Pod UID, container ID, restart count and running startedAt;
// equality of metric values alone is never process identity evidence.
// Missing series (including first appearance) cannot produce a delta.
func Delta(before, after Counters, key Key, beforeProcess, afterProcess Process) (float64, error) {
	if !beforeProcess.valid() || !afterProcess.valid() || beforeProcess != afterProcess {
		return 0, errors.New("missing or changed process identity")
	}
	a, aOK := before[key]
	b, bOK := after[key]
	if !aOK || !bOK {
		return 0, errors.New("retirement series missing; delta unknown")
	}
	if !validCount(a) || !validCount(b) || b < a {
		return 0, errors.New("invalid or reset retirement counter")
	}
	return b - a, nil
}
