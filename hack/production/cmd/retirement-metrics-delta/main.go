// retirement-metrics-delta verifies two completed local capture directories.
// It performs no cluster writes and never makes a fault-acceptance decision.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
)

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("retirement-metrics-delta", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var before, after, stage, outcome string
	var schedule, originNS, offsetNS string
	var duration bool
	var binding retirementmetrics.CaptureBinding
	flags.StringVar(&before, "before", "", "completed earlier metrics capture directory")
	flags.StringVar(&after, "after", "", "completed later metrics capture directory")
	flags.StringVar(&schedule, "schedule", "", "completed schedule directory for the later capture")
	flags.StringVar(&originNS, "fault-origin-ns", "", "independently saved original fault timestamp in Unix nanoseconds")
	flags.StringVar(&offsetNS, "offset-ns", "", "independently selected capture offset in nanoseconds")
	flags.StringVar(&binding.NamespaceUID, "namespace-uid", "", "admitted namespace UID")
	flags.StringVar(&binding.StatefulSetUID, "sts-uid", "", "admitted StatefulSet UID")
	flags.StringVar(&binding.PodUID, "pod-uid", "", "admitted demoted Pod UID")
	flags.StringVar(&binding.Cluster, "cluster", "", "admitted KubeBrain cluster label")
	flags.StringVar(&binding.SpecSHA256, "spec-sha256", "", "canonical admitted StatefulSet spec hash")
	flags.StringVar(&stage, "stage", "", "local or peer")
	flags.StringVar(&outcome, "outcome", "", "retirement outcome")
	flags.BoolVar(&duration, "duration", false, "require and report aggregate completed-operation duration delta")
	if flags.Parse(args) != nil || flags.NArg() != 0 || before == "" || after == "" || (stage != "local" && stage != "peer") || outcome == "" {
		return errors.New("invalid arguments; require before/after, namespace-uid, sts-uid, pod-uid, spec-sha256, cluster, stage and outcome")
	}
	var scheduled bool
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "schedule" || f.Name == "fault-origin-ns" || f.Name == "offset-ns" {
			scheduled = true
		}
	})
	var origin, offset int64
	if scheduled {
		var originErr, offsetErr error
		origin, originErr = strconv.ParseInt(originNS, 10, 64)
		offset, offsetErr = strconv.ParseInt(offsetNS, 10, 64)
		if schedule == "" || originErr != nil || offsetErr != nil ||
			strconv.FormatInt(origin, 10) != originNS || strconv.FormatInt(offset, 10) != offsetNS ||
			origin < 1000000000000000000 || origin >= 9000000000000000000 || offset < 0 || offset >= int64(30*time.Second) {
			return errors.New("scheduled mode requires schedule, canonical fault-origin-ns and offset-ns below 30 seconds")
		}
	}
	var a retirementmetrics.Sample
	var err error
	if scheduled {
		a, err = retirementmetrics.LoadPrefaultCapture(before, binding, time.Unix(0, origin))
	} else {
		a, err = retirementmetrics.LoadCapture(before, binding)
	}
	if err != nil {
		return fmt.Errorf("before capture: %w", err)
	}
	var b retirementmetrics.Sample
	if scheduled {
		b, err = retirementmetrics.LoadScheduledCapture(schedule, after, binding, time.Unix(0, origin), time.Duration(offset))
	} else {
		b, err = retirementmetrics.LoadCapture(after, binding)
	}
	if err != nil {
		return fmt.Errorf("after capture: %w", err)
	}
	delta, err := retirementmetrics.SampleDelta(a, b, retirementmetrics.Key{Stage: stage, Outcome: outcome})
	if err != nil {
		return err
	}
	result := map[string]any{"stage": stage, "outcome": outcome, "count_delta": delta, "scope": "same_process_retirement_counter_only", "fault_acceptance_proven": false, "successor_readiness_proven": false, "event_latency_proven": false}
	if scheduled {
		// Strings preserve nanoseconds through consumers using float64 JSON numbers.
		result["scheduled_capture_verified"] = true
		result["fault_origin_ns"] = originNS
		result["offset_ns"] = offsetNS
	}
	if duration {
		d, err := retirementmetrics.SampleDurationDelta(a, b, retirementmetrics.Key{Stage: stage, Outcome: outcome})
		if err != nil {
			return err
		}
		result["duration_delta"] = d
		result["scope"] = "same_process_retirement_completed_operations"
	}
	return json.NewEncoder(output).Encode(result)
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
