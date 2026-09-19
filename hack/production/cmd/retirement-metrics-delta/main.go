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

	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
)

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("retirement-metrics-delta", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var before, after, stage, outcome string
	var duration bool
	var binding retirementmetrics.CaptureBinding
	flags.StringVar(&before, "before", "", "completed earlier metrics capture directory")
	flags.StringVar(&after, "after", "", "completed later metrics capture directory")
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
	a, err := retirementmetrics.LoadCapture(before, binding)
	if err != nil {
		return fmt.Errorf("before capture: %w", err)
	}
	b, err := retirementmetrics.LoadCapture(after, binding)
	if err != nil {
		return fmt.Errorf("after capture: %w", err)
	}
	delta, err := retirementmetrics.SampleDelta(a, b, retirementmetrics.Key{Stage: stage, Outcome: outcome})
	if err != nil {
		return err
	}
	result := map[string]any{"stage": stage, "outcome": outcome, "count_delta": delta, "scope": "same_process_retirement_counter_only", "fault_acceptance_proven": false, "successor_readiness_proven": false, "event_latency_proven": false}
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
