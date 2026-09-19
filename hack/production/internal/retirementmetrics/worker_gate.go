package retirementmetrics

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
)

// WorkerExpectation is independently admitted, never inferred from worker output.
// MinimumCount must be explicit (including when zero is intentionally allowed).
type WorkerExpectation struct {
	Binding         CaptureBinding
	Offset          time.Duration
	Key             Key
	MinimumCount    *uint64
	RequireDuration bool
}

type WorkerMeasurement struct {
	Count    float64
	Duration *DurationDelta
}

type workerGateState struct {
	expectation WorkerExpectation
	minimum     uint64
	ready       metricsworker.Ready
	baseline    Sample
	started     bool
	completed   bool
}

// WorkerGates supplies Baseline and Completed to metricsworker.Hooks. Methods
// are single-consumer, like that supervisor's serialized callbacks. Attempts
// must never reuse a gate, including after admission failure. admit must
// authenticate bundle/evidence provenance and live ownership, and retain must
// durably preserve the measurement. Neither callback may extend the deadline.
// The supervisor must still join the actual workers; receipts cannot prove it.
type WorkerGates struct {
	workers []workerGateState
	admit   func(context.Context) error
	retain  func(context.Context, int, WorkerMeasurement) error
}

func NewWorkerGates(expected []WorkerExpectation, admit func(context.Context) error, retain func(context.Context, int, WorkerMeasurement) error) (*WorkerGates, error) {
	if len(expected) == 0 || len(expected) > 16 || admit == nil || retain == nil {
		return nil, errors.New("incomplete metric worker gates")
	}
	g := &WorkerGates{admit: admit, retain: retain}
	for _, e := range expected {
		b := e.Binding
		if e.MinimumCount == nil || *e.MinimumCount > 9007199254740991 || e.Offset < 0 || e.Offset >= 30*time.Second || (e.Key.Stage != "local" && e.Key.Stage != "peer") || e.Key.Outcome == "" || b.NamespaceUID == "" || b.StatefulSetUID == "" || b.PodUID == "" || b.Cluster == "" || len(b.SpecSHA256) != 64 {
			return nil, errors.New("invalid independently admitted metric expectation")
		}
		minimum := *e.MinimumCount
		e.MinimumCount = nil // Freeze caller-owned pointer data.
		g.workers = append(g.workers, workerGateState{expectation: e, minimum: minimum})
	}
	return g, nil
}

func (g *WorkerGates) check(ctx context.Context, index int) error {
	if g == nil || ctx == nil || index < 0 || index >= len(g.workers) {
		return errors.New("invalid metric gate invocation")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("metric gate requires bounded context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := g.admit(ctx); err != nil {
		return err
	}
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return ctx.Err()
}

func (g *WorkerGates) Baseline(ctx context.Context, index int, ready metricsworker.Ready) error {
	if err := g.check(ctx, index); err != nil {
		return err
	}
	w := &g.workers[index]
	if w.started {
		return errors.New("metric baseline already consumed")
	}
	w.started = true
	a, err := LoadCapture(ready.Baseline, w.expectation.Binding)
	if err != nil {
		return err
	}
	if err := g.check(ctx, index); err != nil {
		return err
	}
	w.ready, w.baseline = ready, a
	return nil
}

func (g *WorkerGates) Completed(ctx context.Context, index int, result metricsworker.Result, origin time.Time) error {
	if err := g.check(ctx, index); err != nil {
		return err
	}
	w := &g.workers[index]
	if !w.started || w.ready.Worker == "" || w.completed || result.Ready != w.ready {
		return errors.New("metric completion does not match admitted baseline")
	}
	w.completed = true
	deadline, _ := ctx.Deadline()
	if !validFaultOrigin(origin) || origin.After(time.Now()) || deadline.After(origin.Add(30*time.Second)) {
		return errors.New("metric completion requires original fault budget")
	}
	e := w.expectation
	a, b, err := LoadWorkerCaptures(result.Ready.Worker, result.Ready.Baseline, result.Captured.Schedule, result.Captured.Capture, e.Binding, origin, e.Offset)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(a, w.baseline) {
		return errors.New("metric baseline changed after READY")
	}
	count, err := SampleDelta(a, b, e.Key)
	if err != nil {
		return err
	}
	m := WorkerMeasurement{Count: count}
	if e.RequireDuration {
		d, err := SampleDurationDelta(a, b, e.Key)
		if err != nil {
			return err
		}
		m.Duration = &d
	}
	if err := g.check(ctx, index); err != nil {
		return err
	}
	if err := g.retain(ctx, index, m); err != nil {
		return err
	}
	if count < float64(w.minimum) {
		return errors.New("retirement count delta below admitted minimum")
	}
	return g.check(ctx, index)
}
