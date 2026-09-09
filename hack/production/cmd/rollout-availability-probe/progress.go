package main

import (
	"fmt"
	"io"
	"time"
)

// Diagnostic evidence only: never changes deadlines, pacing or success checks.
// Emit at most 100 periodic records plus a final record, regardless of run size.
type probeProgress struct {
	started                         time.Time
	total, completed, stride        int
	backend, public, direct, pacing time.Duration
}

func newProbeProgress(started time.Time, total int) *probeProgress {
	stride := total / 100
	if total%100 != 0 {
		stride++
	}
	if stride < 1 {
		stride = 1
	}
	return &probeProgress{started: started, total: total, stride: stride}
}

func (p *probeProgress) write(w io.Writer, now time.Time, final bool) error {
	if !final && (p.completed == 0 || p.completed%p.stride != 0) {
		return nil
	}
	_, err := fmt.Fprintf(w, "PROBE_PROGRESS at=%s completed=%d total=%d elapsed_ms=%d backend_ms=%d public_ms=%d direct_wait_ms=%d pacing_ms=%d final=%t scope=diagnostic_only\n",
		now.UTC().Format(time.RFC3339Nano), p.completed, p.total, now.Sub(p.started).Milliseconds(),
		p.backend.Milliseconds(), p.public.Milliseconds(), p.direct.Milliseconds(), p.pacing.Milliseconds(), final)
	return err
}
