package main

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestProbeProgressBoundedAndDiagnosticOnly(t *testing.T) {
	for _, total := range []int{1, 99, 100, 101, 6000, 10001} {
		start := time.Unix(0, 0).UTC()
		p := newProbeProgress(start, total)
		var output bytes.Buffer
		for i := 0; i <= total; i++ {
			p.completed = i
			if err := p.write(&output, start.Add(time.Duration(i)*time.Second), false); err != nil {
				t.Fatal(err)
			}
		}
		if lines := strings.Count(output.String(), "\n"); lines > 100 {
			t.Fatalf("total=%d emitted %d periodic records", total, lines)
		}
		if strings.Contains(output.String(), "PROBE_SUMMARY") {
			t.Fatal("diagnostic output must not assert acceptance")
		}
	}
	start := time.Unix(0, 0).UTC()
	p := newProbeProgress(start, 6000)
	p.completed, p.backend, p.public, p.direct, p.pacing = 17, 10*time.Millisecond, 20*time.Millisecond, 30*time.Millisecond, 40*time.Millisecond
	var output bytes.Buffer
	if err := p.write(&output, start.Add(time.Second), true); err != nil {
		t.Fatal(err)
	}
	want := "PROBE_PROGRESS at=1970-01-01T00:00:01Z completed=17 total=6000 elapsed_ms=1000 backend_ms=10 public_ms=20 direct_wait_ms=30 pacing_ms=40 final=true scope=diagnostic_only\n"
	if output.String() != want {
		t.Fatalf("unexpected partial-run diagnostics: %q", output.String())
	}
	if err := p.write(progressFailWriter{}, start, true); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write error lost: %v", err)
	}
}

type progressFailWriter struct{}

func (progressFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
