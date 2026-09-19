package retirementmetrics

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadScheduledCapture(t *testing.T) {
	for _, scenario := range []string{"success", "early-stage", "late-stage", "rearmed", "wrong-clock", "wrong-offset", "negative-offset", "offset-deadline", "missing-complete", "tampered-manifest", "tampered-capture", "foreign-capture", "symlink-input", "fifo-input", "wrong-binding"} {
		t.Run(scenario, func(t *testing.T) {
			capture, binding := captureFixture(t)
			if scenario == "rearmed" {
				trace, err := os.ReadFile(filepath.Join(capture, "timing.tsv"))
				require.NoError(t, err)
				trace = append(trace, []byte("1800000006.000000\trearm-anonymous\tstart\t-\n1800000007.000000\trearm-anonymous\tend\t0\n")...)
				require.NoError(t, os.WriteFile(filepath.Join(capture, "timing.tsv"), trace, 0600))
				writeCaptureManifest(t, capture)
			}
			schedule, err := os.MkdirTemp(filepath.Dir(capture), "schedule-")
			require.NoError(t, err)
			origin := time.Unix(1799999990, 0).UTC()
			offset := 2 * time.Second
			if scenario == "early-stage" {
				offset = 5 * time.Second
			}
			if scenario == "late-stage" {
				origin = time.Unix(1799999971, 0).UTC()
				offset = 0
			}
			input := []byte(fmt.Sprintf("%d\t%d\n", origin.UnixNano(), int64(offset)))
			path := []byte(capture + "\n")
			captureManifest, err := os.ReadFile(filepath.Join(capture, "evidence.sha256"))
			require.NoError(t, err)
			manifest := fmt.Sprintf("%x  %s\n%x  %s\n%x  %s\n", sha256.Sum256(input), filepath.Join(schedule, "input.tsv"), sha256.Sum256(path), filepath.Join(schedule, "capture-path"), sha256.Sum256(captureManifest), filepath.Join(capture, "evidence.sha256"))
			for name, data := range map[string][]byte{"input.tsv": input, "capture-path": path, "evidence.sha256": []byte(manifest), "COMPLETE": []byte("SCHEDULE_COMPLETE_NOT_FAULT_ACCEPTANCE\n")} {
				require.NoError(t, os.WriteFile(filepath.Join(schedule, name), data, 0600))
			}
			switch scenario {
			case "wrong-clock":
				origin = origin.Add(time.Second)
			case "wrong-offset":
				offset++
			case "negative-offset":
				offset = -1
			case "offset-deadline":
				offset = 30 * time.Second
			case "missing-complete":
				require.NoError(t, os.Remove(filepath.Join(schedule, "COMPLETE")))
			case "tampered-manifest":
				require.NoError(t, os.WriteFile(filepath.Join(schedule, "evidence.sha256"), []byte(strings.Replace(manifest, "  ", " *", 1)), 0600))
			case "foreign-capture":
				capture = capture + "-other"
			case "tampered-capture":
				require.NoError(t, os.WriteFile(filepath.Join(capture, "metrics.txt"), []byte("other 1\n"), 0600))
			case "symlink-input", "fifo-input":
				require.NoError(t, os.Rename(filepath.Join(schedule, "input.tsv"), filepath.Join(schedule, "old-input")))
				if scenario == "symlink-input" {
					require.NoError(t, os.Symlink("old-input", filepath.Join(schedule, "input.tsv")))
				} else {
					require.NoError(t, syscall.Mkfifo(filepath.Join(schedule, "input.tsv"), 0600))
				}
			case "wrong-binding":
				binding.PodUID = "other"
			}
			_, err = LoadScheduledCapture(schedule, capture, binding, origin, offset)
			if scenario == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestLoadPrefaultCapture(t *testing.T) {
	for _, scenario := range []string{"success", "before-end", "equal-end", "inside-microsecond", "upper-bound", "invalid-origin", "wrong-binding", "rearmed-before", "rearmed-after"} {
		t.Run(scenario, func(t *testing.T) {
			capture, binding := captureFixture(t)
			sample, err := LoadCapture(capture, binding)
			require.NoError(t, err)
			origin := sample.captureCompleted.Add(time.Second)
			switch scenario {
			case "before-end":
				origin = sample.captureCompleted.Add(-time.Second)
			case "equal-end":
				origin = sample.captureCompleted
			case "inside-microsecond":
				origin = sample.captureCompleted.Add(500 * time.Nanosecond)
			case "upper-bound":
				origin = sample.captureCompleted.Add(time.Microsecond)
			case "invalid-origin":
				origin = time.Time{}
			case "wrong-binding":
				binding.PodUID = "other"
			case "rearmed-before", "rearmed-after":
				trace, err := os.ReadFile(filepath.Join(capture, "timing.tsv"))
				require.NoError(t, err)
				trace = append(trace, []byte("1800000006.000000\trearm-anonymous\tstart\t-\n1800000007.000000\trearm-anonymous\tend\t0\n")...)
				require.NoError(t, os.WriteFile(filepath.Join(capture, "timing.tsv"), trace, 0600))
				writeCaptureManifest(t, capture)
				if scenario == "rearmed-before" {
					origin = time.Unix(1800000008, 0)
				}
			}
			_, err = LoadPrefaultCapture(capture, binding, origin)
			if scenario == "success" || scenario == "rearmed-before" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
