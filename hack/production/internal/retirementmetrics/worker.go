package retirementmetrics

import (
	"errors"
	"os"
	"path/filepath"
	"time"
)

// LoadWorkerCaptures binds the completed protected-metrics-worker receipts to
// independently supplied baseline, schedule, capture, identity and fault clock.
// The caller must first join the actual admitted worker and authenticate its
// artifacts. An exit-code file is not proof of process exit or descendant cleanup.
// This offline check does not establish that verification itself finished within
// the original deadline; live callers must enforce that separately.
func LoadWorkerCaptures(worker, baseline, schedule, capture string, binding CaptureBinding, origin time.Time, offset time.Duration) (Sample, Sample, error) {
	bad := func() (Sample, Sample, error) {
		return Sample{}, Sample{}, errors.New("invalid completed metrics worker evidence")
	}
	owner := filepath.Dir(worker)
	seen := map[string]bool{}
	for _, dir := range []string{worker, baseline, schedule, capture} {
		if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || owner == "/" || filepath.Dir(dir) != owner || seen[dir] {
			return bad()
		}
		seen[dir] = true
	}
	check := func() error {
		info, err := os.Lstat(worker)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
			return errors.New("invalid private worker directory")
		}
		for name, expected := range map[string]string{"baseline-path": baseline + "\n", "schedule-path": schedule + "\n", "exit-code": "0\n"} {
			info, err := os.Lstat(filepath.Join(worker, name))
			if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
				return errors.New("invalid private worker receipt")
			}
			data, err := readScheduleFile(worker, name, 16<<10)
			if err != nil || string(data) != expected {
				return errors.New("worker receipt does not match completed capture")
			}
		}
		return nil
	}
	if err := check(); err != nil {
		return Sample{}, Sample{}, err
	}
	a, err := LoadPrefaultCapture(baseline, binding, origin)
	if err != nil {
		return Sample{}, Sample{}, err
	}
	b, err := LoadScheduledCapture(schedule, capture, binding, origin, offset)
	if err != nil {
		return Sample{}, Sample{}, err
	}
	if err := check(); err != nil {
		return Sample{}, Sample{}, err
	}
	return a, b, nil
}
