package retirementmetrics

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// LoadPrefaultCapture binds the baseline's recorded capture stages to the
// independently saved original clock. It does not timestamp manifest writes or
// prove the local evidence is authentic.
func LoadPrefaultCapture(capture string, binding CaptureBinding, origin time.Time) (Sample, error) {
	if !validFaultOrigin(origin) {
		return Sample{}, errors.New("invalid original fault clock")
	}
	sample, err := LoadCapture(capture, binding)
	if err != nil {
		return Sample{}, err
	}
	// Enclose the final microsecond-resolution stage timestamp conservatively.
	// Prefault anonymous rearming is allowed, but must finish before origin too.
	if !sample.captureCompleted.Add(time.Microsecond).Before(origin) {
		return Sample{}, errors.New("baseline capture did not complete before original fault clock")
	}
	return sample, nil
}

func validFaultOrigin(origin time.Time) bool {
	ns := origin.UnixNano()
	return ns >= 1000000000000000000 && ns < 9000000000000000000 && time.Unix(0, ns).Equal(origin)
}

// LoadScheduledCapture checks the scheduler's exact manifest and the capture's
// existing identity/integrity checks against independently supplied origin and
// offset. It does not prove artifact authenticity, worker exit, or fault success.
func LoadScheduledCapture(schedule, capture string, binding CaptureBinding, origin time.Time, offset time.Duration) (Sample, error) {
	bad := func() (Sample, error) { return Sample{}, errors.New("invalid scheduled metrics evidence") }
	ns := origin.UnixNano()
	if !validFaultOrigin(origin) || offset < 0 || offset >= 30*time.Second {
		return bad()
	}
	if filepath.Dir(schedule) != filepath.Dir(capture) || schedule == capture {
		return bad()
	}
	input, err := readScheduleFile(schedule, "input.tsv", 128)
	if err != nil {
		return bad()
	}
	if string(input) != fmt.Sprintf("%d\t%d\n", ns, int64(offset)) {
		return bad()
	}
	path, err := readScheduleFile(schedule, "capture-path", 16<<10)
	if err != nil || string(path) != capture+"\n" {
		return bad()
	}
	complete, err := readScheduleFile(schedule, "COMPLETE", 128)
	if err != nil || string(complete) != "SCHEDULE_COMPLETE_NOT_FAULT_ACCEPTANCE\n" {
		return bad()
	}
	manifest, err := readScheduleFile(schedule, "evidence.sha256", 64<<10)
	if err != nil {
		return bad()
	}
	captureManifest, err := readScheduleFile(capture, "evidence.sha256", 64<<10)
	if err != nil {
		return bad()
	}
	expected := fmt.Sprintf("%x  %s\n%x  %s\n%x  %s\n", sha256.Sum256(input), filepath.Join(schedule, "input.tsv"), sha256.Sum256(path), filepath.Join(schedule, "capture-path"), sha256.Sum256(captureManifest), filepath.Join(capture, "evidence.sha256"))
	if string(manifest) != expected {
		return bad()
	}
	sample, err := LoadCapture(capture, binding)
	if err != nil {
		return Sample{}, err
	}
	// Stage timestamps have microsecond precision; allow only their truncation
	// at the lower bound and conservatively enclose the last timestamp at upper.
	target, deadline := origin.Add(offset), origin.Add(30*time.Second)
	if sample.captureRearmed || sample.captureStarted.Before(target.Truncate(time.Microsecond)) || sample.started.Before(target) || !sample.completed.Before(deadline) || !sample.captureCompleted.Add(time.Microsecond).Before(deadline) {
		return bad()
	}
	return sample, nil
}

// Read only a fixed filename under a canonical, nonsymlink evidence directory.
// Never follow paths supplied by the manifest or block on a FIFO/device.
func readScheduleFile(dir, name string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || strings.ContainsAny(dir, "\x00\r\n") {
		return nil, errors.New("invalid evidence path")
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("invalid evidence directory")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	before, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Size() > limit {
		return nil, errors.New("invalid evidence file")
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() > limit {
		return nil, errors.New("changed evidence file")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("oversize evidence file")
	}
	return data, nil
}
