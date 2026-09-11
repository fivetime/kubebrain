package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func testOptions(t *testing.T) options {
	t.Helper()
	return options{directory: t.TempDir(), allow: true, count: 3, blockSize: 4096, duration: time.Second}
}

func assertEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("expected no residue, entries=%v err=%v", entries, err)
	}
}

func TestRejectBeforeAnyWrite(t *testing.T) {
	for _, change := range []func(*options){
		func(o *options) { o.allow = false },
		func(o *options) { o.directory = "/" },
		func(o *options) { o.directory = "." },
		func(o *options) { o.count = 0 },
		func(o *options) { o.count = 4097 },
		func(o *options) { o.blockSize = 511 },
		func(o *options) { o.blockSize = 65537 },
		func(o *options) { o.count, o.blockSize = 4096, 65536 },
		func(o *options) { o.duration = 0 },
		func(o *options) { o.duration = time.Minute + 1 },
	} {
		o := testOptions(t)
		dir := o.directory
		change(&o)
		_, err := run(context.Background(), o, func(*os.File) error { t.Fatal("unexpected sync"); return nil })
		if err == nil {
			t.Fatal("invalid options accepted")
		}
		assertEmpty(t, dir)
	}
}

func TestRealDataSyncAndCleanup(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux fdatasync")
	}
	o := testOptions(t)
	r, err := run(context.Background(), o, dataSync)
	if err != nil || !r.CleanupSuccess || r.Completed != o.count || r.Bytes != int64(o.count*o.blockSize) || len(r.Samples) != o.count {
		t.Fatalf("unexpected result %+v, err=%v", r, err)
	}
	var total, maximum int64
	for _, s := range r.Samples {
		total += s.SyncNS
		if s.SyncNS > maximum {
			maximum = s.SyncNS
		}
	}
	if r.SyncMeanMS != float64(total)/float64(o.count)/1e6 || r.SyncP99MS != float64(maximum)/1e6 {
		t.Fatalf("summary does not match raw three-sample population: %+v", r)
	}
	assertEmpty(t, o.directory)
}

func TestExpiredDeadlineCreatesNothing(t *testing.T) {
	o := testOptions(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	r, err := run(ctx, o, func(*os.File) error { t.Fatal("unexpected sync"); return nil })
	if !errors.Is(err, context.DeadlineExceeded) || r.Bytes != 0 {
		t.Fatalf("expired deadline accepted: %+v %v", r, err)
	}
	assertEmpty(t, o.directory)
}

func TestSyncFailurePreservesErrorAndCleans(t *testing.T) {
	o := testOptions(t)
	want := errors.New("injected sync failure")
	r, err := run(context.Background(), o, func(*os.File) error { return want })
	if !errors.Is(err, want) || !r.CleanupSuccess || r.Completed != 0 || r.Bytes != int64(o.blockSize) {
		t.Fatalf("unexpected result %+v, err=%v", r, err)
	}
	assertEmpty(t, o.directory)
}

func TestCancellationBeforeWriteAndAfterFinalSync(t *testing.T) {
	for _, before := range []bool{true, false} {
		o := testOptions(t)
		o.count = 1
		ctx, cancel := context.WithCancel(context.Background())
		if before {
			cancel()
		}
		r, err := run(ctx, o, func(*os.File) error { cancel(); return nil })
		cancel()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("late completion must remain cancellation: %+v %v", r, err)
		}
		if before && r.Bytes != 0 {
			t.Fatalf("wrote under cancelled context: %+v", r)
		}
		assertEmpty(t, o.directory)
	}
}

func TestExistingFilesSurviveAndUnexpectedEntriesNotRecursivelyRemoved(t *testing.T) {
	o := testOptions(t)
	marker := filepath.Join(o.directory, "keep")
	if err := os.WriteFile(marker, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	var extra string
	r, err := run(context.Background(), o, func(f *os.File) error {
		// The private directory is intentionally polluted to prove cleanup never
		// uses recursive deletion, even when the measurement itself succeeds.
		entries, e := os.ReadDir(o.directory)
		if e != nil {
			return e
		}
		for _, entry := range entries {
			if entry.IsDir() {
				extra = filepath.Join(o.directory, entry.Name(), "unexpected")
				return os.WriteFile(extra, []byte("retain"), 0600)
			}
		}
		return errors.New("private directory missing")
	})
	if err == nil || r.CleanupSuccess || extra == "" {
		t.Fatalf("cleanup failure hidden: %+v %v", r, err)
	}
	for path, want := range map[string]string{marker: "untouched", extra: "retain"} {
		value, err := os.ReadFile(path)
		if err != nil || string(value) != want {
			t.Fatalf("unowned file changed: %s %q %v", path, value, err)
		}
	}
}
