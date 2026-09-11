// storage-sync-probe writes only a newly created private directory on an
// explicitly selected test filesystem. It is not a durability or SLO gate.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

type options struct {
	directory string
	allow     bool
	count     int
	blockSize int
	duration  time.Duration
	mode      string
}

type sample struct {
	WriteNS int64 `json:"write_ns"`
	SyncNS  int64 `json:"fdatasync_ns"`
}

type result struct {
	Scope            string   `json:"scope"`
	Mode             string   `json:"mode"`
	PreparationBytes int64    `json:"preparation_bytes"`
	PreparationNS    int64    `json:"preparation_ns"`
	Completed        int      `json:"completed"`
	Requested        int      `json:"requested"`
	BlockSize        int      `json:"block_size"`
	Bytes            int64    `json:"bytes"`
	SyncMeanMS       float64  `json:"fdatasync_mean_ms"`
	SyncP99MS        float64  `json:"fdatasync_p99_nearest_rank_ms"`
	Samples          []sample `json:"samples"`
	CleanupSuccess   bool     `json:"cleanup_success"`
}

func (o options) validate() error {
	if o.mode != "append" && o.mode != "overwrite" {
		return errors.New("mode must be append or overwrite")
	}
	if !o.allow || !filepath.IsAbs(o.directory) || filepath.Clean(o.directory) == "/" {
		return errors.New("require --allow-test-writes and an absolute dedicated test-volume directory (not /)")
	}
	if o.count < 1 || o.count > 4096 || o.blockSize < 512 || o.blockSize > 65536 || int64(o.count)*int64(o.blockSize) > 64<<20 {
		return errors.New("require count 1..4096, block-size 512..65536 and total <=64MiB")
	}
	if o.duration <= 0 || o.duration > time.Minute {
		return errors.New("duration must be positive and at most 1m")
	}
	if o.mode == "overwrite" && int64(o.count)*int64(o.blockSize)*2 > 64<<20 {
		return errors.New("overwrite preparation plus measurement must total <=64MiB")
	}
	return nil
}

func run(ctx context.Context, o options, syncFile func(*os.File) error) (r result, err error) {
	r.Scope, r.Requested, r.BlockSize = "diagnostic_only_not_durability_or_acceptance", o.count, o.blockSize
	r.Mode = o.mode
	if err = o.validate(); err != nil {
		return r, err
	}
	if err = ctx.Err(); err != nil {
		return r, err
	}
	// A private, exclusive directory avoids overwriting any existing file. Never
	// recursively remove it: unexpected extra entries must survive cleanup.
	dir, err := os.MkdirTemp(o.directory, ".kubebrain-sync-probe-")
	if err != nil {
		return r, err
	}
	defer func() {
		removeErr := os.Remove(dir)
		err = errors.Join(err, removeErr)
		r.CleanupSuccess = removeErr == nil && r.CleanupSuccess
	}()
	root, err := os.OpenRoot(dir)
	if err != nil {
		return r, err
	}
	defer root.Close()
	f, err := root.OpenFile("sample", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return r, err
	}
	defer func() {
		closeErr := f.Close()
		removeErr := root.Remove("sample")
		err = errors.Join(err, closeErr, removeErr)
		r.CleanupSuccess = closeErr == nil && removeErr == nil
	}()
	ctx, cancel := context.WithTimeout(ctx, o.duration)
	defer cancel()
	buffer := make([]byte, o.blockSize)
	if o.mode == "overwrite" {
		// Initialize and synchronize a full, non-sparse file before seeking back
		// to offset zero. Preparation belongs to the same deadline/write budget,
		// but never to the measured per-operation histogram population.
		start := time.Now()
		r.PreparationBytes, err = prepareOverwrite(ctx, f, buffer, o.count, syncFile)
		r.PreparationNS = time.Since(start).Nanoseconds()
		if err != nil {
			return r, err
		}
	}
	for range o.count {
		if err = ctx.Err(); err != nil {
			break
		}
		// Fresh incompressible blocks avoid a zero-fill compression shortcut.
		if _, err = rand.Read(buffer); err != nil {
			break
		}
		start := time.Now()
		var n int
		n, err = f.Write(buffer)
		writeTime := time.Since(start)
		r.Bytes += int64(n)
		if err == nil && n != len(buffer) {
			err = io.ErrShortWrite
		}
		if err != nil {
			break
		}
		start = time.Now()
		err = syncFile(f)
		syncTime := time.Since(start)
		if err != nil {
			break
		}
		r.Samples = append(r.Samples, sample{writeTime.Nanoseconds(), syncTime.Nanoseconds()})
	}
	// An in-flight kernel sync cannot be cancelled. A late final completion is
	// still a deadline failure, not an excuse to claim a bounded successful run.
	if err == nil {
		err = ctx.Err()
	}
	r.Completed = len(r.Samples)
	if r.Completed > 0 {
		values := make([]int64, r.Completed)
		var total int64
		for i, s := range r.Samples {
			values[i], total = s.SyncNS, total+s.SyncNS
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		r.SyncMeanMS = float64(total) / float64(r.Completed) / 1e6
		r.SyncP99MS = float64(values[int(math.Ceil(float64(r.Completed)*0.99))-1]) / 1e6
	}
	return r, err
}

func prepareOverwrite(ctx context.Context, f *os.File, buffer []byte, count int, syncFile func(*os.File) error) (written int64, err error) {
	for range count {
		if err = ctx.Err(); err != nil {
			return written, err
		}
		if _, err = rand.Read(buffer); err != nil {
			return written, err
		}
		n, writeErr := f.Write(buffer)
		written += int64(n)
		if writeErr != nil {
			return written, writeErr
		}
		if n != len(buffer) {
			return written, io.ErrShortWrite
		}
	}
	if err = ctx.Err(); err != nil {
		return written, err
	}
	if err = syncFile(f); err != nil {
		return written, err
	}
	if err = ctx.Err(); err != nil {
		return written, err
	}
	_, err = f.Seek(0, io.SeekStart)
	return written, err
}

func main() {
	var o options
	flag.StringVar(&o.directory, "directory", "", "dedicated test-volume directory; never a database data directory")
	flag.BoolVar(&o.allow, "allow-test-writes", false, "explicitly allow bounded writes to this test volume")
	flag.StringVar(&o.mode, "mode", "append", "append to a new file, or overwrite a fully initialized and synced file")
	flag.IntVar(&o.count, "count", 256, "number of write+fdatasync pairs")
	flag.IntVar(&o.blockSize, "block-size", 4096, "bytes written per measured pair")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "soft deadline; cannot interrupt a kernel I/O call")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	r, err := run(ctx, o, dataSync)
	if encodeErr := json.NewEncoder(os.Stdout).Encode(r); encodeErr != nil {
		err = errors.Join(err, encodeErr)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
