package leasefault

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"golang.org/x/sys/unix"
)

// CaptureIsolatedJoinIdentity records the current PID-1 driver's identity once,
// before starting any children. The caller must independently admit the private
// PID namespace and ordinary unmasked proc mount. This cannot establish those
// container properties, acquire ownership, or authorize cluster mutations.
// The returned digest must be pinned in the independently approved plan. Never
// recapture on recovery/restart; any failed write is retained, not overwritten.
func CaptureIsolatedJoinIdentity(ctx context.Context, directory string) (string, error) {
	if err := ownerContext(ctx); err != nil {
		return "", err
	}
	if os.Getpid() != 1 || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == "/" {
		return "", errors.New("isolated Join capture requires PID 1 and a canonical private owner directory")
	}
	var fs unix.Statfs_t
	if err := unix.Statfs("/proc", &fs); err != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		return "", errors.New("isolated Join capture requires procfs")
	}
	root, err := recoveryRoot(directory)
	if err != nil {
		return "", err
	}
	defer root.Close()
	owner, err := root.Stat(".")
	if err != nil {
		return "", err
	}
	stat, ok := owner.Sys().(*syscall.Stat_t)
	if !ok || owner.Mode().Perm() != 0700 || stat.Uid != uint32(os.Geteuid()) {
		return "", errors.New("isolated Join owner must be owned private 0700")
	}
	dir, err := root.Open(".")
	if err != nil {
		return "", err
	}
	defer dir.Close()
	entries, err := dir.ReadDir(1)
	if len(entries) != 0 || !errors.Is(err, io.EOF) {
		return "", errors.New("isolated Join capture requires a fresh empty owner directory")
	}
	namespace, err := os.Stat("/proc/self/ns/pid")
	if err != nil {
		return "", err
	}
	initNamespace, err := os.Stat("/proc/1/ns/pid")
	if err != nil || !os.SameFile(namespace, initNamespace) {
		return "", errors.New("proc PID namespace differs from driver")
	}
	nsStat, ok := namespace.Sys().(*syscall.Stat_t)
	if !ok {
		return "", errors.New("missing PID namespace inode")
	}
	boot, err := planinput.ReadFile("/proc/sys/kernel/random/boot_id", false, 64)
	if err != nil {
		return "", err
	}
	initStat, err := planinput.ReadFile("/proc/1/stat", false, 4096)
	if err != nil {
		return "", err
	}
	end := strings.LastIndex(string(initStat), ") ")
	if end < 0 {
		return "", errors.New("invalid init process stat")
	}
	fields := strings.Fields(string(initStat)[end+2:])
	if len(fields) < 20 {
		return "", errors.New("missing init start ticks")
	}
	data := []byte(fmt.Sprintf("v1\t%d:%d\t%s\t%s\t%d:%d\n", nsStat.Dev, nsStat.Ino, strings.TrimSuffix(string(boot), "\n"), fields[19], stat.Dev, stat.Ino))
	if len(data) > 256 || !isolatedJoinRecord.Match(data) {
		return "", errors.New("invalid captured Join identity")
	}
	processes, err := os.ReadDir("/proc")
	if err != nil {
		return "", err
	}
	for _, process := range processes {
		pid, err := strconv.ParseUint(process.Name(), 10, 64)
		if err == nil && pid != 1 {
			return "", errors.New("isolated Join identity must be captured before starting children")
		}
	}
	checkOwner := func() error {
		current, err := os.Lstat(directory)
		if err != nil || !os.SameFile(owner, current) || current.Mode().Perm() != 0700 {
			return errors.New("isolated Join capture owner changed")
		}
		return ctx.Err()
	}
	if err := checkOwner(); err != nil {
		return "", err
	}
	f, err := root.OpenFile(IsolatedJoinIdentity, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return "", err
	}
	_, writeErr := f.Write(data)
	if err := errors.Join(writeErr, f.Sync(), f.Close(), dir.Sync(), checkOwner()); err != nil {
		return "", err
	}
	digest := planinput.SHA256(data)
	if err := VerifyJoinInputs(ctx, directory, IsolatedJoinScript, map[string]string{filepath.Join(directory, IsolatedJoinIdentity): digest}); err != nil {
		return "", err
	}
	return digest, nil
}
