// Command native-pitr-full-backup runs the supported BR transactional full
// backup shape and emits its execution attestation only after a successful
// child exit and a stable executable digest check.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
)

type options struct {
	brBinary, pdAddrs, storage, ca, cert, key, output string
	cipherMethod, encryptionKeyID, encryptionKeyFile  string
	backupTS                                          uint64
	timeout                                           time.Duration
}

type commandRunner interface {
	Output(context.Context, *os.File, string, ...string) ([]byte, error)
	Run(context.Context, *os.File, string, []string, io.Writer, io.Writer) error
}

type osRunner struct{}

func (osRunner) Output(ctx context.Context, executable *os.File, displayName string, args ...string) ([]byte, error) {
	cmd := commandForOpenExecutable(ctx, executable, displayName, args...)
	output := limitedOutput{remaining: 16 << 10}
	cmd.Stdout, cmd.Stderr = &output, &output
	err := cmd.Run()
	return output.Bytes(), err
}

type limitedOutput struct {
	bytes.Buffer
	remaining int
}

func (w *limitedOutput) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, errors.New("BR version output exceeds 16 KiB")
	}
	w.remaining -= len(p)
	return w.Buffer.Write(p)
}

func (osRunner) Run(ctx context.Context, executable *os.File, displayName string, args []string, stdout, stderr io.Writer) error {
	cmd := commandForOpenExecutable(ctx, executable, displayName, args...)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	return cmd.Run()
}

func commandForOpenExecutable(ctx context.Context, executable *os.File, displayName string, args ...string) *exec.Cmd {
	// Execute the already-open inode that was hashed. Linux resolves this procfs
	// path before applying close-on-exec, eliminating pathname replacement races.
	cmd := exec.CommandContext(ctx, "/proc/self/fd/"+strconv.FormatUint(uint64(executable.Fd()), 10), args...)
	cmd.Args[0] = displayName
	return cmd
}

func main() {
	var o options
	flag.StringVar(&o.brBinary, "br-binary", "br", "BR v7.5.1 executable")
	flag.StringVar(&o.pdAddrs, "pd-addrs", "", "comma-separated source PD host:port addresses")
	flag.StringVar(&o.storage, "storage-prefix", "", "immutable credential-free s3:// backup prefix")
	flag.Uint64Var(&o.backupTS, "backup-ts", 0, "exact PD TSO to back up")
	flag.StringVar(&o.ca, "ca", "", "source CA file")
	flag.StringVar(&o.cert, "cert", "", "source client certificate")
	flag.StringVar(&o.key, "key", "", "source client private key")
	flag.StringVar(&o.output, "attestation-output", "", "new full-backup attestation output file")
	flag.StringVar(&o.cipherMethod, "crypter-method", nativepitr.CipherMethodPlaintext, "plaintext or aes256-ctr")
	flag.StringVar(&o.encryptionKeyID, "encryption-key-id", "", "immutable non-secret encryption key version ID")
	flag.StringVar(&o.encryptionKeyFile, "encryption-key-file", "", "file containing the 64-character hexadecimal AES-256 key")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Hour, "BR backup deadline")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, o, osRunner{}, os.Stderr, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR full backup:", err)
		os.Exit(1)
	}
}

func execute(parent context.Context, o options, runner commandRunner, logs io.Writer, now func() time.Time) (retErr error) {
	if o.backupTS == 0 || o.output == "" || o.timeout <= 0 || o.timeout > 24*time.Hour {
		return errors.New("backup-ts, attestation-output, and a positive timeout no greater than 24h are required")
	}
	if !filepath.IsAbs(o.output) {
		return errors.New("attestation-output must be absolute")
	}
	if _, err := os.Lstat(o.output); err == nil || !errors.Is(err, os.ErrNotExist) {
		return errors.New("attestation-output must not already exist")
	}
	if o.cipherMethod == "" {
		o.cipherMethod = nativepitr.CipherMethodPlaintext
	}
	encryption := nativepitr.EncryptionIdentity{Method: o.cipherMethod, KeyID: o.encryptionKeyID}
	if err := encryption.Validate(); err != nil {
		return err
	}
	var encryptionKey []byte
	runtimeEncryptionKeyFile := ""
	if encryption.Method == nativepitr.CipherMethodPlaintext {
		if o.encryptionKeyFile != "" {
			return errors.New("plaintext backup must not receive an encryption key file")
		}
	} else {
		var err error
		encryptionKey, err = nativepitr.ReadAES256KeyFile(o.encryptionKeyFile)
		if err != nil {
			return err
		}
		var cleanup func() error
		runtimeEncryptionKeyFile, cleanup, err = nativepitr.StageAES256KeyFile(encryptionKey)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, cleanup()) }()
	}
	addrs, pdSHA, err := canonicalPDAddresses(o.pdAddrs)
	if err != nil {
		return err
	}
	if (o.ca == "") != (o.cert == "") || (o.ca == "") != (o.key == "") {
		return errors.New("ca, cert, and key must be supplied together")
	}
	brFile, resolvedBR, brSHA, err := openExecutable(o.brBinary)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, brFile.Close()) }()
	versionBytes, err := runner.Output(parent, brFile, resolvedBR, "--version")
	if err != nil || !nativepitr.PinnedBRVersion(string(versionBytes)) {
		return fmt.Errorf("BR must be exact v7.5.1 build, got %q", versionBytes)
	}
	brVersion := string(versionBytes)
	canonicalArgs := []string{"backup", "txn", "--storage=" + o.storage, "--backupts=" + strconv.FormatUint(o.backupTS, 10), "--crypter.method=" + encryption.Method}
	if encryption.Method == nativepitr.CipherMethodAES256CTR {
		canonicalArgs = append(canonicalArgs, "--crypter.key-id="+encryption.KeyID)
	}
	if _, err := nativepitr.BuildFullBackupAttestationWithEncryption(brVersion, brSHA, pdSHA, o.storage, o.backupTS, encryption, canonicalArgs, 1); err != nil {
		return err
	}
	args := []string{"backup", "txn", "--pd=" + strings.Join(addrs, ","), "--storage=" + o.storage, "--backupts=" + strconv.FormatUint(o.backupTS, 10), "--checksum=false", "--crypter.method=" + encryption.Method}
	if encryption.Method == nativepitr.CipherMethodAES256CTR {
		args = append(args, "--crypter.key-file="+runtimeEncryptionKeyFile)
	}
	args = append(args, "--log-file=/dev/stderr")
	if o.ca != "" {
		for _, path := range []string{o.ca, o.cert, o.key} {
			if !filepath.IsAbs(path) || strings.ContainsAny(path, "\x00\r\n") {
				return errors.New("TLS credential paths must be absolute and single-line")
			}
		}
		args = append(args, "--ca="+o.ca, "--cert="+o.cert, "--key="+o.key)
	}
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	if err := runner.Run(ctx, brFile, resolvedBR, args, logs, logs); err != nil {
		return fmt.Errorf("BR transactional full backup failed: %w", err)
	}
	if encryption.Method == nativepitr.CipherMethodAES256CTR {
		current, err := nativepitr.ReadAES256KeyFile(o.encryptionKeyFile)
		if err != nil || !bytes.Equal(current, encryptionKey) {
			return errors.New("encryption key changed during BR backup")
		}
	}
	receipt, err := nativepitr.BuildFullBackupAttestationWithEncryption(brVersion, brSHA, pdSHA, o.storage, o.backupTS, encryption, canonicalArgs, now().UTC().Unix())
	if err != nil {
		return err
	}
	return writeReceiptAtomic(o.output, receipt)
}

func canonicalPDAddresses(raw string) ([]string, string, error) {
	parts := strings.Split(raw, ",")
	if raw == "" || len(parts) == 0 {
		return nil, "", errors.New("pd-addrs is required")
	}
	seen := make(map[string]bool, len(parts))
	for _, addr := range parts {
		host, portText, err := net.SplitHostPort(addr)
		port, portErr := strconv.Atoi(portText)
		if err != nil || portErr != nil || host == "" || port < 1 || port > 65535 || strings.ContainsAny(addr, "\x00\r\n") || seen[addr] {
			return nil, "", errors.New("pd-addrs must contain unique host:port addresses")
		}
		seen[addr] = true
	}
	addrs := append([]string(nil), parts...)
	sort.Strings(addrs)
	encoded, _ := json.Marshal(addrs)
	digest := sha256.Sum256(encoded)
	return addrs, hex.EncodeToString(digest[:]), nil
}

func openExecutable(name string) (*os.File, string, string, error) {
	resolved, err := exec.LookPath(name)
	if err != nil {
		return nil, "", "", fmt.Errorf("resolve BR executable: %w", err)
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return nil, "", "", err
	}
	f, err := os.Open(resolved)
	if err != nil {
		return nil, "", "", fmt.Errorf("open BR executable: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return nil, "", "", errors.Join(err, f.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, "", "", errors.Join(errors.New("BR executable must be a regular file"), f.Close())
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, "", "", errors.Join(fmt.Errorf("hash BR executable: %w", err), f.Close())
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, "", "", errors.Join(fmt.Errorf("rewind BR executable: %w", err), f.Close())
	}
	return f, resolved, hex.EncodeToString(h.Sum(nil)), nil
}

func writeReceiptAtomic(path string, receipt nativepitr.FullBackupAttestation) (retErr error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".native-pitr-full-backup-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if removeErr := os.Remove(tmpName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, fmt.Errorf("remove full-backup attestation temporary file: %w", removeErr))
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return errors.Join(err, tmp.Close())
	}
	encoded, err := json.Marshal(receipt)
	var writeErr, syncErr error
	if err == nil {
		encoded = append(encoded, '\n')
		_, writeErr = tmp.Write(encoded)
	}
	if err == nil && writeErr == nil {
		syncErr = tmp.Sync()
	}
	if err := errors.Join(err, writeErr, syncErr, tmp.Close()); err != nil {
		return err
	}
	if err := os.Link(tmpName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.New("attestation-output appeared during backup")
		}
		return fmt.Errorf("publish full-backup attestation: %w", err)
	}
	if err := os.Remove(tmpName); err != nil {
		return fmt.Errorf("remove published attestation temporary link: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open full-backup attestation directory: %w", err)
	}
	syncErr = directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		syncErr = fmt.Errorf("sync full-backup attestation directory: %w", syncErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close full-backup attestation directory: %w", closeErr)
	}
	return errors.Join(syncErr, closeErr)
}
