package main

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/tlscertreload"
)

const maxAlertBody = 1 << 20
const maxPolicyOutput = 8 << 10

type policyRunner interface {
	Run(context.Context, []byte) error
}

type commandRunner struct {
	executable string
	tempDir    string
}

type cappedWriter struct {
	w         io.Writer
	remaining int
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	originalLength := len(p)
	if len(p) > w.remaining {
		p = p[:w.remaining]
	}
	if len(p) != 0 {
		written, err := w.w.Write(p)
		w.remaining -= written
		if err != nil {
			return written, err
		}
	}
	return originalLength, nil
}

func (r commandRunner) Run(ctx context.Context, body []byte) error {
	f, err := os.CreateTemp(r.tempDir, "alert-*.json")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(body)
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	cmd := exec.CommandContext(ctx, r.executable)
	cmd.Env = append(os.Environ(), "ALERT_INPUT="+name)
	var output strings.Builder
	limitedOutput := &cappedWriter{w: &output, remaining: maxPolicyOutput}
	cmd.Stdout = limitedOutput
	cmd.Stderr = limitedOutput
	if err = cmd.Run(); err != nil {
		return fmt.Errorf("policy runner failed: %w: %s", err, strings.TrimSpace(output.String()))
	}
	return nil
}

func requestHasBody(r *http.Request) bool {
	return r.ContentLength != 0 || len(r.TransferEncoding) != 0
}

func handler(token []byte, runner policyRunner, timeout time.Duration, concurrency int) http.Handler {
	semaphore := make(chan struct{}, concurrency)
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.RawQuery != "" || requestHasBody(r) {
			http.Error(w, "malformed readiness request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/api/v1/alerts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.RawQuery != "" {
			http.Error(w, "query is not allowed", http.StatusBadRequest)
			return
		}
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if len(provided) != len(token) || subtle.ConstantTimeCompare([]byte(provided), token) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || !strings.EqualFold(mediaType, "application/json") {
			http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAlertBody))
		if err != nil {
			http.Error(w, "alert payload is too large", http.StatusRequestEntityTooLarge)
			return
		}
		if len(body) == 0 {
			http.Error(w, "alert payload is empty", http.StatusBadRequest)
			return
		}
		select {
		case semaphore <- struct{}{}:
			defer func() { <-semaphore }()
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "receiver is busy", http.StatusTooManyRequests)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err = runner.Run(ctx, body); err != nil {
			log.Printf("repair alert rejected: %v", err)
			http.Error(w, "alert policy rejected payload", http.StatusUnprocessableEntity)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	return mux
}

func main() {
	var address, certFile, keyFile, tokenFile, executable, tempDir string
	var timeout, reload time.Duration
	flag.StringVar(&address, "listen-address", ":8443", "HTTPS listen address")
	flag.StringVar(&certFile, "tls-cert-file", "", "server TLS certificate")
	flag.StringVar(&keyFile, "tls-key-file", "", "server TLS key")
	flag.StringVar(&tokenFile, "bearer-token-file", "", "Alertmanager bearer token file")
	flag.StringVar(&executable, "policy-executable", "", "repair request policy executable")
	flag.StringVar(&tempDir, "temp-dir", "/tmp", "private temporary directory")
	flag.DurationVar(&timeout, "policy-timeout", 30*time.Second, "policy execution timeout")
	flag.DurationVar(&reload, "tls-reload-interval", 30*time.Second, "TLS reload interval")
	flag.Parse()
	if certFile == "" || keyFile == "" || tokenFile == "" || executable == "" || timeout <= 0 || reload <= 0 {
		log.Fatal("TLS, bearer token, policy executable, and positive timeouts are required")
	}
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		log.Fatal(err)
	}
	token = []byte(strings.TrimSpace(string(token)))
	if len(token) < 32 {
		log.Fatal("bearer token must contain at least 32 non-space bytes")
	}
	info, err := os.Stat(executable)
	if err != nil || info.Mode()&0111 == 0 {
		log.Fatal("policy executable is not executable")
	}
	if info, err = os.Stat(tempDir); err != nil || !info.IsDir() || !filepath.IsAbs(tempDir) {
		log.Fatal("temp-dir must be an existing absolute directory")
	}
	certificate, err := tlscertreload.New(certFile, keyFile)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() { _ = certificate.Run(ctx, reload, func(err error) { log.Printf("TLS reload failed: %v", err) }) }()
	server := &http.Server{Addr: address, Handler: handler(token, commandRunner{executable, tempDir}, timeout, 1), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: timeout + 5*time.Second, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 16 << 10, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: certificate.GetCertificate}}
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServeTLS("", "") }()
	select {
	case err = <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}
}
