package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/parameterbroker"
	"github.com/kubewharf/kubebrain/hack/production/internal/tlscertreload"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var address, namespace, audience, certFile, keyFile, kubeconfig string
	var certReloadInterval time.Duration
	flag.StringVar(&address, "listen-address", ":8443", "HTTPS listen address")
	flag.StringVar(&namespace, "namespace", "kubebrain-operations", "operation namespace")
	flag.StringVar(&audience, "token-audience", "kubebrain-operation-parameters", "required projected service account token audience")
	flag.StringVar(&certFile, "tls-cert-file", "", "HTTPS server certificate")
	flag.StringVar(&keyFile, "tls-key-file", "", "HTTPS server private key")
	flag.DurationVar(&certReloadInterval, "tls-reload-interval", 30*time.Second, "TLS certificate reload interval")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "optional kubeconfig; in-cluster credentials are used by default")
	flag.Parse()
	if certFile == "" || keyFile == "" {
		log.Fatal("--tls-cert-file and --tls-key-file are required")
	}
	certificate, err := tlscertreload.New(certFile, keyFile)
	if err != nil {
		log.Fatal(err)
	}
	if certReloadInterval <= 0 {
		log.Fatal("--tls-reload-interval must be positive")
	}
	config, err := kubernetesConfig(kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	config.UserAgent = "kubebrain-operation-parameter-broker"
	tokens, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	handler, err := parameterbroker.NewHandler(tokens, dynamicClient, namespace, audience)
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) {
		if err := certificate.ValidAt(time.Now()); err != nil {
			http.Error(response, "TLS certificate is not ready", http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/", handler)
	server := &http.Server{
		Addr: address, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: time.Minute,
		MaxHeaderBytes: 16 << 10,
		TLSConfig: &tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: certificate.GetCertificate,
		},
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		_ = certificate.Run(ctx, certReloadInterval, func(err error) {
			log.Printf("TLS certificate reload failed; retaining previous certificate: %v", err)
		})
	}()
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServeTLS("", "") }()
	select {
	case err = <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
			_ = server.Close()
		}
	}
}

func kubernetesConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if value := os.Getenv("KUBECONFIG"); value != "" {
		return clientcmd.BuildConfigFromFlags("", value)
	}
	return rest.InClusterConfig()
}
