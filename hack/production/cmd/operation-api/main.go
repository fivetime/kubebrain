package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationapi"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/internal/tlscertreload"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var inClusterConfig = rest.InClusterConfig

func main() {
	var address, namespace, issuer, audience, tenantClaim, instancesClaim string
	var certFile, keyFile, kubeconfig string
	var oidcCacheTTL, oidcRefreshBackoff, certReloadInterval, dependencyRequestTimeout time.Duration
	flag.StringVar(&address, "listen-address", ":8443", "HTTPS listen address")
	flag.StringVar(&namespace, "namespace", "kubebrain-operations", "operation namespace")
	flag.StringVar(&issuer, "oidc-issuer", "", "trusted OIDC issuer URL")
	flag.StringVar(&audience, "oidc-audience", "", "required OIDC audience")
	flag.StringVar(&tenantClaim, "oidc-tenant-claim", "tenant", "OIDC tenant claim")
	flag.StringVar(&instancesClaim, "oidc-instances-claim", "kubebrain_instances", "OIDC allowed instances claim")
	flag.DurationVar(&oidcCacheTTL, "oidc-jwks-cache-ttl", 5*time.Minute, "OIDC JWKS cache lifetime")
	flag.DurationVar(&oidcRefreshBackoff, "oidc-jwks-refresh-backoff", 5*time.Second, "fail-closed retry delay after a JWKS refresh failure or unknown key ID")
	flag.StringVar(&certFile, "tls-cert-file", "", "HTTPS server certificate")
	flag.StringVar(&keyFile, "tls-key-file", "", "HTTPS server private key")
	flag.DurationVar(&certReloadInterval, "tls-reload-interval", 30*time.Second, "TLS certificate reload interval")
	flag.DurationVar(&dependencyRequestTimeout, "dependency-request-timeout", 5*time.Second,
		"deadline for OIDC and Kubernetes Operation API requests")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "optional kubeconfig; in-cluster credentials are used by default")
	flag.Parse()
	if certFile == "" || keyFile == "" {
		log.Fatal("--tls-cert-file and --tls-key-file are required")
	}
	if err := namespaceinventory.ValidateOne(namespace); err != nil {
		log.Fatal("--namespace: ", err)
	}
	certificate, err := tlscertreload.New(certFile, keyFile)
	if err != nil {
		log.Fatal(err)
	}
	if certReloadInterval <= 0 {
		log.Fatal("--tls-reload-interval must be positive")
	}
	if dependencyRequestTimeout <= 0 {
		log.Fatal("--dependency-request-timeout must be positive")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	config, err := kubernetesConfig(kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	config.UserAgent = "kubebrain-operation-api"
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	authenticator, err := operationapi.NewOIDCAuthenticator(ctx, operationapi.OIDCConfig{
		Issuer: issuer, Audience: audience, TenantClaim: tenantClaim, InstancesClaim: instancesClaim,
		CacheTTL: oidcCacheTTL, RefreshBackoff: oidcRefreshBackoff,
	})
	if err != nil {
		log.Fatal(err)
	}
	handler, err := operationapi.NewHandler(
		authenticator, operationqueue.New(client, namespace), dependencyRequestTimeout,
	)
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", readyzHandler(certificate, handler))
	mux.Handle("/", handler)
	server := newHTTPServer(address, mux, certificate)
	go func() {
		_ = certificate.Run(ctx, certReloadInterval, func(err error) {
			log.Printf("TLS certificate reload failed; retaining previous certificate: %v", err)
		})
	}()
	errs := make(chan error, 1)
	go func() {
		errs <- server.ListenAndServeTLS("", "")
	}()
	select {
	case err = <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
			_ = server.Close()
		}
	}
}

type serverCertificate interface {
	GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error)
}

func newHTTPServer(address string, handler http.Handler, certificate serverCertificate) *http.Server {
	return &http.Server{
		Addr: address, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
		MaxHeaderBytes: 32 << 10,
		TLSConfig: &tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: certificate.GetCertificate,
		},
	}
}

type readyCertificate interface {
	ValidAt(time.Time) error
}

type readyDependency interface {
	Ready(context.Context) error
}

func readyzHandler(certificate readyCertificate, dependency readyDependency) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Cache-Control", "no-store")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		if request.Method != http.MethodGet {
			response.Header().Set("Allow", http.MethodGet)
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := certificate.ValidAt(time.Now()); err != nil {
			http.Error(response, "TLS certificate is not ready", http.StatusServiceUnavailable)
			return
		}
		if err := dependency.Ready(request.Context()); err != nil {
			log.Printf("readiness Operation API probe failed: %v", err)
			http.Error(response, "Kubernetes Operation API is not ready", http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	}
}

func kubernetesConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if config, err := inClusterConfig(); err == nil {
		return config, nil
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loading, &clientcmd.ConfigOverrides{},
	).ClientConfig()
}
