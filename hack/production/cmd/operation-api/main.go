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

	"github.com/kubewharf/kubebrain/hack/production/internal/operationapi"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var address, namespace, issuer, audience, tenantClaim, instancesClaim string
	var certFile, keyFile, kubeconfig string
	flag.StringVar(&address, "listen-address", ":8443", "HTTPS listen address")
	flag.StringVar(&namespace, "namespace", "kubebrain-operations", "operation namespace")
	flag.StringVar(&issuer, "oidc-issuer", "", "trusted OIDC issuer URL")
	flag.StringVar(&audience, "oidc-audience", "", "required OIDC audience")
	flag.StringVar(&tenantClaim, "oidc-tenant-claim", "tenant", "OIDC tenant claim")
	flag.StringVar(&instancesClaim, "oidc-instances-claim", "kubebrain_instances", "OIDC allowed instances claim")
	flag.StringVar(&certFile, "tls-cert-file", "", "HTTPS server certificate")
	flag.StringVar(&keyFile, "tls-key-file", "", "HTTPS server private key")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "optional kubeconfig; in-cluster credentials are used by default")
	flag.Parse()
	if certFile == "" || keyFile == "" {
		log.Fatal("--tls-cert-file and --tls-key-file are required")
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
	})
	if err != nil {
		log.Fatal(err)
	}
	handler, err := operationapi.NewHandler(authenticator, operationqueue.New(client, namespace))
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{
		Addr: address, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
		MaxHeaderBytes: 32 << 10,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	errs := make(chan error, 1)
	go func() {
		errs <- server.ListenAndServeTLS(certFile, keyFile)
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

func kubernetesConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if value := os.Getenv("KUBECONFIG"); value != "" {
		return clientcmd.BuildConfigFromFlags("", value)
	}
	return rest.InClusterConfig()
}
