package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kubernetesCallOptionsOutcome struct {
	GetCode             codes.Code
	GetReceiveLimit     bool
	OptimisticPutCode   codes.Code
	PutSendLimit        bool
	RejectedPutNotSaved bool
}

// TestKubernetesClientCallOptionsDifferentialAgainstReferenceEtcd pins
// upstream etcd 2bcaed1e0. The Kubernetes wrapper must use the high-level KV
// client so Config's default gRPC call options are not bypassed.
func TestKubernetesClientCallOptionsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := kubernetesCallOptionsOutcome{
		GetCode: codes.ResourceExhausted, GetReceiveLimit: true,
		OptimisticPutCode: codes.ResourceExhausted, PutSendLimit: true,
		RejectedPutNotSaved: true,
	}
	referenceOutcome := runKubernetesCallOptionsScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runKubernetesCallOptionsScenario(t, compatEndpoint(t), "kubebrain"))
}

func runKubernetesCallOptionsScenario(t *testing.T, endpoint, instance string) kubernetesCallOptionsOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := "/dbaas-kubernetes-call-options/" + instance + "/"
	largeGetKey := prefix + "large-get"
	rejectedPutKey := prefix + "rejected-put"

	seed, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, seed.Close()) })
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = seed.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	_, err = seed.Put(ctx, largeGetKey, strings.Repeat("g", 4096))
	require.NoError(t, err)

	receiveLimited, err := kubernetes.New(clientv3.Config{
		Endpoints:          []string{endpoint},
		DialTimeout:        5 * time.Second,
		MaxCallRecvMsgSize: 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, receiveLimited.Close()) })
	_, getErr := receiveLimited.Kubernetes.Get(ctx, largeGetKey, kubernetes.GetOptions{})

	sendLimited, err := kubernetes.New(clientv3.Config{
		Endpoints:          []string{endpoint},
		DialTimeout:        5 * time.Second,
		MaxCallSendMsgSize: 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sendLimited.Close()) })
	_, putErr := sendLimited.Kubernetes.OptimisticPut(
		ctx, rejectedPutKey, []byte(strings.Repeat("p", 4096)), 0, kubernetes.PutOptions{},
	)
	rejected, err := seed.Get(ctx, rejectedPutKey)
	require.NoError(t, err)

	return kubernetesCallOptionsOutcome{
		GetCode:             status.Code(getErr),
		GetReceiveLimit:     strings.Contains(status.Convert(getErr).Message(), "received message larger than max"),
		OptimisticPutCode:   status.Code(putErr),
		PutSendLimit:        strings.Contains(status.Convert(putErr).Message(), "trying to send message larger than max"),
		RejectedPutNotSaved: len(rejected.Kvs) == 0,
	}
}
