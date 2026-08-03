package compat

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"google.golang.org/grpc/status"
)

type emptyKeyNamespaceOutcome struct {
	PointDeleteCode    string
	PointDeleteMessage string
	VisibleKeys        []string
	Deleted            int64
	Remaining          int
}

func TestEmptyKeyNamespaceDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run empty-key namespace differential tests")
	}

	want := runEmptyKeyNamespaceScenario(t, reference, "etcd")
	got := runEmptyKeyNamespaceScenario(t, compatEndpoint(t), "kubebrain")
	require.Equal(t, want, got)
}

func runEmptyKeyNamespaceScenario(t *testing.T, endpoint, instance string) emptyKeyNamespaceOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := string(bytes.Repeat([]byte{0xff}, 64)) +
		fmt.Sprintf("/dbaas-empty-key/%s/%d/", instance, time.Now().UnixNano())
	end := clientv3.GetPrefixRangeEnd(prefix)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithRange(end))
	})

	for _, key := range []string{"a", "b"} {
		_, err = client.Put(ctx, prefix+key, "value-"+key)
		require.NoError(t, err)
	}
	_, pointDeleteErr := client.Delete(ctx, "")

	namespaced := namespace.NewKV(client.KV, prefix)
	visible, err := namespaced.Get(ctx, "", clientv3.WithFromKey())
	require.NoError(t, err)
	visibleKeys := make([]string, len(visible.Kvs))
	for i, kv := range visible.Kvs {
		visibleKeys[i] = string(kv.Key)
	}
	deleted, err := namespaced.Delete(ctx, "", clientv3.WithFromKey())
	require.NoError(t, err)
	remaining, err := client.Get(ctx, prefix, clientv3.WithRange(end))
	require.NoError(t, err)

	return emptyKeyNamespaceOutcome{
		PointDeleteCode:    status.Code(pointDeleteErr).String(),
		PointDeleteMessage: status.Convert(pointDeleteErr).Message(),
		VisibleKeys:        visibleKeys,
		Deleted:            deleted.Deleted,
		Remaining:          len(remaining.Kvs),
	}
}
