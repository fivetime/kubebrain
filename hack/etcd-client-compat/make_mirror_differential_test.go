package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type makeMirrorOutcome struct {
	BaseCopied              bool
	Final                   []string
	UpdateAppliedAtomically bool
}

func TestMakeMirrorBidirectionalDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run make-mirror differential tests")
	}
	etcdctl := os.Getenv("ETCDCTL_BIN")
	if etcdctl == "" {
		etcdctl = "/root/etcd/bin/etcdctl"
	}
	if _, err := os.Stat(etcdctl); err != nil {
		t.Skipf("etcdctl binary unavailable: %v", err)
	}

	referenceToKubeBrain := runMakeMirrorDirection(
		t, etcdctl, reference, compatEndpoint(t), "reference-to-kubebrain",
	)
	kubeBrainToReference := runMakeMirrorDirection(
		t, etcdctl, compatEndpoint(t), reference, "kubebrain-to-reference",
	)
	want := makeMirrorOutcome{
		BaseCopied:              true,
		Final:                   []string{"a=updated-a", "c=created-c"},
		UpdateAppliedAtomically: true,
	}
	require.Equal(t, want, referenceToKubeBrain)
	require.Equal(t, referenceToKubeBrain, kubeBrainToReference)
}

func TestMakeMirrorPaginatedBaseDifferentialFromKubeBrain(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run paginated make-mirror tests")
	}
	etcdctl := os.Getenv("ETCDCTL_BIN")
	if etcdctl == "" {
		etcdctl = "/root/etcd/bin/etcdctl"
	}
	if _, err := os.Stat(etcdctl); err != nil {
		t.Skipf("etcdctl binary unavailable: %v", err)
	}

	source, err := clientv3.New(clientv3.Config{
		Endpoints: []string{compatEndpoint(t)}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	destination, err := clientv3.New(clientv3.Config{
		Endpoints: []string{reference}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, destination.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	base := fmt.Sprintf("/dbaas-make-mirror/paginated/%d/", time.Now().UnixNano())
	sourcePrefix := base + "source/"
	destinationPrefix := base + "destination/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		_, sourceCleanupErr := source.Delete(cleanupCtx, base, clientv3.WithPrefix())
		require.NoError(t, sourceCleanupErr)
		_, destinationCleanupErr := destination.Delete(cleanupCtx, base, clientv3.WithPrefix())
		require.NoError(t, destinationCleanupErr)
	})

	const keyCount = 1001
	for start := 0; start < keyCount; start += 100 {
		end := start + 100
		if end > keyCount {
			end = keyCount
		}
		ops := make([]clientv3.Op, 0, end-start)
		for index := start; index < end; index++ {
			key := fmt.Sprintf("%skey-%04d", sourcePrefix, index)
			ops = append(ops, clientv3.OpPut(key, fmt.Sprintf("value-%04d", index)))
		}
		response, txnErr := source.Txn(ctx).Then(ops...).Commit()
		require.NoError(t, txnErr)
		require.True(t, response.Succeeded)
	}

	stopMirror := startMakeMirror(t, etcdctl, compatEndpoint(t), reference, sourcePrefix, destinationPrefix)
	require.Eventually(t, func() bool {
		response, getErr := destination.Get(ctx, destinationPrefix, clientv3.WithPrefix())
		if getErr != nil || len(response.Kvs) != keyCount {
			return false
		}
		values := mirrorValueSlice(response.Kvs, destinationPrefix)
		return values[0] == "key-0000=value-0000" &&
			values[keyCount-1] == "key-1000=value-1000"
	}, 30*time.Second, 50*time.Millisecond)
	stopMirror()
}

func TestMirrorValueSlicePreservesRangeOrder(t *testing.T) {
	kvs := []*mvccpb.KeyValue{
		{Key: []byte("/mirror/b"), Value: []byte("second")},
		{Key: []byte("/mirror/a"), Value: []byte("first")},
	}
	require.Equal(t, []string{"b=second", "a=first"}, mirrorValueSlice(kvs, "/mirror/"))
}

func runMakeMirrorDirection(t *testing.T, etcdctl, sourceEndpoint, destinationEndpoint, direction string) makeMirrorOutcome {
	t.Helper()
	source, err := clientv3.New(clientv3.Config{
		Endpoints: []string{sourceEndpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	destination, err := clientv3.New(clientv3.Config{
		Endpoints: []string{destinationEndpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, destination.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	base := fmt.Sprintf("/dbaas-make-mirror/%s/%d/", direction, time.Now().UnixNano())
	sourcePrefix := base + "source/"
	destinationPrefix := base + "destination/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, sourceCleanupErr := source.Delete(cleanupCtx, base, clientv3.WithPrefix())
		require.NoError(t, sourceCleanupErr)
		_, destinationCleanupErr := destination.Delete(cleanupCtx, base, clientv3.WithPrefix())
		require.NoError(t, destinationCleanupErr)
	})

	seed, err := source.Txn(ctx).Then(
		clientv3.OpPut(sourcePrefix+"a", "seed-a"),
		clientv3.OpPut(sourcePrefix+"b", "seed-b"),
	).Commit()
	require.NoError(t, err)
	require.True(t, seed.Succeeded)

	stopMirror := startMakeMirror(t, etcdctl, sourceEndpoint, destinationEndpoint, sourcePrefix, destinationPrefix)

	baseCopied := false
	require.Eventually(t, func() bool {
		response, getErr := destination.Get(ctx, destinationPrefix, clientv3.WithPrefix())
		if getErr != nil {
			return false
		}
		baseCopied = mirrorValues(response.Kvs, destinationPrefix) ==
			"a=seed-a,b=seed-b"
		return baseCopied
	}, 10*time.Second, 20*time.Millisecond)

	update, err := source.Txn(ctx).Then(
		clientv3.OpPut(sourcePrefix+"a", "updated-a"),
		clientv3.OpDelete(sourcePrefix+"b"),
		clientv3.OpPut(sourcePrefix+"c", "created-c"),
	).Commit()
	require.NoError(t, err)
	require.True(t, update.Succeeded)

	var final []string
	updateAppliedAtomically := false
	require.Eventually(t, func() bool {
		response, getErr := destination.Get(ctx, destinationPrefix, clientv3.WithPrefix())
		if getErr != nil || mirrorValues(response.Kvs, destinationPrefix) !=
			"a=updated-a,c=created-c" {
			return false
		}
		final = mirrorValueSlice(response.Kvs, destinationPrefix)
		updateAppliedAtomically = len(response.Kvs) == 2 &&
			response.Kvs[0].ModRevision == response.Kvs[1].ModRevision
		return updateAppliedAtomically
	}, 10*time.Second, 20*time.Millisecond)

	stopMirror()
	return makeMirrorOutcome{
		BaseCopied:              baseCopied,
		Final:                   final,
		UpdateAppliedAtomically: updateAppliedAtomically,
	}
}

func startMakeMirror(t *testing.T, etcdctl, sourceEndpoint, destinationEndpoint, sourcePrefix, destinationPrefix string) func() {
	t.Helper()
	return startMirrorCommand(t, etcdctl,
		"--endpoints="+sourceEndpoint,
		"make-mirror",
		"--prefix="+sourcePrefix,
		"--dest-prefix="+destinationPrefix,
		destinationEndpoint,
	)
}

func startMirrorCommand(t *testing.T, commandName string, args ...string) func() {
	t.Helper()
	return startCompatCommand(t, commandName, args...)
}

func mirrorValues(kvs []*mvccpb.KeyValue, prefix string) string {
	return strings.Join(mirrorValueSlice(kvs, prefix), ",")
}

func mirrorValueSlice(kvs []*mvccpb.KeyValue, prefix string) []string {
	values := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		values = append(values, strings.TrimPrefix(string(kv.Key), prefix)+"="+string(kv.Value))
	}
	return values
}
