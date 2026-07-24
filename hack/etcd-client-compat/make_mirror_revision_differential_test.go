package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type makeMirrorRevisionOutcome struct {
	HistoricalFinal           []string
	HistoricalUpdateAtomic    bool
	CompactedExitNonZero      bool
	CompactedMessageCanonical bool
	CompactedDestinationEmpty bool
}

func TestMakeMirrorRevisionAndCompactionDifferential(t *testing.T) {
	referenceEndpoint := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubeBrainEndpoint := os.Getenv("KUBEBRAIN_MIRROR_COMPACTION_ENDPOINT")
	if referenceEndpoint == "" || kubeBrainEndpoint == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_MIRROR_COMPACTION_ENDPOINT to disposable instances")
	}
	require.NotEqual(t, mirrorEndpointIdentity(referenceEndpoint), mirrorEndpointIdentity(kubeBrainEndpoint))
	if mainEndpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT"); mainEndpoint != "" {
		require.NotEqual(t, mirrorEndpointIdentity(mainEndpoint), mirrorEndpointIdentity(kubeBrainEndpoint),
			"KUBEBRAIN_MIRROR_COMPACTION_ENDPOINT must not be the shared main endpoint")
	}
	etcdctl := os.Getenv("ETCDCTL_BIN")
	if etcdctl == "" {
		etcdctl = "/root/etcd/bin/etcdctl"
	}
	if _, err := os.Stat(etcdctl); err != nil {
		t.Skipf("etcdctl binary unavailable: %v", err)
	}

	referenceToKubeBrain := runMakeMirrorRevisionScenario(
		t, etcdctl, referenceEndpoint, kubeBrainEndpoint, "revision-reference-to-kubebrain",
	)
	kubeBrainToReference := runMakeMirrorRevisionScenario(
		t, etcdctl, kubeBrainEndpoint, referenceEndpoint, "revision-kubebrain-to-reference",
	)
	want := makeMirrorRevisionOutcome{
		HistoricalFinal:           []string{"a=updated-a", "c=created-c"},
		HistoricalUpdateAtomic:    true,
		CompactedExitNonZero:      true,
		CompactedMessageCanonical: true,
		CompactedDestinationEmpty: true,
	}
	require.Equal(t, want, referenceToKubeBrain)
	require.Equal(t, referenceToKubeBrain, kubeBrainToReference)
}

func runMakeMirrorRevisionScenario(
	t *testing.T,
	etcdctl, sourceEndpoint, destinationEndpoint, direction string,
) makeMirrorRevisionOutcome {
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	base := fmt.Sprintf("/dbaas-make-mirror-revision/%s/%d/", direction, time.Now().UnixNano())
	historicalSourcePrefix := base + "historical-source/"
	historicalDestinationPrefix := base + "historical-destination/"
	compactedSourcePrefix := base + "compacted-source/"
	compactedDestinationPrefix := base + "compacted-destination/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, sourceCleanupErr := source.Delete(cleanupCtx, base, clientv3.WithPrefix())
		require.NoError(t, sourceCleanupErr)
		_, destinationCleanupErr := destination.Delete(cleanupCtx, base, clientv3.WithPrefix())
		require.NoError(t, destinationCleanupErr)
	})

	seed, err := source.Txn(ctx).Then(
		clientv3.OpPut(historicalSourcePrefix+"a", "seed-a"),
		clientv3.OpPut(historicalSourcePrefix+"b", "seed-b"),
		clientv3.OpPut(historicalSourcePrefix+"ignored", "base-only"),
	).Commit()
	require.NoError(t, err)
	require.True(t, seed.Succeeded)
	update, err := source.Txn(ctx).Then(
		clientv3.OpPut(historicalSourcePrefix+"a", "updated-a"),
		clientv3.OpDelete(historicalSourcePrefix+"b"),
		clientv3.OpPut(historicalSourcePrefix+"c", "created-c"),
	).Commit()
	require.NoError(t, err)
	require.True(t, update.Succeeded)

	stopMirror := startMirrorCommand(
		t,
		etcdctl,
		"--endpoints="+sourceEndpoint,
		"make-mirror",
		"--prefix="+historicalSourcePrefix,
		"--dest-prefix="+historicalDestinationPrefix,
		fmt.Sprintf("--rev=%d", update.Header.Revision),
		destinationEndpoint,
	)
	var historicalFinal []string
	historicalUpdateAtomic := false
	require.Eventually(t, func() bool {
		response, getErr := destination.Get(ctx, historicalDestinationPrefix, clientv3.WithPrefix())
		if getErr != nil || mirrorValues(response.Kvs, historicalDestinationPrefix) !=
			"a=updated-a,c=created-c" {
			return false
		}
		historicalFinal = mirrorValueSlice(response.Kvs, historicalDestinationPrefix)
		historicalUpdateAtomic = len(response.Kvs) == 2 &&
			response.Kvs[0].ModRevision == response.Kvs[1].ModRevision
		return historicalUpdateAtomic
	}, 10*time.Second, 20*time.Millisecond)
	stopMirror()

	compacted, err := source.Put(ctx, compactedSourcePrefix+"key", "compacted")
	require.NoError(t, err)
	newer, err := source.Put(ctx, compactedSourcePrefix+"newer", "newer")
	require.NoError(t, err)
	require.Greater(t, newer.Header.Revision, compacted.Header.Revision)
	_, err = source.Compact(ctx, newer.Header.Revision)
	require.NoError(t, err)

	commandCtx, commandCancel := context.WithTimeout(ctx, 10*time.Second)
	defer commandCancel()
	makeMirrorArgs := []string{
		"--endpoints=" + sourceEndpoint,
		"make-mirror",
		"--prefix=" + compactedSourcePrefix,
		"--dest-prefix=" + compactedDestinationPrefix,
		fmt.Sprintf("--rev=%d", compacted.Header.Revision),
		destinationEndpoint,
	}
	output, commandErr := runCompatCommandContext(t, commandCtx, etcdctl, makeMirrorArgs, nil)
	require.Error(t, commandErr, "make-mirror unexpectedly accepted compacted revision")
	require.NotErrorIs(t, commandCtx.Err(), context.DeadlineExceeded, "make-mirror hung at compacted revision")
	compactedMessageCanonical := strings.Contains(
		string(output),
		"etcdserver: mvcc: required revision has been compacted",
	)
	require.True(t, compactedMessageCanonical, "unexpected make-mirror error: %s", output)
	compactedDestination, err := destination.Get(ctx, compactedDestinationPrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	return makeMirrorRevisionOutcome{
		HistoricalFinal:           historicalFinal,
		HistoricalUpdateAtomic:    historicalUpdateAtomic,
		CompactedExitNonZero:      commandErr != nil,
		CompactedMessageCanonical: compactedMessageCanonical,
		CompactedDestinationEmpty: len(compactedDestination.Kvs) == 0,
	}
}
