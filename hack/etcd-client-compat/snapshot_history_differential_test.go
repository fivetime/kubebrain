package compat

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type normalizedSnapshotVersion struct {
	Value     string
	Version   int64
	Lease     int64
	Create    bool
	Tombstone bool
}

func TestSnapshotRetainedHistoryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT")
	}
	key := []byte(fmt.Sprintf("/compat/snapshot-history/%d", time.Now().UnixNano()))
	want := []normalizedSnapshotVersion{
		{Value: "v1", Version: 1, Create: true},
		{Value: "v2", Version: 2},
		{Tombstone: true},
		{Value: "v3", Version: 1, Create: true},
	}
	for name, endpoint := range map[string]string{"reference": reference, "kubebrain": kubebrain} {
		t.Run(name, func(t *testing.T) {
			client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
			require.NoError(t, err)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err = client.Put(ctx, string(key), "v1")
			require.NoError(t, err)
			_, err = client.Put(ctx, string(key), "v2")
			require.NoError(t, err)
			_, err = client.Delete(ctx, string(key))
			require.NoError(t, err)
			_, err = client.Put(ctx, string(key), "v3")
			require.NoError(t, err)
			defer func() { _, _ = client.Delete(context.Background(), string(key)) }()

			backendBytes := downloadSnapshotBackend(t, endpoint)
			path := t.TempDir() + "/snapshot.db"
			require.NoError(t, os.WriteFile(path, backendBytes, 0o600))
			require.Equal(t, want, snapshotVersionsForKey(t, path, key))
		})
	}
}

func TestSnapshotRetainedLeaseHistoryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT")
	}
	key := []byte(fmt.Sprintf("/compat/snapshot-lease-history/%d", time.Now().UnixNano()))
	leaseA := time.Now().UnixNano() & ((1 << 62) - 1)
	if leaseA == 0 {
		leaseA = 1
	}
	leaseB := leaseA + 1
	want := []normalizedSnapshotVersion{
		{Value: "v1", Version: 1, Lease: leaseA, Create: true},
		{Value: "v2", Version: 2},
		{Value: "v3", Version: 3, Lease: leaseB},
		{Tombstone: true},
		{Value: "v4", Version: 1, Lease: leaseA, Create: true},
	}
	for name, endpoint := range map[string]string{"reference": reference, "kubebrain": kubebrain} {
		t.Run(name, func(t *testing.T) {
			client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
			require.NoError(t, err)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			rawLease := etcdserverpb.NewLeaseClient(client.ActiveConnection())
			for _, id := range []int64{leaseA, leaseB} {
				_, err = rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
				require.NoError(t, err)
				defer func(id int64) {
					_, _ = rawLease.LeaseRevoke(context.Background(), &etcdserverpb.LeaseRevokeRequest{ID: id})
				}(id)
			}
			_, err = client.Put(ctx, string(key), "v1", clientv3.WithLease(clientv3.LeaseID(leaseA)))
			require.NoError(t, err)
			_, err = client.Put(ctx, string(key), "v2")
			require.NoError(t, err)
			_, err = client.Put(ctx, string(key), "v3", clientv3.WithLease(clientv3.LeaseID(leaseB)))
			require.NoError(t, err)
			_, err = client.Delete(ctx, string(key))
			require.NoError(t, err)
			_, err = client.Put(ctx, string(key), "v4", clientv3.WithLease(clientv3.LeaseID(leaseA)))
			require.NoError(t, err)
			defer func() { _, _ = client.Delete(context.Background(), string(key)) }()

			backendBytes := downloadSnapshotBackend(t, endpoint)
			path := filepath.Join(t.TempDir(), "snapshot.db")
			require.NoError(t, os.WriteFile(path, backendBytes, 0o600))
			require.Equal(t, want, snapshotVersionsForKey(t, path, key))
		})
	}
}

func TestKubeBrainSnapshotLeaseHistoryRestoresIntoOfficialEtcd(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	etcdutl := os.Getenv("ETCDUTL_BINARY")
	etcd := os.Getenv("REFERENCE_ETCD_BINARY")
	if endpoint == "" || etcdutl == "" || etcd == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT, ETCDUTL_BINARY, and REFERENCE_ETCD_BINARY")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := []byte(fmt.Sprintf("/compat/snapshot-official-restore/%d", time.Now().UnixNano()))
	leaseID := time.Now().UnixNano() & ((1 << 62) - 1)
	if leaseID == 0 {
		leaseID = 1
	}
	rawLease := etcdserverpb.NewLeaseClient(client.ActiveConnection())
	_, err = rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: leaseID, TTL: 300})
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, string(key))
		_, _ = rawLease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	}()
	first, err := client.Put(ctx, string(key), "leased-v1", clientv3.WithLease(clientv3.LeaseID(leaseID)))
	require.NoError(t, err)
	second, err := client.Put(ctx, string(key), "unleased-v2")
	require.NoError(t, err)
	latest, err := client.Put(ctx, string(key), "leased-v3", clientv3.WithLease(clientv3.LeaseID(leaseID)))
	require.NoError(t, err)

	snapshotPath := filepath.Join(t.TempDir(), "kubebrain-snapshot.db")
	require.NoError(t, os.WriteFile(snapshotPath, downloadSnapshotBackend(t, endpoint), 0o600))
	restoredDir := filepath.Join(t.TempDir(), "restored.etcd")
	const restoredEndpoint = "127.0.0.1:42479"
	restore := exec.CommandContext(ctx, etcdutl, "snapshot", "restore", snapshotPath,
		"--skip-hash-check", "--data-dir", restoredDir, "--name", "a3537-restored",
		"--initial-cluster", "a3537-restored=http://127.0.0.1:42480",
		"--initial-advertise-peer-urls", "http://127.0.0.1:42480")
	output, err := restore.CombinedOutput()
	require.NoError(t, err, string(output))
	stop := startCompatCommand(t, etcd,
		"--name", "a3537-restored", "--data-dir", restoredDir,
		"--listen-client-urls", "http://"+restoredEndpoint,
		"--advertise-client-urls", "http://"+restoredEndpoint,
		"--listen-peer-urls", "http://127.0.0.1:42480",
		"--initial-advertise-peer-urls", "http://127.0.0.1:42480",
		"--initial-cluster", "a3537-restored=http://127.0.0.1:42480")
	defer stop()
	conn := newRawCompatConn(t, restoredEndpoint)
	defer conn.Close()
	kv := etcdserverpb.NewKVClient(conn)
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer callCancel()
		_, callErr := kv.Range(callCtx, &etcdserverpb.RangeRequest{Key: key})
		return callErr == nil
	}, 10*time.Second, 50*time.Millisecond)

	for _, tc := range []struct {
		revision, lease int64
		value           string
	}{
		{revision: first.Header.Revision, value: "leased-v1", lease: leaseID},
		{revision: second.Header.Revision, value: "unleased-v2"},
		{revision: latest.Header.Revision, value: "leased-v3", lease: leaseID},
	} {
		response, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: tc.revision})
		require.NoError(t, rangeErr)
		require.Len(t, response.Kvs, 1)
		require.Equal(t, tc.value, string(response.Kvs[0].Value))
		require.Equal(t, tc.lease, response.Kvs[0].Lease)
	}
	ttl, err := etcdserverpb.NewLeaseClient(conn).LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{
		ID: leaseID, Keys: true,
	})
	require.NoError(t, err)
	require.Positive(t, ttl.TTL)
	require.LessOrEqual(t, ttl.TTL, int64(300))
	require.Equal(t, int64(300), ttl.GrantedTTL)
	require.Equal(t, [][]byte{key}, ttl.Keys)

	watch, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	defer watch.CloseSend()
	require.NoError(t, watch.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: key, StartRevision: first.Header.Revision, PrevKv: true,
		},
	}}))
	var events []*mvccpb.Event
	for len(events) < 3 {
		response, recvErr := watch.Recv()
		require.NoError(t, recvErr)
		require.Zero(t, response.CompactRevision)
		events = append(events, response.Events...)
	}
	require.Len(t, events, 3)
	for i, want := range []struct {
		value     string
		lease     int64
		prevValue string
		prevLease int64
	}{
		{value: "leased-v1", lease: leaseID},
		{value: "unleased-v2", prevValue: "leased-v1", prevLease: leaseID},
		{value: "leased-v3", lease: leaseID, prevValue: "unleased-v2"},
	} {
		require.Equal(t, mvccpb.PUT, events[i].Type)
		require.Equal(t, string(key), string(events[i].Kv.Key))
		require.Equal(t, want.value, string(events[i].Kv.Value))
		require.Equal(t, want.lease, events[i].Kv.Lease)
		if i == 0 {
			require.Nil(t, events[i].PrevKv)
			continue
		}
		require.NotNil(t, events[i].PrevKv)
		require.Equal(t, want.prevValue, string(events[i].PrevKv.Value))
		require.Equal(t, want.prevLease, events[i].PrevKv.Lease)
	}
}

func TestSnapshotTxnSubrevisionOrderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT")
	}
	base := fmt.Sprintf("/compat/snapshot-subrevision/%d/", time.Now().UnixNano())
	keys := [][]byte{[]byte(base + "z"), []byte(base + "a"), []byte(base + "m")}
	for name, endpoint := range map[string]string{"reference": reference, "kubebrain": kubebrain} {
		t.Run(name, func(t *testing.T) {
			client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
			require.NoError(t, err)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			response, err := client.Txn(ctx).Then(
				clientv3.OpPut(string(keys[0]), "z"),
				clientv3.OpPut(string(keys[1]), "a"),
				clientv3.OpPut(string(keys[2]), "m"),
			).Commit()
			require.NoError(t, err)
			defer func() { _, _ = client.Delete(context.Background(), base, clientv3.WithPrefix()) }()
			backendBytes := downloadSnapshotBackend(t, endpoint)
			path := t.TempDir() + "/snapshot.db"
			require.NoError(t, os.WriteFile(path, backendBytes, 0o600))
			require.Equal(t, keys, snapshotKeysAtRevision(t, path, response.Header.Revision, keys))
		})
	}
}

func TestSnapshotOmitsRedundantRevisionMarker(t *testing.T) {
	path := os.Getenv("KUBEBRAIN_SNAPSHOT_ARTIFACT")
	if path == "" {
		t.Skip("set KUBEBRAIN_SNAPSHOT_ARTIFACT")
	}
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	var maxRevision int64
	var realAtMax, markersAtMax int
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("key")).ForEach(func(revisionKey, value []byte) error {
			revision := int64(binary.BigEndian.Uint64(revisionKey[:8]))
			var kv mvccpb.KeyValue
			if err := proto.Unmarshal(value, &kv); err != nil {
				return err
			}
			if revision > maxRevision {
				maxRevision, realAtMax, markersAtMax = revision, 0, 0
			}
			if revision != maxRevision {
				return nil
			}
			if len(kv.Key) == 0 {
				markersAtMax++
			} else {
				realAtMax++
			}
			return nil
		})
	}))
	require.NotZero(t, maxRevision)
	require.Positive(t, realAtMax, "artifact must exercise a real event at its snapshot revision")
	require.Zero(t, markersAtMax, "a real row already pins currentRev; an empty restore marker is redundant")
}

func downloadSnapshotBackend(t *testing.T, endpoint string) []byte {
	t.Helper()
	target := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := etcdserverpb.NewMaintenanceClient(conn).Snapshot(ctx, &etcdserverpb.SnapshotRequest{})
	require.NoError(t, err)
	var responses []*etcdserverpb.SnapshotResponse
	for {
		response, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		require.NoError(t, recvErr)
		responses = append(responses, response)
	}
	require.GreaterOrEqual(t, len(responses), 2)
	var out []byte
	for _, response := range responses[:len(responses)-1] {
		out = append(out, response.Blob...)
	}
	return out
}

func snapshotVersionsForKey(t *testing.T, path string, key []byte) []normalizedSnapshotVersion {
	t.Helper()
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	var versions []normalizedSnapshotVersion
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("key"))
		require.NotNil(t, bucket)
		return bucket.ForEach(func(revisionKey, value []byte) error {
			var kv mvccpb.KeyValue
			if err := proto.Unmarshal(value, &kv); err != nil {
				return err
			}
			if !bytes.Equal(kv.Key, key) {
				return nil
			}
			tombstone := len(revisionKey) == 18 && revisionKey[17] == 't'
			mainRevision := int64(binary.BigEndian.Uint64(revisionKey[:8]))
			versions = append(versions, normalizedSnapshotVersion{
				Value: string(kv.Value), Version: kv.Version, Lease: kv.Lease,
				Create: kv.CreateRevision == mainRevision, Tombstone: tombstone,
			})
			return nil
		})
	}))
	return versions
}

func snapshotKeysAtRevision(t *testing.T, path string, revision int64, wanted [][]byte) [][]byte {
	t.Helper()
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	wantedSet := make(map[string]struct{}, len(wanted))
	for _, key := range wanted {
		wantedSet[string(key)] = struct{}{}
	}
	var keys [][]byte
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("key")).ForEach(func(revisionKey, value []byte) error {
			if int64(binary.BigEndian.Uint64(revisionKey[:8])) != revision {
				return nil
			}
			var kv mvccpb.KeyValue
			if err := proto.Unmarshal(value, &kv); err != nil {
				return err
			}
			if _, ok := wantedSet[string(kv.Key)]; ok {
				keys = append(keys, append([]byte(nil), kv.Key...))
			}
			return nil
		})
	}))
	return keys
}
