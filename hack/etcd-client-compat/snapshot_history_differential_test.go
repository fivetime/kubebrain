package compat

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
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
	Create    bool
	Tombstone bool
}

func TestSnapshotRetainedHistoryMatchesReferenceEtcd(t *testing.T) {
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
				Value: string(kv.Value), Version: kv.Version,
				Create: kv.CreateRevision == mainRevision, Tombstone: tombstone,
			})
			return nil
		})
	}))
	return versions
}
