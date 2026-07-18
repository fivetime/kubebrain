package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type result struct {
	Format         string `json:"format"`
	PutRevision    int64  `json:"put_revision"`
	ReadRevision   int64  `json:"read_revision"`
	DeleteRevision int64  `json:"delete_revision"`
	LeaseTTL       int64  `json:"lease_ttl"`
}

func main() {
	prefix := os.Getenv("AUDIT_PREFIX")
	if prefix == "" {
		prefix = "/__kubebrain/audit"
	}
	if !strings.HasPrefix(prefix, "/") || strings.ContainsAny(prefix, "\n\r\t") {
		log.Fatal("AUDIT_PREFIX must be an absolute key prefix without control characters")
	}
	nonce := make([]byte, 24)
	if _, err := rand.Read(nonce); err != nil {
		log.Fatalf("generate audit nonce: %v", err)
	}
	token := hex.EncodeToString(nonce)
	key := strings.TrimSuffix(prefix, "/") + "/" + token

	client, err := etcdutil.NewClientFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	timeout, err := etcdutil.TimeoutFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	lease, err := client.Grant(ctx, 60)
	if err != nil {
		log.Fatalf("grant audit lease: %v", err)
	}
	revoked := false
	defer func() {
		if revoked {
			return
		}
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), timeout)
		defer revokeCancel()
		if _, revokeErr := client.Revoke(revokeCtx, lease.ID); revokeErr != nil {
			log.Printf("revoke audit lease %d: %v", lease.ID, revokeErr)
		}
	}()

	put, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, token, clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil {
		log.Fatalf("conditionally create audit key: %v", err)
	}
	if !put.Succeeded || put.Header == nil || put.Header.Revision <= 0 {
		log.Fatal("audit key unexpectedly existed or put returned no revision")
	}

	read, err := client.Get(ctx, key)
	if err != nil {
		log.Fatalf("linearizable read audit key: %v", err)
	}
	if read.Header == nil || read.Header.Revision < put.Header.Revision || len(read.Kvs) != 1 ||
		string(read.Kvs[0].Value) != token || read.Kvs[0].Lease != int64(lease.ID) {
		log.Fatal("audit read did not observe the leased value written by the probe")
	}

	deleteResponse, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", token)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		log.Fatalf("conditionally delete audit key: %v", err)
	}
	if !deleteResponse.Succeeded || deleteResponse.Header == nil ||
		deleteResponse.Header.Revision < read.Header.Revision {
		log.Fatal("audit delete compare failed or returned an invalid revision")
	}
	absent, err := client.Get(ctx, key)
	if err != nil {
		log.Fatalf("confirm audit key deletion: %v", err)
	}
	if len(absent.Kvs) != 0 || absent.Header == nil ||
		absent.Header.Revision < deleteResponse.Header.Revision {
		log.Fatal("audit key remained visible after conditional deletion")
	}
	if _, err := client.Revoke(ctx, lease.ID); err != nil {
		log.Fatalf("revoke audit lease: %v", err)
	}
	revoked = true

	output := result{
		Format: "kubebrain.etcd-audit-probe.v1", PutRevision: put.Header.Revision,
		ReadRevision: read.Header.Revision, DeleteRevision: deleteResponse.Header.Revision,
		LeaseTTL: lease.TTL,
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(output); err != nil {
		log.Fatal(fmt.Errorf("encode audit result: %w", err))
	}
}
