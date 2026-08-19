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
	if err := validateAuditPrefix(prefix); err != nil {
		log.Fatal(err)
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
	clusterID, leaseID, grantedTTL, grantRevision, err := validateAuditGrant(lease, 60)
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
		revokeResponse, revokeErr := client.Revoke(revokeCtx, leaseID)
		if revokeErr == nil {
			revokeErr = validateAuditRevoke(revokeResponse, clusterID, grantRevision)
		}
		if revokeErr != nil {
			log.Printf("revoke audit lease %d: %v", leaseID, revokeErr)
		}
	}()

	put, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, token, clientv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		log.Fatalf("conditionally create audit key: %v", err)
	}
	putRevision, err := validateAuditPut(put, clusterID, grantRevision)
	if err != nil {
		log.Fatalf("conditionally create audit key: %v", err)
	}

	read, err := client.Get(ctx, key)
	if err != nil {
		log.Fatalf("linearizable read audit key: %v", err)
	}
	readRevision, err := validateAuditRead(read, clusterID, putRevision, []byte(key), []byte(token), leaseID)
	if err != nil {
		log.Fatalf("linearizable read audit key: %v", err)
	}

	deleteResponse, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", token)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		log.Fatalf("conditionally delete audit key: %v", err)
	}
	deleteRevision, err := validateAuditDelete(deleteResponse, clusterID, readRevision)
	if err != nil {
		log.Fatalf("conditionally delete audit key: %v", err)
	}
	absent, err := client.Get(ctx, key)
	if err != nil {
		log.Fatalf("confirm audit key deletion: %v", err)
	}
	absentRevision, err := validateAuditAbsent(absent, clusterID, deleteRevision)
	if err != nil {
		log.Fatalf("confirm audit key deletion: %v", err)
	}
	revokeResponse, err := client.Revoke(ctx, leaseID)
	if err != nil {
		log.Fatalf("revoke audit lease: %v", err)
	}
	if err := validateAuditRevoke(revokeResponse, clusterID, absentRevision); err != nil {
		log.Fatalf("revoke audit lease: %v", err)
	}
	revoked = true

	output := result{
		Format: "kubebrain.etcd-audit-probe.v1", PutRevision: putRevision,
		ReadRevision: readRevision, DeleteRevision: deleteRevision,
		LeaseTTL: grantedTTL,
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(output); err != nil {
		log.Fatal(fmt.Errorf("encode audit result: %w", err))
	}
}

func validateAuditPrefix(prefix string) error {
	if prefix == "" || !strings.HasPrefix(prefix, "/") || containsControlCharacter(prefix) {
		return fmt.Errorf("AUDIT_PREFIX must be an absolute key prefix without control characters")
	}
	trimmed := strings.TrimRight(prefix, "/")
	if trimmed == "" || trimmed == "/registry" || strings.HasPrefix(trimmed+"/", "/registry/") {
		return fmt.Errorf("AUDIT_PREFIX must not target root or Kubernetes /registry data")
	}
	return nil
}

func containsControlCharacter(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool {
		return r < 0x20 || r == 0x7f
	}) >= 0
}
