package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type txnIntervalOutcome struct {
	Name      string
	Code      string
	Message   string
	Succeeded bool
}

func TestTxnIntervalDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runTxnIntervalScenario(t, reference, "etcd"),
		runTxnIntervalScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runTxnIntervalScenario(t *testing.T, endpoint, instance string) []txnIntervalOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	client := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-txn-interval/%s/%d/", instance, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	tests := []struct {
		name    string
		success []*etcdserverpb.RequestOp
	}{
		{
			name: "from-key-delete-before-put-in-range",
			success: []*etcdserverpb.RequestOp{
				deleteRequestOp([]byte(prefix+"m"), []byte{0}),
				putRequestOp([]byte(prefix+"z"), "value"),
			},
		},
		{
			name: "put-before-from-key-delete-in-range",
			success: []*etcdserverpb.RequestOp{
				putRequestOp([]byte(prefix+"z"), "value"),
				deleteRequestOp([]byte(prefix+"m"), []byte{0}),
			},
		},
		{
			name: "from-key-delete-with-put-before-range",
			success: []*etcdserverpb.RequestOp{
				deleteRequestOp([]byte(prefix+"m"), []byte{0}),
				putRequestOp([]byte(prefix+"a"), "value"),
			},
		},
		{
			name: "empty-range-with-put-at-start",
			success: []*etcdserverpb.RequestOp{
				deleteRequestOp([]byte(prefix+"m"), []byte(prefix+"m")),
				putRequestOp([]byte(prefix+"m"), "value"),
			},
		},
		{
			name: "reversed-range-with-put-at-start",
			success: []*etcdserverpb.RequestOp{
				deleteRequestOp([]byte(prefix+"z"), []byte(prefix+"m")),
				putRequestOp([]byte(prefix+"z"), "value"),
			},
		},
	}

	outcomes := make([]txnIntervalOutcome, 0, len(tests))
	for _, test := range tests {
		resp, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{Success: test.success})
		outcome := txnIntervalOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		}
		if resp != nil {
			outcome.Succeeded = resp.Succeeded
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func deleteRequestOp(key, rangeEnd []byte) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
	}}
}
