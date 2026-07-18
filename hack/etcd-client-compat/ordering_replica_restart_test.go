package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/ordering"
)

type orderingReplicaRestartOutcome struct {
	ReplicaUIDChanged      bool
	LatestValueVisible     bool
	SerializableRevisionOK bool
	TxnRevisionOK          bool
	AllEndpointsMonotonic  bool
	OrderViolations        int32
}

func TestOrderingWrapperSurvivesReplicaRestart(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_ORDERING_ENDPOINTS")
	namespace := os.Getenv("KUBEBRAIN_ORDERING_NAMESPACE")
	victimPod := os.Getenv("KUBEBRAIN_ORDERING_VICTIM_POD")
	if rawEndpoints == "" || namespace == "" || victimPod == "" {
		t.Skip("set KUBEBRAIN_ORDERING_ENDPOINTS, KUBEBRAIN_ORDERING_NAMESPACE, and KUBEBRAIN_ORDERING_VICTIM_POD")
	}
	endpoints := strings.Split(rawEndpoints, ",")
	require.Len(t, endpoints, 3)
	kubeContext := os.Getenv("KUBEBRAIN_ORDERING_CONTEXT")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	writer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoints[0]}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoints[0]}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("/dbaas-ordering-restart/%d/", time.Now().UnixNano())
	key := prefix + "key"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, cleanupErr := writer.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cleanupErr)
	})

	seed, err := writer.Put(ctx, key, "seed")
	require.NoError(t, err)
	var violations atomic.Int32
	errOrderViolation := errors.New("ordering violation")
	orderedKV := ordering.NewKV(client.KV, func(clientv3.Op, clientv3.OpResponse, int64) error {
		violations.Add(1)
		return errOrderViolation
	})
	initial, err := orderedKV.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, seed.Header.Revision, initial.Header.Revision)

	oldUID := kubectlPodField(t, kubeContext, namespace, victimPod, "{.metadata.uid}")
	deleteArgs := kubectlContextArgs(kubeContext,
		"-n", namespace, "delete", "pod", victimPod, "--wait=true", "--timeout=60s")
	output, err := exec.CommandContext(ctx, "kubectl", deleteArgs...).CombinedOutput()
	require.NoError(t, err, string(output))

	var latestRevision int64
	const writes = 8
	for index := 0; index < writes; index++ {
		value := fmt.Sprintf("after-restart-%d", index)
		response, txnErr := writer.Txn(ctx).Then(
			clientv3.OpPut(key, value),
			clientv3.OpPut(prefix+"side", value),
		).Commit()
		require.NoError(t, txnErr)
		require.True(t, response.Succeeded)
		latestRevision = response.Header.Revision
	}

	var newUID string
	require.Eventually(t, func() bool {
		newUID = kubectlPodFieldNoFail(kubeContext, namespace, victimPod, "{.metadata.uid}")
		ready := kubectlPodFieldNoFail(
			kubeContext, namespace, victimPod,
			"{.status.containerStatuses[0].ready}",
		)
		return newUID != "" && newUID != oldUID && ready == "true"
	}, 90*time.Second, 500*time.Millisecond)

	client.SetEndpoints(endpoints[2])
	var restartedRead *clientv3.GetResponse
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		response, getErr := orderedKV.Get(callCtx, key, clientv3.WithSerializable())
		if getErr != nil {
			return false
		}
		restartedRead = response
		return len(response.Kvs) == 1 &&
			string(response.Kvs[0].Value) == "after-restart-7" &&
			response.Header.Revision >= latestRevision
	}, 30*time.Second, 200*time.Millisecond)

	txn, err := orderedKV.Txn(ctx).Then(
		clientv3.OpGet(key, clientv3.WithSerializable()),
		clientv3.OpGet(prefix+"side", clientv3.WithSerializable()),
	).Commit()
	require.NoError(t, err)
	require.Len(t, txn.Responses, 2)
	require.GreaterOrEqual(t, txn.Header.Revision, latestRevision)

	allEndpointsMonotonic := true
	for _, endpoint := range []string{endpoints[1], endpoints[0], endpoints[2]} {
		client.SetEndpoints(endpoint)
		var response *clientv3.GetResponse
		require.Eventually(t, func() bool {
			callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
			defer callCancel()
			var getErr error
			response, getErr = orderedKV.Get(callCtx, key, clientv3.WithSerializable())
			if getErr != nil {
				return false
			}
			return true
		}, 15*time.Second, 200*time.Millisecond)
		allEndpointsMonotonic = allEndpointsMonotonic &&
			response.Header.Revision >= latestRevision &&
			len(response.Kvs) == 1 &&
			string(response.Kvs[0].Value) == "after-restart-7"
	}

	outcome := orderingReplicaRestartOutcome{
		ReplicaUIDChanged:      newUID != oldUID,
		LatestValueVisible:     string(restartedRead.Kvs[0].Value) == "after-restart-7",
		SerializableRevisionOK: restartedRead.Header.Revision >= latestRevision,
		TxnRevisionOK:          txn.Header.Revision >= latestRevision,
		AllEndpointsMonotonic:  allEndpointsMonotonic,
		OrderViolations:        violations.Load(),
	}
	require.Equal(t, orderingReplicaRestartOutcome{
		ReplicaUIDChanged:      true,
		LatestValueVisible:     true,
		SerializableRevisionOK: true,
		TxnRevisionOK:          true,
		AllEndpointsMonotonic:  true,
	}, outcome)
}

func kubectlPodField(t *testing.T, kubeContext, namespace, pod, jsonPath string) string {
	t.Helper()
	value := kubectlPodFieldNoFail(kubeContext, namespace, pod, jsonPath)
	require.NotEmpty(t, value)
	return value
}

func kubectlPodFieldNoFail(kubeContext, namespace, pod, jsonPath string) string {
	args := kubectlContextArgs(kubeContext,
		"-n", namespace, "get", "pod", pod, "-o", "jsonpath="+jsonPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "kubectl", args...).Output()
	if err != nil {
		return ""
	}
	return string(output)
}

func kubectlContextArgs(kubeContext string, args ...string) []string {
	if kubeContext == "" {
		return args
	}
	return append([]string{"--context", kubeContext}, args...)
}
