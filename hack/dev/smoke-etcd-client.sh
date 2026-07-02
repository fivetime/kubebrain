#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

cd "$WORK_DIR"
go mod init kubebrain-smoke >/dev/null
go get go.etcd.io/etcd/client/v3@v3.5.2 >/dev/null

cat > main.go <<'GOEOF'
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const futureRevisionOffset = int64(1_000_000_000)

func must(label string, err error) {
	if err != nil {
		panic(fmt.Sprintf("%s: %v", label, err))
	}
}

func main() {
	endpoint := os.Getenv("ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:3379"
	}

	timeoutSeconds := 120
	if raw := os.Getenv("SMOKE_TIMEOUT_SECONDS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		must("parse SMOKE_TIMEOUT_SECONDS", err)
		if parsed <= 0 {
			panic("SMOKE_TIMEOUT_SECONDS must be positive")
		}
		timeoutSeconds = parsed
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 3 * time.Second,
	})
	must("create etcd client", err)
	defer cli.Close()

	prefix := fmt.Sprintf("/registry/smoke/%d", time.Now().UnixNano())
	key := prefix + "/key"
	createResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "v1", clientv3.WithPrevKV())).
		Commit()
	must("create txn", err)
	if !createResp.Succeeded ||
		len(createResp.Responses) != 1 ||
		createResp.Responses[0].GetResponsePut().PrevKv != nil {
		panic("expected create txn with prev kv to succeed with nil prev kv")
	}
	fmt.Printf("create succeeded=%v rev=%d\n", createResp.Succeeded, createResp.Header.Revision)

	getResp, err := cli.Get(ctx, key)
	must("get", err)
	if getResp.Count != 1 {
		panic(fmt.Sprintf("expected one key, got %d", getResp.Count))
	}
	fmt.Printf("get value=%s modrev=%d\n", getResp.Kvs[0].Value, getResp.Kvs[0].ModRevision)

	updateResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", getResp.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(key, "v2", clientv3.WithPrevKV())).
		Else(clientv3.OpGet(key)).
		Commit()
	must("update txn", err)
	if !updateResp.Succeeded ||
		len(updateResp.Responses) != 1 ||
		updateResp.Responses[0].GetResponsePut().PrevKv == nil ||
		string(updateResp.Responses[0].GetResponsePut().PrevKv.Value) != "v1" {
		panic("expected update txn to return prev kv")
	}
	fmt.Printf("update succeeded=%v rev=%d\n", updateResp.Succeeded, updateResp.Header.Revision)

	directKey := prefix + "/direct"
	directPutResp, err := cli.Put(ctx, directKey, "direct-v1")
	must("direct put create", err)
	fmt.Printf("direct put create rev=%d\n", directPutResp.Header.Revision)

	directOverwriteResp, err := cli.Put(ctx, directKey, "direct-v2", clientv3.WithPrevKV())
	must("direct put overwrite", err)
	fmt.Printf("direct put overwrite prev=%s rev=%d\n",
		directOverwriteResp.PrevKv.Value,
		directOverwriteResp.Header.Revision)

	casDeleteKey := prefix + "/cas-delete"
	casDeletePutResp, err := cli.Put(ctx, casDeleteKey, "delete-me")
	must("cas delete seed put", err)
	casDeleteResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(casDeleteKey), "=", casDeletePutResp.Header.Revision)).
		Then(clientv3.OpDelete(casDeleteKey, clientv3.WithPrevKV())).
		Commit()
	must("cas delete txn", err)
	if !casDeleteResp.Succeeded ||
		len(casDeleteResp.Responses) != 1 ||
		casDeleteResp.Responses[0].GetResponseDeleteRange().Deleted != 1 ||
		len(casDeleteResp.Responses[0].GetResponseDeleteRange().PrevKvs) != 1 ||
		string(casDeleteResp.Responses[0].GetResponseDeleteRange().PrevKvs[0].Value) != "delete-me" {
		panic("expected cas delete txn to return deleted prev kv")
	}
	staleDeleteResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(casDeleteKey), "=", casDeletePutResp.Header.Revision)).
		Then(clientv3.OpDelete(casDeleteKey)).
		Commit()
	must("stale cas delete txn", err)
	if staleDeleteResp.Succeeded || len(staleDeleteResp.Responses) != 0 {
		panic("expected stale cas delete txn to fail without responses")
	}
	fmt.Println("cas delete ok")

	createConflictResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(directKey), "=", 0)).
		Then(clientv3.OpPut(directKey, "should-not-write")).
		Else(clientv3.OpGet(directKey)).
		Commit()
	must("create conflict txn with fallback get", err)
	if createConflictResp.Succeeded || len(createConflictResp.Responses) != 1 ||
		len(createConflictResp.Responses[0].GetResponseRange().Kvs) != 1 ||
		string(createConflictResp.Responses[0].GetResponseRange().Kvs[0].Value) != "direct-v2" {
		panic("expected create conflict txn to return existing key")
	}
	fmt.Printf("create conflict fallback value=%s\n",
		createConflictResp.Responses[0].GetResponseRange().Kvs[0].Value)

	leaseResp, err := cli.Grant(ctx, 30)
	must("lease grant", err)
	_, err = cli.Put(ctx, prefix+"/lease", "leased", clientv3.WithLease(leaseResp.ID))
	must("lease put", err)
	leaseCompareResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.LeaseValue(prefix+"/lease"), "=", int64(leaseResp.ID))).
		Then(clientv3.OpPut(prefix+"/lease", "lease-matched", clientv3.WithLease(leaseResp.ID)), clientv3.OpGet(prefix+"/lease")).
		Else(clientv3.OpPut(prefix+"/lease", "lease-not-matched", clientv3.WithLease(leaseResp.ID))).
		Commit()
	must("lease compare txn", err)
	if !leaseCompareResp.Succeeded ||
		len(leaseCompareResp.Responses) != 2 ||
		string(leaseCompareResp.Responses[1].GetResponseRange().Kvs[0].Value) != "lease-matched" {
		panic("unexpected lease compare txn response")
	}
	fmt.Println("lease compare txn ok")
	_, err = cli.Put(ctx, prefix+"/lease", "ignore-lease-updated", clientv3.WithIgnoreLease())
	must("put ignore lease", err)
	leaseResp2, err := cli.Grant(ctx, 30)
	must("second lease grant", err)
	_, err = cli.Put(ctx, prefix+"/lease", "", clientv3.WithLease(leaseResp2.ID), clientv3.WithIgnoreValue())
	must("put ignore value", err)
	ignoreValueGet, err := cli.Get(ctx, prefix+"/lease")
	must("get after ignore value", err)
	if len(ignoreValueGet.Kvs) != 1 || string(ignoreValueGet.Kvs[0].Value) != "ignore-lease-updated" {
		panic("expected ignore value put to preserve current value")
	}
	keepAliveResp, err := cli.KeepAliveOnce(ctx, leaseResp.ID)
	must("lease keepalive", err)
	fmt.Printf("lease keepalive id=%d ttl=%d\n", keepAliveResp.ID, keepAliveResp.TTL)
	ttlResp, err := cli.TimeToLive(ctx, leaseResp.ID, clientv3.WithAttachedKeys())
	must("lease ttl", err)
	if len(ttlResp.Keys) != 0 {
		panic("expected ignore value put to move key off original lease")
	}
	fmt.Printf("lease ttl granted=%d keys=%d\n", ttlResp.GrantedTTL, len(ttlResp.Keys))
	ttlResp2, err := cli.TimeToLive(ctx, leaseResp2.ID, clientv3.WithAttachedKeys())
	must("second lease ttl", err)
	if len(ttlResp2.Keys) != 1 || string(ttlResp2.Keys[0]) != prefix+"/lease" {
		panic("expected ignore value put to bind key to second lease")
	}
	fmt.Printf("second lease ttl granted=%d keys=%d\n", ttlResp2.GrantedTTL, len(ttlResp2.Keys))
	_, err = cli.Revoke(ctx, leaseResp2.ID)
	must("second lease revoke", err)
	_, err = cli.Revoke(ctx, leaseResp.ID)
	must("lease revoke", err)

	watchCtx, cancelWatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWatch()
	watchPrefix := prefix + "/watch"
	watchCh := cli.Watch(watchCtx, watchPrefix, clientv3.WithPrefix())

	watchKey := watchPrefix + "/item"
	_, err = cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(watchKey), "=", 0)).
		Then(clientv3.OpPut(watchKey, "event")).
		Commit()
	must("watch put txn", err)

	var watchResp clientv3.WatchResponse
	for resp := range watchCh {
		if resp.Err() != nil {
			panic(resp.Err())
		}
		if len(resp.Events) == 0 {
			continue
		}
		watchResp = resp
		break
	}
	if len(watchResp.Events) == 0 {
		panic("expected watch event")
	}
	fmt.Printf("watch event type=%s key=%s value=%s\n",
		watchResp.Events[0].Type.String(),
		watchResp.Events[0].Kv.Key,
		watchResp.Events[0].Kv.Value)

	watchUpdateKey := watchPrefix + "/update-kind"
	watchUpdateSeed, err := cli.Put(ctx, watchUpdateKey, "before")
	must("watch update seed", err)
	watchUpdateCtx, cancelWatchUpdate := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWatchUpdate()
	watchUpdateCh := cli.Watch(watchUpdateCtx, watchUpdateKey, clientv3.WithRev(watchUpdateSeed.Header.Revision+1), clientv3.WithPrevKV())
	_, err = cli.Put(ctx, watchUpdateKey, "after")
	must("watch update put", err)
	var watchUpdateResp clientv3.WatchResponse
	for resp := range watchUpdateCh {
		if resp.Err() != nil {
			panic(resp.Err())
		}
		if len(resp.Events) == 0 {
			continue
		}
		watchUpdateResp = resp
		break
	}
	if len(watchUpdateResp.Events) != 1 {
		panic("expected one watch update event")
	}
	updateEvent := watchUpdateResp.Events[0]
	if updateEvent.Type != clientv3.EventTypePut ||
		updateEvent.IsCreate() ||
		updateEvent.PrevKv == nil ||
		string(updateEvent.PrevKv.Value) != "before" ||
		string(updateEvent.Kv.Value) != "after" {
		prevValue := "<nil>"
		if updateEvent.PrevKv != nil {
			prevValue = string(updateEvent.PrevKv.Value)
		}
		panic(fmt.Sprintf("expected watch update event to be PUT/MODIFIED with prev kv: type=%s isCreate=%v value=%s prev=%s mod=%d create=%d",
			updateEvent.Type.String(),
			updateEvent.IsCreate(),
			string(updateEvent.Kv.Value),
			prevValue,
			updateEvent.Kv.ModRevision,
			updateEvent.Kv.CreateRevision))
	}
	fmt.Println("watch update prev kv ok")

	exactWatchCtx, cancelExactWatch := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelExactWatch()
	exactWatchKey := watchPrefix + "/exact"
	exactWatchCh := cli.Watch(exactWatchCtx, exactWatchKey)
	_, err = cli.Put(ctx, exactWatchKey+"/child", "child")
	must("watch exact child put", err)
	select {
	case resp := <-exactWatchCh:
		if resp.Err() != nil {
			panic(resp.Err())
		}
		if len(resp.Events) > 0 {
			panic(fmt.Sprintf("exact watch unexpectedly received child key %s", resp.Events[0].Kv.Key))
		}
	case <-time.After(500 * time.Millisecond):
	}
	_, err = cli.Put(ctx, exactWatchKey, "exact")
	must("watch exact put", err)
	var exactWatchResp clientv3.WatchResponse
	for resp := range exactWatchCh {
		if resp.Err() != nil {
			panic(resp.Err())
		}
		if len(resp.Events) == 0 {
			continue
		}
		exactWatchResp = resp
		break
	}
	if len(exactWatchResp.Events) != 1 || string(exactWatchResp.Events[0].Kv.Key) != exactWatchKey {
		panic("expected exact watch to receive only exact key")
	}
	fmt.Println("watch exact key filtering ok")

	rangeWatchCtx, cancelRangeWatch := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelRangeWatch()
	rangeWatchStart := watchPrefix + "/range/a"
	rangeWatchEnd := watchPrefix + "/range/c"
	rangeWatchCh := cli.Watch(rangeWatchCtx, rangeWatchStart, clientv3.WithRange(rangeWatchEnd))
	_, err = cli.Put(ctx, watchPrefix+"/range/d", "outside")
	must("watch range outside put", err)
	select {
	case resp := <-rangeWatchCh:
		if resp.Err() != nil {
			panic(resp.Err())
		}
		if len(resp.Events) > 0 {
			panic(fmt.Sprintf("range watch unexpectedly received outside key %s", resp.Events[0].Kv.Key))
		}
	case <-time.After(500 * time.Millisecond):
	}
	_, err = cli.Put(ctx, watchPrefix+"/range/b", "inside")
	must("watch range inside put", err)
	var rangeWatchResp clientv3.WatchResponse
	for resp := range rangeWatchCh {
		if resp.Err() != nil {
			panic(resp.Err())
		}
		if len(resp.Events) == 0 {
			continue
		}
		rangeWatchResp = resp
		break
	}
	if len(rangeWatchResp.Events) != 1 || string(rangeWatchResp.Events[0].Kv.Key) != watchPrefix+"/range/b" {
		panic("expected range watch to receive key inside [a,c)")
	}
	fmt.Println("watch arbitrary range filtering ok")

	filterWatchCtx, cancelFilterWatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFilterWatch()
	filterWatchKey := watchPrefix + "/filter-put"
	filterWatchCh := cli.Watch(filterWatchCtx, filterWatchKey, clientv3.WithFilterPut())
	_, err = cli.Put(ctx, filterWatchKey, "put-filtered")
	must("watch filter put seed", err)
	_, err = cli.Delete(ctx, filterWatchKey)
	must("watch filter put delete", err)
	var filterWatchResp clientv3.WatchResponse
	for resp := range filterWatchCh {
		if resp.Err() != nil {
			panic(resp.Err())
		}
		if len(resp.Events) == 0 {
			continue
		}
		filterWatchResp = resp
		break
	}
	if len(filterWatchResp.Events) != 1 || filterWatchResp.Events[0].Type != clientv3.EventTypeDelete {
		panic("expected watch WithFilterPut to deliver only delete event")
	}
	fmt.Println("watch filter put ok")

	rangePrefix := prefix + "/range/"
	for _, rangeKey := range []string{rangePrefix + "a", rangePrefix + "b"} {
		_, err = cli.Put(ctx, rangeKey, rangeKey)
		must("direct range seed put", err)
	}
	rangeFilterOld, err := cli.Put(ctx, rangePrefix+"filter-old", "old")
	must("range filter old put", err)
	rangeFilterNew, err := cli.Put(ctx, rangePrefix+"filter-new", "new")
	must("range filter new put", err)
	sortedResp, err := cli.Get(ctx, rangePrefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	must("sorted range", err)
	if len(sortedResp.Kvs) != 4 ||
		string(sortedResp.Kvs[0].Key) != rangePrefix+"filter-old" ||
		string(sortedResp.Kvs[1].Key) != rangePrefix+"filter-new" ||
		string(sortedResp.Kvs[2].Key) != rangePrefix+"b" ||
		string(sortedResp.Kvs[3].Key) != rangePrefix+"a" {
		panic("expected key-desc sorted range response")
	}
	fmt.Println("sorted range ok")
	createSortedResp, err := cli.Get(ctx, rangePrefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend))
	must("range create revision sort", err)
	if len(createSortedResp.Kvs) != 4 ||
		createSortedResp.Kvs[0].CreateRevision > createSortedResp.Kvs[1].CreateRevision ||
		createSortedResp.Kvs[1].CreateRevision > createSortedResp.Kvs[2].CreateRevision ||
		createSortedResp.Kvs[2].CreateRevision > createSortedResp.Kvs[3].CreateRevision {
		panic("expected create revision sorted range response")
	}
	versionSortedResp, err := cli.Get(ctx, rangePrefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByVersion, clientv3.SortAscend))
	must("range version sort", err)
	if len(versionSortedResp.Kvs) != 4 ||
		versionSortedResp.Kvs[0].Version > versionSortedResp.Kvs[1].Version ||
		versionSortedResp.Kvs[1].Version > versionSortedResp.Kvs[2].Version ||
		versionSortedResp.Kvs[2].Version > versionSortedResp.Kvs[3].Version {
		panic("expected version sorted range response")
	}
	fmt.Println("create/version sorted range ok")
	emptyRangeResp, err := cli.Get(ctx, rangePrefix+"a", clientv3.WithRange(rangePrefix+"a"))
	must("empty same-bound range", err)
	if emptyRangeResp.Count != 0 || len(emptyRangeResp.Kvs) != 0 || emptyRangeResp.More {
		panic(fmt.Sprintf("expected empty same-bound range, got count=%d kvs=%d more=%v", emptyRangeResp.Count, len(emptyRangeResp.Kvs), emptyRangeResp.More))
	}
	reverseDeleteResp, err := cli.Delete(ctx, rangePrefix+"b", clientv3.WithRange(rangePrefix+"a"), clientv3.WithPrevKV())
	must("empty reverse range delete", err)
	if reverseDeleteResp.Deleted != 0 || len(reverseDeleteResp.PrevKvs) != 0 {
		panic(fmt.Sprintf("expected empty reverse range delete, got deleted=%d prev=%d", reverseDeleteResp.Deleted, len(reverseDeleteResp.PrevKvs)))
	}
	bAfterReverseDelete, err := cli.Get(ctx, rangePrefix+"b")
	must("range b after reverse delete", err)
	if bAfterReverseDelete.Count != 1 || len(bAfterReverseDelete.Kvs) != 1 {
		panic("expected reverse range delete to preserve range b key")
	}
	emptyTxnDeleteResp, err := cli.Txn(ctx).
		Then(clientv3.OpDelete(rangePrefix+"b", clientv3.WithRange(rangePrefix+"b"), clientv3.WithPrevKV())).
		Commit()
	must("empty same-bound txn delete", err)
	if !emptyTxnDeleteResp.Succeeded ||
		len(emptyTxnDeleteResp.Responses) != 1 ||
		emptyTxnDeleteResp.Responses[0].GetResponseDeleteRange().Deleted != 0 ||
		len(emptyTxnDeleteResp.Responses[0].GetResponseDeleteRange().PrevKvs) != 0 {
		panic("expected empty same-bound txn delete to delete nothing")
	}
	bAfterTxnDelete, err := cli.Get(ctx, rangePrefix+"b")
	must("range b after empty txn delete", err)
	if bAfterTxnDelete.Count != 1 || len(bAfterTxnDelete.Kvs) != 1 {
		panic("expected empty txn delete to preserve range b key")
	}
	fmt.Println("empty non-from-key range and delete ok")
	_, err = cli.Get(ctx, rangePrefix, clientv3.WithPrefix(), clientv3.WithRev(sortedResp.Header.Revision+futureRevisionOffset))
	if err == nil {
		panic("expected future revision range to fail")
	}
	fmt.Printf("future revision range failed as expected: %v\n", err)
	filterResp, err := cli.Get(ctx, rangePrefix, clientv3.WithPrefix(), clientv3.WithMinModRev(rangeFilterNew.Header.Revision))
	must("range min mod revision filter", err)
	if filterResp.Count != 1 || len(filterResp.Kvs) != 1 || string(filterResp.Kvs[0].Key) != rangePrefix+"filter-new" {
		panic("expected min mod revision filter to return only newest key")
	}
	countFilterResp, err := cli.Get(ctx, rangePrefix, clientv3.WithPrefix(), clientv3.WithMaxModRev(rangeFilterOld.Header.Revision), clientv3.WithCountOnly())
	must("range max mod revision count filter", err)
	if countFilterResp.Count != 3 || len(countFilterResp.Kvs) != 0 {
		panic(fmt.Sprintf("expected max mod revision count filter count=3 kvs=0, got count=%d kvs=%d", countFilterResp.Count, len(countFilterResp.Kvs)))
	}
	fmt.Println("range mod revision filters ok")
	keysOnlyResp, err := cli.Get(ctx, rangePrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	must("keys only range", err)
	if keysOnlyResp.Count != 4 || len(keysOnlyResp.Kvs) != 4 {
		panic(fmt.Sprintf("expected four keys-only kvs, got count=%d len=%d", keysOnlyResp.Count, len(keysOnlyResp.Kvs)))
	}
	for _, kv := range keysOnlyResp.Kvs {
		if len(kv.Value) != 0 {
			panic(fmt.Sprintf("expected keys-only response to omit value for key %s", kv.Key))
		}
	}
	fmt.Println("keys only range ok")
	rangeDeleteResp, err := cli.Delete(ctx,
		rangePrefix,
		clientv3.WithRange(prefix+"/range0"),
		clientv3.WithPrevKV())
	must("direct range delete", err)
	fmt.Printf("direct range delete deleted=%d prev=%d\n",
		rangeDeleteResp.Deleted,
		len(rangeDeleteResp.PrevKvs))

	statusResp, err := cli.Status(ctx, endpoint)
	must("maintenance status", err)
	fmt.Printf("maintenance status version=%s rev=%d\n", statusResp.Version, statusResp.Header.Revision)

	memberResp, err := cli.MemberList(ctx)
	must("cluster member list", err)
	if len(memberResp.Members) == 0 {
		panic("expected at least one cluster member")
	}
	fmt.Printf("cluster member list members=%d\n", len(memberResp.Members))

	hashResp, err := cli.HashKV(ctx, endpoint, 0)
	must("maintenance hashkv", err)
	fmt.Printf("maintenance hashkv hash=%d compact=%d\n", hashResp.Hash, hashResp.CompactRevision)

	compactRev := statusResp.Header.Revision
	if compactRev > 0 {
		compactResp, err := cli.Compact(ctx, compactRev)
		must("maintenance compact", err)
		fmt.Printf("maintenance compact rev=%d\n", compactResp.Header.Revision)
		_, err = cli.Get(ctx, rangePrefix+"filter-old", clientv3.WithRev(rangeFilterOld.Header.Revision))
		if err == nil {
			panic("expected compacted revision range to fail")
		}
		fmt.Printf("compacted revision range failed as expected: %v\n", err)
		compactedWatchCtx, compactedWatchCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer compactedWatchCancel()
		compactedWatchCh := cli.Watch(compactedWatchCtx, rangePrefix+"filter-old", clientv3.WithRev(rangeFilterOld.Header.Revision))
		var compactedWatchResp clientv3.WatchResponse
		for resp := range compactedWatchCh {
			if resp.Err() == nil && !resp.Canceled {
				continue
			}
			compactedWatchResp = resp
			break
		}
		if compactedWatchResp.Err() == nil {
			panic("expected compacted watch error")
		}
		if !compactedWatchResp.Canceled {
			panic("expected compacted watch response to be canceled")
		}
		if compactedWatchResp.CompactRevision != compactRev {
			panic(fmt.Sprintf("expected compacted watch compact revision %d, got %d", compactRev, compactedWatchResp.CompactRevision))
		}
		fmt.Printf("compacted watch failed as expected: compact=%d err=%v\n",
			compactedWatchResp.CompactRevision,
			compactedWatchResp.Err())
	}

	alarmResp, err := cli.AlarmList(ctx)
	must("maintenance alarm list", err)
	fmt.Printf("maintenance alarms=%d\n", len(alarmResp.Alarms))

	_, err = cli.Defragment(ctx, endpoint)
	must("maintenance defragment", err)
	fmt.Println("maintenance defragment ok")

	multiKey := prefix + "/multi"
	multiDeleteKey := prefix + "/multi-delete"
	_, err = cli.Put(ctx, multiDeleteKey, "delete-me")
	must("simple multi-op txn delete seed", err)
	multiResp, err := cli.Txn(ctx).
		Then(clientv3.OpPut(multiKey, "multi-v1"), clientv3.OpGet(multiKey), clientv3.OpDelete(multiDeleteKey, clientv3.WithPrevKV())).
		Commit()
	must("simple multi-op txn", err)
	if !multiResp.Succeeded || len(multiResp.Responses) != 3 {
		panic("expected simple multi-op txn to return three success responses")
	}
	if string(multiResp.Responses[1].GetResponseRange().Kvs[0].Value) != "multi-v1" ||
		multiResp.Responses[2].GetResponseDeleteRange().Deleted != 1 ||
		string(multiResp.Responses[2].GetResponseDeleteRange().PrevKvs[0].Value) != "delete-me" {
		panic("unexpected simple multi-op txn response")
	}
	fmt.Println("simple multi-op txn ok")

	emptyTxnResp, err := cli.Txn(ctx).Commit()
	must("empty txn", err)
	if !emptyTxnResp.Succeeded || len(emptyTxnResp.Responses) != 0 {
		panic("unexpected empty txn response")
	}
	fmt.Println("empty txn ok")

	nestedKey := prefix + "/nested"
	nestedTxn := clientv3.OpTxn(
		nil,
		[]clientv3.Op{
			clientv3.OpPut(nestedKey, "nested-v1"),
			clientv3.OpGet(nestedKey),
		},
		nil,
	)
	nestedResp, err := cli.Txn(ctx).Then(nestedTxn).Commit()
	must("nested txn", err)
	if !nestedResp.Succeeded ||
		len(nestedResp.Responses) != 1 ||
		!nestedResp.Responses[0].GetResponseTxn().Succeeded ||
		len(nestedResp.Responses[0].GetResponseTxn().Responses) != 2 ||
		string(nestedResp.Responses[0].GetResponseTxn().Responses[1].GetResponseRange().Kvs[0].Value) != "nested-v1" {
		panic("unexpected nested txn response")
	}
	fmt.Println("nested txn ok")

	nestedPathCompareKey := prefix + "/nested-path/compare"
	nestedPathResultKey := prefix + "/nested-path/result"
	nestedPathTxn := clientv3.OpTxn(
		[]clientv3.Cmp{
			clientv3.Compare(clientv3.Value(nestedPathCompareKey), "=", "outer"),
		},
		[]clientv3.Op{clientv3.OpPut(nestedPathResultKey, "nested-success")},
		[]clientv3.Op{clientv3.OpPut(nestedPathResultKey, "nested-failure")},
	)
	nestedPathResp, err := cli.Txn(ctx).
		Then(clientv3.OpPut(nestedPathCompareKey, "outer"), nestedPathTxn).
		Commit()
	must("nested txn compare path", err)
	if !nestedPathResp.Succeeded ||
		len(nestedPathResp.Responses) != 2 ||
		nestedPathResp.Responses[1].GetResponseTxn().Succeeded {
		panic("unexpected nested txn compare path response")
	}
	nestedPathGet, err := cli.Get(ctx, nestedPathResultKey)
	must("nested txn compare path get", err)
	if len(nestedPathGet.Kvs) != 1 || string(nestedPathGet.Kvs[0].Value) != "nested-failure" {
		panic("expected nested txn compare path to be computed before outer put")
	}
	fmt.Println("nested txn compare path ok")

	futureTxnKey := prefix + "/txn-future-rev"
	futureTxnStatus, err := cli.Status(ctx, endpoint)
	must("future txn status", err)
	_, err = cli.Txn(ctx).
		Then(
			clientv3.OpPut(futureTxnKey, "should-not-write"),
			clientv3.OpGet(futureTxnKey, clientv3.WithRev(futureTxnStatus.Header.Revision+futureRevisionOffset)),
		).
		Commit()
	if err == nil {
		panic("expected txn range at future revision to fail")
	}
	futureTxnGet, err := cli.Get(ctx, futureTxnKey)
	must("future txn get", err)
	if len(futureTxnGet.Kvs) != 0 {
		panic("expected txn future revision failure to prevent prior put")
	}
	fmt.Println("txn future revision precheck ok")

	compareValueKey := prefix + "/compare-value"
	_, err = cli.Put(ctx, compareValueKey, "expected")
	must("compare value seed put", err)
	compareValueResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(compareValueKey), "=", "expected")).
		Then(clientv3.OpPut(compareValueKey, "matched"), clientv3.OpGet(compareValueKey)).
		Else(clientv3.OpPut(compareValueKey, "not-matched")).
		Commit()
	must("compare value txn", err)
	if !compareValueResp.Succeeded ||
		len(compareValueResp.Responses) != 2 ||
		string(compareValueResp.Responses[1].GetResponseRange().Kvs[0].Value) != "matched" {
		panic("unexpected compare value txn response")
	}
	fmt.Println("compare value txn ok")

	rangeComparePrefix := prefix + "/range-compare/"
	rangeCompareA, err := cli.Put(ctx, rangeComparePrefix+"a", "same")
	must("range compare seed a", err)
	rangeCompareB, err := cli.Put(ctx, rangeComparePrefix+"b", "same")
	must("range compare seed b", err)
	rangeCompareResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(rangeComparePrefix), ">", rangeCompareA.Header.Revision-1).WithPrefix()).
		Then(clientv3.OpGet(rangeComparePrefix, clientv3.WithPrefix())).
		Commit()
	must("range compare mod txn", err)
	if !rangeCompareResp.Succeeded ||
		len(rangeCompareResp.Responses) != 1 ||
		len(rangeCompareResp.Responses[0].GetResponseRange().Kvs) != 2 {
		panic("unexpected range compare mod txn response")
	}
	rangeCompareFailResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(rangeComparePrefix), ">", rangeCompareB.Header.Revision).WithPrefix()).
		Then(clientv3.OpPut(rangeComparePrefix+"c", "should-not-write")).
		Else(clientv3.OpGet(rangeComparePrefix, clientv3.WithPrefix())).
		Commit()
	must("range compare mod failure txn", err)
	if rangeCompareFailResp.Succeeded ||
		len(rangeCompareFailResp.Responses) != 1 ||
		len(rangeCompareFailResp.Responses[0].GetResponseRange().Kvs) != 2 {
		panic("unexpected range compare mod failure txn response")
	}
	fmt.Println("range compare txn ok")

	compareVersionKey := prefix + "/compare-version"
	_, err = cli.Put(ctx, compareVersionKey, "exists")
	must("compare version seed put", err)
	compareVersionResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(compareVersionKey), "=", 1)).
		Then(clientv3.OpPut(compareVersionKey, "version-matched"), clientv3.OpGet(compareVersionKey)).
		Else(clientv3.OpPut(compareVersionKey, "version-not-matched")).
		Commit()
	must("compare version txn", err)
	if !compareVersionResp.Succeeded ||
		len(compareVersionResp.Responses) != 2 ||
		string(compareVersionResp.Responses[1].GetResponseRange().Kvs[0].Value) != "version-matched" ||
		compareVersionResp.Responses[1].GetResponseRange().Kvs[0].Version != 2 {
		panic("unexpected compare version txn response")
	}
	fmt.Println("compare version txn ok")

	fromKeyPrefix := fmt.Sprintf("~kubebrain-smoke/%d/", time.Now().UnixNano())
	for _, suffix := range []string{"a", "b", "c"} {
		_, err = cli.Put(ctx, fromKeyPrefix+suffix, suffix)
		must("from-key seed put", err)
	}
	fromKeyResp, err := cli.Get(ctx, fromKeyPrefix+"b", clientv3.WithFromKey(), clientv3.WithLimit(2))
	must("from-key range", err)
	if len(fromKeyResp.Kvs) != 2 ||
		string(fromKeyResp.Kvs[0].Key) != fromKeyPrefix+"b" ||
		string(fromKeyResp.Kvs[1].Key) != fromKeyPrefix+"c" {
		panic("unexpected from-key range response")
	}
	fromKeyDeleteResp, err := cli.Delete(ctx, fromKeyPrefix+"b", clientv3.WithFromKey(), clientv3.WithPrevKV())
	must("from-key delete", err)
	if fromKeyDeleteResp.Deleted != 2 || len(fromKeyDeleteResp.PrevKvs) != 2 {
		panic(fmt.Sprintf("expected from-key delete to remove two keys, got deleted=%d prev=%d",
			fromKeyDeleteResp.Deleted,
			len(fromKeyDeleteResp.PrevKvs)))
	}
	_, err = cli.Delete(ctx, fromKeyPrefix, clientv3.WithPrefix())
	must("from-key cleanup", err)
	fmt.Println("from-key range and delete ok")

	deleteResp, err := cli.Txn(ctx).
		Then(clientv3.OpGet(key), clientv3.OpDelete(key)).
		Commit()
	must("delete txn", err)
	fmt.Printf("delete succeeded=%v rev=%d\n", deleteResp.Succeeded, deleteResp.Header.Revision)
}
GOEOF

ENDPOINT="$ENDPOINT" go run main.go
