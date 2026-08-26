package main

import (
	"bytes"
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func validateResponseHeader(header *etcdserverpb.ResponseHeader, clusterID uint64, minRevision int64) (uint64, int64, error) {
	if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.Revision <= 0 || header.Revision < minRevision {
		return 0, 0, errors.New("invalid response header")
	}
	if clusterID != 0 && header.ClusterId != clusterID {
		return 0, 0, fmt.Errorf("response cluster ID changed from %d to %d", clusterID, header.ClusterId)
	}
	return header.ClusterId, header.Revision, nil
}

// validateTxnOperationHeader follows etcd's Txn wire shape: the outer header
// carries the serving cluster/member identity, while nested operation headers
// are allowed to carry only the shared transaction revision. If an
// implementation does populate nested identity fields, require them to agree
// with the outer response instead of accepting a split identity.
func validateTxnOperationHeader(header, outer *etcdserverpb.ResponseHeader) error {
	if header == nil || outer == nil || header.Revision <= 0 || header.Revision != outer.Revision {
		return errors.New("invalid transaction operation response header")
	}
	identityEmpty := header.ClusterId == 0 && header.MemberId == 0
	identityComplete := header.ClusterId != 0 && header.MemberId != 0
	if !identityEmpty && (!identityComplete || header.ClusterId != outer.ClusterId || header.MemberId != outer.MemberId) {
		return errors.New("transaction operation response identity differs from outer header")
	}
	if header.RaftTerm != 0 && header.RaftTerm != outer.RaftTerm {
		return errors.New("transaction operation response Raft term differs from outer header")
	}
	return nil
}

func validateEmptyNestedTxnHeader(header *etcdserverpb.ResponseHeader) error {
	if header == nil || header.ClusterId != 0 || header.MemberId != 0 || header.Revision != 0 || header.RaftTerm != 0 {
		return errors.New("invalid nested transaction response header")
	}
	return nil
}

func validateNestedFailureTxnResponse(response *clientv3.TxnResponse, clusterID uint64, previousRevision int64,
	seeds streamProbeNestedSeeds, outerValue string,
) (int64, error) {
	if response == nil || !response.Succeeded || len(response.Responses) != 2 {
		return 0, errors.New("nested Snapshot transaction returned an invalid outer response")
	}
	_, revision, err := validateResponseHeader(response.Header, clusterID, previousRevision)
	if err != nil {
		return 0, fmt.Errorf("nested Snapshot transaction returned an invalid outer header: %w", err)
	}
	if revision <= previousRevision {
		return 0, fmt.Errorf("nested Snapshot transaction revision=%d did not advance previous revision=%d", revision, previousRevision)
	}
	if seeds.outerPut == nil || seeds.deleted == nil || seeds.innerPut == nil ||
		len(seeds.outerPut.events) == 0 || len(seeds.deleted.events) == 0 || len(seeds.innerPut.events) == 0 {
		return 0, errors.New("nested Snapshot transaction seed expectations are incomplete")
	}
	outerPut := response.Responses[0].GetResponsePut()
	if outerPut == nil || outerPut.PrevKv != nil {
		return 0, errors.New("nested Snapshot transaction returned an invalid outer Put response")
	}
	if err := validateTxnOperationHeader(outerPut.Header, response.Header); err != nil {
		return 0, fmt.Errorf("nested Snapshot transaction outer Put header: %w", err)
	}
	nested := response.Responses[1].GetResponseTxn()
	if nested == nil || nested.Succeeded || len(nested.Responses) != 3 {
		return 0, errors.New("nested Snapshot transaction did not return the selected failure branch")
	}
	if err := validateEmptyNestedTxnHeader(nested.Header); err != nil {
		return 0, fmt.Errorf("nested Snapshot transaction header: %w", err)
	}
	stagedRange := nested.Responses[0].GetResponseRange()
	if stagedRange == nil {
		return 0, errors.New("nested Snapshot transaction returned an empty staged Range response")
	}
	if err := validateTxnOperationHeader(stagedRange.Header, response.Header); err != nil {
		return 0, fmt.Errorf("nested Snapshot transaction staged Range header: %w", err)
	}
	if stagedRange.More || stagedRange.Count != 1 || len(stagedRange.Kvs) != 1 {
		return 0, errors.New("nested Snapshot transaction returned invalid staged Range cardinality")
	}
	stagedKV := stagedRange.Kvs[0]
	initialOuter := seeds.outerPut.events[0]
	if stagedKV == nil || string(stagedKV.Key) != seeds.outerPut.key || string(stagedKV.Value) != outerValue ||
		stagedKV.CreateRevision != initialOuter.createRevision || stagedKV.ModRevision != revision || stagedKV.Version != 2 || stagedKV.Lease != 0 {
		return 0, errors.New("nested Snapshot transaction returned invalid staged Range data")
	}
	deleted := nested.Responses[1].GetResponseDeleteRange()
	if deleted == nil || deleted.Deleted != 1 || len(deleted.PrevKvs) != 0 {
		return 0, errors.New("nested Snapshot transaction returned an invalid Delete response")
	}
	if err := validateTxnOperationHeader(deleted.Header, response.Header); err != nil {
		return 0, fmt.Errorf("nested Snapshot transaction Delete header: %w", err)
	}
	innerPut := nested.Responses[2].GetResponsePut()
	if innerPut == nil || innerPut.PrevKv != nil {
		return 0, errors.New("nested Snapshot transaction returned an invalid inner Put response")
	}
	if err := validateTxnOperationHeader(innerPut.Header, response.Header); err != nil {
		return 0, fmt.Errorf("nested Snapshot transaction inner Put header: %w", err)
	}
	return revision, nil
}

func validateMultilevelSuccessTxnResponse(response *clientv3.TxnResponse, clusterID uint64, previousRevision int64,
	seeds streamProbeMultilevelSeeds, outerValue, middleValue string,
) (int64, error) {
	if response == nil || !response.Succeeded || len(response.Responses) != 2 {
		return 0, errors.New("multilevel Snapshot transaction returned an invalid outer response")
	}
	_, revision, err := validateResponseHeader(response.Header, clusterID, previousRevision)
	if err != nil {
		return 0, fmt.Errorf("multilevel Snapshot transaction returned an invalid outer header: %w", err)
	}
	if revision <= previousRevision {
		return 0, fmt.Errorf("multilevel Snapshot transaction revision=%d did not advance previous revision=%d", revision, previousRevision)
	}
	for _, seed := range []*streamProbeExpectation{seeds.outerPut, seeds.middlePut, seeds.deleted, seeds.innerPut} {
		if seed == nil || len(seed.events) == 0 {
			return 0, errors.New("multilevel Snapshot transaction seed expectations are incomplete")
		}
	}
	outerPut := response.Responses[0].GetResponsePut()
	if outerPut == nil || outerPut.PrevKv != nil {
		return 0, errors.New("multilevel Snapshot transaction returned an invalid outer Put response")
	}
	if err := validateTxnOperationHeader(outerPut.Header, response.Header); err != nil {
		return 0, fmt.Errorf("multilevel Snapshot transaction outer Put header: %w", err)
	}
	levelOne := response.Responses[1].GetResponseTxn()
	if levelOne == nil || !levelOne.Succeeded || len(levelOne.Responses) != 3 {
		return 0, errors.New("multilevel Snapshot transaction did not return the first success branch")
	}
	if err := validateEmptyNestedTxnHeader(levelOne.Header); err != nil {
		return 0, fmt.Errorf("multilevel Snapshot transaction first nested header: %w", err)
	}
	if err := validateStagedTxnRange(levelOne.Responses[0].GetResponseRange(), response.Header,
		seeds.outerPut, outerValue, revision, "first nested"); err != nil {
		return 0, err
	}
	middlePut := levelOne.Responses[1].GetResponsePut()
	if middlePut == nil || middlePut.PrevKv != nil {
		return 0, errors.New("multilevel Snapshot transaction returned an invalid middle Put response")
	}
	if err := validateTxnOperationHeader(middlePut.Header, response.Header); err != nil {
		return 0, fmt.Errorf("multilevel Snapshot transaction middle Put header: %w", err)
	}
	levelTwo := levelOne.Responses[2].GetResponseTxn()
	if levelTwo == nil || !levelTwo.Succeeded || len(levelTwo.Responses) != 3 {
		return 0, errors.New("multilevel Snapshot transaction did not return the second success branch")
	}
	if err := validateEmptyNestedTxnHeader(levelTwo.Header); err != nil {
		return 0, fmt.Errorf("multilevel Snapshot transaction second nested header: %w", err)
	}
	if err := validateStagedTxnRange(levelTwo.Responses[0].GetResponseRange(), response.Header,
		seeds.middlePut, middleValue, revision, "second nested"); err != nil {
		return 0, err
	}
	deleted := levelTwo.Responses[1].GetResponseDeleteRange()
	if deleted == nil || deleted.Deleted != 1 || len(deleted.PrevKvs) != 0 {
		return 0, errors.New("multilevel Snapshot transaction returned an invalid Delete response")
	}
	if err := validateTxnOperationHeader(deleted.Header, response.Header); err != nil {
		return 0, fmt.Errorf("multilevel Snapshot transaction Delete header: %w", err)
	}
	innerPut := levelTwo.Responses[2].GetResponsePut()
	if innerPut == nil || innerPut.PrevKv != nil {
		return 0, errors.New("multilevel Snapshot transaction returned an invalid inner Put response")
	}
	if err := validateTxnOperationHeader(innerPut.Header, response.Header); err != nil {
		return 0, fmt.Errorf("multilevel Snapshot transaction inner Put header: %w", err)
	}
	return revision, nil
}

func validateStagedTxnRange(response *etcdserverpb.RangeResponse, outerHeader *etcdserverpb.ResponseHeader,
	seed *streamProbeExpectation, value string, revision int64, label string,
) error {
	if response == nil {
		return fmt.Errorf("multilevel Snapshot transaction returned an empty %s staged Range response", label)
	}
	if err := validateTxnOperationHeader(response.Header, outerHeader); err != nil {
		return fmt.Errorf("multilevel Snapshot transaction %s staged Range header: %w", label, err)
	}
	if response.More || response.Count != 1 || len(response.Kvs) != 1 {
		return fmt.Errorf("multilevel Snapshot transaction returned invalid %s staged Range cardinality", label)
	}
	kv := response.Kvs[0]
	initial := seed.events[0]
	if kv == nil || string(kv.Key) != seed.key || string(kv.Value) != value ||
		kv.CreateRevision != initial.createRevision || kv.ModRevision != revision || kv.Version != 2 || kv.Lease != 0 {
		return fmt.Errorf("multilevel Snapshot transaction returned invalid %s staged Range data", label)
	}
	return nil
}

func validateDeleteResponse(response *clientv3.DeleteResponse, clusterID uint64, minRevision int64) (uint64, int64, error) {
	if response == nil {
		return 0, 0, errors.New("delete returned an empty response")
	}
	validatedCluster, revision, err := validateResponseHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, 0, fmt.Errorf("delete: %w", err)
	}
	if response.Deleted < 0 || len(response.PrevKvs) != 0 {
		return 0, 0, errors.New("delete returned inconsistent count or unrequested previous values")
	}
	return validatedCluster, revision, nil
}

func validateGrantResponse(response *clientv3.LeaseGrantResponse, clusterID uint64, minRevision, requestedTTL int64) (clientv3.LeaseID, int64, error) {
	if response == nil {
		return clientv3.NoLease, 0, errors.New("lease grant returned an empty response")
	}
	_, revision, err := validateResponseHeader(response.ResponseHeader, clusterID, minRevision)
	if err != nil {
		return clientv3.NoLease, 0, fmt.Errorf("lease grant: %w", err)
	}
	if response.ID == clientv3.NoLease || response.Error != "" || response.TTL < requestedTTL || response.TTL > clientv3.MaxLeaseTTL {
		return clientv3.NoLease, 0, fmt.Errorf("lease grant returned invalid ID, TTL, or legacy error: id=%d ttl=%d error=%q", response.ID, response.TTL, response.Error)
	}
	return response.ID, revision, nil
}

func validateKeepAliveResponse(response *clientv3.LeaseKeepAliveResponse, clusterID uint64, minRevision int64, leaseID clientv3.LeaseID, grantedTTL int64) (int64, error) {
	if response == nil {
		return 0, errors.New("lease keepalive returned an empty response")
	}
	_, revision, err := validateResponseHeader(response.ResponseHeader, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("lease keepalive: %w", err)
	}
	if response.ID != leaseID || response.TTL <= 0 || response.TTL > grantedTTL {
		return 0, fmt.Errorf("lease keepalive returned invalid identity or TTL: id=%d ttl=%d", response.ID, response.TTL)
	}
	return revision, nil
}

func validatePutResponse(response *clientv3.PutResponse, clusterID uint64, minRevision int64) (int64, error) {
	if response == nil {
		return 0, errors.New("put returned an empty response")
	}
	_, revision, err := validateResponseHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("put: %w", err)
	}
	if response.PrevKv != nil {
		return 0, errors.New("put returned an unrequested previous value")
	}
	if revision <= minRevision {
		return 0, errors.New("put response revision did not advance")
	}
	return revision, nil
}

func validateObservedPut(response *clientv3.GetResponse, clusterID uint64, minRevision int64, key, value string) (int64, error) {
	if response == nil {
		return 0, errors.New("put reconciliation returned an empty response")
	}
	_, _, err := validateResponseHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("put reconciliation: %w", err)
	}
	if response.More || response.Count != 1 || len(response.Kvs) != 1 {
		return 0, errors.New("put reconciliation returned inconsistent count or pagination metadata")
	}
	kv := response.Kvs[0]
	if kv == nil || !bytes.Equal(kv.Key, []byte(key)) || !bytes.Equal(kv.Value, []byte(value)) || kv.Lease != 0 || kv.CreateRevision <= 0 || kv.ModRevision <= minRevision || kv.ModRevision > response.Header.Revision || kv.Version <= 0 {
		return 0, errors.New("put reconciliation returned invalid key/value or MVCC metadata")
	}
	return kv.ModRevision, nil
}

func validateTimeToLiveResponse(response *clientv3.LeaseTimeToLiveResponse, clusterID uint64, minRevision int64, leaseID clientv3.LeaseID, grantedTTL int64, key string) (int64, error) {
	if response == nil {
		return 0, errors.New("lease verification returned an empty response")
	}
	_, revision, err := validateResponseHeader(response.ResponseHeader, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("lease verification: %w", err)
	}
	if response.ID != leaseID || response.TTL <= 0 || response.TTL > grantedTTL || response.GrantedTTL != grantedTTL || len(response.Keys) != 1 || !bytes.Equal(response.Keys[0], []byte(key)) {
		return 0, errors.New("lease verification returned invalid identity, TTL, or attached keys")
	}
	return revision, nil
}

func validateAbsentRange(response *clientv3.GetResponse, clusterID uint64, minRevision int64) error {
	if response == nil {
		return errors.New("cleanup range returned an empty response")
	}
	if _, _, err := validateResponseHeader(response.Header, clusterID, minRevision); err != nil {
		return fmt.Errorf("cleanup range: %w", err)
	}
	if response.More || response.Count != 0 || len(response.Kvs) != 0 {
		return errors.New("cleanup range returned keys or inconsistent count metadata")
	}
	return nil
}

func validateCreatedWatch(response clientv3.WatchResponse, clusterID uint64, minRevision int64) (int64, error) {
	if response.Err() != nil || !response.Created || response.Canceled || response.CompactRevision != 0 || len(response.Events) != 0 {
		return 0, errors.New("watch creation returned an invalid envelope")
	}
	_, revision, err := validateResponseHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("watch creation: %w", err)
	}
	return revision, nil
}

func validatePutWatch(response clientv3.WatchResponse, clusterID uint64, putRevision int64, key, value string) (int64, error) {
	if response.Err() != nil || response.Canceled || response.Created || response.CompactRevision != 0 || len(response.Events) != 1 {
		return 0, errors.New("put watch returned an invalid envelope")
	}
	_, revision, err := validateResponseHeader(response.Header, clusterID, putRevision)
	if err != nil {
		return 0, fmt.Errorf("put watch: %w", err)
	}
	event := response.Events[0]
	if event == nil || event.Type != mvccpb.PUT || event.Kv == nil || event.PrevKv != nil || !bytes.Equal(event.Kv.Key, []byte(key)) || !bytes.Equal(event.Kv.Value, []byte(value)) || event.Kv.ModRevision != putRevision || event.Kv.CreateRevision <= 0 || event.Kv.Version <= 0 || event.Kv.Lease != 0 {
		return 0, errors.New("put watch returned invalid event identity or MVCC metadata")
	}
	return revision, nil
}
