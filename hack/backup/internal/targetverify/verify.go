package targetverify

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const rangePageLimit int64 = 1000

// FetchPrefix reads one coherent prefix snapshot. A current read pins the
// first page's revision for every continuation request.
func FetchPrefix(ctx context.Context, cli *clientv3.Client, prefix string, revision int64, admission *ResponseAdmission) ([]*mvccpb.KeyValue, int64, error) {
	start := []byte(prefix)
	end := []byte(clientv3.GetPrefixRangeEnd(prefix))
	var result []*mvccpb.KeyValue
	snapshotRevision := revision
	expectedCount := int64(-1)
	for {
		opts := []clientv3.OpOption{clientv3.WithRange(string(end)), clientv3.WithLimit(rangePageLimit)}
		if snapshotRevision > 0 {
			opts = append(opts, clientv3.WithRev(snapshotRevision))
		}
		response, err := cli.Get(ctx, string(start), opts...)
		if err != nil {
			return nil, 0, err
		}
		if err := admission.AdmitRange(response); err != nil {
			return nil, 0, err
		}
		pinnedRevision, remaining, err := ValidateRangePage(response, snapshotRevision, start, end, expectedCount)
		if err != nil {
			return nil, 0, err
		}
		snapshotRevision = pinnedRevision
		result = append(result, response.Kvs...)
		expectedCount = remaining
		if !response.More {
			return result, snapshotRevision, nil
		}
		start = append(append([]byte(nil), response.Kvs[len(response.Kvs)-1].Key...), 0)
	}
}

func ValidateRangePage(response *clientv3.GetResponse, snapshotRevision int64, start, end []byte, expectedCount int64) (int64, int64, error) {
	if response == nil {
		return 0, 0, errors.New("target range returned an empty response")
	}
	if response.Header == nil || response.Header.Revision <= 0 {
		return 0, 0, errors.New("target range returned no valid response revision")
	}
	if snapshotRevision == 0 {
		snapshotRevision = response.Header.Revision
	}
	if response.Header.Revision < snapshotRevision {
		return 0, 0, fmt.Errorf("target range response revision %d is behind snapshot revision %d", response.Header.Revision, snapshotRevision)
	}
	if response.Count < 0 || response.Count < int64(len(response.Kvs)) {
		return 0, 0, fmt.Errorf("target range returned count %d for %d records", response.Count, len(response.Kvs))
	}
	if expectedCount >= 0 && response.Count != expectedCount {
		return 0, 0, fmt.Errorf("target range count %d does not continue previous remaining count %d", response.Count, expectedCount)
	}
	if int64(len(response.Kvs)) > rangePageLimit {
		return 0, 0, fmt.Errorf("target range returned %d records above page limit %d", len(response.Kvs), rangePageLimit)
	}
	if response.More != (response.Count > int64(len(response.Kvs))) {
		return 0, 0, errors.New("target range returned inconsistent count/more metadata")
	}
	if response.More && int64(len(response.Kvs)) != rangePageLimit {
		return 0, 0, errors.New("target range indicated more records after a non-full page")
	}
	for i, kv := range response.Kvs {
		if kv == nil {
			return 0, 0, fmt.Errorf("target range returned a nil record at index %d", i)
		}
		if len(kv.Key) == 0 || bytes.Compare(kv.Key, start) < 0 || (!bytes.Equal(end, []byte{0}) && bytes.Compare(kv.Key, end) >= 0) {
			return 0, 0, fmt.Errorf("target range returned key %q outside requested range [%q,%q)", kv.Key, start, end)
		}
		if i > 0 && bytes.Compare(response.Kvs[i-1].Key, kv.Key) >= 0 {
			return 0, 0, errors.New("target range records are not in strict ascending key order")
		}
		rec := record.Record{CreateRevision: kv.CreateRevision, ModRevision: kv.ModRevision, Version: kv.Version}
		if err := backupfile.ValidateRecordMetadata(rec, snapshotRevision); err != nil {
			return 0, 0, fmt.Errorf("target range record %q: %w", kv.Key, err)
		}
	}
	return snapshotRevision, response.Count - int64(len(response.Kvs)), nil
}

func ValidateLease(response *clientv3.LeaseTimeToLiveResponse, id, grantedTTL, minRevision int64, expectedKeys []string) error {
	if response == nil {
		return fmt.Errorf("physical lease %d returned an empty TTL response", id)
	}
	if int64(response.ID) != id {
		return fmt.Errorf("physical lease %d TTL response returned mismatched ID %d", id, response.ID)
	}
	if response.ResponseHeader == nil || response.ResponseHeader.Revision <= 0 {
		return fmt.Errorf("physical lease %d returned no valid response revision", id)
	}
	if response.ResponseHeader.Revision < minRevision {
		return fmt.Errorf("physical lease %d response revision %d is behind current range revision %d", id, response.ResponseHeader.Revision, minRevision)
	}
	if response.TTL <= 0 || response.GrantedTTL != grantedTTL || response.GrantedTTL > clientv3.MaxLeaseTTL {
		return fmt.Errorf("physical lease identity/TTL mismatch for %d", id)
	}
	keys := make([]string, len(response.Keys))
	seen := make(map[string]struct{}, len(response.Keys))
	for i, key := range response.Keys {
		if len(key) == 0 {
			return fmt.Errorf("physical lease %d returned an empty attached key", id)
		}
		keys[i] = string(key)
		if _, exists := seen[keys[i]]; exists {
			return fmt.Errorf("physical lease %d returned duplicate attached key %q", id, key)
		}
		seen[keys[i]] = struct{}{}
	}
	sort.Strings(keys)
	want := append([]string(nil), expectedKeys...)
	sort.Strings(want)
	if !equalStrings(want, keys) {
		return fmt.Errorf("physical lease attached keys mismatch for %d", id)
	}
	return nil
}

func ValidateProbeGrant(response *clientv3.LeaseGrantResponse, requestedTTL int64) (clientv3.LeaseID, error) {
	if response == nil {
		return clientv3.NoLease, errors.New("probe lease grant returned an empty response")
	}
	if response.ID == clientv3.NoLease {
		return clientv3.NoLease, errors.New("probe lease grant returned a zero lease ID")
	}
	if response.ResponseHeader == nil || response.ResponseHeader.Revision <= 0 {
		return clientv3.NoLease, errors.New("probe lease grant returned no valid response revision")
	}
	if response.Error != "" {
		return clientv3.NoLease, fmt.Errorf("probe lease grant returned legacy error %q", response.Error)
	}
	if response.TTL < requestedTTL || response.TTL > clientv3.MaxLeaseTTL {
		return clientv3.NoLease, fmt.Errorf("probe lease grant returned invalid TTL %d for request %d", response.TTL, requestedTTL)
	}
	return response.ID, nil
}

func ValidateProbePutTxn(response *clientv3.TxnResponse) (int64, error) {
	if response == nil {
		return 0, errors.New("probe put returned an empty transaction response")
	}
	if !response.Succeeded {
		return 0, errors.New("watch probe key unexpectedly existed")
	}
	if response.Header == nil || response.Header.Revision <= 0 {
		return 0, errors.New("probe put returned no valid transaction revision")
	}
	if len(response.Responses) != 1 || response.Responses[0] == nil || response.Responses[0].GetResponsePut() == nil {
		return 0, errors.New("probe put returned no single put response")
	}
	put := response.Responses[0].GetResponsePut()
	if put.Header == nil || put.Header.Revision != response.Header.Revision {
		return 0, errors.New("probe put response revision differs from transaction revision")
	}
	if put.PrevKv != nil {
		return 0, errors.New("probe put returned an unrequested previous key")
	}
	return response.Header.Revision, nil
}

func ValidateProbeGet(response *clientv3.GetResponse, key, value []byte, leaseID clientv3.LeaseID, putRevision int64) (int64, error) {
	if response == nil {
		return 0, errors.New("probe get returned an empty response")
	}
	if response.Header == nil || response.Header.Revision < putRevision {
		return 0, errors.New("probe get returned no valid response revision")
	}
	if response.More || response.Count != 1 || len(response.Kvs) != 1 {
		return 0, errors.New("probe get returned inconsistent count/more metadata")
	}
	kv := response.Kvs[0]
	if kv == nil || !bytes.Equal(kv.Key, key) || !bytes.Equal(kv.Value, value) || kv.Lease != int64(leaseID) {
		return 0, errors.New("linearizable probe read mismatch")
	}
	if kv.CreateRevision != putRevision || kv.ModRevision != putRevision || kv.Version != 1 {
		return 0, errors.New("probe get returned invalid new-key MVCC metadata")
	}
	return response.Header.Revision, nil
}

func ValidateProbeDeleteTxn(response *clientv3.TxnResponse, minRevision int64) (int64, error) {
	if response == nil {
		return 0, errors.New("probe delete returned an empty transaction response")
	}
	if !response.Succeeded {
		return 0, errors.New("watch probe key changed before delete")
	}
	if response.Header == nil || response.Header.Revision <= minRevision {
		return 0, errors.New("probe delete returned no new transaction revision")
	}
	if len(response.Responses) != 1 || response.Responses[0] == nil || response.Responses[0].GetResponseDeleteRange() == nil {
		return 0, errors.New("probe delete returned no single delete response")
	}
	deleted := response.Responses[0].GetResponseDeleteRange()
	if deleted.Header == nil || deleted.Header.Revision != response.Header.Revision {
		return 0, errors.New("probe delete response revision differs from transaction revision")
	}
	if deleted.Deleted != 1 || len(deleted.PrevKvs) != 0 {
		return 0, errors.New("probe delete did not delete exactly one key without previous values")
	}
	return response.Header.Revision, nil
}

func ValidateProbeRevoke(response *clientv3.LeaseRevokeResponse, minRevision int64) error {
	if response == nil {
		return errors.New("probe lease revoke returned an empty response")
	}
	if response.Header == nil || response.Header.Revision < minRevision || response.Header.Revision <= 0 {
		return errors.New("probe lease revoke returned no valid response revision")
	}
	return nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
