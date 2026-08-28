// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

const (
	fixtureOwnershipFormat = "kubebrain.rollout-fixture-owner.v1"
	fixturePrefixRoot      = "/kubebrain-rollout-availability/"
	fixtureOwnerKeyRoot    = "/kubebrain-rollout-fixture-owners/"
)

type fixtureOwnerIdentity struct {
	Namespace      string
	ProbePod       string
	ProbePodUID    string
	StatefulSet    string
	StatefulSetUID string
}

type fixtureOwnershipReceipt struct {
	Format         string   `json:"format"`
	Prefix         string   `json:"prefix"`
	Namespace      string   `json:"namespace"`
	ProbePod       string   `json:"probe_pod"`
	ProbePodUID    string   `json:"probe_pod_uid"`
	StatefulSet    string   `json:"statefulset"`
	StatefulSetUID string   `json:"statefulset_uid"`
	LeaseIDs       []string `json:"lease_ids"`
}

type fixtureOwnership struct {
	receipt fixtureOwnershipReceipt
	value   []byte
}

type fixtureCleanupSummary struct {
	Status   string
	OwnerUID string
	Keys     int64
	Users    int
	Roles    int
	Leases   int
}

func newFixtureOwnership(prefix string, identity fixtureOwnerIdentity) (*fixtureOwnership, error) {
	if err := validateFixtureOwnerIdentity(prefix, identity, true); err != nil {
		return nil, err
	}
	receipt := fixtureOwnershipReceipt{
		Format: fixtureOwnershipFormat, Prefix: prefix, Namespace: identity.Namespace,
		ProbePod: identity.ProbePod, ProbePodUID: identity.ProbePodUID,
		StatefulSet: identity.StatefulSet, StatefulSetUID: identity.StatefulSetUID,
		LeaseIDs: []string{},
	}
	value, err := encodeFixtureOwnershipReceipt(receipt)
	if err != nil {
		return nil, err
	}
	return &fixtureOwnership{receipt: receipt, value: value}, nil
}

func validateFixtureOwnerIdentity(prefix string, identity fixtureOwnerIdentity, requirePodUID bool) error {
	if !isDNSLabel(identity.Namespace) || !isDNSLabel(identity.ProbePod) || !isDNSLabel(identity.StatefulSet) {
		return errors.New("fixture owner namespace, probe Pod, and StatefulSet must be lowercase DNS labels")
	}
	if prefix != fixturePrefixRoot+identity.ProbePod+"/" {
		return fmt.Errorf("fixture prefix %q must be the exclusive probe Pod prefix %q", prefix, fixturePrefixRoot+identity.ProbePod+"/")
	}
	if requirePodUID && !isCanonicalKubernetesUID(identity.ProbePodUID) {
		return errors.New("fixture owner probe Pod UID must be a canonical lowercase UUID")
	}
	if identity.ProbePodUID != "" && !isCanonicalKubernetesUID(identity.ProbePodUID) {
		return errors.New("fixture owner probe Pod UID must be empty or a canonical lowercase UUID")
	}
	if !isCanonicalKubernetesUID(identity.StatefulSetUID) {
		return errors.New("fixture owner StatefulSet UID must be a canonical lowercase UUID")
	}
	return nil
}

func isCanonicalKubernetesUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func encodeFixtureOwnershipReceipt(receipt fixtureOwnershipReceipt) ([]byte, error) {
	if receipt.Format != fixtureOwnershipFormat || receipt.Prefix == "" || receipt.Namespace == "" ||
		receipt.ProbePod == "" || receipt.ProbePodUID == "" || receipt.StatefulSet == "" || receipt.StatefulSetUID == "" ||
		receipt.LeaseIDs == nil {
		return nil, errors.New("fixture ownership receipt is incomplete")
	}
	seen := make(map[string]struct{}, len(receipt.LeaseIDs))
	for _, leaseID := range receipt.LeaseIDs {
		parsed, err := strconv.ParseInt(leaseID, 10, 64)
		if err != nil || parsed <= 0 || strconv.FormatInt(parsed, 10) != leaseID {
			return nil, fmt.Errorf("fixture ownership receipt has invalid lease ID %q", leaseID)
		}
		if _, duplicate := seen[leaseID]; duplicate {
			return nil, fmt.Errorf("fixture ownership receipt has duplicate lease ID %q", leaseID)
		}
		seen[leaseID] = struct{}{}
	}
	return json.Marshal(receipt)
}

func decodeFixtureOwnershipReceipt(value []byte) (fixtureOwnershipReceipt, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var receipt fixtureOwnershipReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return fixtureOwnershipReceipt{}, fmt.Errorf("decode fixture ownership receipt: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fixtureOwnershipReceipt{}, fmt.Errorf("decode fixture ownership receipt: %w", err)
	}
	canonical, err := encodeFixtureOwnershipReceipt(receipt)
	if err != nil {
		return fixtureOwnershipReceipt{}, err
	}
	if !bytes.Equal(canonical, value) {
		return fixtureOwnershipReceipt{}, errors.New("fixture ownership receipt is not canonical JSON")
	}
	return receipt, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func (ownership *fixtureOwnership) markerKey() string {
	return legacyFixtureMarkerKey(ownership.receipt.Prefix)
}

func legacyFixtureMarkerKey(prefix string) string {
	digest := sha256.Sum256([]byte(prefix))
	return fmt.Sprintf("%s%x", fixtureOwnerKeyRoot, digest[:])
}

func (ownership *fixtureOwnership) claim(ctx context.Context, client *clientv3.Client) (uint64, int64, error) {
	if ownership == nil || client == nil {
		return 0, 0, errors.New("fixture ownership claim requires ownership and client")
	}
	before, err := client.Get(ctx, ownership.receipt.Prefix, clientv3.WithPrefix(), clientv3.WithLimit(2))
	if err != nil {
		return 0, 0, fmt.Errorf("inspect fixture prefix before ownership claim: %w", err)
	}
	if err := validateAbsentRange(before, 0, 1); err != nil {
		return 0, 0, fmt.Errorf("fixture prefix is not empty before ownership claim: %w", err)
	}
	clusterID, revision, err := validateResponseHeader(before.Header, 0, 1)
	if err != nil {
		return 0, 0, fmt.Errorf("fixture prefix identity before ownership claim: %w", err)
	}
	claimed, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(ownership.markerKey()), "=", 0)).
		Then(clientv3.OpPut(ownership.markerKey(), string(ownership.value))).
		Commit()
	if err != nil {
		return 0, 0, fmt.Errorf("create fixture ownership marker: %w", err)
	}
	if claimed == nil || claimed.Header == nil || !claimed.Succeeded || claimed.Header.ClusterId != clusterID ||
		claimed.Header.Revision <= revision {
		return 0, 0, fmt.Errorf("create fixture ownership marker returned invalid response: %+v", claimed)
	}
	revision = claimed.Header.Revision
	confirmed, err := client.Get(ctx, ownership.markerKey())
	if err != nil {
		return 0, 0, fmt.Errorf("confirm fixture ownership marker: %w", err)
	}
	if err := validateFixtureOwnershipMarker(confirmed, ownership, clusterID, revision); err != nil {
		return 0, 0, fmt.Errorf("confirm fixture ownership marker: %w", err)
	}
	prefixAfterClaim, err := client.Get(ctx, ownership.receipt.Prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return 0, 0, fmt.Errorf("confirm empty fixture prefix after ownership claim: %w", err)
	}
	if err := validateAbsentRange(prefixAfterClaim, clusterID, revision); err != nil {
		rollback, rollbackErr := client.Txn(ctx).
			If(
				clientv3.Compare(clientv3.Value(ownership.markerKey()), "=", string(ownership.value)),
				clientv3.Compare(clientv3.Version(ownership.markerKey()), "=", 1),
			).
			Then(clientv3.OpDelete(ownership.markerKey())).
			Commit()
		if rollbackErr != nil || rollback == nil || rollback.Header == nil || !rollback.Succeeded ||
			rollback.Header.ClusterId != clusterID || rollback.Header.Revision <= revision {
			return 0, 0, fmt.Errorf("confirm empty fixture prefix after ownership claim: %w; ownership marker rollback failed: response=%+v error=%v", err, rollback, rollbackErr)
		}
		return 0, 0, fmt.Errorf("confirm empty fixture prefix after ownership claim: %w", err)
	}
	return clusterID, confirmed.Header.Revision, nil
}

func validateFixtureOwnershipMarker(response *clientv3.GetResponse, ownership *fixtureOwnership,
	clusterID uint64, minimumRevision int64,
) error {
	if response == nil || response.Header == nil || response.Header.ClusterId != clusterID ||
		response.Header.Revision < minimumRevision || response.More || response.Count != 1 || len(response.Kvs) != 1 {
		return fmt.Errorf("ownership marker has invalid envelope: %+v", response)
	}
	marker := response.Kvs[0]
	if marker == nil || string(marker.Key) != ownership.markerKey() || !bytes.Equal(marker.Value, ownership.value) ||
		marker.Lease != 0 || marker.CreateRevision <= 0 || marker.ModRevision < marker.CreateRevision ||
		marker.Version != int64(len(ownership.receipt.LeaseIDs)+1) {
		return fmt.Errorf("ownership marker is invalid: %+v", marker)
	}
	return nil
}

func (ownership *fixtureOwnership) recordLease(ctx context.Context, client *clientv3.Client,
	leaseID clientv3.LeaseID, clusterID uint64, minimumRevision int64,
) (int64, error) {
	if ownership == nil || client == nil || leaseID <= 0 || clusterID == 0 || minimumRevision <= 0 {
		return minimumRevision, errors.New("fixture lease ownership requires ownership, client, lease, cluster, and revision")
	}
	leaseText := strconv.FormatInt(int64(leaseID), 10)
	if slices.Contains(ownership.receipt.LeaseIDs, leaseText) {
		return minimumRevision, fmt.Errorf("fixture lease %s is already recorded", leaseText)
	}
	nextReceipt := ownership.receipt
	nextReceipt.LeaseIDs = append(slices.Clone(ownership.receipt.LeaseIDs), leaseText)
	nextValue, err := encodeFixtureOwnershipReceipt(nextReceipt)
	if err != nil {
		return minimumRevision, err
	}
	expectedVersion := int64(len(ownership.receipt.LeaseIDs) + 1)
	updated, err := client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(ownership.markerKey()), "=", string(ownership.value)),
			clientv3.Compare(clientv3.Version(ownership.markerKey()), "=", expectedVersion),
		).
		Then(clientv3.OpPut(ownership.markerKey(), string(nextValue))).
		Commit()
	if err != nil {
		return minimumRevision, fmt.Errorf("record fixture lease %s: %w", leaseText, err)
	}
	if updated == nil || updated.Header == nil || !updated.Succeeded || updated.Header.ClusterId != clusterID ||
		updated.Header.Revision <= minimumRevision {
		return minimumRevision, fmt.Errorf("record fixture lease %s returned invalid response: %+v", leaseText, updated)
	}
	ownership.receipt = nextReceipt
	ownership.value = nextValue
	return updated.Header.Revision, nil
}

func readFixtureOwnership(ctx context.Context, client *clientv3.Client, prefix string,
	expected fixtureOwnerIdentity,
) (*fixtureOwnership, *mvccpb.KeyValue, uint64, int64, error) {
	digest := sha256.Sum256([]byte(prefix))
	markerKey := fmt.Sprintf("%s%x", fixtureOwnerKeyRoot, digest[:])
	response, err := client.Get(ctx, markerKey)
	if err != nil {
		return nil, nil, 0, 0, fmt.Errorf("read fixture ownership marker: %w", err)
	}
	if response == nil || response.Header == nil || response.Header.ClusterId == 0 || response.Header.MemberId == 0 ||
		response.Header.RaftTerm == 0 || response.Header.Revision <= 0 || response.More || response.Count != int64(len(response.Kvs)) {
		return nil, nil, 0, 0, fmt.Errorf("read fixture ownership marker returned invalid envelope: %+v", response)
	}
	if len(response.Kvs) == 0 {
		return nil, nil, response.Header.ClusterId, response.Header.Revision, nil
	}
	if len(response.Kvs) != 1 || response.Kvs[0] == nil || string(response.Kvs[0].Key) != markerKey {
		return nil, nil, 0, 0, fmt.Errorf("read fixture ownership marker returned invalid keys: %+v", response.Kvs)
	}
	receipt, err := decodeFixtureOwnershipReceipt(response.Kvs[0].Value)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	identity := fixtureOwnerIdentity{
		Namespace: receipt.Namespace, ProbePod: receipt.ProbePod, ProbePodUID: receipt.ProbePodUID,
		StatefulSet: receipt.StatefulSet, StatefulSetUID: receipt.StatefulSetUID,
	}
	if err := validateFixtureOwnerIdentity(prefix, identity, true); err != nil {
		return nil, nil, 0, 0, err
	}
	if identity.Namespace != expected.Namespace || identity.ProbePod != expected.ProbePod ||
		identity.StatefulSet != expected.StatefulSet || identity.StatefulSetUID != expected.StatefulSetUID ||
		(expected.ProbePodUID != "" && identity.ProbePodUID != expected.ProbePodUID) {
		return nil, nil, 0, 0, fmt.Errorf("fixture ownership identity mismatch: got=%+v expected=%+v", identity, expected)
	}
	ownership := &fixtureOwnership{receipt: receipt, value: slices.Clone(response.Kvs[0].Value)}
	marker := response.Kvs[0]
	if marker.Lease != 0 || marker.CreateRevision <= 0 || marker.ModRevision < marker.CreateRevision ||
		marker.Version != int64(len(receipt.LeaseIDs)+1) {
		return nil, nil, 0, 0, fmt.Errorf("fixture ownership marker metadata is invalid: %+v", marker)
	}
	return ownership, proto.Clone(marker).(*mvccpb.KeyValue), response.Header.ClusterId, response.Header.Revision, nil
}

func (ownership *fixtureOwnership) release(ctx context.Context, client *clientv3.Client,
	clusterID uint64, minimumRevision int64,
) (int64, error) {
	if ownership == nil || client == nil || clusterID == 0 || minimumRevision <= 0 {
		return minimumRevision, errors.New("fixture ownership release requires ownership, client, cluster, and revision")
	}
	released, err := client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(ownership.markerKey()), "=", string(ownership.value)),
			clientv3.Compare(clientv3.Version(ownership.markerKey()), "=", int64(len(ownership.receipt.LeaseIDs)+1)),
		).
		Then(clientv3.OpDelete(ownership.receipt.Prefix, clientv3.WithPrefix()), clientv3.OpDelete(ownership.markerKey())).
		Commit()
	if err != nil {
		return minimumRevision, fmt.Errorf("release fixture ownership: %w", err)
	}
	if released == nil || released.Header == nil || !released.Succeeded || released.Header.ClusterId != clusterID ||
		released.Header.Revision < minimumRevision || len(released.Responses) != 2 ||
		released.Responses[0].GetResponseDeleteRange() == nil || released.Responses[1].GetResponseDeleteRange() == nil ||
		released.Responses[1].GetResponseDeleteRange().Deleted != 1 {
		return minimumRevision, fmt.Errorf("release fixture ownership returned invalid response: %+v", released)
	}
	if err := verifyFixtureAbsent(ctx, client, newSnapshotAuthFixture(ownership.receipt.Prefix), ownership.receipt.Prefix); err != nil {
		return minimumRevision, err
	}
	return released.Header.Revision, nil
}

func cleanupOwnedFixture(ctx context.Context, client *clientv3.Client, prefix string,
	expected fixtureOwnerIdentity, commandTimeout time.Duration,
) (fixtureCleanupSummary, error) {
	if client == nil || commandTimeout <= 0 {
		return fixtureCleanupSummary{}, errors.New("owned fixture cleanup requires client and positive timeout")
	}
	if err := validateFixtureOwnerIdentity(prefix, expected, false); err != nil {
		return fixtureCleanupSummary{}, err
	}
	ownership, _, clusterID, revision, err := readFixtureOwnership(ctx, client, prefix, expected)
	if err != nil {
		return fixtureCleanupSummary{}, err
	}
	fixture := newSnapshotAuthFixture(prefix)
	if ownership == nil {
		if err := verifyFixtureAbsent(ctx, client, fixture, prefix); err != nil {
			return fixtureCleanupSummary{}, fmt.Errorf("fixture ownership marker is absent but residue remains: %w", err)
		}
		return fixtureCleanupSummary{Status: "absent"}, nil
	}
	ownedKeysResponse, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("count owned fixture keys: %w", err)
	}
	if ownedKeysResponse == nil || ownedKeysResponse.Header == nil || ownedKeysResponse.Header.ClusterId != clusterID ||
		ownedKeysResponse.Header.Revision < revision || ownedKeysResponse.More || len(ownedKeysResponse.Kvs) != 0 || ownedKeysResponse.Count < 0 {
		return fixtureCleanupSummary{}, fmt.Errorf("count owned fixture keys returned invalid response: %+v", ownedKeysResponse)
	}
	ownedKeyCount := ownedKeysResponse.Count
	presentUsers, presentRoles, err := fixture.inspectOwnedState(ctx, client)
	if err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("inspect owned Snapshot auth fixture: %w", err)
	}
	if err := fixture.deleteOwnedState(ctx, client, presentUsers, presentRoles); err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("delete owned Snapshot auth fixture: %w", err)
	}
	for _, leaseText := range ownership.receipt.LeaseIDs {
		leaseValue, parseErr := strconv.ParseInt(leaseText, 10, 64)
		if parseErr != nil || leaseValue <= 0 {
			return fixtureCleanupSummary{}, fmt.Errorf("invalid owned lease ID %q", leaseText)
		}
		revokeCtx, cancel := context.WithTimeout(ctx, commandTimeout)
		revoked, revokeErr := client.Revoke(revokeCtx, clientv3.LeaseID(leaseValue))
		cancel()
		missing, revokeErr := classifyCleanupLeaseError(revokeErr)
		if revokeErr != nil {
			return fixtureCleanupSummary{}, fmt.Errorf("revoke owned fixture lease %s: %w", leaseText, revokeErr)
		}
		if !missing {
			if revoked == nil || revoked.Header == nil || revoked.Header.ClusterId != clusterID || revoked.Header.Revision < revision {
				return fixtureCleanupSummary{}, fmt.Errorf("revoke owned fixture lease %s returned invalid response: %+v", leaseText, revoked)
			}
			revision = max(revision, revoked.Header.Revision)
		}
	}
	deleteCtx, cancel := context.WithTimeout(ctx, commandTimeout)
	deleted, err := client.Txn(deleteCtx).
		If(
			clientv3.Compare(clientv3.Value(ownership.markerKey()), "=", string(ownership.value)),
			clientv3.Compare(clientv3.Version(ownership.markerKey()), "=", int64(len(ownership.receipt.LeaseIDs)+1)),
		).
		Then(clientv3.OpDelete(prefix, clientv3.WithPrefix()), clientv3.OpDelete(ownership.markerKey())).
		Commit()
	cancel()
	if err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("delete owned fixture prefix: %w", err)
	}
	if deleted == nil || deleted.Header == nil || !deleted.Succeeded || deleted.Header.ClusterId != clusterID ||
		deleted.Header.Revision < revision || len(deleted.Responses) != 2 || deleted.Responses[0].GetResponseDeleteRange() == nil ||
		deleted.Responses[1].GetResponseDeleteRange() == nil || deleted.Responses[1].GetResponseDeleteRange().Deleted != 1 {
		return fixtureCleanupSummary{}, fmt.Errorf("delete owned fixture prefix returned invalid response: %+v", deleted)
	}
	if err := verifyFixtureAbsent(ctx, client, fixture, prefix); err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("verify owned fixture cleanup: %w", err)
	}
	return fixtureCleanupSummary{
		Status: "recovered", OwnerUID: ownership.receipt.ProbePodUID, Keys: ownedKeyCount,
		Users: len(presentUsers), Roles: len(presentRoles), Leases: len(ownership.receipt.LeaseIDs),
	}, nil
}

func verifyFixtureAbsent(ctx context.Context, client *clientv3.Client, fixture *snapshotAuthFixture, prefix string) error {
	response, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return fmt.Errorf("verify fixture prefix absence: %w", err)
	}
	if err := validateAbsentRange(response, 0, 1); err != nil {
		return fmt.Errorf("verify fixture prefix absence: %w", err)
	}
	digest := sha256.Sum256([]byte(prefix))
	markerKey := fmt.Sprintf("%s%x", fixtureOwnerKeyRoot, digest[:])
	marker, err := client.Get(ctx, markerKey)
	if err != nil {
		return fmt.Errorf("verify fixture ownership marker absence: %w", err)
	}
	if err := validateAbsentRange(marker, response.Header.ClusterId, response.Header.Revision); err != nil {
		return fmt.Errorf("verify fixture ownership marker absence: %w", err)
	}
	users, roles, err := fixture.inspectOwnedState(ctx, client)
	if err != nil {
		return err
	}
	if len(users) != 0 || len(roles) != 0 {
		return fmt.Errorf("owned auth identities remain: users=%s roles=%s", strings.Join(users, ","), strings.Join(roles, ","))
	}
	return nil
}

func revokeUnrecordedLease(parent context.Context, client *clientv3.Client, leaseID clientv3.LeaseID,
	timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	_, err := client.Revoke(ctx, leaseID)
	missing, err := classifyCleanupLeaseError(err)
	if missing {
		return nil
	}
	return err
}
