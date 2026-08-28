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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const externalFixtureLeaseCount = 3

func parseFixtureLeaseIDs(value string) ([]clientv3.LeaseID, error) {
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	ids := make([]clientv3.LeaseID, 0, len(parts))
	seen := make(map[clientv3.LeaseID]struct{}, len(parts))
	for _, part := range parts {
		parsed, err := strconv.ParseInt(part, 10, 64)
		if err != nil || parsed <= 0 || strconv.FormatInt(parsed, 10) != part {
			return nil, fmt.Errorf("fixture lease IDs must be canonical positive int64 decimals: %q", part)
		}
		id := clientv3.LeaseID(parsed)
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("fixture lease IDs must be unique: %d", id)
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

func validateExternalFixtureLeaseIDs(ids []clientv3.LeaseID) error {
	if len(ids) != externalFixtureLeaseCount {
		return fmt.Errorf("external fixture ownership requires exactly %d lease IDs", externalFixtureLeaseCount)
	}
	seen := make(map[clientv3.LeaseID]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			return fmt.Errorf("external fixture ownership has invalid lease ID %d", id)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("external fixture ownership repeats lease ID %d", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func claimExternalFixtureOwnership(ctx context.Context, client *clientv3.Client, prefix string,
	identity fixtureOwnerIdentity, leaseIDs []clientv3.LeaseID,
) (uint64, int64, error) {
	if client == nil {
		return 0, 0, errors.New("external fixture ownership claim requires a client")
	}
	if err := validateFixtureOwnerIdentity(prefix, identity, true); err != nil {
		return 0, 0, err
	}
	if err := validateExternalFixtureLeaseIDs(leaseIDs); err != nil {
		return 0, 0, err
	}
	fixture := newSnapshotAuthFixture(prefix)
	if err := verifyFixtureAbsent(ctx, client, fixture, prefix); err != nil {
		return 0, 0, fmt.Errorf("external fixture ownership is not empty: %w", err)
	}
	response, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return 0, 0, fmt.Errorf("read external fixture identity: %w", err)
	}
	clusterID, revision, err := validateResponseHeader(response.Header, 0, 1)
	if err != nil {
		return 0, 0, fmt.Errorf("read external fixture identity: %w", err)
	}
	for _, leaseID := range leaseIDs {
		ttl, ttlErr := client.TimeToLive(ctx, leaseID)
		missing, ttlErr := classifyExternalFixtureLeaseAbsence(ttl, ttlErr)
		if ttlErr != nil {
			return 0, 0, fmt.Errorf("inspect external fixture lease %d: %w", leaseID, ttlErr)
		}
		if !missing {
			return 0, 0, fmt.Errorf("external fixture lease %d already exists", leaseID)
		}
	}
	return clusterID, revision, nil
}

func grantFixtureLease(ctx context.Context, client *clientv3.Client, id clientv3.LeaseID,
	ttl int64,
) (*clientv3.LeaseGrantResponse, error) {
	if client == nil || id <= 0 || ttl <= 0 {
		return nil, errors.New("fixture lease grant requires client, ID, and TTL")
	}
	response, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{
		ID: int64(id), TTL: ttl,
	})
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("fixture lease grant returned an empty response")
	}
	if response.Header == nil || response.ID != int64(id) || response.TTL != ttl || response.Error != "" {
		return nil, fmt.Errorf("fixture lease grant returned an invalid response: %+v", response)
	}
	return &clientv3.LeaseGrantResponse{
		ResponseHeader: response.Header, ID: clientv3.LeaseID(response.ID), TTL: response.TTL, Error: response.Error,
	}, nil
}

func releaseExternalFixtureOwnership(ctx context.Context, client *clientv3.Client, prefix string,
	clusterID uint64, minimumRevision int64,
) (int64, error) {
	if client == nil || prefix == "" || clusterID == 0 || minimumRevision <= 0 {
		return minimumRevision, errors.New("external fixture ownership release requires client, prefix, cluster, and revision")
	}
	released, err := client.Delete(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return minimumRevision, fmt.Errorf("release external fixture ownership: %w", err)
	}
	if released == nil || released.Header == nil || released.Header.ClusterId != clusterID ||
		released.Header.Revision < minimumRevision || released.Deleted < 0 {
		return minimumRevision, fmt.Errorf("release external fixture ownership returned invalid response: %+v", released)
	}
	if err := verifyFixtureAbsent(ctx, client, newSnapshotAuthFixture(prefix), prefix); err != nil {
		return minimumRevision, err
	}
	return released.Header.Revision, nil
}

func cleanupExternallyOwnedFixture(ctx context.Context, client *clientv3.Client, prefix string,
	expected fixtureOwnerIdentity, leaseIDs []clientv3.LeaseID, commandTimeout time.Duration,
) (fixtureCleanupSummary, error) {
	if client == nil || commandTimeout <= 0 {
		return fixtureCleanupSummary{}, errors.New("external fixture cleanup requires client and positive timeout")
	}
	if err := validateFixtureOwnerIdentity(prefix, expected, true); err != nil {
		return fixtureCleanupSummary{}, err
	}
	if err := validateExternalFixtureLeaseIDs(leaseIDs); err != nil {
		return fixtureCleanupSummary{}, err
	}
	fixture := newSnapshotAuthFixture(prefix)
	marker, err := client.Get(ctx, legacyFixtureMarkerKey(prefix))
	if err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("inspect legacy fixture marker: %w", err)
	}
	if err := validateAbsentRange(marker, 0, 1); err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("external fixture ownership conflicts with a legacy marker: %w", err)
	}
	ownedKeys, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("count external fixture keys: %w", err)
	}
	if ownedKeys == nil || ownedKeys.Header == nil || ownedKeys.Header.ClusterId == 0 || ownedKeys.Header.MemberId == 0 ||
		ownedKeys.Header.RaftTerm == 0 || ownedKeys.Header.Revision <= 0 || ownedKeys.More || len(ownedKeys.Kvs) != 0 || ownedKeys.Count < 0 {
		return fixtureCleanupSummary{}, fmt.Errorf("count external fixture keys returned invalid response: %+v", ownedKeys)
	}
	clusterID, revision := ownedKeys.Header.ClusterId, ownedKeys.Header.Revision
	presentUsers, presentRoles, err := fixture.inspectOwnedState(ctx, client)
	if err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("inspect external Snapshot auth fixture: %w", err)
	}
	liveLeases := 0
	for _, leaseID := range leaseIDs {
		ttl, ttlErr := client.TimeToLive(ctx, leaseID)
		missing, ttlErr := classifyExternalFixtureLeaseAbsence(ttl, ttlErr)
		if ttlErr != nil {
			return fixtureCleanupSummary{}, fmt.Errorf("inspect external fixture lease %d: %w", leaseID, ttlErr)
		}
		if !missing {
			liveLeases++
		}
	}
	if ownedKeys.Count == 0 && len(presentUsers) == 0 && len(presentRoles) == 0 && liveLeases == 0 {
		return fixtureCleanupSummary{Status: "absent"}, nil
	}
	if err := fixture.deleteOwnedState(ctx, client, presentUsers, presentRoles); err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("delete external Snapshot auth fixture: %w", err)
	}
	for _, leaseID := range leaseIDs {
		revokeCtx, cancel := context.WithTimeout(ctx, commandTimeout)
		revoked, revokeErr := client.Revoke(revokeCtx, leaseID)
		cancel()
		missing, revokeErr := classifyCleanupLeaseError(revokeErr)
		if revokeErr != nil {
			return fixtureCleanupSummary{}, fmt.Errorf("revoke external fixture lease %d: %w", leaseID, revokeErr)
		}
		if !missing {
			if revoked == nil || revoked.Header == nil || revoked.Header.ClusterId != clusterID || revoked.Header.Revision < revision {
				return fixtureCleanupSummary{}, fmt.Errorf("revoke external fixture lease %d returned invalid response: %+v", leaseID, revoked)
			}
			revision = max(revision, revoked.Header.Revision)
		}
	}
	deleteCtx, cancel := context.WithTimeout(ctx, commandTimeout)
	deleted, err := client.Delete(deleteCtx, prefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("delete external fixture prefix: %w", err)
	}
	if deleted == nil || deleted.Header == nil || deleted.Header.ClusterId != clusterID ||
		deleted.Header.Revision < revision || deleted.Deleted < 0 {
		return fixtureCleanupSummary{}, fmt.Errorf("delete external fixture prefix returned invalid response: %+v", deleted)
	}
	if err := verifyFixtureAbsent(ctx, client, fixture, prefix); err != nil {
		return fixtureCleanupSummary{}, fmt.Errorf("verify external fixture cleanup: %w", err)
	}
	return fixtureCleanupSummary{
		Status: "recovered", OwnerUID: expected.ProbePodUID, Keys: ownedKeys.Count,
		Users: len(presentUsers), Roles: len(presentRoles), Leases: len(leaseIDs),
	}, nil
}

func classifyExternalFixtureLeaseAbsence(response *clientv3.LeaseTimeToLiveResponse, err error) (bool, error) {
	if errors.Is(err, rpctypes.ErrLeaseNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if response == nil || response.ResponseHeader == nil {
		return false, errors.New("lease TTL returned an empty response")
	}
	return response.TTL < 0, nil
}
