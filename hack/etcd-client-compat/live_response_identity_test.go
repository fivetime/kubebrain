package compat

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

const (
	directExpectedClusterIDEnv = "KUBEBRAIN_DIRECT_EXPECTED_CLUSTER_ID"
	directExpectedMemberIDsEnv = "KUBEBRAIN_DIRECT_EXPECTED_MEMBER_IDS"
	directMinimumRevisionsEnv  = "KUBEBRAIN_DIRECT_MIN_REVISIONS"
)

// liveResponseIdentityAdmission binds responses observed during a live
// multi-endpoint scenario. Runner pre/postflight topology checks cannot catch a
// transient Service or tunnel swap which is restored before postflight.
type liveResponseIdentityAdmission struct {
	mu                       sync.Mutex
	clusterID                uint64
	endpointMembers          map[int]uint64
	endpointMinimumRevisions map[int]int64
	frozenTopology           bool
}

func newLiveResponseIdentityAdmission(t *testing.T) *liveResponseIdentityAdmission {
	t.Helper()
	admission, err := liveResponseIdentityAdmissionFromEnvironment()
	require.NoError(t, err)
	return admission
}

func liveResponseIdentityAdmissionFromEnvironment() (*liveResponseIdentityAdmission, error) {
	clusterRaw, clusterSet := os.LookupEnv(directExpectedClusterIDEnv)
	membersRaw, membersSet := os.LookupEnv(directExpectedMemberIDsEnv)
	revisionsRaw, revisionsSet := os.LookupEnv(directMinimumRevisionsEnv)
	if !clusterSet && !membersSet && !revisionsSet {
		return &liveResponseIdentityAdmission{}, nil
	}
	if !clusterSet || !membersSet || !revisionsSet {
		return nil, errors.New("direct response identity baseline environment is incomplete")
	}
	clusterID, err := strconv.ParseUint(clusterRaw, 10, 64)
	if err != nil || clusterID == 0 {
		return nil, fmt.Errorf("invalid direct response cluster ID %q", clusterRaw)
	}
	var memberIDs []uint64
	if err := json.Unmarshal([]byte(membersRaw), &memberIDs); err != nil {
		return nil, fmt.Errorf("invalid direct response member IDs: %w", err)
	}
	var minimumRevisions []int64
	if err := json.Unmarshal([]byte(revisionsRaw), &minimumRevisions); err != nil {
		return nil, fmt.Errorf("invalid direct response minimum revisions: %w", err)
	}
	if len(memberIDs) == 0 || len(memberIDs) != len(minimumRevisions) {
		return nil, errors.New("direct response identity baseline lengths differ or are empty")
	}
	admission := &liveResponseIdentityAdmission{
		clusterID:                clusterID,
		endpointMembers:          make(map[int]uint64, len(memberIDs)),
		endpointMinimumRevisions: make(map[int]int64, len(memberIDs)),
		frozenTopology:           true,
	}
	seenMembers := make(map[uint64]struct{}, len(memberIDs))
	for index, memberID := range memberIDs {
		if memberID == 0 || minimumRevisions[index] <= 0 {
			return nil, fmt.Errorf("direct response identity baseline endpoint %d is zero", index)
		}
		if _, duplicate := seenMembers[memberID]; duplicate {
			return nil, fmt.Errorf("direct response identity baseline repeats member %d", memberID)
		}
		seenMembers[memberID] = struct{}{}
		admission.endpointMembers[index] = memberID
		admission.endpointMinimumRevisions[index] = minimumRevisions[index]
	}
	return admission, nil
}

func (a *liveResponseIdentityAdmission) admitHeader(
	endpointIndex int,
	header *etcdserverpb.ResponseHeader,
	minimumRevision int64,
) error {
	return a.admit(endpointIndex, header, minimumRevision, true)
}

func (a *liveResponseIdentityAdmission) admitIdentityHeader(
	endpointIndex int,
	header *etcdserverpb.ResponseHeader,
) error {
	return a.admit(endpointIndex, header, 0, false)
}

func (a *liveResponseIdentityAdmission) admit(
	endpointIndex int,
	header *etcdserverpb.ResponseHeader,
	minimumRevision int64,
	requireRevision bool,
) error {
	if endpointIndex < 0 {
		return errors.New("live response endpoint index is negative")
	}
	if header == nil || header.GetClusterId() == 0 || header.GetMemberId() == 0 || (requireRevision && header.GetRevision() <= 0) {
		return errors.New("live response returned an invalid header")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if requireRevision {
		if frozenMinimum, exists := a.endpointMinimumRevisions[endpointIndex]; exists && frozenMinimum > minimumRevision {
			minimumRevision = frozenMinimum
		}
		if header.GetRevision() < minimumRevision {
			return fmt.Errorf("live response revision %d is below required %d", header.GetRevision(), minimumRevision)
		}
	}
	if a.clusterID != 0 && header.GetClusterId() != a.clusterID {
		return fmt.Errorf("live response cluster ID changed from %d to %d", a.clusterID, header.GetClusterId())
	}
	if a.endpointMembers == nil {
		a.endpointMembers = make(map[int]uint64)
	}
	if memberID, exists := a.endpointMembers[endpointIndex]; exists {
		if header.GetMemberId() != memberID {
			return fmt.Errorf("live response endpoint %d member changed from %d to %d", endpointIndex, memberID, header.GetMemberId())
		}
	} else if a.frozenTopology {
		return fmt.Errorf("live response endpoint %d is outside the frozen topology", endpointIndex)
	} else {
		for otherIndex, memberID := range a.endpointMembers {
			if memberID == header.GetMemberId() {
				return fmt.Errorf("live response endpoints %d and %d both identify member %d", otherIndex, endpointIndex, memberID)
			}
		}
		a.endpointMembers[endpointIndex] = header.GetMemberId()
	}
	if a.clusterID == 0 {
		a.clusterID = header.GetClusterId()
	}
	return nil
}

func (a *liveResponseIdentityAdmission) expectedMemberID(endpointIndex int) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	memberID, exists := a.endpointMembers[endpointIndex]
	if !exists || memberID == 0 {
		return 0, fmt.Errorf("live response endpoint %d has no admitted member", endpointIndex)
	}
	return memberID, nil
}

func TestLiveResponseIdentityAdmissionRejectsTransientTopologyDrift(t *testing.T) {
	header := func(clusterID, memberID uint64, revision int64) *etcdserverpb.ResponseHeader {
		return &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID, Revision: revision}
	}
	admission := &liveResponseIdentityAdmission{}
	require.NoError(t, admission.admitHeader(0, header(7, 11, 10), 10))
	require.NoError(t, admission.admitHeader(1, header(7, 12, 11), 10))
	require.NoError(t, admission.admitHeader(2, header(7, 13, 12), 10))
	require.ErrorContains(t, admission.admitHeader(1, header(7, 13, 13), 10), "member changed")
	require.ErrorContains(t, admission.admitHeader(1, header(8, 12, 13), 10), "cluster ID changed")
	require.ErrorContains(t, admission.admitHeader(1, header(7, 12, 9), 10), "below required")
	duplicate := &liveResponseIdentityAdmission{}
	require.NoError(t, duplicate.admitHeader(0, header(7, 11, 10), 1))
	require.ErrorContains(t, duplicate.admitHeader(1, header(7, 11, 10), 1), "both identify member")

	for name, malformed := range map[string]*etcdserverpb.ResponseHeader{
		"nil":           nil,
		"zero cluster":  header(0, 11, 1),
		"zero member":   header(7, 0, 1),
		"zero revision": header(7, 11, 0),
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, (&liveResponseIdentityAdmission{}).admitHeader(0, malformed, 1))
		})
	}
	require.Error(t, (&liveResponseIdentityAdmission{}).admitHeader(-1, header(7, 11, 1), 1))
	require.NoError(t, (&liveResponseIdentityAdmission{}).admitIdentityHeader(0, header(7, 11, 0)))

	t.Run("concurrent stable identity", func(t *testing.T) {
		concurrent := &liveResponseIdentityAdmission{}
		errors := make(chan error, 64)
		var wg sync.WaitGroup
		for i := 0; i < cap(errors); i++ {
			wg.Add(1)
			go func(revision int64) {
				defer wg.Done()
				errors <- concurrent.admitHeader(0, header(7, 11, revision), 1)
			}(int64(i + 1))
		}
		wg.Wait()
		close(errors)
		for err := range errors {
			require.NoError(t, err)
		}
	})
}

func TestLiveResponseIdentityAdmissionConsumesRunnerBaseline(t *testing.T) {
	t.Setenv(directExpectedClusterIDEnv, "7")
	t.Setenv(directExpectedMemberIDsEnv, "[11,12,13]")
	t.Setenv(directMinimumRevisionsEnv, "[10,20,30]")
	admission, err := liveResponseIdentityAdmissionFromEnvironment()
	require.NoError(t, err)
	require.NoError(t, admission.admitHeader(0, &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 11, Revision: 10}, 1))
	require.ErrorContains(t, admission.admitHeader(1, &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 13, Revision: 20}, 1), "member changed")
	require.ErrorContains(t, admission.admitHeader(1, &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 12, Revision: 19}, 1), "below required")
	require.ErrorContains(t, admission.admitHeader(3, &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 14, Revision: 40}, 1), "outside the frozen topology")
	memberID, err := admission.expectedMemberID(2)
	require.NoError(t, err)
	require.Equal(t, uint64(13), memberID)
	_, err = admission.expectedMemberID(3)
	require.ErrorContains(t, err, "no admitted member")

	for name, env := range map[string]map[string]string{
		"partial": {directExpectedClusterIDEnv: "7"},
		"bad cluster": {
			directExpectedClusterIDEnv: "zero", directExpectedMemberIDsEnv: "[11]", directMinimumRevisionsEnv: "[10]",
		},
		"duplicate member": {
			directExpectedClusterIDEnv: "7", directExpectedMemberIDsEnv: "[11,11]", directMinimumRevisionsEnv: "[10,10]",
		},
		"length mismatch": {
			directExpectedClusterIDEnv: "7", directExpectedMemberIDsEnv: "[11,12]", directMinimumRevisionsEnv: "[10]",
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, key := range []string{directExpectedClusterIDEnv, directExpectedMemberIDsEnv, directMinimumRevisionsEnv} {
				t.Setenv(key, "")
				require.NoError(t, os.Unsetenv(key))
			}
			for key, value := range env {
				t.Setenv(key, value)
			}
			_, parseErr := liveResponseIdentityAdmissionFromEnvironment()
			require.Error(t, parseErr)
		})
	}
}
