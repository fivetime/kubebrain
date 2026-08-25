package compat

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// liveResponseIdentityAdmission binds responses observed during a live
// multi-endpoint scenario. Runner pre/postflight topology checks cannot catch a
// transient Service or tunnel swap which is restored before postflight.
type liveResponseIdentityAdmission struct {
	mu              sync.Mutex
	clusterID       uint64
	endpointMembers map[int]uint64
}

func (a *liveResponseIdentityAdmission) admitHeader(
	endpointIndex int,
	header *etcdserverpb.ResponseHeader,
	minimumRevision int64,
) error {
	if endpointIndex < 0 {
		return errors.New("live response endpoint index is negative")
	}
	if header == nil || header.GetClusterId() == 0 || header.GetMemberId() == 0 || header.GetRevision() <= 0 {
		return errors.New("live response returned an invalid header")
	}
	if header.GetRevision() < minimumRevision {
		return fmt.Errorf("live response revision %d is below required %d", header.GetRevision(), minimumRevision)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
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
