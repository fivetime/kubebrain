package nativepitr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeTargetSnapshotEmptyIsStrictAndBounded(t *testing.T) {
	r := validTarget()
	b, err := json.Marshal(r)
	require.NoError(t, err)
	got, err := DecodeTargetSnapshotEmpty(strings.NewReader(string(b)))
	require.NoError(t, err)
	require.Equal(t, r.ClusterID, got.ClusterID)

	_, err = DecodeTargetSnapshotEmpty(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
	unknown := strings.TrimSuffix(string(b), "}") + `,"unknown":true}`
	_, err = DecodeTargetSnapshotEmpty(strings.NewReader(unknown))
	require.Error(t, err)

	r.HistoricalMVCCAbsenceProven = true
	require.ErrorContains(t, r.Validate(), "overclaims")
	r = validTarget()
	r.VisibleCommittedKeyCount = 1
	require.ErrorContains(t, r.Validate(), "empty full")
	r = validTarget()
	r.PDAddrs = []string{"z:2379", "a:2379"}
	require.ErrorContains(t, r.Validate(), "unsorted")
}
