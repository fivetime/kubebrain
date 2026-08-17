package tikv

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestCheckpointStoresForRangeUsesRegionVoters(t *testing.T) {
	stores := []*metapb.Store{
		{Id: 1, State: metapb.StoreState_Up},
		{Id: 2, State: metapb.StoreState_Up},
		{Id: 3, State: metapb.StoreState_Up},
		{Id: 4, State: metapb.StoreState_Up},
		{Id: 5, State: metapb.StoreState_Up}, // Empty expansion Store.
		{Id: 6, State: metapb.StoreState_Offline},
	}
	regions := []*pd.Region{
		{Meta: &metapb.Region{Id: 10, Peers: []*metapb.Peer{
			{StoreId: 1}, {StoreId: 2}, {StoreId: 3},
		}}},
		{Meta: &metapb.Region{Id: 11, Peers: []*metapb.Peer{
			{StoreId: 1},
			{StoreId: 2},
			{StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			{StoreId: 3, IsWitness: true},
			{StoreId: 6},
		}}},
	}

	storeIDs, err := checkpointStoresForRange(stores, regions)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4}, storeIDs)
}

func TestCheckpointStoresForRangeRequiresTwoActiveDataVoters(t *testing.T) {
	stores := []*metapb.Store{
		{Id: 1, State: metapb.StoreState_Up},
		{Id: 2, State: metapb.StoreState_Up},
		{Id: 3, State: metapb.StoreState_Up},
	}
	regions := []*pd.Region{{Meta: &metapb.Region{Id: 10, Peers: []*metapb.Peer{
		{StoreId: 1},
		{StoreId: 2, Role: metapb.PeerRole_Learner},
		{StoreId: 3, IsWitness: true},
	}}}}

	storeIDs, err := checkpointStoresForRange(stores, regions)
	require.Nil(t, storeIDs)
	require.ErrorContains(t, err, "Region 10 has only 1 active non-witness TiKV voters")
}
