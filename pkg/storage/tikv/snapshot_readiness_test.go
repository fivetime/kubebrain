package tikv

import (
	"testing"

	"github.com/gogo/protobuf/proto"
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
	require.ErrorContains(t, err, "Region 10 has only 1 active non-witness TiKV voter Stores")
}

func TestCheckpointStoresForRangeCountsUniqueVoterStores(t *testing.T) {
	stores := []*metapb.Store{{Id: 1, State: metapb.StoreState_Up}}
	regions := []*pd.Region{{Meta: &metapb.Region{Id: 10, Peers: []*metapb.Peer{
		{Id: 11, StoreId: 1},
		{Id: 12, StoreId: 1},
	}}}}

	storeIDs, err := checkpointStoresForRange(stores, regions)
	require.Nil(t, storeIDs)
	require.ErrorContains(t, err, "Region 10 has only 1 active non-witness TiKV voter Stores")
}

func TestValidateCheckpointTopologyAllowsUnrelatedEmptyStore(t *testing.T) {
	stores, regions := stableCheckpointTopology()
	afterStores := cloneStores(stores)
	afterStores = append(afterStores, &metapb.Store{Id: 4, State: metapb.StoreState_Up})
	afterRegions := cloneRegions(regions)

	require.NoError(t, validateCheckpointTopology(
		stores, regions, []uint64{1, 2, 3}, afterStores, afterRegions, []uint64{1, 2, 3},
	))
}

func TestValidateCheckpointTopologyRejectsRegionChange(t *testing.T) {
	stores, regions := stableCheckpointTopology()
	afterRegions := cloneRegions(regions)
	afterRegions[0].Meta.RegionEpoch.Version++

	err := validateCheckpointTopology(
		stores, regions, []uint64{1, 2, 3}, cloneStores(stores), afterRegions, []uint64{1, 2, 3},
	)
	require.ErrorContains(t, err, "Region 10 changed")
}

func TestValidateCheckpointTopologyRejectsSelectedStoreChange(t *testing.T) {
	stores, regions := stableCheckpointTopology()
	afterStores := cloneStores(stores)
	afterStores[1].Address = "replacement:20160"

	err := validateCheckpointTopology(
		stores, regions, []uint64{1, 2, 3}, afterStores, cloneRegions(regions), []uint64{1, 2, 3},
	)
	require.ErrorContains(t, err, "TiKV Store 2 changed")
}

func TestValidateCheckpointTopologyRejectsSelectedStoreSetChange(t *testing.T) {
	stores, regions := stableCheckpointTopology()

	err := validateCheckpointTopology(
		stores, regions, []uint64{1, 2, 3}, cloneStores(stores), cloneRegions(regions), []uint64{1, 2},
	)
	require.ErrorContains(t, err, "Store topology changed")
}

func TestValidateCheckpointTopologyRejectsRegionSplit(t *testing.T) {
	stores, regions := stableCheckpointTopology()
	afterRegions := cloneRegions(regions)
	afterRegions = append(afterRegions, &pd.Region{Meta: &metapb.Region{
		Id: 20, RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers: []*metapb.Peer{{Id: 21, StoreId: 1}, {Id: 22, StoreId: 2}, {Id: 23, StoreId: 3}},
	}})

	err := validateCheckpointTopology(
		stores, regions, []uint64{1, 2, 3}, cloneStores(stores), afterRegions, []uint64{1, 2, 3},
	)
	require.ErrorContains(t, err, "Region topology changed")
}

func stableCheckpointTopology() ([]*metapb.Store, []*pd.Region) {
	stores := []*metapb.Store{
		{Id: 1, Address: "store1:20160", State: metapb.StoreState_Up},
		{Id: 2, Address: "store2:20160", State: metapb.StoreState_Up},
		{Id: 3, Address: "store3:20160", State: metapb.StoreState_Up},
	}
	regions := []*pd.Region{{Meta: &metapb.Region{
		Id: 10, RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers: []*metapb.Peer{{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}},
	}}}
	return stores, regions
}

func cloneStores(stores []*metapb.Store) []*metapb.Store {
	cloned := make([]*metapb.Store, len(stores))
	for index, store := range stores {
		cloned[index] = proto.Clone(store).(*metapb.Store)
	}
	return cloned
}

func cloneRegions(regions []*pd.Region) []*pd.Region {
	cloned := make([]*pd.Region, len(regions))
	for index, region := range regions {
		cloned[index] = &pd.Region{Meta: proto.Clone(region.Meta).(*metapb.Region)}
	}
	return cloned
}
