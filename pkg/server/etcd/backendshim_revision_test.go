// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	prommetrics "github.com/kubewharf/kubebrain/pkg/metrics/prometheus"
)

type rangeRevisionProbeBackend struct {
	backend.Backend
	getRevision   uint64
	listRevision  uint64
	countRevision uint64
}

func (b *rangeRevisionProbeBackend) Get(_ context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	b.getRevision = req.Revision
	return &proto.GetResponse{Header: &proto.ResponseHeader{Revision: 11}}, nil
}

func (b *rangeRevisionProbeBackend) List(_ context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	b.listRevision = req.Revision
	return &proto.RangeResponse{Header: &proto.ResponseHeader{Revision: 11}}, nil
}

func (b *rangeRevisionProbeBackend) CountAtRevision(_ context.Context, _, _ []byte, revision uint64) (int64, bool) {
	b.countRevision = revision
	return 0, true
}

func (b *rangeRevisionProbeBackend) GetCurrentRevision() uint64 { return 11 }

func TestBackendShimNormalizesSignedRangeRevision(t *testing.T) {
	metricCli := prommetrics.NewMetrics()
	for _, tc := range []struct {
		name string
		wire int64
		want uint64
	}{
		{name: "minus one", wire: -1, want: 0},
		{name: "minimum int64", wire: math.MinInt64, want: 0},
		{name: "positive", wire: 7, want: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &rangeRevisionProbeBackend{}
			shim := NewBackendShim(probe, metricCli)
			req := &etcdserverpb.RangeRequest{
				Key: []byte("/signed-revision/"), RangeEnd: []byte("/signed-revision0"), Revision: tc.wire,
			}

			_, err := shim.Get(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, tc.want, probe.getRevision, "point Range revision")
			_, err = shim.List(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, tc.want, probe.listRevision, "range List revision")
			_, err = shim.Count(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, tc.want, probe.countRevision, "CountOnly revision")
		})
	}
}
