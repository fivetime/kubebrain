// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
)

func TestEtcdMVCCOperationCountersUseUpstreamNames(t *testing.T) {
	ctrl := gomock.NewController(t)
	metricCli := mock.NewMockMetrics(ctrl)
	metricCli.EXPECT().EmitCounter("etcd.mvcc.range_total", 1).Return(nil)
	metricCli.EXPECT().EmitCounter("etcd.mvcc.put_total", 1).Return(nil)
	metricCli.EXPECT().EmitCounter("etcd.mvcc.delete_total", 1).Return(nil)
	metricCli.EXPECT().EmitCounter("etcd.mvcc.txn_total", 1).Return(nil)

	emitEtcdMVCCRangeCounter(metricCli, 1)
	emitEtcdMVCCPutCounter(metricCli, 1)
	emitEtcdMVCCDeleteCounter(metricCli, 1)
	emitEtcdMVCCTxnCounter(metricCli, 1)
}

func TestEtcdMVCCOperationCountersArePreinitialized(t *testing.T) {
	ctrl := gomock.NewController(t)
	metricCli := mock.NewMockMetrics(ctrl)
	metricCli.EXPECT().EmitCounter("etcd.mvcc.range_total", 0).Return(nil)
	metricCli.EXPECT().EmitCounter("etcd.mvcc.put_total", 0).Return(nil)
	metricCli.EXPECT().EmitCounter("etcd.mvcc.delete_total", 0).Return(nil)
	metricCli.EXPECT().EmitCounter("etcd.mvcc.txn_total", 0).Return(nil)

	initEtcdMVCCOperationCounters(metricCli)
}
