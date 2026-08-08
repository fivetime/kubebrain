// Copyright 2022 ByteDance and/or its affiliates
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
	"context"

	"k8s.io/klog/v2"
)

// RefreshAuthMetrics emits etcd-compatible auth store gauges from KubeBrain's
// persisted auth config. It intentionally reads only the auth config revision:
// this is the same value returned by AuthStatus.AuthRevision and used by auth
// token/revision fences.
func (s *RPCServer) RefreshAuthMetrics(ctx context.Context) {
	if s == nil || s.tokens == nil || s.metricCli == nil {
		return
	}
	config, err := s.tokens.snapshots.repo.loadConfig(ctx)
	if err != nil {
		s.metricCli.EmitCounter("auth.revision.refresh.err", 1)
		klog.ErrorS(err, "refresh auth revision metric failed")
		return
	}
	s.metricCli.EmitGauge("etcd_debugging.auth.revision", config.Revision)
}
