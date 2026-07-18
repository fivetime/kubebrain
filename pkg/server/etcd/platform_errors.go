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

const (
	alarmMutationUnsupportedMessage  = "etcd alarm mutation does not represent TiKV capacity; use PD/TiKV alerts and DBaaS remediation"
	snapshotUnsupportedMessage       = "etcd snapshot is unavailable on TiKV; use the DBaaS logical backup and restore workflow"
	moveLeaderUnsupportedMessage     = "KubeBrain leadership is managed automatically; use DBaaS rollout or failover orchestration"
	downgradeUnsupportedMessage      = "in-place etcd protocol downgrade is unavailable; use a DBaaS versioned rollout or rollback"
	memberMutationUnsupportedMessage = "KubeBrain replicas are stateless; scale or reconfigure them through the DBaaS control plane"
)
