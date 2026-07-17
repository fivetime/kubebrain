// Copyright 2023 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package leader

import "context"

// Stub is an implement of LeaderElection for test
type Stub struct {
	ElectionInfo
	Term uint64
}

// Campaign implements LeaderElection interface
func (s *Stub) Campaign(context.Context) {
}

// GetLeaderInfo implements LeaderElection interface
func (s *Stub) GetLeaderInfo() string {
	return s.ElectionInfo.LeaderAddress
}

func (s *Stub) LeadershipTerm(context.Context) (uint64, error) {
	if s.Term == 0 {
		return 1, nil
	}
	return s.Term, nil
}

func (s *Stub) CurrentLeadershipTerm() uint64 {
	if s.Term == 0 {
		return 1
	}
	return s.Term
}

// IsLeader implements LeaderElection interface
func (s *Stub) IsLeader() bool {
	return s.ElectionInfo.IsLeader
}

// EpochAndLeadingFresh implements LeaderElection interface. The stub has no real
// lease, so it reports a fixed epoch and treats leadership as always fresh.
func (s *Stub) EpochAndLeadingFresh() (uint64, bool) {
	return 0, s.ElectionInfo.IsLeader
}

// GetElectionInfo implements LeaderElection interface
func (s *Stub) GetElectionInfo() (ElectionInfo, error) {
	return s.ElectionInfo, nil
}
