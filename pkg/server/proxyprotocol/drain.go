// Copyright 2026 The KubeBrain Authors.
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

package proxyprotocol

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const peerDrainedBeforeAdmissionMessage = "kubebrain: peer drained before request admission"

// clientDrainedBeforeAdmissionMessage is the exact clientv3 mutable-RPC safe
// retry sentinel. KubeBrain emits it only after its unary drain fence proves
// the retiring public endpoint did not enter the request handler.
const clientDrainedBeforeAdmissionMessage = "there is no connection available"

const countIndexNotReadyMessage = "count index not ready (rebuilding); fall back locally"

// ErrPeerDrainedBeforeAdmission is safe for an internal follower proxy to
// replay: the retiring leader's admission write fence proves the request never
// entered its RPC handler. It must never classify a generic leader loss.
var ErrPeerDrainedBeforeAdmission = status.Error(codes.Aborted, peerDrainedBeforeAdmissionMessage)

var ErrClientDrainedBeforeAdmission = status.Error(codes.Unavailable, clientDrainedBeforeAdmissionMessage)

// ErrCountIndexNotReady asks a follower to fall back to its local count path.
// It is an application-level decline, not evidence that the peer transport is
// unhealthy, even though Unavailable is used to keep the internal RPC cheap.
var ErrCountIndexNotReady = status.Error(codes.Unavailable, countIndexNotReadyMessage)

func IsPeerDrainedBeforeAdmission(err error) bool {
	return errors.Is(err, ErrPeerDrainedBeforeAdmission) ||
		(status.Code(err) == codes.Aborted && status.Convert(err).Message() == peerDrainedBeforeAdmissionMessage)
}

func IsCountIndexNotReady(err error) bool {
	return errors.Is(err, ErrCountIndexNotReady) ||
		(status.Code(err) == codes.Unavailable && status.Convert(err).Message() == countIndexNotReadyMessage)
}
