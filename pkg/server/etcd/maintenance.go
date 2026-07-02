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
	"context"
	"encoding/binary"
	"hash/crc32"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maintenanceVersion = "kubebrain"

func (s *RPCServer) Alarm(context.Context, *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
	s.metricCli.EmitCounter("maintenance.alarm", 1)
	return &etcdserverpb.AlarmResponse{
		Header: s.maintenanceHeader(),
		Alarms: nil,
	}, nil
}

func (s *RPCServer) Status(ctx context.Context, _ *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	s.metricCli.EmitCounter("maintenance.status", 1)
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, err
	}
	revision := s.backend.GetCurrentRevision()
	return &etcdserverpb.StatusResponse{
		Header:           s.maintenanceHeader(),
		Version:          maintenanceVersion,
		Leader:           s.memberIDFromAddress(s.peers.GetLeaderInfo()),
		RaftIndex:        revision,
		RaftAppliedIndex: revision,
		DbSize:           0,
		DbSizeInUse:      0,
		Errors:           nil,
		IsLearner:        false,
	}, nil
}

func (s *RPCServer) Defragment(context.Context, *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	s.metricCli.EmitCounter("maintenance.defragment", 1)
	return &etcdserverpb.DefragmentResponse{
		Header: s.maintenanceHeader(),
	}, nil
}

func (s *RPCServer) Hash(ctx context.Context, _ *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	s.metricCli.EmitCounter("maintenance.hash", 1)
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, err
	}
	revision := s.backend.GetCurrentRevision()
	return &etcdserverpb.HashResponse{
		Header: s.maintenanceHeader(),
		Hash:   revisionHash(revision),
	}, nil
}

func (s *RPCServer) HashKV(ctx context.Context, req *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	s.metricCli.EmitCounter("maintenance.hashkv", 1)
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, err
	}
	revision := s.backend.GetCurrentRevision()
	if req.GetRevision() > 0 {
		if err := s.checkRequestedRevision(context.Background(), req.GetRevision()); err != nil {
			return nil, err
		}
		revision = uint64(req.GetRevision())
	}
	compactRevision, err := s.backend.GetCompactRevision(context.Background())
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.HashKVResponse{
		Header:          s.maintenanceHeader(),
		Hash:            revisionHash(revision),
		CompactRevision: int64(compactRevision),
	}, nil
}

func (s *RPCServer) Snapshot(*etcdserverpb.SnapshotRequest, etcdserverpb.Maintenance_SnapshotServer) error {
	s.metricCli.EmitCounter("maintenance.snapshot", 1)
	return status.Errorf(codes.Unimplemented, "snapshot is not supported")
}

func (s *RPCServer) MoveLeader(context.Context, *etcdserverpb.MoveLeaderRequest) (*etcdserverpb.MoveLeaderResponse, error) {
	s.metricCli.EmitCounter("maintenance.moveleader", 1)
	return nil, status.Errorf(codes.Unimplemented, "move leader is not supported")
}

func (s *RPCServer) Downgrade(context.Context, *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
	s.metricCli.EmitCounter("maintenance.downgrade", 1)
	return nil, status.Errorf(codes.Unimplemented, "downgrade is not supported")
}

func (s *RPCServer) maintenanceHeader() *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		Revision: int64(s.backend.GetCurrentRevision()),
	}
}

func (s *RPCServer) memberIDFromAddress(address string) uint64 {
	if address == "" || address == "empty" {
		address = s.backend.GetResourceLock().Identity()
	}
	return uint64(crc32.ChecksumIEEE([]byte(address)))
}

func revisionHash(revision uint64) uint32 {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], revision)
	return crc32.ChecksumIEEE(buf[:])
}
