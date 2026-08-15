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

package brain

import (
	"context"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	b "github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/streamerror"
	"github.com/kubewharf/kubebrain/pkg/util"
)

// stripKvs removes the inline-metadata envelope (approach A) from each value so
// KubeBrain-native clients get the raw value. Passthrough for legacy values.
func stripKvs(kvs []*proto.KeyValue) error {
	for _, kv := range kvs {
		if kv != nil {
			var err error
			kv.Value, err = b.StripInlineValueChecked(kv.Value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) Get(ctx context.Context, r *proto.GetRequest) (*proto.GetResponse, error) {
	if len(r.Key) == 0 {
		return nil, fmt.Errorf("invailid empty key in get request")
	}
	start := time.Now()
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return &proto.GetResponse{}, err
	}
	response, err := s.backend.Get(ctx, r)
	if err != nil {
		klog.ErrorS(err, "brain server get failed", "key", util.LoggedKey(r.Key), "revision", r.Revision)
	}
	if response != nil && response.Kv != nil {
		response.Kv.Value, err = b.StripInlineValueChecked(response.Kv.Value)
		if err != nil {
			response = nil
		}
	}
	// emit metrics
	s.emitMethodMetric(readMetric, "get", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(readMetric, "get", response.Kv != nil, response.Size())
	}
	return response, err
}

func (s *Server) Range(ctx context.Context, r *proto.RangeRequest) (*proto.RangeResponse, error) {
	if len(r.Key) == 0 || len(r.End) == 0 {
		return nil, fmt.Errorf("empty key or end in range request")
	}
	start := time.Now()
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return &proto.RangeResponse{}, err
	}
	response, err := s.backend.List(ctx, r)
	if err != nil {
		klog.ErrorS(err, "brain server range failed", "key", util.LoggedKey(r.Key), "end", util.LoggedKey(r.End), "limit", r.Limit, "revision", r.Revision)
	}
	if response != nil {
		if stripErr := stripKvs(response.Kvs); stripErr != nil {
			response = nil
			err = stripErr
		}
	}
	// emit metrics
	s.emitMethodMetric(readMetric, "range", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(readMetric, "range", true, response.Size())
	}
	return response, err
}

func (s *Server) Count(ctx context.Context, r *proto.CountRequest) (*proto.CountResponse, error) {
	if len(r.Key) == 0 || len(r.End) == 0 {
		return nil, fmt.Errorf("empty key or end in count request")
	}
	start := time.Now()
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return &proto.CountResponse{}, err
	}
	response, err := s.backend.Count(ctx, r)
	if err != nil {
		klog.ErrorS(err, "brain server count failed", "key", util.LoggedKey(r.Key), "end", util.LoggedKey(r.End))
	}
	// emit metrics
	s.emitMethodMetric(readMetric, "count", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(readMetric, "count", true, response.Size())
	}
	return response, err
}

func (s *Server) ListPartition(ctx context.Context, r *proto.ListPartitionRequest) (*proto.ListPartitionResponse, error) {
	if len(r.Key) == 0 || len(r.End) == 0 {
		return nil, fmt.Errorf("empty key or end in list partition request")
	}
	start := time.Now()
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return &proto.ListPartitionResponse{}, err
	}
	response, err := s.backend.GetPartitions(ctx, r)
	if err != nil {
		klog.ErrorS(err, "brain server list-partition failed", "key", util.LoggedKey(r.Key), "end", util.LoggedKey(r.End))
	}
	s.emitMethodMetric(readMetric, "list-partition", err, time.Since(start))
	if response != nil {
		s.emitResponseDetailMetric(readMetric, "list-partition", true, response.Size())
	}
	return response, err
}

func (s *Server) RangeStream(r *proto.RangeRequest, server proto.Read_RangeStreamServer) error {
	if len(r.Key) == 0 || len(r.End) == 0 {
		return fmt.Errorf("empty key or end in range stream request")
	}
	start := time.Now()
	if err := s.peers.SyncReadRevision(server.Context()); err != nil {
		return err
	}
	// RangeStream (user-key entrypoint) encodes the range into the object
	// keyspace; ListByStream takes already-encoded partition borders and would
	// silently stream nothing for a raw user key (the bug this replaces).
	ch, err := s.backend.RangeStream(server.Context(), r.Key, r.End, r.Revision)
	if err != nil {
		s.emitMethodMetric(readMetric, "range-stream", err, time.Since(start))
		klog.ErrorS(err, "backend list by stream failed", "key", util.LoggedKey(r.Key), "end", util.LoggedKey(r.End), "revision", r.Revision)
		return err
	}
	responseSize := 0
	for response := range ch {
		if response.GetErr() != "" {
			if typedErr, ok := streamerror.Decode(response.GetErr()); ok {
				s.emitMethodMetric(readMetric, "range-stream", typedErr, time.Since(start))
				return typedErr
			}
		}
		if response.RangeResponse != nil {
			if err = stripKvs(response.RangeResponse.Kvs); err != nil {
				s.emitMethodMetric(readMetric, "range-stream", err, time.Since(start))
				return err
			}
		}
		responseSize += response.Size()
		err = server.Send(response)
		if err != nil {
			s.emitMethodMetric(readMetric, "range-stream-send", err, time.Since(start))
			klog.ErrorS(err, "send range stream response to client failed", "key", util.LoggedKey(r.Key), "end", util.LoggedKey(r.End), "revision", r.Revision)
			return err
		}
	}
	s.emitMethodMetric(readMetric, "range-stream", nil, time.Since(start))
	s.emitResponseDetailMetric(readMetric, "range-stream", true, responseSize)
	klog.InfoS("brain server range stream finished", "error", err, "key", util.LoggedKey(r.Key), "end", util.LoggedKey(r.End))
	return nil
}
