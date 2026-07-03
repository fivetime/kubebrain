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

package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	ibadger "github.com/kubewharf/kubebrain/pkg/storage/badger"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
	imetrics "github.com/kubewharf/kubebrain/pkg/storage/metrics"
	itikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
)

type storageType int

const (
	memKvStorage storageType = iota
	tiKvStorage
	badgerStorage
	metricsWrapper
)

// test config
const (
	interval = 100 * time.Millisecond
	timeout  = 5 * time.Second
)

// msg
const (
	expectAEvent       = "expect a Event"
	expectAMvccPBEvent = "expect a mvccpb.Event"
	expectNoEvent      = "expect no Event"

	noExpectedEvent   = "no expected event"
	setWatcherError   = "set watcher error"
	backendIsNotReady = "backend is not ready"
)

var (
	skippedTests = []string{
		".*/range/native/count.*", // todo: release Count later
	}
	storages = map[string]storageType{
		"memKv": memKvStorage,
		"tiKv":  tiKvStorage,
		//"badger":          badgerStorage,
		"metrics-wrapper": metricsWrapper,
	}
)

func checkSkip(t *testing.T) {
	for _, exp := range skippedTests {
		fmt.Println(t.Name())
		if b, _ := regexp.MatchString(exp, t.Name()); b {
			t.SkipNow()
		}
	}
}

type suite struct {
	backend Backend
	kv      storage.KvStorage
	metrics metrics.Metrics
	ctrl    *gomock.Controller
	ast     *assert.Assertions
	ctx     context.Context
}

func newTestSuites(t *testing.T, st storageType) (s suite, close func()) {

	ast := assert.New(t)
	ctrl := gomock.NewController(t)

	m := mock.NewMinimalMetrics(ctrl)
	kv := newTestKvStorage(t, ast, st, m)
	config := Config{
		Prefix:   prefix,
		Identity: getStorageIdentity(),
	}
	backend := NewBackend(kv, config, m)
	initRevision := uint64(time.Now().UnixNano())
	klog.InfoS("init", "revision", initRevision)
	backend.SetCurrentRevision(initRevision)
	ctx, cancel := context.WithCancel(context.Background())
	s = suite{
		backend: backend,
		kv:      kv,
		metrics: m,
		ctrl:    ctrl,
		ast:     ast,
		ctx:     ctx,
	}
	close = func() {
		ctrl.Finish()
		clear(ast, s.kv, prefix)
		err := kv.Close()
		ast.NoError(err)
		cancel()
	}
	return s, close
}

func getStorageIdentity() string {
	return path.Join(prefix, "identities", strconv.Itoa(int(atomic.AddInt64(&identities, 1))))
}

func newTestKvStorage(t *testing.T, ast *assert.Assertions, st storageType, m metrics.Metrics) storage.KvStorage {
	switch st {
	case memKvStorage:
		return imemkv.NewKvStorage()
	case tiKvStorage:
		return newTestRefactorTiKVStorage(ast)
	case badgerStorage:
		return newBadgerStorage(t, ast)
	case metricsWrapper:
		return imetrics.NewKvStorage(newBadgerStorage(t, ast), m)
	default:
		ast.FailNow("invalid storage")
		return nil
	}
}

func newBadgerStorage(t *testing.T, ast *assert.Assertions) storage.KvStorage {
	p := path.Join(t.TempDir(), "badger")
	st, err := ibadger.NewKvStorage(ibadger.Config{Dir: p})
	if err != nil {
		ast.FailNow(err.Error())
	}
	return st
}

func newTestRefactorTiKVStorage(ast *assert.Assertions) storage.KvStorage {
	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	ast.NoError(err)
	testutils.BootstrapWithMultiRegions(cluster)
	store, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	ast.NoError(err)
	return itikv.NewKvStoreWithStorage([]*tikv.KVStore{store})
}

func newEvent(eventType proto.Event_EventType, rev uint64, cur *proto.KeyValue) *proto.Event {
	return &proto.Event{
		Type:     eventType,
		Revision: rev,
		Kv:       cur,
	}
}

func newKeyValue(key, value string, modRevision uint64) *proto.KeyValue {
	valueBytes := []byte(value)
	if value == "" {
		valueBytes = nil
	}
	return &proto.KeyValue{
		Key:      []byte(key),
		Revision: modRevision,
		Value:    valueBytes,
	}
}

func newCreateRequest(key, val string) *proto.CreateRequest {
	return &proto.CreateRequest{
		Key:   []byte(key),
		Value: []byte(val),
		Lease: 0,
	}
}

func newCreateResponse(revision uint64, succeeded bool) *proto.CreateResponse {
	return &proto.CreateResponse{
		Header:    responseHeader(revision),
		Succeeded: succeeded,
	}
}

func newUpdateResponse(revision uint64, succeeded bool, kv *proto.KeyValue) *proto.UpdateResponse {
	return &proto.UpdateResponse{
		Header:    responseHeader(revision),
		Succeeded: succeeded,
		Kv:        kv,
	}
}

func newGetResponse(revision uint64, kv *proto.KeyValue) *proto.GetResponse {
	return &proto.GetResponse{
		Header: responseHeader(revision),
		Kv:     kv,
	}
}
func newRangeResponse(revision uint64, kvs ...*proto.KeyValue) *proto.RangeResponse {
	return &proto.RangeResponse{
		Header: responseHeader(revision),
		Kvs:    kvs,
	}
}

func newLimitedRangeResponse(revision uint64, count int64, kvs ...*proto.KeyValue) *proto.RangeResponse {
	return &proto.RangeResponse{
		Header: responseHeader(revision),
		Kvs:    kvs,
		More:   int(count) > len(kvs),
	}
}

func newCountResponse(revision uint64, count int64) *proto.CountResponse {
	return &proto.CountResponse{
		Header: responseHeader(revision),
		Count:  uint64(count),
	}
}

func newDelResponse(revision uint64, succeeded bool, kv *proto.KeyValue) *proto.DeleteResponse {
	return &proto.DeleteResponse{
		Header:    responseHeader(revision),
		Kv:        kv,
		Succeeded: succeeded,
	}
}

func newRangeRequest(revision uint64, key string, rangeEnd string, limit int64) *proto.RangeRequest {
	var rangeEndBytes []byte
	if rangeEnd != "" {
		rangeEndBytes = []byte(rangeEnd)
	}
	return &proto.RangeRequest{
		Key:      []byte(key),
		End:      rangeEndBytes,
		Limit:    limit,
		Revision: revision,
	}
}

func newCountRequest(revision int64, key string) *proto.CountRequest {
	return &proto.CountRequest{
		Key: []byte(key),
		End: PrefixEnd([]byte(key)),
	}
}

func newGetRequest(revision uint64, key string) *proto.GetRequest {
	return &proto.GetRequest{
		Key:      []byte(key),
		Revision: revision,
	}
}

func newDelRequest(revision uint64, key string) *proto.DeleteRequest {
	return &proto.DeleteRequest{
		Key:      []byte(key),
		Revision: revision,
	}
}

func encodeRevisionKey(userKey []byte) (internalKey []byte) {
	cdr := coder.NewNormalCoder()
	return cdr.EncodeObjectKey(userKey, 0)
}

// clear list all key-value pairs with given prefix and delete them to reset data set in storage
func clear(ast *assert.Assertions, st storage.KvStorage, prefix string) {
	// remove key with given prefix in storage
	iter, err := st.Iter(context.Background(), encodeRevisionKey([]byte(prefix)), encodeRevisionKey(PrefixEnd([]byte(prefix))), 0, 0)
	ast.NoError(err)
	ctx := context.Background()
	for {
		err = iter.Next(ctx)
		if err != nil {
			ast.Equal(io.EOF, err)
			return
		}
		klog.InfoS("clear", "key", string(iter.Key()))
		batch := st.BeginBatchWrite()
		batch.DelCurrent(iter)
		err = batch.Commit(ctx)
		ast.NoError(err)
	}
}

func getEnd(prefix []byte) (end []byte) {
	end = make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	return
}

const (
	testKey = "/test/key"
	testVal = "/test/value"
)

var (
	testID     = fmt.Sprint(time.Now().UnixNano())
	prefix     = fmt.Sprintf("/kubebrain/unit_test/%s", testID)
	identities = int64(0)
)

type testcase interface {
	// run performs test logic and check return and event
	run(t *testing.T, s suite, output <-chan []*proto.Event)
}

type createTestcase struct {
	description   string
	putReq        *proto.CreateRequest
	expectedResp  *proto.CreateResponse
	expectedEvent *proto.Event
}

func (ct *createTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(ct.description, func(t *testing.T) {
		checkSkip(t)
		ast := assert.New(t)
		backend := s.backend
		resp, err := backend.Create(context.Background(), ct.putReq)
		ast.Equal(ct.expectedResp, resp)
		if ct.expectedResp == nil {
			ast.Error(err)
		} else {
			ast.NoError(err)
		}

		// sleep for a while and then check watcher
		waitUntilRevisionEqualOrTimeout(backend, resp.GetHeader().GetRevision())
		if ct.expectedEvent != nil {
			actual := nextRealEventBatch(output)
			if !ast.Equal(1, len(actual), expectAEvent) {
				ast.FailNow(noExpectedEvent)
			}
			if !ast.Equal(1, len(actual), expectAMvccPBEvent) {
				return
			}
			ast.Equal(ct.expectedEvent, actual[0])
			return
		}
		ast.True(noPendingRealEvent(output), expectNoEvent)
		return
	})

}

type deleteTestcase struct {
	description   string
	key           string
	expectedResp  *proto.DeleteResponse
	expectedEvent *proto.Event
}

func (dt *deleteTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(dt.description, func(t *testing.T) {
		checkSkip(t)
		ast := assert.New(t)
		backend := s.backend
		resp, err := backend.Delete(context.Background(), &proto.DeleteRequest{
			Key:      []byte(dt.key),
			Revision: 0,
		})
		ast.Equal(dt.expectedResp, resp)
		if dt.expectedResp == nil {
			ast.Error(err)
		} else {
			ast.NoError(err)
		}

		// sleep for a while and then check watcher
		waitUntilRevisionEqualOrTimeout(backend, resp.GetHeader().GetRevision())
		if dt.expectedEvent != nil {
			actual := nextRealEventBatch(output)
			if !ast.Equal(1, len(actual), expectAEvent) {
				ast.FailNow(noExpectedEvent)
			}
			if !ast.Equal(1, len(actual), expectAMvccPBEvent) {
				return
			}
			ast.Equal(dt.expectedEvent, actual[0])
			return
		}
		ast.True(noPendingRealEvent(output), expectNoEvent)
	})
}

type getTestcase struct {
	description  string
	getReq       *proto.GetRequest
	expectedResp *proto.GetResponse
}

func (gt *getTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(gt.description, func(st *testing.T) {
		checkSkip(st)
		backend := s.backend
		ast := assert.New(st)
		resp, err := backend.Get(context.Background(), gt.getReq)
		if resp != nil {
			ast.NoError(err)
		} else {
			ast.Error(err)
		}
		ast.Equal(gt.expectedResp, resp)

		time.Sleep(interval)
		ast.True(noPendingRealEvent(output), expectNoEvent)
	})
}

type rangeTestcase struct {
	description  string
	rangeReq     *proto.RangeRequest
	expectedResp *proto.RangeResponse
}

func (rt *rangeTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(rt.description, func(st *testing.T) {
		checkSkip(st)
		backend := s.backend
		ast := assert.New(st)

		resp, err := backend.List(context.Background(), rt.rangeReq)
		if resp != nil {
			ast.NoError(err)
		} else {
			ast.Error(err)
		}
		ast.Equal(rt.expectedResp, resp)

		time.Sleep(interval)
		ast.True(noPendingRealEvent(output), expectNoEvent)
	})
}

type countTestcase struct {
	description  string
	countReq     *proto.CountRequest
	expectedResp *proto.CountResponse
}

func (rt *countTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(rt.description, func(st *testing.T) {
		checkSkip(st)
		backend := s.backend
		ast := assert.New(st)

		resp, err := backend.Count(context.Background(), rt.countReq)
		if resp != nil {
			ast.NoError(err)
		} else {
			ast.Error(err)
		}
		ast.Equal(rt.expectedResp, resp)

		time.Sleep(interval)
		ast.True(noPendingRealEvent(output), expectNoEvent)
	})
}

type partitionsTestcase struct {
	description  string
	rangeReq     *proto.RangeRequest
	expectedResp *proto.RangeResponse
}

func (gpt *partitionsTestcase) run(t *testing.T, s suite, _ <-chan []*proto.Event) {
	t.Run(gpt.description, func(t *testing.T) {
		checkSkip(t)
		ast := assert.New(t)
		backend := s.backend
		lpReq := &proto.ListPartitionRequest{Key: gpt.rangeReq.Key, End: gpt.rangeReq.End}
		resp, err := backend.GetPartitions(s.ctx, lpReq)
		if gpt.expectedResp == nil {
			ast.Error(err)
			ast.Nil(resp)
			return
		}
		ast.NoError(err)
		ps := extractPartitions(resp)

		var kvs []*proto.KeyValue
		for i := 1; i < len(ps); i++ {
			wrCh, err := backend.ListByStream(s.ctx, ps[i-1], ps[i], 0)
			ast.NoError(err)
			for wr := range wrCh {
				kvs = append(kvs, wr.RangeResponse.Kvs...)
			}
		}
		ast.Equal(sortKvs(gpt.expectedResp.Kvs), sortKvs(kvs))
	})
}

func sortKvs(kvs []*proto.KeyValue) []*proto.KeyValue {
	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	return kvs
}

func extractPartitions(resp *proto.ListPartitionResponse) [][]byte {
	ret := make([][]byte, len(resp.PartitionKeys))
	for idx, kv := range resp.PartitionKeys {
		ret[idx] = kv
	}
	return ret
}

type updateTestcase struct {
	description   string
	key           string
	value         string
	revision      uint64
	expectedResp  *proto.UpdateResponse
	expectedEvent *proto.Event
}

func (ut *updateTestcase) run(t *testing.T, s suite, output <-chan []*proto.Event) {
	t.Run(ut.description, func(t *testing.T) {
		checkSkip(t)
		backend := s.backend
		ast := assert.New(t)
		resp, err := backend.Update(context.Background(), &proto.UpdateRequest{
			Kv: &proto.KeyValue{
				Key:      []byte(ut.key),
				Value:    []byte(ut.value),
				Revision: ut.revision,
			},
			Lease: 0,
		})
		ast.Equal(ut.expectedResp, resp)
		if ut.expectedResp == nil {
			ast.Error(err)
		} else {
			ast.NoError(err)
		}

		// sleep for a while and then check watcher
		waitUntilRevisionEqualOrTimeout(backend, resp.GetHeader().GetRevision())
		if ut.expectedEvent != nil {
			actual := nextRealEventBatch(output)
			if !ast.Equal(1, len(actual), expectAEvent) {
				ast.FailNow(noExpectedEvent)
			}
			if !ast.Equal(1, len(actual), expectAMvccPBEvent) {
				return
			}
			ast.Equal(ut.expectedEvent, actual[0])
			return
		}
		ast.True(noPendingRealEvent(output), expectNoEvent)
	})
}

func testBackendCreate(t *testing.T, targetStorage storageType) {
	s, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := s.ast
	backend := s.backend

	testKey := path.Join(prefix, testKey)
	initRevision := backend.GetCurrentRevision()
	ctx, cancel := context.WithCancel(context.Background())
	output, err := backend.Watch(ctx, prefix+"/", 0) // must end with slash as prefix
	if !ast.NoError(err) {
		ast.FailNow(setWatcherError)
	}
	defer cancel()

	// Table Driven Tests
	testcases := []createTestcase{
		{
			description:   "create once",
			putReq:        newCreateRequest(testKey, testVal),
			expectedResp:  newCreateResponse(initRevision+1, true),
			expectedEvent: newEvent(proto.Event_CREATE, initRevision+1, newKeyValue(testKey, testVal, initRevision+1)),
		},
		{
			description:  "create twice",
			putReq:       newCreateRequest(testKey, path.Join(testVal, "2")),
			expectedResp: newCreateResponse(initRevision+2, false),
		},
	}

	for _, testcase := range testcases {
		testcase.run(t, s, output)
	}

}

func testBackendDelete(t *testing.T, targetStorage storageType) {
	s, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := s.ast
	backend := s.backend

	// prepare context
	testKey := path.Join(prefix, testKey)
	resp, err := backend.Create(context.Background(), newCreateRequest(testKey, testVal))
	if !ast.NoError(err) {
		ast.FailNow("can not preset kv pair")
	}

	// ! must sleep here to ensure async event has been processed
	waitUntilRevisionEqualOrTimeout(backend, resp.GetHeader().GetRevision())
	initRevision := backend.GetCurrentRevision()
	if !ast.Equal(resp.Header.Revision, initRevision) {
		ast.FailNow(backendIsNotReady)
	}

	ctx, cancel := context.WithCancel(context.Background())
	output, err := backend.Watch(ctx, prefix+"/", 0)
	if !ast.NoError(err) {
		ast.FailNow(setWatcherError)
	}
	defer cancel()

	// Table Driven Tests
	// NOTICE: testcase depends on the sequence of running to generate context.
	//         please run all testcase in sequence but not only some of them while debugging.
	testcases := []deleteTestcase{
		{
			description: "key not found",
			key:         path.Join(prefix, "/test/key/not/found"),
			// todo(xueyingcai): delete key which doesn't exist in storage doesn't return error, so expect a non-nil expectedResp
			expectedResp: newDelResponse(initRevision+1, false, nil),
		},
		{
			description:   "delete success",
			key:           testKey,
			expectedResp:  newDelResponse(initRevision+2, true, newKeyValue(testKey, testVal, initRevision)),
			expectedEvent: newEvent(proto.Event_DELETE, initRevision+2, newKeyValue(testKey, testVal, initRevision)),
		},
		// todo(xueyingcai): add testcase about revision
	}

	for _, testcase := range testcases {
		testcase.run(t, s, output)
	}
}

func testBackendUpdate(t *testing.T, targetStorage storageType) {
	s, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := s.ast
	backend := s.backend

	// prepare context
	testKey := path.Join(prefix, testKey)
	initRevision := backend.GetCurrentRevision()

	ctx, cancel := context.WithCancel(context.Background())
	output, err := backend.Watch(ctx, prefix+"/", 0)
	if !ast.NoError(err) {
		ast.FailNow(setWatcherError)
	}
	defer cancel()

	// Table Driven Tests
	// NOTICE: testcase depends on the sequence of running to generate context.
	//         please run all testcase in sequence but not only some of them while debugging.
	testcases := []updateTestcase{
		{
			description:   "update a key which does not exist without revision",
			key:           testKey,
			value:         testVal,
			expectedResp:  newUpdateResponse(initRevision+1, true, nil),
			expectedEvent: newEvent(proto.Event_CREATE, initRevision+1, newKeyValue(testKey, testVal, initRevision+1)),
		},
		{
			description:  "update a key which exists without revision",
			key:          testKey,
			value:        testVal,
			expectedResp: newUpdateResponse(initRevision+2, false, newKeyValue(testKey, testVal, initRevision+1)),
		},
		{
			description:   "update a key which exists with valid revision",
			key:           testKey,
			value:         testVal,
			revision:      initRevision + 1,
			expectedResp:  newUpdateResponse(initRevision+3, true, nil), // valid revision should return PutResponse
			expectedEvent: newEvent(proto.Event_PUT, initRevision+3, newKeyValue(testKey, testVal, initRevision+3)),
		},
		{
			description: "update a key which exist with invalid revision",
			key:         testKey,
			value:       testVal,
			// todo
			revision:     initRevision + 1,
			expectedResp: newUpdateResponse(initRevision+4, false, newKeyValue(testKey, testVal, initRevision+3)),
		},
	}
	for _, testcase := range testcases {
		testcase.run(t, s, output)
	}
}

func testBackendRange(t *testing.T, targetStorage storageType) {

	s, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := s.ast
	backend := s.backend

	// prepare context
	testKey := path.Join(prefix, testKey)
	endKey := prefixEnd(testKey)
	format := func(prefix string, i int) string { return path.Join(prefix, fmt.Sprintf("%05d", i)) }
	const injectLen = 10 // decreasing `injectLen` should be carefully

	var resp *proto.CreateResponse
	var err error
	invalidRevision := backend.GetCurrentRevision()

	kvList := make([]*proto.KeyValue, injectLen)
	for i := 0; i < injectLen; i++ {
		key := format(testKey, i)
		val := format(testVal, i)
		resp, err = backend.Create(context.Background(), &proto.CreateRequest{
			Key:   []byte(key),
			Value: []byte(val),
			Lease: 0,
		})
		if !ast.NoError(err) {
			ast.FailNow("can not preset kv pair")
		}
		kvList[i] = newKeyValue(key, val, resp.Header.Revision)
	}
	// after preset, we have data below (rev means the latest revision)
	// 0. rev = ${latestRevision}
	// 1. key = f(i)
	// 2. val = g(i)
	// 3. revision = rev - (injectLen-1) + i
	//
	// revision  = rev-(injectLen-1)      rev-(injectLen-2)   rev-(injectLen-3)    ...     rev-1      rev
	// i         =       0                       1                   2             ...  injectLen-2  injectLen-1

	// ! must sleep here to ensure async event has been processed
	waitUntilRevisionEqualOrTimeout(backend, resp.GetHeader().GetRevision())
	initRevision := backend.GetCurrentRevision()
	if !ast.Equal(resp.Header.Revision, initRevision) {
		ast.FailNow(backendIsNotReady)
	}

	ctx, cancel := context.WithCancel(context.Background())
	output, err := backend.Watch(ctx, prefix+"/", 0)
	if !ast.NoError(err) {
		ast.FailNow(setWatcherError)
	}
	defer cancel()

	t.Run("native", func(t *testing.T) {
		// ! NOTICE: range operation is run on the snapshot define by revision.
		// !         revision of KeyValue returned in RangeResponse <= revision in RangeRequest.
		testcases := []testcase{
			// * get
			// 1. no use of revision in api server now
			// 2. no use of revision in response
			&getTestcase{
				description:  "get a existent key with valid revision",
				getReq:       newGetRequest(0, format(testKey, injectLen-1)),
				expectedResp: newGetResponse(initRevision, newKeyValue(format(testKey, injectLen-1), format(testVal, injectLen-1), initRevision)),
			},
			&getTestcase{
				description:  "get a existent key with revision after it was created",
				getReq:       newGetRequest(initRevision, format(testKey, injectLen-2)),
				expectedResp: newGetResponse(initRevision, newKeyValue(format(testKey, injectLen-2), format(testVal, injectLen-2), initRevision-1)),
			},
			&getTestcase{
				description:  "get a existent key with revision before it was created",
				getReq:       newGetRequest(invalidRevision, format(testKey, injectLen-1)),
				expectedResp: newGetResponse(initRevision, nil),
			},
			&getTestcase{
				description:  "get a nonexistent key",
				getReq:       newGetRequest(0, format(testKey, -1)),
				expectedResp: newGetResponse(initRevision, nil),
			},
			// * list
			&rangeTestcase{
				description:  "list with prefix",
				rangeReq:     newRangeRequest(0, testKey, endKey, 0),
				expectedResp: newRangeResponse(initRevision, kvList...),
			},
			&rangeTestcase{
				description:  "list with range end",
				rangeReq:     newRangeRequest(0, testKey, format(testKey, injectLen-2), 0),
				expectedResp: newRangeResponse(initRevision, kvList[:injectLen-2]...),
			},
			&rangeTestcase{
				//! COMPATIBILITY PROBLEM:
				//! etcd expected below response
				//! expectedResp: newLimitedRangeResponse(initRevision, injectLen, kvList[:injectLen-4]...),
				// todo(xueyingcai): if limit is set and there is more number of kvs matching conditions,
				//                   etcd returns the count of all kvs that match the conditions,
				//                   but kv storage backend just try to scan at most limit+1 kvs that
				//                   matching conditions to avoid scanning all kvs.
				description:  "list with range end & limit",
				rangeReq:     newRangeRequest(0, testKey, format(testKey, injectLen-2), injectLen-4),
				expectedResp: newLimitedRangeResponse(initRevision, injectLen-3, kvList[:injectLen-4]...),
			},
			&rangeTestcase{
				description:  "list with invalid prefix",
				rangeReq:     newRangeRequest(0, endKey, format(endKey, injectLen-2), 0),
				expectedResp: newRangeResponse(initRevision), // diff: kvs []*KeyValue(nil) <=> []*KeyValue{}
			},
			&rangeTestcase{
				description: "list with invalid range end",
				rangeReq:    newRangeRequest(0, format(endKey, injectLen-2), endKey, 0),
			},
			&rangeTestcase{
				description: "list with range end & limit & revision",
				// revision is set to the 2nd input value from last
				rangeReq:     newRangeRequest(initRevision-2, format(testKey, 1), format(testKey, injectLen-1), injectLen-5),
				expectedResp: newLimitedRangeResponse(initRevision, injectLen-4, kvList[1:injectLen-4]...),
			},
			&rangeTestcase{
				description:  "list with dir prefix",
				rangeReq:     newRangeRequest(0, testKey, prefixEnd(testKey), injectLen-5),
				expectedResp: newLimitedRangeResponse(initRevision, injectLen-4, kvList[0:injectLen-5]...),
			},
			// * count
			// todo(xueyingcai): Count does not limit revision now
			&countTestcase{
				description:  "count with valid prefix",
				countReq:     newCountRequest(0, testKey),
				expectedResp: newCountResponse(initRevision, injectLen),
			},
			&countTestcase{
				description:  "count with invalid prefix",
				countReq:     newCountRequest(0, endKey),
				expectedResp: newCountResponse(initRevision, 0),
			},
		}
		for _, testcase := range testcases {
			testcase.run(t, s, output)
		}
	})

	// todo(xueyingcai): GetPartitions and RangeStream are tightly coupled right now, test individually after refactoring
	t.Run("partitions", func(t *testing.T) {
		testcases := []partitionsTestcase{
			{
				description:  "list with prefix",
				rangeReq:     newRangeRequest(0, testKey, endKey, 0),
				expectedResp: newRangeResponse(initRevision, kvList...),
			},
			{
				description:  "list with range end",
				rangeReq:     newRangeRequest(0, testKey, format(testKey, injectLen-2), 0),
				expectedResp: newRangeResponse(initRevision, kvList[:injectLen-2]...),
			},
		}

		for _, testcase := range testcases {
			testcase.run(t, s, nil)
		}
	})
}

func testBackendRangeKubernetesPagination(t *testing.T, targetStorage storageType) {
	s, closer := newTestSuites(t, targetStorage)
	defer closer()

	ast := s.ast
	backend := s.backend
	baseKey := path.Join(prefix, "pagination")
	const objectCount = 20
	const pageSize = int64(5)

	for i := 0; i < objectCount; i++ {
		key := path.Join(baseKey, fmt.Sprintf("object-%02d", i))
		_, err := backend.Create(context.Background(), newCreateRequest(key, fmt.Sprintf("value-%02d", i)))
		if !ast.NoError(err) {
			ast.FailNow("can not preset paginated kv pair")
		}
	}
	waitUntilRevisionEqualOrTimeout(backend, backend.GetCurrentRevision())
	readRevision := backend.GetCurrentRevision()

	continueKey := baseKey
	seen := make(map[string]struct{}, objectCount)
	var ordered []string
	for {
		resp, err := backend.List(context.Background(), newRangeRequest(readRevision, continueKey, prefixEnd(baseKey), pageSize))
		if !ast.NoError(err) {
			ast.FailNow("paginated range failed")
		}
		if len(resp.Kvs) == 0 {
			ast.False(resp.More, "empty page must not advertise more results")
			break
		}
		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			if _, ok := seen[key]; ok {
				ast.FailNowf("duplicate key in paginated range", "duplicate key %q after keys %v", key, ordered)
			}
			seen[key] = struct{}{}
			ordered = append(ordered, key)
		}
		if !resp.More {
			break
		}
		continueKey = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}

	ast.Len(ordered, objectCount)
	for i := 0; i < objectCount; i++ {
		ast.Equal(path.Join(baseKey, fmt.Sprintf("object-%02d", i)), ordered[i])
	}
}

func TestBackendListFromKeyEnd(t *testing.T) {
	s, closer := newTestSuites(t, memKvStorage)
	defer closer()

	ast := s.ast
	backend := s.backend
	baseKey := path.Join(prefix, "from-key")
	for _, suffix := range []string{"a", "b", "c"} {
		_, err := backend.Create(context.Background(), newCreateRequest(path.Join(baseKey, suffix), suffix))
		if !ast.NoError(err) {
			ast.FailNow("can not preset from-key kv pair")
		}
	}
	waitUntilRevisionEqualOrTimeout(backend, backend.GetCurrentRevision())

	resp, err := backend.List(context.Background(), &proto.RangeRequest{
		Key: []byte(path.Join(baseKey, "b")),
		End: []byte{0},
	})
	if !ast.NoError(err) {
		ast.FailNow("from-key list failed")
	}
	if ast.GreaterOrEqual(len(resp.Kvs), 2) {
		ast.Equal([]byte(path.Join(baseKey, "b")), resp.Kvs[0].Key)
		ast.Equal([]byte(path.Join(baseKey, "c")), resp.Kvs[1].Key)
	}
}

func testBackendCompact(t *testing.T, targetStorage storageType) {
	// todo(xueyingcai): add test
	suite, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := suite.ast
	testKey := path.Join(prefix, testKey)
	{
		// create
		resp1, err := suite.backend.Create(suite.ctx, &proto.CreateRequest{
			Key:   []byte(testKey),
			Value: []byte(testVal),
			Lease: 0,
		})
		ast.NoError(err)
		newVal := path.Join(testVal, "new")
		rev1 := resp1.Header.Revision
		// update
		resp2, err := suite.backend.Update(suite.ctx, &proto.UpdateRequest{
			Kv: &proto.KeyValue{
				Key:      []byte(testKey),
				Value:    []byte(newVal),
				Revision: rev1,
			},
			Lease: 0,
		})
		ast.NoError(err)
		ast.Equal(newUpdateResponse(rev1+1, true, nil), resp2)
		rev2 := resp2.Header.Revision

		// wait for revision update
		waitUntilRevisionEqualOrTimeout(suite.backend, resp2.GetHeader().GetRevision())

		// get
		resp, err := suite.backend.Get(suite.ctx, newGetRequest(rev1, testKey))
		ast.NoError(err)
		ast.Equal(newGetResponse(rev2, newKeyValue(testKey, testVal, rev1)), resp)

		// compaction
		_, err = suite.backend.Compact(suite.ctx, 0)
		ast.NoError(err)

		// check
		resp, err = suite.backend.Get(suite.ctx, newGetRequest(rev1, testKey))
		ast.NoError(err)
		ast.Equal(newGetResponse(rev2+1, nil), resp)

		// delete
		dresp, err := suite.backend.Delete(suite.ctx, newDelRequest(0, testKey))
		ast.NoError(err)
		ast.True(dresp.Succeeded)

		// wait for revision update
		waitUntilRevisionEqualOrTimeout(suite.backend, dresp.GetHeader().GetRevision())

		// compaction
		_, err = suite.backend.Compact(suite.ctx, 0)
		ast.NoError(err)

		resp, err = suite.backend.Get(suite.ctx, newGetRequest(0, testKey))
		ast.NoError(err)
		ast.Equal(newGetResponse(dresp.Header.Revision+1, nil), resp)
	}
}

func testBackendReadHeadersStayAboveCompactRevision(t *testing.T, targetStorage storageType) {
	suite, closer := newTestSuites(t, targetStorage)
	defer closer()
	ast := suite.ast
	testKey := path.Join(prefix, t.Name(), testKey)

	createResp, err := suite.backend.Create(suite.ctx, &proto.CreateRequest{
		Key:   []byte(testKey),
		Value: []byte(testVal),
		Lease: 0,
	})
	ast.NoError(err)
	waitUntilRevisionEqualOrTimeout(suite.backend, createResp.GetHeader().GetRevision())

	compactRev := createResp.GetHeader().GetRevision()
	_, err = suite.backend.Compact(suite.ctx, compactRev)
	ast.NoError(err)

	// Simulate a restarted or lagging node that has read the durable compact
	// record but has not yet rebuilt its local current revision cache.
	suite.backend.SetCurrentRevision(compactRev - 1)
	expectedHeaderRev := compactRev + 1

	getResp, err := suite.backend.Get(suite.ctx, newGetRequest(0, testKey))
	ast.NoError(err)
	ast.Equal(expectedHeaderRev, getResp.GetHeader().GetRevision())
	ast.Equal(expectedHeaderRev, suite.backend.GetCurrentRevision())

	suite.backend.SetCurrentRevision(compactRev - 1)
	listResp, err := suite.backend.List(suite.ctx, newRangeRequest(0, testKey, string(PrefixEnd([]byte(testKey))), 0))
	ast.NoError(err)
	ast.Equal(expectedHeaderRev, listResp.GetHeader().GetRevision())
	ast.Equal(expectedHeaderRev, suite.backend.GetCurrentRevision())

	suite.backend.SetCurrentRevision(compactRev - 1)
	countResp, err := suite.backend.Count(suite.ctx, &proto.CountRequest{Key: []byte(testKey), End: PrefixEnd([]byte(testKey))})
	ast.NoError(err)
	ast.Equal(expectedHeaderRev, countResp.GetHeader().GetRevision())
	ast.Equal(expectedHeaderRev, suite.backend.GetCurrentRevision())
}

func testBackEnd(t *testing.T, st storageType) {
	t.Run("create", func(t *testing.T) {
		testBackendCreate(t, st)
	})

	t.Run("delete", func(t *testing.T) {
		testBackendDelete(t, st)
	})

	t.Run("update", func(t *testing.T) {
		testBackendUpdate(t, st)
	})

	t.Run("range", func(t *testing.T) {
		testBackendRange(t, st)
	})

	t.Run("range_kubernetes_pagination", func(t *testing.T) {
		testBackendRangeKubernetesPagination(t, st)
	})

	t.Run("compact", func(t *testing.T) {
		testBackendCompact(t, st)
	})

	t.Run("read_headers_stay_above_compact_revision", func(t *testing.T) {
		testBackendReadHeadersStayAboveCompactRevision(t, st)
	})

	t.Run("resource_lock", func(t *testing.T) {
		testBackendResourceLock(t, st)
	})
	t.Run("resource_lock_release_after_renew", func(t *testing.T) {
		testBackendResourceLockReleaseAfterRenew(t, st)
	})

	t.Run("delete_and_create", func(t *testing.T) {
		testBackendDeleteAndCreate(t, st)
	})

	t.Run("write_and_watch", func(t *testing.T) {
		testBackendWriteAndWatch(t, st)
	})
}

type resourceLockTestWrapper struct {
	resourcelock.Interface
	sync.Locker
	*sync.Cond
}

func newResourceLockTestWrapper(itf resourcelock.Interface) *resourceLockTestWrapper {
	var locker sync.Locker = &sync.Mutex{}
	rtw := &resourceLockTestWrapper{
		Interface: itf,
		Cond:      sync.NewCond(locker),
		Locker:    locker,
	}
	return rtw
}

func (rtw *resourceLockTestWrapper) Create(ler resourcelock.LeaderElectionRecord) error {
	err := rtw.Interface.Create(ler)
	rtw.Signal()
	return err
}

func (rtw *resourceLockTestWrapper) Update(ler resourcelock.LeaderElectionRecord) error {
	err := rtw.Interface.Update(ler)
	rtw.Signal()
	return err
}

func (rtw *resourceLockTestWrapper) Get() (*resourcelock.LeaderElectionRecord, error) {
	ler, err := rtw.Interface.Get()
	fmt.Println(ler, err)
	rtw.Signal()
	return ler, err
}

func (rtw *resourceLockTestWrapper) WaitForUsed(atLeaseTimes int) {
	rtw.Lock()
	defer rtw.Unlock()
	for i := 0; i < atLeaseTimes; i++ {
		rtw.Wait()
	}
}

func testBackendResourceLock(t *testing.T, targetStorage storageType) {
	if raceDetectorEnabled {
		// The concurrent leader-election this exercises trips a known data race
		// INSIDE vendored k8s.io/client-go 2019 leaderelection (LeaderElector
		// observedRecord/observedTime, fixed upstream later), not in KubeBrain
		// code. It passes without -race. Skip under -race to keep the race build
		// signal clean; see docs/known-flaky and the finding notes.
		t.Skip("skipping under -race: vendored client-go leaderelection data race, not KubeBrain code")
	}
	suiteA, closerA := newTestSuites(t, targetStorage)
	defer closerA()
	backendA := suiteA.backend
	backendB := NewBackend(suiteA.kv, Config{
		Prefix:   prefix,
		Identity: getStorageIdentity(),
	}, suiteA.metrics)

	ast := assert.New(t)

	rlA := newResourceLockTestWrapper(backendA.GetResourceLock())
	rlB := newResourceLockTestWrapper(backendB.GetResourceLock())

	var startLeadingCounter, stopLeadingCounter int64 = 0, 0
	var wg sync.WaitGroup
	lecA := leaderelection.LeaderElectionConfig{
		Name:            path.Join(prefix, "resource_lock"),
		Lock:            rlA,
		ReleaseOnCancel: true,
		RenewDeadline:   50 * interval,
		LeaseDuration:   80 * interval,
		RetryPeriod:     interval,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				fmt.Println("start")
				atomic.AddInt64(&startLeadingCounter, 1)
				wg.Done()
			},
			OnStoppedLeading: func() {
				fmt.Println("stop")
				atomic.AddInt64(&stopLeadingCounter, 1)
				wg.Done()
			},
		},
	}
	lecB := lecA
	lecB.Lock = rlB

	leA, err := leaderelection.NewLeaderElector(lecA)
	ast.NoError(err)
	leB, err := leaderelection.NewLeaderElector(lecB)
	ast.NoError(err)

	ctxA, cancelA := context.WithCancel(suiteA.ctx)
	ctxB, cancelB := context.WithCancel(context.Background())

	t.Run("acquire_a", func(t *testing.T) {
		ast := assert.New(t)
		wg.Add(1)
		go leA.Run(ctxA)

		wg.Wait()
		ast.Equal(int64(1), atomic.LoadInt64(&startLeadingCounter))
		ast.Equal(int64(0), atomic.LoadInt64(&stopLeadingCounter))
	})

	t.Run("acquire_b", func(t *testing.T) {
		ast := assert.New(t)

		go leB.Run(ctxB)

		// ensure signal is called more than twice to ensure Create and Update could be called
		rlB.WaitForUsed(2)
		ast.Equal(int64(1), atomic.LoadInt64(&startLeadingCounter))
		ast.Equal(int64(0), atomic.LoadInt64(&stopLeadingCounter))
	})

	t.Run("cancel_a", func(t *testing.T) {
		ast := assert.New(t)
		wg.Add(2)
		cancelA()

		wg.Wait()
		ast.Equal(int64(2), atomic.LoadInt64(&startLeadingCounter))
		ast.Equal(int64(1), atomic.LoadInt64(&stopLeadingCounter))
	})

	t.Run("cancel_b", func(t *testing.T) {
		ast := assert.New(t)
		wg.Add(1)
		cancelB()

		wg.Wait()
		ast.Equal(int64(2), atomic.LoadInt64(&startLeadingCounter))
		ast.Equal(int64(2), atomic.LoadInt64(&stopLeadingCounter))
	})

}

func testBackendResourceLockReleaseAfterRenew(t *testing.T, targetStorage storageType) {
	suite, closer := newTestSuites(t, targetStorage)
	defer closer()

	lock := suite.backend.GetResourceLock()
	first := resourcelock.LeaderElectionRecord{
		HolderIdentity:       lock.Identity(),
		LeaseDurationSeconds: 8,
		LeaderTransitions:    1,
	}
	suite.ast.NoError(lock.Create(first))

	second := first
	second.RenewTime = first.RenewTime
	suite.ast.NoError(lock.Update(second))

	release := resourcelock.LeaderElectionRecord{
		LeaderTransitions: second.LeaderTransitions,
	}
	suite.ast.NoError(lock.Update(release))

	record, err := lock.Get()
	suite.ast.NoError(err)
	suite.ast.Empty(record.HolderIdentity)
	suite.ast.Contains(lock.Describe(), "empty,")
}

func testBackendDeleteAndCreate(t *testing.T, targetStorage storageType) {
	suite, closer := newTestSuites(t, targetStorage)
	defer closer()
	initRevision := suite.backend.GetCurrentRevision()
	ast := suite.ast

	ch, err := suite.backend.Watch(suite.ctx, prefix, 0)
	ast.NoError(err)

	testKey := path.Join(prefix, "delete/and/create")
	val1 := "val1"
	val2 := "val2"
	tcs := []testcase{
		&createTestcase{
			description:   "first create",
			putReq:        newCreateRequest(testKey, val1),
			expectedResp:  newCreateResponse(initRevision+1, true),
			expectedEvent: newEvent(proto.Event_CREATE, initRevision+1, newKeyValue(testKey, val1, initRevision+1)),
		},
		&deleteTestcase{
			description:   "delete",
			key:           testKey,
			expectedResp:  newDelResponse(initRevision+2, true, newKeyValue(testKey, val1, initRevision+1)),
			expectedEvent: newEvent(proto.Event_DELETE, initRevision+2, newKeyValue(testKey, val1, initRevision+1)),
		},
		&createTestcase{
			description:   "twice create",
			putReq:        newCreateRequest(testKey, val2),
			expectedResp:  newCreateResponse(initRevision+3, true),
			expectedEvent: newEvent(proto.Event_CREATE, initRevision+3, newKeyValue(testKey, val2, initRevision+3)),
		},
		&getTestcase{
			description:  "check",
			getReq:       newGetRequest(0, testKey),
			expectedResp: newGetResponse(initRevision+3, newKeyValue(testKey, val2, initRevision+3)),
		},
	}

	for _, tc := range tcs {
		tc.run(t, suite, ch)
	}
}

func testBackendWriteAndWatch(t *testing.T, targetStorage storageType) {

	suite, closer := newTestSuites(t, targetStorage)
	defer closer()
	initRevision := suite.backend.GetCurrentRevision()
	ast := suite.ast
	p := "create/and/watch"
	times := 10
	var cresp *proto.CreateResponse
	var err error
	for i := 0; i < times; i++ {
		testKey := path.Join(prefix, p, strconv.Itoa(i))
		cresp, err = suite.backend.Create(suite.ctx, newCreateRequest(testKey, testVal))
		if !ast.NoError(err) {
			ast.FailNow("can not preset kv pair")
		}
		ast.Equal(initRevision+uint64(i+1), cresp.Header.Revision)
		ast.True(cresp.Succeeded)
	}

	var dresp *proto.DeleteResponse
	for i := 0; i < times; i++ {
		testKey := path.Join(prefix, p, strconv.Itoa(i))
		dresp, err = suite.backend.Delete(suite.ctx, newDelRequest(initRevision+uint64(i+1), testKey))
		if !ast.NoError(err) {
			ast.FailNow("can not delete kv pair")
		}
		ast.Equal(initRevision+uint64(i+times+1), dresp.Header.Revision)
		ast.True(dresp.Succeeded)
	}

	waitUntilRevisionEqualOrTimeout(suite.backend, dresp.GetHeader().GetRevision())
	ctx, cancel := context.WithCancel(suite.ctx)
	ch, err := suite.backend.Watch(ctx, prefix, initRevision)
	ast.NoError(err)
	ast.NotNil(ch)
	cancel()

	t.Run("check all events", func(t *testing.T) {
		ast := assert.New(t)
		events := getEventsFromRev(suite.ctx, suite.backend, initRevision+1, times*2)
		t.Logf("get events size=%d", len(events))
		// check create events
		for i := 0; i < times; i++ {
			event := events[i]
			ast.Equal(proto.Event_CREATE, event.Type)
			ast.Equal(initRevision+uint64(i+1), event.Revision)
			ast.True(strings.HasSuffix(string(event.Kv.Key), strconv.Itoa(i)))
			ast.Equal([]byte(testVal), event.Kv.Value)
		}

		// check delete events
		offset := times
		for i := 0; i < times; i++ {
			event := events[i+offset]
			ast.Equal(proto.Event_DELETE, event.Type)
			ast.Equal(initRevision+uint64(i+offset+1), event.Revision)
			ast.True(strings.HasSuffix(string(event.Kv.Key), strconv.Itoa(i)))
			ast.Equal([]byte(testVal), event.Kv.Value)
		}
	})

	t.Run("check delete event", func(t *testing.T) {
		events := getEventsFromRev(suite.ctx, suite.backend, initRevision+uint64(times+1), times)
		t.Logf("get events size=%d", len(events))
		// check delete events
		offset := times
		for i := 0; i < times; i++ {
			event := events[i]
			ast.Equal(proto.Event_DELETE, event.Type)
			ast.Equal(initRevision+uint64(i+offset+1), event.Revision)
			ast.True(strings.HasSuffix(string(event.Kv.Key), strconv.Itoa(i)))
			ast.Equal([]byte(testVal), event.Kv.Value)
		}
	})
}

func TestBackendDeleteRangeWatchEventsShareRevision(t *testing.T) {
	suite, closer := newTestSuites(t, memKvStorage)
	defer closer()

	baseKey := path.Join(prefix, "delete-range-watch")
	keys := []string{
		path.Join(baseKey, "a"),
		path.Join(baseKey, "b"),
		path.Join(baseKey, "c"),
	}
	for _, key := range keys {
		resp, err := suite.backend.Create(suite.ctx, newCreateRequest(key, testVal))
		suite.ast.NoError(err)
		suite.ast.True(resp.Succeeded)
	}
	kvs := make([]*proto.KeyValue, 0, len(keys))
	for _, key := range keys {
		getResp, err := suite.backend.Get(suite.ctx, newGetRequest(0, key))
		suite.ast.NoError(err)
		if getResp.Kv != nil {
			kvs = append(kvs, getResp.Kv)
		}
	}
	suite.ast.Len(kvs, len(keys))

	deleteResp, err := suite.backend.DeleteRange(suite.ctx, kvs)
	suite.ast.NoError(err)
	suite.ast.True(deleteResp.Succeeded)
	suite.ast.Len(deleteResp.Kvs, len(keys))

	waitUntilRevisionEqualOrTimeout(suite.backend, deleteResp.Header.Revision)
	events := getEventsFromRev(suite.ctx, suite.backend, deleteResp.Header.Revision, len(keys))
	suite.ast.Len(events, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, event := range events {
		suite.ast.Equal(proto.Event_DELETE, event.Type)
		suite.ast.Equal(deleteResp.Header.Revision, event.Revision)
		seen[string(event.Kv.Key)] = struct{}{}
	}
	for _, key := range keys {
		_, ok := seen[key]
		suite.ast.True(ok, "missing delete event for %s", key)
	}
}

func TestBackendWatchHistoryFallbackAfterRestart(t *testing.T) {
	suite, closer := newTestSuites(t, memKvStorage)
	defer closer()

	baseKey := path.Join(prefix, "watch-history-fallback")
	keyA := path.Join(baseKey, "a")
	keyB := path.Join(baseKey, "b")
	fromRevision := suite.backend.GetCurrentRevision()

	createA, err := suite.backend.Create(suite.ctx, newCreateRequest(keyA, "a-1"))
	suite.ast.NoError(err)
	createB, err := suite.backend.Create(suite.ctx, newCreateRequest(keyB, "b-1"))
	suite.ast.NoError(err)
	updateA, err := suite.backend.Update(suite.ctx, &proto.UpdateRequest{
		Kv: &proto.KeyValue{
			Key:      []byte(keyA),
			Value:    []byte("a-2"),
			Revision: createA.Header.Revision,
		},
	})
	suite.ast.NoError(err)
	deleteB, err := suite.backend.Delete(suite.ctx, &proto.DeleteRequest{
		Key:      []byte(keyB),
		Revision: createB.Header.Revision,
	})
	suite.ast.NoError(err)
	waitUntilRevisionEqualOrTimeout(suite.backend, deleteB.Header.Revision)

	restarted := NewBackend(suite.kv, Config{Prefix: prefix, Identity: getStorageIdentity()}, suite.metrics)
	restarted.SetCurrentRevision(suite.backend.GetCurrentRevision())

	events := getEventsFromRev(suite.ctx, restarted, fromRevision, 4)
	suite.ast.Len(events, 4)
	suite.ast.Equal(createA.Header.Revision, events[0].Revision)
	suite.ast.Equal(createB.Header.Revision, events[1].Revision)
	suite.ast.Equal(updateA.Header.Revision, events[2].Revision)
	suite.ast.Equal(deleteB.Header.Revision, events[3].Revision)
	suite.ast.Equal(proto.Event_DELETE, events[3].Type)
	suite.ast.Equal([]byte(keyB), events[3].Kv.Key)
	suite.ast.Equal([]byte("b-1"), events[3].Kv.Value)
}

func TestBackendWatchHistoryFallbackIncludesStartRevision(t *testing.T) {
	suite, closer := newTestSuites(t, memKvStorage)
	defer closer()

	baseKey := path.Join(prefix, "watch-history-start-revision")
	keyA := path.Join(baseKey, "a")
	keyB := path.Join(baseKey, "b")

	createA, err := suite.backend.Create(suite.ctx, newCreateRequest(keyA, "a-1"))
	suite.ast.NoError(err)
	createB, err := suite.backend.Create(suite.ctx, newCreateRequest(keyB, "b-1"))
	suite.ast.NoError(err)
	updateA, err := suite.backend.Update(suite.ctx, &proto.UpdateRequest{
		Kv: &proto.KeyValue{
			Key:      []byte(keyA),
			Value:    []byte("a-2"),
			Revision: createA.Header.Revision,
		},
	})
	suite.ast.NoError(err)
	waitUntilRevisionEqualOrTimeout(suite.backend, updateA.Header.Revision)

	restarted := NewBackend(suite.kv, Config{Prefix: prefix, Identity: getStorageIdentity()}, suite.metrics)
	restarted.SetCurrentRevision(suite.backend.GetCurrentRevision())

	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	ch, err := restarted.Watch(ctx, baseKey, createA.Header.Revision)
	suite.ast.NoError(err)

	select {
	case events := <-ch:
		if suite.ast.Len(events, 3) {
			suite.ast.Equal(createA.Header.Revision, events[0].Revision)
			suite.ast.Equal(proto.Event_CREATE, events[0].Type)
			suite.ast.Equal([]byte(keyA), events[0].Kv.Key)
			suite.ast.Equal(createB.Header.Revision, events[1].Revision)
			suite.ast.Equal(updateA.Header.Revision, events[2].Revision)
		}
	case <-time.After(timeout):
		t.Fatal("timed out waiting for history watch events")
	}
}

func TestBackendWatchHistoryFallbackWhenCacheOldestIsTooNew(t *testing.T) {
	suite, closer := newTestSuites(t, memKvStorage)
	defer closer()

	baseKey := path.Join(prefix, "watch-history-cache-low")
	keyA := path.Join(baseKey, "a")
	keyB := path.Join(baseKey, "b")

	createA, err := suite.backend.Create(suite.ctx, newCreateRequest(keyA, "a-1"))
	suite.ast.NoError(err)
	createB, err := suite.backend.Create(suite.ctx, newCreateRequest(keyB, "b-1"))
	suite.ast.NoError(err)
	updateA, err := suite.backend.Update(suite.ctx, &proto.UpdateRequest{
		Kv: &proto.KeyValue{
			Key:      []byte(keyA),
			Value:    []byte("a-2"),
			Revision: createA.Header.Revision,
		},
	})
	suite.ast.NoError(err)
	waitUntilRevisionEqualOrTimeout(suite.backend, updateA.Header.Revision)

	b := suite.backend.(*backend)
	b.watchCache.Reset()
	b.watchCache.Add(newEvent(proto.Event_PUT, updateA.Header.Revision, newKeyValue(keyA, "a-2", updateA.Header.Revision)))

	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	ch, err := b.Watch(ctx, baseKey, createA.Header.Revision)
	suite.ast.NoError(err)

	select {
	case events := <-ch:
		if suite.ast.Len(events, 3) {
			suite.ast.Equal(createA.Header.Revision, events[0].Revision)
			suite.ast.Equal(createB.Header.Revision, events[1].Revision)
			suite.ast.Equal(updateA.Header.Revision, events[2].Revision)
		}
	case <-time.After(timeout):
		t.Fatal("timed out waiting for low-cache history watch events")
	}
}

func TestBackendWatchCatchUpAdvancesFromLastMatchingPrefixEvent(t *testing.T) {
	suite, closer := newTestSuites(t, memKvStorage)
	defer closer()

	b := suite.backend.(*backend)
	targetPrefix := path.Join(prefix, "watch-prefix-catch-up")
	otherPrefix := path.Join(prefix, "watch-prefix-other")

	b.watchCache.Reset()
	b.SetCurrentRevision(12)
	b.watchCache.Add(newEvent(proto.Event_PUT, 10, newKeyValue(path.Join(targetPrefix, "a"), "target-10", 10)))
	b.watchCache.Add(newEvent(proto.Event_PUT, 11, newKeyValue(path.Join(otherPrefix, "b"), "other-11", 11)))
	b.watchCache.Add(newEvent(proto.Event_PUT, 12, newKeyValue(path.Join(otherPrefix, "c"), "other-12", 12)))

	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	ch, err := b.Watch(ctx, targetPrefix, 10)
	suite.ast.NoError(err)

	select {
	case events := <-ch:
		if suite.ast.Len(events, 1) {
			suite.ast.Equal(uint64(10), events[0].Revision)
			suite.ast.Equal([]byte(path.Join(targetPrefix, "a")), events[0].Kv.Key)
		}
	case <-time.After(timeout):
		t.Fatal("timed out waiting for cached prefix event")
	}

	b.watchChan <- []*proto.Event{
		newEvent(proto.Event_PUT, 11, newKeyValue(path.Join(targetPrefix, "live"), "target-11", 11)),
	}

	select {
	case events := <-ch:
		if suite.ast.Len(events, 1) {
			suite.ast.Equal(uint64(11), events[0].Revision)
			suite.ast.Equal([]byte(path.Join(targetPrefix, "live")), events[0].Kv.Key)
		}
	case <-time.After(timeout):
		t.Fatal("timed out waiting for live prefix event after mixed-prefix catch-up")
	}
}

func TestBackendWatchHistoryFallbackRespectsCompaction(t *testing.T) {
	suite, closer := newTestSuites(t, memKvStorage)
	defer closer()

	key := path.Join(prefix, "watch-history-compact", "a")
	fromRevision := suite.backend.GetCurrentRevision()
	createResp, err := suite.backend.Create(suite.ctx, newCreateRequest(key, "v1"))
	suite.ast.NoError(err)
	waitUntilRevisionEqualOrTimeout(suite.backend, createResp.Header.Revision)

	_, err = suite.backend.Compact(suite.ctx, createResp.Header.Revision)
	suite.ast.NoError(err)

	restarted := NewBackend(suite.kv, Config{Prefix: prefix, Identity: getStorageIdentity()}, suite.metrics)
	restarted.SetCurrentRevision(suite.backend.GetCurrentRevision())

	ch, err := restarted.Watch(suite.ctx, prefix, fromRevision)
	suite.ast.Error(err)
	suite.ast.Nil(ch)
	// The revision is genuinely below the compact watermark, so the error must
	// signal compaction (so the client re-lists) rather than being flattened into
	// a generic "empty cache" message that the client would retry forever (#55).
	suite.ast.Contains(err.Error(), "compacted")
}

func getEventsFromRev(ctx context.Context, b Backend, fromRev uint64, size int) []*proto.Event {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, _ := b.Watch(ctx, prefix, fromRev)
	allEvents := make([]*proto.Event, 0, size)
	for events := range ch {
		allEvents = append(allEvents, events...)
		if len(allEvents) == cap(allEvents) {
			cancel()
			break
		}
	}
	return allEvents
}

func TestUncertainRewrite(t *testing.T) {
	defaultRetryInterval, defaultCheckInterval := retryInterval, checkInterval
	retryInterval, checkInterval = 100*time.Millisecond, 100*time.Millisecond
	defer func() { retryInterval, checkInterval = defaultRetryInterval, defaultCheckInterval }()

	s, closer := newTestSuites(t, badgerStorage)
	defer closer()
	b := s.backend.(*backend)
	testKey := path.Join(prefix, testKey)
	initRev := b.GetCurrentRevision()
	invalidPrevRev := initRev - 1
	t.Log("init revision", initRev)
	{
		// 1
		t.Log("put while key doesn't exist, expect nothing change")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), initRev+1, invalidPrevRev, false, proto.Event_PUT, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+1)
		resp, err := b.Get(s.ctx, newGetRequest(0, testVal))
		s.ast.NoError(err)
		s.ast.Nil(resp.Kv)
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// 2
		t.Log("del while key doesn't exist, expect nothing change")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), initRev+2, invalidPrevRev, false, proto.Event_DELETE, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+2)
		resp, err := b.Get(s.ctx, newGetRequest(0, testVal))
		s.ast.NoError(err)
		s.ast.Nil(resp.Kv)
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// 3
		t.Log("write the key and expected no error")
		rev, err := b.create(s.ctx, []byte(testKey), []byte(testVal))
		s.ast.Equal(initRev+3, rev)
		s.ast.NoError(err)

		// 4
		t.Log("imitate the uncertain case, there should be an async retry to write the same data again")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), rev, 0, false, proto.Event_PUT, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+4)
		resp, err := b.Get(s.ctx, newGetRequest(0, testKey))
		s.ast.NoError(err)
		s.ast.NotNil(resp.Kv)
		s.ast.Equal(int64(initRev)+4, int64(resp.Kv.Revision))
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// 5
		t.Log("imitate there is something changed and result is uncertain, expected prev revision is not equal to the current, just do nothing")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), initRev+5, invalidPrevRev, false, proto.Event_PUT, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+5)
		resp, err := b.Get(s.ctx, newGetRequest(0, testKey))
		s.ast.NoError(err)
		s.ast.NotNil(resp.Kv)
		s.ast.Equal(int64(initRev)+4, int64(resp.Kv.Revision))
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// 6
		t.Log("delete")
		rev, kv, err := b.delete(s.ctx, 0, []byte(testKey))
		s.ast.NotNil(kv)
		s.ast.Equal(int64(initRev)+4, int64(kv.Revision))
		s.ast.Equal(int64(initRev)+6, int64(rev))
		s.ast.NoError(err)

		t.Log("compact before notifying to imitate conflict")
		err = b.compact(s.ctx, initRev)
		s.ast.NoError(err)

		// 7
		t.Log("imitate the uncertain case, there should be an async retry to write the same data again, expected there is an async retry")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), rev, initRev+4, false, proto.Event_DELETE, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+7)
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// 8
		t.Log("delete while there is uncertain result")
		b.notify(s.ctx, []byte(testKey), []byte(testVal), initRev+8, invalidPrevRev, false, proto.Event_PUT, storage.ErrUncertainResult)
		waitUntilRetryQueueDrainOrTimeout(s.ctx, b, initRev+8)
		t.Log("revision", b.GetCurrentRevision())
	}

	{
		// check event
		watchCtx, cancel := context.WithCancel(s.ctx)
		t.Log("watch rev", initRev+4)
		ch, err := b.Watch(watchCtx, testKey, initRev+4)
		s.ast.NoError(err)

		expectedEvents := []*proto.Event{
			newEvent(proto.Event_PUT, initRev+4, newKeyValue(testKey, testVal, initRev+4)),
			// todo: if the event if right?
			newEvent(proto.Event_DELETE, initRev+7, newKeyValue(testKey, testVal, initRev+4)),
		}
		var actualEvents []*proto.Event

		for batch := range ch {
			actualEvents = append(actualEvents, batch...)
			for _, ev := range batch {
				klog.InfoS(ev.Type.String(), ev.Kv == nil)
			}

			if len(actualEvents) == len(expectedEvents) {
				break
			}
		}
		cancel()
		s.ast.Equal(expectedEvents, actualEvents)
	}
}

func waitUntilRetryQueueDrainOrTimeout(ctx context.Context, b *backend, expectedRev uint64) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if b.GetCurrentRevision() == expectedRev && b.asyncFifoRetry.Size() == 0 {
				return
			}
		}
	}
}

func TestWatchEventOverflowResetsWatchState(t *testing.T) {
	s, closer := newTestSuites(t, memKvStorage)
	defer closer()

	b := s.backend.(*backend)
	watchCtx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	watcher, err := b.watcherHub.AddWatcher(watchCtx, nil)
	s.ast.NoError(err)

	revision := b.GetCurrentRevision() + watchersChanCapacity
	s.ast.NotPanics(func() {
		b.notify(s.ctx, []byte(prefix+"/overflow"), []byte("value"), revision, 0, true, proto.Event_PUT, nil)
	})
	s.ast.Equal(revision, b.GetCurrentRevision())
	s.ast.True(b.watchCache.FindEvents(revision).empty)

	select {
	case _, ok := <-watcher:
		s.ast.False(ok)
	case <-time.After(time.Second):
		t.Fatal("watcher was not closed after watch event overflow")
	}
}

func TestBackend(t *testing.T) {
	for name, st := range storages {
		t.Run(name, func(t *testing.T) {
			testBackEnd(t, st)
		})
	}
}

func prefixEnd(p string) string {
	return string(PrefixEnd([]byte(p)))
}

// nextRealEventBatch returns the next real (non-progress-marker) event batch from
// output, waiting up to timeout. The watcher hub now fans an in-band progress
// marker (a nil-Kv sentinel batch) to every subscriber each second so a quiet
// watch's progress can advance; these low-level event tests must skip them.
// Returns nil on timeout.
func nextRealEventBatch(output <-chan []*proto.Event) []*proto.Event {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case evs := <-output:
			if isProgressMarker(evs) {
				continue
			}
			return evs
		case <-ctx.Done():
			return nil
		}
	}
}

// noPendingRealEvent reports that no real (non-marker) event batch is buffered on
// output within a short settle window, ignoring progress markers.
func noPendingRealEvent(output <-chan []*proto.Event) bool {
	deadline := time.Now().Add(interval)
	for {
		select {
		case evs := <-output:
			if isProgressMarker(evs) {
				continue
			}
			return false
		default:
			if time.Now().After(deadline) {
				return true
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func waitUntilEventChanFilledOrTimeout(eventChan <-chan []*proto.Event) {

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if len(eventChan) != 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func waitUntilRevisionEqualOrTimeout(b Backend, expectedRev uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if b.GetCurrentRevision() == expectedRev {
				time.Sleep(interval)
				return
			}
		}
	}
}

// TestOrphanIndexSelfHeal reproduces the "poison key" shape diagnosed on the TiKV
// dev cluster (object versions present, revision-index rev=0 slot missing) and
// asserts the write path now self-heals it. Such an orphan was left by a pre-#31
// compaction/retry race; it is read-visible (reads scan object keys, ignoring the
// index) but was permanently un-writable (Update/Delete CAS the missing index and
// fail forever). healOrphanIndex restores the index so the next write recovers.
func TestOrphanIndexSelfHeal(t *testing.T) {
	s, closeFn := newTestSuites(t, memKvStorage)
	defer closeFn()
	b := s.backend.(*backend)

	// orphan removes the revision-index (rev=0) slot, leaving the object keys, then
	// checks the key is still read-visible but the index is gone.
	orphan := func(key string) {
		s.ast.NoError(s.kv.Del(s.ctx, encodeRevisionKey([]byte(key))))
		_, gerr := s.kv.Get(s.ctx, encodeRevisionKey([]byte(key)))
		s.ast.ErrorIs(gerr, storage.ErrKeyNotFound)
		_, _, rerr := b.get(s.ctx, []byte(key), 0)
		s.ast.NoError(rerr) // object still present -> read-visible
	}

	t.Run("delete self-heals orphan", func(t *testing.T) {
		key := path.Join(prefix, "orphan-del")
		_, err := s.backend.Create(s.ctx, newCreateRequest(key, "v1"))
		s.ast.NoError(err)
		orphan(key)

		// Without the heal, delete is a poisoned no-op: CAS on the missing index fails.
		noHeal, err := b.deleteOnce(s.ctx, newDelRequest(0, key), false)
		s.ast.NoError(err)
		s.ast.False(noHeal.Succeeded)
		_, _, rerr := b.get(s.ctx, []byte(key), 0)
		s.ast.NoError(rerr) // still there

		// With the heal (default path), delete succeeds and the key is gone.
		res, err := s.backend.Delete(s.ctx, newDelRequest(0, key))
		s.ast.NoError(err)
		s.ast.True(res.Succeeded)
		_, _, rerr = b.get(s.ctx, []byte(key), 0)
		s.ast.ErrorIs(rerr, storage.ErrKeyNotFound)
	})

	t.Run("update self-heals orphan", func(t *testing.T) {
		key := path.Join(prefix, "orphan-upd")
		cresp, err := s.backend.Create(s.ctx, newCreateRequest(key, "v1"))
		s.ast.NoError(err)
		modRev := cresp.Header.Revision
		orphan(key)

		// Without the heal, update is a poisoned no-op.
		noHeal, err := b.updateOnce(s.ctx, &proto.UpdateRequest{
			Kv: &proto.KeyValue{Key: []byte(key), Value: []byte("v2"), Revision: modRev},
		}, false)
		s.ast.NoError(err)
		s.ast.False(noHeal.Succeeded)

		// With the heal, update succeeds and the new value is readable.
		res, err := s.backend.Update(s.ctx, &proto.UpdateRequest{
			Kv: &proto.KeyValue{Key: []byte(key), Value: []byte("v2"), Revision: modRev},
		})
		s.ast.NoError(err)
		s.ast.True(res.Succeeded)
		val, _, rerr := b.get(s.ctx, []byte(key), 0)
		s.ast.NoError(rerr)
		s.ast.Equal("v2", string(val))
	})
}

// TestBackendDeleteRangeChunksLargeRange deletes a range larger than
// deleteRangeChunkSize to exercise the multi-chunk path: every key must be
// deleted and reported, even though the chunks commit at their own revisions
// (a large range cannot be one atomic TiKV txn).
func TestBackendDeleteRangeChunksLargeRange(t *testing.T) {
	suite, closer := newTestSuites(t, memKvStorage)
	defer closer()

	const n = 300 // > deleteRangeChunkSize (128): spans 3 chunks
	baseKey := path.Join(prefix, "delete-range-chunk")
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := path.Join(baseKey, fmt.Sprintf("k%04d", i))
		keys = append(keys, key)
		resp, err := suite.backend.Create(suite.ctx, newCreateRequest(key, testVal))
		suite.ast.NoError(err)
		suite.ast.True(resp.Succeeded)
	}

	kvs := make([]*proto.KeyValue, 0, n)
	for _, key := range keys {
		getResp, err := suite.backend.Get(suite.ctx, newGetRequest(0, key))
		suite.ast.NoError(err)
		suite.ast.NotNil(getResp.Kv)
		kvs = append(kvs, getResp.Kv)
	}
	suite.ast.Len(kvs, n)

	deleteResp, err := suite.backend.DeleteRange(suite.ctx, kvs)
	suite.ast.NoError(err)
	suite.ast.True(deleteResp.Succeeded)
	suite.ast.Len(deleteResp.Kvs, n, "every key in the range must be reported deleted")

	// All keys are gone.
	for _, key := range keys {
		getResp, err := suite.backend.Get(suite.ctx, newGetRequest(0, key))
		suite.ast.NoError(err)
		suite.ast.Nil(getResp.Kv, "key %s should be deleted", key)
	}
}
