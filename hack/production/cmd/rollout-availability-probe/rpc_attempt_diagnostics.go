package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

const (
	publicRPCAttemptRingCapacity = 256
	publicRPCAttemptOutputLimit  = 64
)

type rpcAttemptContextKey struct {
	recorder *rpcAttemptRecorder
}

type rpcAttemptTracker struct {
	mu                  sync.Mutex
	method              string
	nameResolutionDelay bool
	current             *rpcAttemptRecord
}

type rpcAttemptRecord struct {
	method              string
	remote              string
	servingMemberID     string
	nameResolutionDelay bool
	transparentRetry    bool
	begin               time.Time
	pick                time.Time
	outHeader           time.Time
	inHeader            time.Time
	end                 time.Time
	code                codes.Code
}

type rpcAttemptRecorder struct {
	mu         sync.Mutex
	records    []rpcAttemptRecord
	next       int
	count      int
	capacity   int
	overwrites int
	now        func() time.Time
}

type rpcAttemptEvidence struct {
	Method                    string `json:"method"`
	Remote                    string `json:"remote"`
	ServingMemberID           string `json:"serving_member_id"`
	NameResolutionDelay       bool   `json:"name_resolution_delay"`
	TransparentRetry          bool   `json:"transparent_retry"`
	OperationToBeginMicros    int64  `json:"operation_to_begin_us"`
	BeginToPickMicros         *int64 `json:"begin_to_pick_us"`
	BeginToOutHeaderMicros    *int64 `json:"begin_to_out_header_us"`
	OutHeaderToInHeaderMicros *int64 `json:"out_header_to_in_header_us"`
	InHeaderToEndMicros       *int64 `json:"in_header_to_end_us"`
	TotalMicros               int64  `json:"total_us"`
	Code                      string `json:"code"`
}

type rpcAttemptEvidenceReport struct {
	Attempts       []rpcAttemptEvidence `json:"attempts"`
	Omitted        int                  `json:"omitted"`
	RingOverwrites int                  `json:"ring_overwrites"`
}

func newRPCAttemptRecorder(capacity int, now func() time.Time) *rpcAttemptRecorder {
	if capacity < 1 {
		capacity = 1
	}
	if now == nil {
		now = time.Now
	}
	return &rpcAttemptRecorder{
		records:  make([]rpcAttemptRecord, capacity),
		capacity: capacity,
		now:      now,
	}
}

func normalizeDiagnosticRPCMethod(fullMethod string) string {
	switch fullMethod {
	case "/etcdserverpb.KV/Range":
		return "range"
	case "/etcdserverpb.KV/Txn":
		return "txn"
	case "/etcdserverpb.KV/Put":
		return "put"
	case "/etcdserverpb.KV/DeleteRange":
		return "delete_range"
	default:
		return ""
	}
}

func (recorder *rpcAttemptRecorder) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if recorder == nil || info == nil {
		return ctx
	}
	method := normalizeDiagnosticRPCMethod(info.FullMethodName)
	if method == "" {
		return ctx
	}
	return context.WithValue(ctx, rpcAttemptContextKey{recorder: recorder}, &rpcAttemptTracker{
		method: method, nameResolutionDelay: info.NameResolutionDelay,
	})
}

func (*rpcAttemptRecorder) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*rpcAttemptRecorder) HandleConn(context.Context, stats.ConnStats) {}

func (recorder *rpcAttemptRecorder) HandleRPC(ctx context.Context, event stats.RPCStats) {
	tracker, ok := ctx.Value(rpcAttemptContextKey{recorder: recorder}).(*rpcAttemptTracker)
	if !ok || tracker == nil || event == nil || !event.IsClient() {
		return
	}

	tracker.mu.Lock()
	switch value := event.(type) {
	case *stats.Begin:
		tracker.current = &rpcAttemptRecord{
			method: tracker.method, remote: "unknown", servingMemberID: "unknown",
			nameResolutionDelay: tracker.nameResolutionDelay,
			transparentRetry:    value.IsTransparentRetryAttempt, begin: value.BeginTime,
		}
	case *stats.DelayedPickComplete:
		if tracker.current != nil {
			tracker.current.pick = recorder.now()
		}
	case *stats.OutHeader:
		if tracker.current != nil {
			tracker.current.outHeader = recorder.now()
			tracker.current.remote = canonicalDiagnosticRemote(value.RemoteAddr)
		}
	case *stats.InHeader:
		if tracker.current != nil {
			tracker.current.inHeader = recorder.now()
		}
	case *stats.InPayload:
		if tracker.current != nil {
			tracker.current.servingMemberID = diagnosticServingMemberID(value.Payload)
		}
	case *stats.End:
		if tracker.current == nil {
			tracker.mu.Unlock()
			return
		}
		completed := *tracker.current
		tracker.current = nil
		completed.end = value.EndTime
		completed.code = diagnosticRPCCode(value.Error)
		tracker.mu.Unlock()
		recorder.append(completed)
		return
	}
	tracker.mu.Unlock()
}

func diagnosticServingMemberID(payload any) string {
	var header *etcdserverpb.ResponseHeader
	switch response := payload.(type) {
	case *etcdserverpb.RangeResponse:
		header = response.GetHeader()
	case *etcdserverpb.TxnResponse:
		header = response.GetHeader()
	case *etcdserverpb.PutResponse:
		header = response.GetHeader()
	case *etcdserverpb.DeleteRangeResponse:
		header = response.GetHeader()
	}
	if header == nil || header.GetMemberId() == 0 {
		return "unknown"
	}
	return strconv.FormatUint(header.GetMemberId(), 16)
}

func diagnosticRPCCode(err error) codes.Code {
	switch {
	case err == nil:
		return codes.OK
	case errors.Is(err, context.Canceled):
		return codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded
	default:
		return status.Code(err)
	}
}

func canonicalDiagnosticRemote(address net.Addr) string {
	if address == nil {
		return "unknown"
	}
	host, portText, err := net.SplitHostPort(address.String())
	if err != nil {
		return "unknown"
	}
	ip := net.ParseIP(host)
	port, err := strconv.Atoi(portText)
	if ip == nil || err != nil || port < 1 || port > 65535 || strconv.Itoa(port) != portText {
		return "unknown"
	}
	return net.JoinHostPort(ip.String(), portText)
}

func (recorder *rpcAttemptRecorder) append(record rpcAttemptRecord) {
	recorder.mu.Lock()
	if recorder.count == recorder.capacity {
		recorder.overwrites++
	}
	recorder.records[recorder.next] = record
	recorder.next = (recorder.next + 1) % recorder.capacity
	if recorder.count < recorder.capacity {
		recorder.count++
	}
	recorder.mu.Unlock()
}

func (recorder *rpcAttemptRecorder) reset() {
	recorder.mu.Lock()
	recorder.next = 0
	recorder.count = 0
	recorder.overwrites = 0
	recorder.mu.Unlock()
}

func (recorder *rpcAttemptRecorder) snapshot() ([]rpcAttemptRecord, int) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	result := make([]rpcAttemptRecord, recorder.count)
	start := (recorder.next - recorder.count + recorder.capacity) % recorder.capacity
	for index := range result {
		result[index] = recorder.records[(start+index)%recorder.capacity]
	}
	return result, recorder.overwrites
}

func (recorder *rpcAttemptRecorder) formatEvidence(windowStart, windowEnd time.Time, limit int) string {
	records, ringOverwrites := recorder.snapshot()
	matching := make([]rpcAttemptRecord, 0, len(records))
	for _, record := range records {
		if record.begin.Before(windowStart) || record.begin.After(windowEnd) {
			continue
		}
		matching = append(matching, record)
	}
	if limit < 0 {
		limit = 0
	}
	omitted := max(0, len(matching)-limit)
	if omitted > 0 {
		matching = matching[omitted:]
	}
	evidence := make([]rpcAttemptEvidence, 0, len(matching))
	for _, record := range matching {
		evidence = append(evidence, record.evidence(windowStart))
	}
	report := rpcAttemptEvidenceReport{Attempts: evidence, Omitted: omitted, RingOverwrites: ringOverwrites}
	encoded, err := json.Marshal(report)
	if err != nil {
		return `{"attempts":[],"omitted":0,"ring_overwrites":0}`
	}
	return string(encoded)
}

func (record rpcAttemptRecord) evidence(windowStart time.Time) rpcAttemptEvidence {
	operationToBegin := record.begin.Sub(windowStart).Microseconds()
	if operationToBegin < 0 {
		operationToBegin = 0
	}
	total := record.end.Sub(record.begin).Microseconds()
	if record.begin.IsZero() || record.end.IsZero() || total < 0 {
		total = 0
	}
	return rpcAttemptEvidence{
		Method: record.method, Remote: record.remote, ServingMemberID: record.servingMemberID,
		NameResolutionDelay: record.nameResolutionDelay, TransparentRetry: record.transparentRetry,
		OperationToBeginMicros:    operationToBegin,
		BeginToPickMicros:         phaseMicros(record.begin, record.pick),
		BeginToOutHeaderMicros:    phaseMicros(record.begin, record.outHeader),
		OutHeaderToInHeaderMicros: phaseMicros(record.outHeader, record.inHeader),
		InHeaderToEndMicros:       phaseMicros(record.inHeader, record.end),
		TotalMicros:               total, Code: record.code.String(),
	}
}

func phaseMicros(start, end time.Time) *int64 {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return nil
	}
	value := end.Sub(start).Microseconds()
	return &value
}
