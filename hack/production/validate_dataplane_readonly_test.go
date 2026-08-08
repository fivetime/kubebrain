package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateDataplaneReadonlyProbe(t *testing.T) {
	for _, tc := range []struct {
		name              string
		podsJSON          string
		readyz            string
		readyzVerbose     string
		readyzExcludeData string
		livez             string
		livezVerbose      string
		livezExclude      string
		livezNamedVerbose string
		healthJSON        string
		healthMethodCode  string
		healthMethodAllow string
		httpHeaderType    string
		httpHeaderNosniff string
		versionHeaderType string
		serialHealthJSON  string
		count             string
		statusJSON        string
		gatewayJSON       string
		authJSON          string
		alarmJSON         string
		versionJSON       string
		infoVersionJSON   string
		infoMetrics       string
		clientMetrics     string
		infoDebugVars     string
		clientDebugVars   string
		debugVarsHeader   string
		infoPprof         string
		clientPprof       string
		extraEnv          []string
		wantTimeout       []string
		wantOK            bool
		wantOutput        string
	}{
		{
			name: "passes read only dataplane gate",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantTimeout: []string{
				"10s kubectl",
				"10s curl",
				"10s go",
			},
			wantOK:     true,
			wantOutput: "dataplane readonly gate passed",
		},
		{
			name: "reports legacy health endpoints in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOK:     true,
			wantOutput: "health=true, serializable_health=true",
		},
		{
			name: "reports livez endpoints in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_READYZ_NAMED_CHECKS=1", "EXPECTED_LIVEZ_NAMED_CHECKS=1", "EXPECTED_HEALTH_EXCLUDE_CHECKS=1", "EXPECTED_HEALTH_METHOD_CHECKS=1", "EXPECTED_HTTP_HEADER_CHECKS=1"},
			wantOK:     true,
			wantOutput: "readyz_verbose=ok, readyz_data_corruption=ok, readyz_serializable_read=ok, readyz_linearizable_read=ok, readyz_non_learner=ok, readyz_named_checks=ok, health_exclude_checks=ok, livez=ok, livez_serializable_read=ok, livez_named_checks=ok, health_method_checks=ok, http_header_checks=ok",
		},
		{
			name: "rejects unhealthy livez endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			livez:      "not ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "livez mismatch",
		},
		{
			name: "rejects malformed livez exclude endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:       "ok",
			livezExclude: "[+]serializable_read ok\nok",
			count:        "4",
			statusJSON:   `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:     []string{"EXPECTED_HEALTH_EXCLUDE_CHECKS=1"},
			wantOutput:   "livez exclude mismatch",
		},
		{
			name: "rejects malformed livez named endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:            "ok",
			livezNamedVerbose: "ok",
			count:             "4",
			statusJSON:        `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:          []string{"EXPECTED_LIVEZ_NAMED_CHECKS=1"},
			wantOutput:        "livez serializable_read mismatch",
		},
		{
			name: "rejects malformed readyz verbose endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:        "ok",
			readyzVerbose: "[+]data_corruption ok\n[+]serializable_read ok\nok",
			count:         "4",
			statusJSON:    `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:      []string{"EXPECTED_READYZ_NAMED_CHECKS=1"},
			wantOutput:    "readyz verbose mismatch: expected linearizable_read ok",
		},
		{
			name: "rejects malformed readyz exclude endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:            "ok",
			readyzExcludeData: "[+]data_corruption ok\n[+]serializable_read ok\nok",
			count:             "4",
			statusJSON:        `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:          []string{"EXPECTED_HEALTH_EXCLUDE_CHECKS=1"},
			wantOutput:        "readyz exclude mismatch: expected data_corruption to be excluded",
		},
		{
			name: "rejects unhealthy legacy health endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			healthJSON: `{"health":"false","reason":"RAFT NO LEADER"}`,
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "health mismatch",
		},
		{
			name: "rejects malformed health method endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			healthMethodCode: "200",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:         []string{"EXPECTED_HEALTH_METHOD_CHECKS=1"},
			wantOutput:       "livez method mismatch: expected HTTP 405",
		},
		{
			name: "rejects malformed health method allow header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:            "ok",
			healthMethodAllow: "POST",
			count:             "4",
			statusJSON:        `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:          []string{"EXPECTED_HEALTH_METHOD_CHECKS=1"},
			wantOutput:        "livez method mismatch: expected Allow: GET",
		},
		{
			name: "rejects malformed health content type header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:         "ok",
			httpHeaderType: "application/json",
			count:          "4",
			statusJSON:     `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:       []string{"EXPECTED_HTTP_HEADER_CHECKS=1"},
			wantOutput:     "livez header mismatch: expected Content-Type text/plain; charset=utf-8",
		},
		{
			name: "rejects missing health nosniff header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:            "ok",
			httpHeaderNosniff: "missing",
			count:             "4",
			statusJSON:        `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:          []string{"EXPECTED_HTTP_HEADER_CHECKS=1"},
			wantOutput:        "livez header mismatch: expected X-Content-Type-Options nosniff",
		},
		{
			name: "rejects malformed version content type header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:            "ok",
			versionHeaderType: "text/plain; charset=utf-8",
			count:             "4",
			statusJSON:        `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv:          []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_HTTP_HEADER_CHECKS=1"},
			wantOutput:        "client version header mismatch: expected Content-Type application/json",
		},
		{
			name: "reports info metrics boundary in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOK:     true,
			wantOutput: "info_metrics=ok, client_metrics=404, server_identity_metrics=ok, grpc_metrics=ok, network_metrics=ok, runtime_metrics=ok, fd_metrics=ok, server_state_metrics=ok, health_metrics=ok, auth_metrics=ok, quota_metrics=ok, mvcc_db_size_metrics=ok, mvcc_revision_metrics=ok, mvcc_watch_metrics=ok, promhttp_metrics=ok",
		},
		{
			name: "rejects missing info server version metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: "etcd_cluster_version{cluster=\"default\",cluster_version=\"3.7\"} 1\ngrpc_server_handled_total{grpc_code=\"OK\"} 1\n",
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput:  "info metrics mismatch: expected etcd_server_version server_version=3.7.0",
		},
		{
			name: "rejects missing info network known peers metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_network_known_peers{Local=\"abc\",Remote=\"abc\"} 1\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_network_known_peers",
		},
		{
			name: "rejects missing info go runtime metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
				`etcd_debugging_mvcc_compact_revision{cluster="default"} 3`,
				`etcd_debugging_mvcc_watch_stream_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_watcher_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_slow_watcher_total{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected go_info",
		},
		{
			name: "rejects missing info server identity metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
				`etcd_debugging_mvcc_compact_revision{cluster="default"} 3`,
				`etcd_debugging_mvcc_watch_stream_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_watcher_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_slow_watcher_total{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_go_version server_go_version=go*",
		},
		{
			name: "rejects exposed client metrics endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:        "ok",
			count:         "4",
			statusJSON:    `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			clientMetrics: "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nmetrics\n",
			extraEnv:      []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput:    "client metrics mismatch: expected HTTP 404",
		},
		{
			name: "rejects missing info go scheduler metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected go_sched_gomaxprocs_threads",
		},
		{
			name: "rejects missing info go gc metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected go_gc_gogc_percent",
		},
		{
			name: "rejects missing info grpc started metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected grpc_server_started_total",
		},
		{
			name: "rejects missing info promhttp metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
				`etcd_debugging_mvcc_compact_revision{cluster="default"} 3`,
				`etcd_debugging_mvcc_watch_stream_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_watcher_total{cluster="default"} 0`,
				`etcd_debugging_mvcc_slow_watcher_total{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected promhttp_metric_handler_requests_total",
		},
		{
			name: "rejects missing info fd metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected os_fd_used",
		},
		{
			name: "rejects missing info server state metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_is_leader",
		},
		{
			name: "rejects missing info leader changes metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Replace(
				defaultInfoMetrics(""),
				"etcd_server_leader_changes_seen_total{cluster=\"default\"} 1\n",
				"",
				1,
			),
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_leader_changes_seen_total",
		},
		{
			name: "rejects missing info health metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_health_failures",
		},
		{
			name: "rejects missing info auth revision metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_auth_revision",
		},
		{
			name: "rejects missing info quota metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_server_quota_backend_bytes",
		},
		{
			name: "rejects missing info mvcc db size metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_mvcc_db_total_size_in_use_in_bytes",
		},
		{
			name: "rejects missing info mvcc current revision metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_current_revision",
		},
		{
			name: "rejects missing info mvcc compact revision metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_compact_revision",
		},
		{
			name: "rejects missing info mvcc watch stream metric",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"storageVersion":"3.7.0","dbSizeInUse":88,"leader":456,"raftTerm":8,"raftIndex":7,"raftAppliedIndex":7,"downgradeInfo":{}}}]`,
			infoMetrics: strings.Join([]string{
				`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
				`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
				`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
				`etcd_server_id{cluster="default",server_id="e3f"} 1`,
				`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
				`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
				`go_info{version="go1.26.5"} 1`,
				`go_goroutines 12`,
				`go_threads 7`,
				`go_gc_gogc_percent 100`,
				`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
				`go_sched_gomaxprocs_threads 80`,
				`os_fd_used 64`,
				`os_fd_limit 1048576`,
				`etcd_server_has_leader{cluster="default"} 1`,
				`etcd_server_is_leader{cluster="default"} 1`,
				`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
				`etcd_server_is_learner{cluster="default"} 0`,
				`etcd_server_health_success{cluster="default"} 0`,
				`etcd_server_health_failures{cluster="default"} 0`,
				`etcd_debugging_auth_revision{cluster="default"} 1`,
				`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
				`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
				`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
				`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
				`etcd_debugging_mvcc_compact_revision{cluster="default"} 3`,
				`promhttp_metric_handler_requests_in_flight 1`,
				`promhttp_metric_handler_requests_total{code="200"} 1`,
			}, "\n") + "\n",
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0", "EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "info metrics mismatch: expected etcd_debugging_mvcc_watch_stream_total",
		},
		{
			name: "reports info debug vars boundary in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_DEBUG_VARS_CHECKS=1"},
			wantOK:     true,
			wantOutput: "info_debug_vars=ok, client_debug_vars=404",
		},
		{
			name: "rejects malformed info debug vars JSON",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:        "ok",
			count:         "4",
			statusJSON:    `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			infoDebugVars: `{"cmdline":"kubebrain","memstats":{}}`,
			extraEnv:      []string{"EXPECTED_DEBUG_VARS_CHECKS=1"},
			wantOutput:    "info debug vars mismatch: expected cmdline array",
		},
		{
			name: "rejects exposed client debug vars endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			clientDebugVars: "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n\r\n{\"cmdline\":[],\"memstats\":{}}\n",
			extraEnv:        []string{"EXPECTED_DEBUG_VARS_CHECKS=1"},
			wantOutput:      "client debug vars mismatch: expected HTTP 404",
		},
		{
			name: "rejects malformed info debug vars content type",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			debugVarsHeader: "application/json",
			extraEnv:        []string{"EXPECTED_DEBUG_VARS_CHECKS=1"},
			wantOutput:      "info debug vars header mismatch: expected Content-Type application/json; charset=utf-8",
		},
		{
			name: "rejects malformed info debug vars method guard",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			healthMethodCode: "200",
			extraEnv:         []string{"EXPECTED_DEBUG_VARS_CHECKS=1"},
			wantOutput:       "info debug vars method mismatch: expected HTTP 405",
		},
		{
			name: "reports pprof disabled boundary in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_PPROF_DISABLED_CHECKS=1"},
			wantOK:     true,
			wantOutput: "client_pprof=404, info_pprof=404",
		},
		{
			name: "rejects exposed info pprof endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			infoPprof:  "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<html>pprof</html>\n",
			extraEnv:   []string{"EXPECTED_PPROF_DISABLED_CHECKS=1"},
			wantOutput: "info pprof mismatch: expected HTTP 404",
		},
		{
			name: "rejects exposed client pprof endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			clientPprof: "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n" +
				"<html>pprof</html>\n",
			extraEnv:   []string{"EXPECTED_PPROF_DISABLED_CHECKS=1"},
			wantOutput: "client pprof mismatch: expected HTTP 404",
		},
		{
			name: "rejects unhealthy serializable health endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:           "ok",
			serialHealthJSON: `{"health":"false","reason":"ALARM NOSPACE"}`,
			count:            "4",
			statusJSON:       `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput:       "serializable health mismatch",
		},
		{
			name: "passes camelcase diagnostic envelopes",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"clusterId":123,"memberId":456,"revision":7},"db_size":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"clusterId":123,"memberId":456,"revision":7},"hash":111,"compactRevision":3}}]`,
			},
			wantOK:     true,
			wantOutput: "dataplane readonly gate passed",
		},
		{
			name: "reports pinned status version in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
			},
			wantOK:     true,
			wantOutput: "status_version=3.7.0",
		},
		{
			name: "reports status storage version in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","storageVersion":"3.6","dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_storage_versions=3.6",
		},
		{
			name: "reports snakecase status storage version in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","storage_version":"3.6","dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_storage_versions=3.6",
		},
		{
			name: "reports full status storage version in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","storageVersion":"3.7.0","dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_storage_versions=3.7.0",
		},
		{
			name: "reports status db size quota in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"dbSizeQuota":2147483648}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "min_status_db_size_quota=2147483648",
		},
		{
			name: "reports snakecase status db size quota in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"db_size_quota":2147483648}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "min_status_db_size_quota=2147483648",
		},
		{
			name: "reports status learner envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"isLearner":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_is_learners=false",
		},
		{
			name: "reports snakecase status learner envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"is_learner":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_is_learners=false",
		},
		{
			name: "reports status downgrade info in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"downgradeInfo":{"enabled":true,"targetVersion":"3.6.0"}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_downgrade_enableds=true, status_downgrade_target_versions=3.6.0",
		},
		{
			name: "reports snakecase status downgrade info in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"downgrade_info":{"enabled":true,"target_version":"3.6.0"}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_downgrade_enableds=true, status_downgrade_target_versions=3.6.0",
		},
		{
			name: "allows disabled status downgrade info without target",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"downgradeInfo":{"enabled":false}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_downgrade_enableds=false",
		},
		{
			name: "allows idle protojson status downgrade info object",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99,"downgradeInfo":{}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_errors=empty",
		},
		{
			name: "reports gateway status envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
			},
			wantOK:     true,
			wantOutput: "version_etcdserver=3.7.0, version_etcdcluster=3.7, version_storage=3.7.0, info_version_storage=3.7.0, gateway_db_size_in_use=88, gateway_db_size_quota=2147483648, gateway_is_learner=false, gateway_leader_id=456, gateway_raft_term=8, gateway_raft_index=7, gateway_raft_applied_index=7, gateway_raft_indexes_match_revision=true, gateway_downgrade_info=object",
		},
		{
			name: "rejects malformed version storage envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			versionJSON: `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":false}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput:  "/version storage must be a storage semver string",
		},
		{
			name: "rejects version storage drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			versionJSON: `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.6.0"}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput:  "/version storage mismatch",
		},
		{
			name: "rejects info version storage drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:          "ok",
			count:           "4",
			statusJSON:      `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			infoVersionJSON: `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.6.0"}`,
			extraEnv:        []string{"EXPECTED_STATUS_CLUSTER_ID=123", "EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput:      "info /version storage mismatch",
		},
		{
			name: "reports gateway auth status envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			authJSON:   `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"authRevision":"5"}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "gateway_auth_enabled=false, gateway_auth_revision=5",
		},
		{
			name: "reports gateway alarm envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"version":"3.7.0","dbSize":99,"raftTerm":8}}]`,
			alarmJSON:  `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"}}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "gateway_alarms=empty",
		},
		{
			name: "rejects malformed gateway alarm envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			alarmJSON:  `{"header":{"cluster_id":"123","member_id":"456","revision":"7"},"alarms":false}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway alarm alarms must be an array",
		},
		{
			name: "rejects non empty gateway alarm list",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			alarmJSON:  `{"header":{"cluster_id":"123","member_id":"456","revision":"7"},"alarms":[{"memberID":"456","alarm":"NOSPACE"}]}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway alarm list must be empty",
		},
		{
			name: "rejects malformed gateway auth enabled envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			authJSON:   `{"header":{"cluster_id":"123","member_id":"456","revision":"7"},"enabled":"false","authRevision":"5"}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway auth status enabled must be boolean",
		},
		{
			name: "rejects malformed gateway auth revision envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			authJSON:   `{"header":{"cluster_id":"123","member_id":"456","revision":"7"},"authRevision":false}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "gateway auth status authRevision must be non-negative",
		},
		{
			name: "rejects malformed gateway status db size quota envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":0,"isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      false,
			wantOutput:  "gateway status dbSizeQuota must be positive",
		},
		{
			name: "rejects malformed gateway status learner envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":"false","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      false,
			wantOutput:  "gateway status isLearner must be boolean",
		},
		{
			name: "rejects gateway status db size in use beyond db size",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"100","dbSizeQuota":"2147483648","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      false,
			wantOutput:  "gateway status dbSizeInUse must not exceed dbSize",
		},
		{
			name: "rejects gateway status applied index beyond raft index",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"8","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      false,
			wantOutput:  "gateway status raftAppliedIndex must not exceed raftIndex",
		},
		{
			name: "rejects malformed gateway status storage version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":false,"dbSize":"99","dbSizeInUse":"88","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      false,
			wantOutput:  "gateway status storageVersion must be a storage semver string",
		},
		{
			name: "rejects malformed gateway status downgrade info envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:      "ok",
			count:       "4",
			statusJSON:  `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			gatewayJSON: `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":false}`,
			extraEnv:    []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:      false,
			wantOutput:  "gateway status downgradeInfo envelope invalid",
		},
		{
			name: "reports status leader and raft term in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"leader":456,"raftTerm":8}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_leader_ids=456, status_raft_terms=8",
		},
		{
			name: "reports snakecase status raft term in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raftTerm":8},"dbSize":99,"leader_id":456,"raft_term":8}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_leader_ids=456, status_raft_terms=8",
		},
		{
			name: "reports camelcase status leader in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"leaderId":456,"raftTerm":8}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_leader_ids=456, status_raft_terms=8",
		},
		{
			name: "reports status raft indexes in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":9},"dbSize":99,"raftIndex":9,"raftAppliedIndex":9}}]`,
			authJSON:   `{"header":{"cluster_id":"123","member_id":"456","revision":"9"},"authRevision":"5"}`,
			alarmJSON:  `{"header":{"cluster_id":"123","member_id":"456","revision":"9"}}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "min_status_raft_index=9, min_status_raft_applied_index=9, raft_indexes_match_revision=true",
		},
		{
			name: "reports snakecase status raft indexes in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":9},"dbSize":99,"raft_index":9,"raft_applied_index":9}}]`,
			authJSON:   `{"header":{"cluster_id":"123","member_id":"456","revision":"9"},"authRevision":"5"}`,
			alarmJSON:  `{"header":{"cluster_id":"123","member_id":"456","revision":"9"}}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "min_status_raft_index=9, min_status_raft_applied_index=9, raft_indexes_match_revision=true",
		},
		{
			name: "reports status db size in use in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeInUse":88}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "min_status_db_size=99, min_status_db_size_in_use=88",
		},
		{
			name: "reports snakecase status db size in use in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"db_size":99,"db_size_in_use":88}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "min_status_db_size=99, min_status_db_size_in_use=88",
		},
		{
			name: "reports empty status errors in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":[]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_errors=empty",
		},
		{
			name: "reports uppercase empty status errors in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"Errors":[]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOK:     true,
			wantOutput: "status_errors=empty",
		},
		{
			name: "reports hashkv raft term in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
			},
			wantOK:     true,
			wantOutput: "revisions_match=true, hashkv_raft_terms=8, raft_terms_match=true",
		},
		{
			name: "reports camelcase hashkv raft term in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raftTerm":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raftTerm":8},"hash":111,"compact_revision":3}}]`,
			},
			wantOK:     true,
			wantOutput: "hashkv_raft_terms=8, raft_terms_match=true",
		},
		{
			name: "reports gateway hash and hashkv envelope in summary",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"hash":111,"compact_revision":3}}]`,
				`FAKE_GATEWAY_HASH_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":222}`,
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":"111","compact_revision":"3","hash_revision":"7"}`,
			},
			wantOK:     true,
			wantOutput: "gateway_hash=222, gateway_hashkv_hash=111, gateway_hashkv_hash_revision=7, gateway_hashkv_compact_revision=3, gateway_hashkv_revisions_match=true",
		},
		{
			name: "rejects gateway hashkv hash drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7"},"hash":"222","compact_revision":"3","hash_revision":"7"}`,
			},
			wantOutput: "gateway hashkv hash mismatch",
		},
		{
			name: "rejects gateway hashkv compact revision beyond hash revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7"},"hash":"111","compact_revision":"8","hash_revision":"7"}`,
			},
			wantOutput: "gateway hashkv compact revision must not exceed hash revision",
		},
		{
			name: "rejects non ready replica",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"False"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "KubeBrain Ready pod count mismatch",
		},
		{
			name: "rejects readyz failure",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "starting",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			wantOutput: "readyz mismatch",
		},
		{
			name: "rejects count drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "5",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_PREFIX_COUNT=4"},
			wantOutput: "prefix count mismatch",
		},
		{
			name: "rejects count drift on additional status endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_PREFIX_COUNT=4",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"FAKE_PREFIX_COUNTS=http://127.0.0.1:2379=4\nhttp://127.0.0.2:2379=5",
			},
			wantOutput: "prefix count mismatch for http://127.0.0.2:2379",
		},
		{
			name: "rejects count drift on bootstrap endpoint when status endpoints override",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"ENDPOINT=http://127.0.0.9:2379",
				"EXPECTED_PREFIX_COUNT=4",
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				"FAKE_PREFIX_COUNTS=http://127.0.0.9:2379=5\nhttp://127.0.0.1:2379=4\nhttp://127.0.0.2:2379=4",
			},
			wantOutput: "prefix count mismatch for http://127.0.0.9:2379",
		},
		{
			name: "rejects status cluster id drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":321,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantTimeout: []string{
				"10s etcdctl",
			},
			wantOutput: "status cluster ID mismatch",
		},
		{
			name: "rejects missing status cluster id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status cluster ID is required",
		},
		{
			name: "rejects string status numeric fields",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":"123","member_id":"456","revision":"7"},"dbSize":"99"}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status numeric fields must be JSON numbers",
		},
		{
			name: "rejects boolean status header identity envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":false,"clusterId":123,"member_id":false,"memberId":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status numeric fields must be JSON numbers",
		},
		{
			name: "rejects fractional status numeric fields",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123.5,"member_id":456.5,"revision":7.5},"dbSize":99.5}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status numeric fields must be JSON integers",
		},
		{
			name: "rejects mismatched status raft term envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":9}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft term envelope invalid",
		},
		{
			name: "rejects status raft term missing top level pair",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft term envelope invalid",
		},
		{
			name: "rejects string status raft term envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":"8"},"dbSize":99,"raftTerm":"8"}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft term envelope invalid",
		},
		{
			name: "rejects fractional status raft term envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8.5},"dbSize":99,"raftTerm":8.5}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft term envelope invalid",
		},
		{
			name: "rejects boolean status raft term envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":false},"dbSize":99,"raftTerm":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft term envelope invalid",
		},
		{
			name: "rejects status raft applied index beyond raft index",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"raftIndex":8,"raftAppliedIndex":9}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft index envelope invalid",
		},
		{
			name: "rejects status raft index missing applied pair",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"raftIndex":7}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft index envelope invalid",
		},
		{
			name: "rejects fractional status raft index envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"raftIndex":7.5,"raftAppliedIndex":7.5}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft index envelope invalid",
		},
		{
			name: "rejects boolean status raft index envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"raftIndex":false,"raftAppliedIndex":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft index envelope invalid",
		},
		{
			name: "rejects status raft indexes that do not match revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"raftIndex":8,"raftAppliedIndex":8}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status raft index envelope invalid",
		},
		{
			name: "rejects status db size in use beyond db size",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeInUse":100}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeInUse envelope invalid",
		},
		{
			name: "rejects string status db size in use envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeInUse":"88"}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeInUse envelope invalid",
		},
		{
			name: "rejects fractional status db size in use envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeInUse":88.5}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeInUse envelope invalid",
		},
		{
			name: "rejects boolean status db size in use envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeInUse":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeInUse envelope invalid",
		},
		{
			name: "rejects malformed status version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7","dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status version envelope invalid",
		},
		{
			name: "rejects boolean status version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":false,"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status version envelope invalid",
		},
		{
			name: "rejects malformed status storage version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"storageVersion":"3.x","dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status storageVersion envelope invalid",
		},
		{
			name: "rejects boolean status storage version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"storageVersion":false,"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status storageVersion envelope invalid",
		},
		{
			name: "rejects snakecase boolean status storage version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"storage_version":false,"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status storageVersion envelope invalid",
		},
		{
			name: "rejects non positive status db size quota envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeQuota":0}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeQuota envelope invalid",
		},
		{
			name: "rejects boolean status db size quota envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"dbSizeQuota":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeQuota envelope invalid",
		},
		{
			name: "rejects snakecase boolean status db size quota envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"db_size_quota":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSizeQuota envelope invalid",
		},
		{
			name: "rejects string status learner envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"isLearner":"false"}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status isLearner envelope invalid",
		},
		{
			name: "rejects snakecase string status learner envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"is_learner":"false"}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status isLearner envelope invalid",
		},
		{
			name: "rejects status downgrade enabled without target",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgradeInfo":{"enabled":true}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects boolean status downgrade target version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgradeInfo":{"enabled":true,"targetVersion":false}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects snakecase boolean status downgrade target version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgrade_info":{"enabled":true,"target_version":false}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects short status downgrade target version envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgradeInfo":{"enabled":true,"targetVersion":"3.6"}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects boolean status downgrade info envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgradeInfo":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects snakecase boolean status downgrade info envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"downgrade_info":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status downgradeInfo envelope invalid",
		},
		{
			name: "rejects status version mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.6.0","dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_STATUS_VERSION=3.7.0",
			},
			wantOutput: "status version mismatch",
		},
		{
			name: "rejects non positive status leader envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"leader":0,"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status leader envelope invalid",
		},
		{
			name: "rejects boolean status leader envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"leader":false,"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status leader envelope invalid",
		},
		{
			name: "rejects non empty status errors",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":["etcdserver: no leader"]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status errors must be empty",
		},
		{
			name: "rejects uppercase non empty status errors",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"Errors":["etcdserver: no leader"]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status errors must be empty",
		},
		{
			name: "rejects boolean status errors envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":false}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status errors must be empty",
		},
		{
			name: "rejects boolean status errors element envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"errors":[false]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status errors must be empty",
		},
		{
			name: "rejects uppercase boolean status errors element envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99,"Errors":[false]}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status errors must be empty",
		},
		{
			name: "rejects status response missing header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status header is required",
		},
		{
			name: "rejects status response missing status",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379"}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status payload is required",
		},
		{
			name: "rejects hashkv hash drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":222,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv hash mismatch",
		},
		{
			name: "rejects hashkv cluster id drift",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":321,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv cluster ID mismatch",
		},
		{
			name: "rejects missing hashkv cluster id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"member_id":456,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv cluster ID is required",
		},
		{
			name: "rejects string hashkv numeric fields",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":"123","member_id":"456","revision":"7"},"hash":"111","compact_revision":"3","hash_revision":"7"}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON numbers",
		},
		{
			name: "rejects boolean hashkv hash envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":false,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON numbers",
		},
		{
			name: "rejects boolean hashkv hash revision envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3,"hashRevision":false}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON numbers",
		},
		{
			name: "rejects partial hashkv hash revision envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":7},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3,"hash_revision":7}},
					{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":7},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "hashkv hash revision must be present on all endpoints when present",
		},
		{
			name: "rejects hashkv hash revision mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3,"hashRevision":6}}]`,
			},
			wantOutput: "hashkv hash revision must match header revision",
		},
		{
			name: "rejects boolean hashkv header identity envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":false,"clusterId":123,"member_id":false,"memberId":456,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON numbers",
		},
		{
			name: "rejects fractional hashkv numeric fields",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123.5,"member_id":456.5,"revision":7.5},"hash":111.5,"compact_revision":3.5,"hash_revision":7.5}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON integers",
		},
		{
			name: "rejects hashkv response missing header",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"hash":111,"compact_revision":0}}]`,
			},
			wantOutput: "hashkv header is required",
		},
		{
			name: "rejects hashkv response missing hashkv",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379"}]`,
			},
			wantOutput: "hashkv payload is required",
		},
		{
			name: "rejects hashkv response missing hash",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=0",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"compact_revision":0}}]`,
			},
			wantOutput: "hashkv hash is required",
		},
		{
			name: "rejects hashkv response missing compact revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111}}]`,
			},
			wantOutput: "hashkv compact revision is required",
		},
		{
			name: "rejects boolean hashkv compact revision envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":false,"compactRevision":3}}]`,
			},
			wantOutput: "hashkv numeric fields must be JSON numbers",
		},
		{
			name: "rejects duplicate hashkv member ids across endpoints",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}},
					{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":8},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "hashkv member IDs must be unique",
		},
		{
			name: "rejects zero hashkv member id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":0,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv member ID must be positive",
		},
		{
			name: "rejects missing hashkv member id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv member ID is required",
		},
		{
			name: "rejects hashkv endpoint set mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}},
					{"Endpoint":"http://127.0.0.3:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":8},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "hashkv endpoint set mismatch",
		},
		{
			name: "rejects hashkv response missing endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv endpoint set mismatch",
		},
		{
			name: "rejects hashkv endpoint count mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}
				]`,
			},
			wantOutput: "hashkv endpoint count mismatch",
		},
		{
			name: "rejects non array hashkv response",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON={"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}`,
			},
			wantOutput: "hashkv endpoint count mismatch",
		},
		{
			name: "rejects hashkv compact revision beyond hash revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":8}}]`,
			},
			wantOutput: "hashkv compact revision must not exceed hash revision",
		},
		{
			name: "rejects hashkv compact revision beyond explicit hash revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":8,"hash_revision":7}}]`,
			},
			wantOutput: "hashkv compact revision must not exceed hash revision",
		},
		{
			name: "rejects negative hashkv revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":-1},"hash":111,"compact_revision":-1}}]`,
			},
			wantOutput: "hashkv revision must be non-negative",
		},
		{
			name: "rejects missing hashkv revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456},"hash":111,"compact_revision":0}}]`,
			},
			wantOutput: "hashkv revision is required",
		},
		{
			name: "rejects non positive hashkv raft term",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":0},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv raft term envelope invalid",
		},
		{
			name: "rejects boolean hashkv raft term envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":false},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "hashkv raft term envelope invalid",
		},
		{
			name: "rejects status and hashkv raft term mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":8},"dbSize":99,"raftTerm":8}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7,"raft_term":9},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "status/hashkv raft term mismatch",
		},
		{
			name: "rejects status and hashkv revision mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":8},"hash":111,"compact_revision":3}}]`,
			},
			wantOutput: "status/hashkv revision mismatch",
		},
		{
			name: "rejects negative hashkv compact revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":-1}}]`,
			},
			wantOutput: "hashkv compact revision must be non-negative",
		},
		{
			name: "rejects hashkv compact revision beyond hash revision on one endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"EXPECTED_HASHKV_HASH=111",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
				`FAKE_HASHKV_JSON=[
					{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":5},"hash":111,"compact_revision":1}},
					{"Endpoint":"http://127.0.0.2:2379","HashKV":{"header":{"cluster_id":123,"member_id":789,"revision":8},"hash":111,"compact_revision":9}}
				]`,
			},
			wantOutput: "hashkv compact revision must not exceed hash revision",
		},
		{
			name: "rejects zero status member id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":0,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status member ID must be positive",
		},
		{
			name: "rejects missing status member id",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status member ID is required",
		},
		{
			name: "rejects negative status revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":-1},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status revision must be non-negative",
		},
		{
			name: "rejects missing status revision",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status revision is required",
		},
		{
			name: "rejects negative status db size",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":-1}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSize must be positive",
		},
		{
			name: "rejects zero status db size",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":0}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSize must be positive",
		},
		{
			name: "rejects missing status db size",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7}}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSize is required",
		},
		{
			name: "rejects boolean status db size envelope",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":false,"db_size":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status numeric fields must be JSON numbers",
		},
		{
			name: "rejects status db size in use without db size",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSizeInUse":88}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status dbSize is required",
		},
		{
			name: "rejects duplicate status member ids across endpoints",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.2:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
			},
			wantOutput: "status member IDs must be unique",
		},
		{
			name: "rejects status endpoint set mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}},
				{"Endpoint":"http://127.0.0.3:2379","Status":{"header":{"cluster_id":123,"member_id":789,"revision":8},"dbSize":100}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
			},
			wantOutput: "status endpoint set mismatch",
		},
		{
			name: "rejects status response missing endpoint",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}
			]`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status endpoint set mismatch",
		},
		{
			name: "rejects status endpoint count mismatch",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz: "ok",
			count:  "4",
			statusJSON: `[
				{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}
			]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.2:2379",
			},
			wantOutput: "status endpoint count mismatch",
		},
		{
			name: "rejects non array status response",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}`,
			extraEnv:   []string{"EXPECTED_STATUS_CLUSTER_ID=123"},
			wantOutput: "status endpoint count mismatch",
		},
		{
			name: "rejects empty status endpoint entry before commands",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,",
			},
			wantOutput: "STATUS_ENDPOINTS contains an empty endpoint",
		},
		{
			name: "rejects duplicate status endpoints before commands",
			podsJSON: `{"items":[
				{"metadata":{"name":"kubebrain-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"kubebrain-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv: []string{
				"EXPECTED_STATUS_CLUSTER_ID=123",
				"STATUS_ENDPOINTS=http://127.0.0.1:2379,http://127.0.0.1:2379",
			},
			wantOutput: "STATUS_ENDPOINTS must not contain duplicate endpoints",
		},
		{
			name:       "rejects unsafe endpoint before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"ENDPOINT=http://127.0.0.1:2379\nbad"},
			wantOutput: "ENDPOINT contains unsupported characters",
		},
		{
			name:       "rejects invalid probe timeout before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"PROBE_TIMEOUT=forever"},
			wantOutput: "PROBE_TIMEOUT must be a positive duration",
		},
		{
			name:       "rejects hashkv hash without expected cluster id before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_HASHKV_HASH=111"},
			wantOutput: "EXPECTED_HASHKV_HASH requires EXPECTED_STATUS_CLUSTER_ID",
		},
		{
			name:       "rejects malformed expected status version before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_VERSION=3.7"},
			wantOutput: "EXPECTED_STATUS_VERSION must be empty or a semver string",
		},
		{
			name:       "rejects expected status version without expected cluster id before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"version":"3.7.0","dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_STATUS_VERSION=3.7.0"},
			wantOutput: "EXPECTED_STATUS_VERSION requires EXPECTED_STATUS_CLUSTER_ID",
		},
		{
			name:       "rejects malformed expected livez named checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_LIVEZ_NAMED_CHECKS=true"},
			wantOutput: "EXPECTED_LIVEZ_NAMED_CHECKS must be empty or 1",
		},
		{
			name:       "rejects malformed expected health exclude checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_HEALTH_EXCLUDE_CHECKS=true"},
			wantOutput: "EXPECTED_HEALTH_EXCLUDE_CHECKS must be empty or 1",
		},
		{
			name:       "rejects malformed expected health method checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_HEALTH_METHOD_CHECKS=true"},
			wantOutput: "EXPECTED_HEALTH_METHOD_CHECKS must be empty or 1",
		},
		{
			name:       "rejects malformed expected http header checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_HTTP_HEADER_CHECKS=true"},
			wantOutput: "EXPECTED_HTTP_HEADER_CHECKS must be empty or 1",
		},
		{
			name:       "rejects malformed expected info metrics checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_INFO_METRICS_CHECKS=true"},
			wantOutput: "EXPECTED_INFO_METRICS_CHECKS must be empty or 1",
		},
		{
			name:       "rejects info metrics checks without expected status version before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_INFO_METRICS_CHECKS=1"},
			wantOutput: "EXPECTED_INFO_METRICS_CHECKS requires EXPECTED_STATUS_VERSION",
		},
		{
			name:       "rejects malformed expected debug vars checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_DEBUG_VARS_CHECKS=true"},
			wantOutput: "EXPECTED_DEBUG_VARS_CHECKS must be empty or 1",
		},
		{
			name:       "rejects malformed expected pprof disabled checks flag before commands",
			podsJSON:   `{"items":[]}`,
			readyz:     "ok",
			count:      "4",
			statusJSON: `[{"Endpoint":"http://127.0.0.1:2379","Status":{"header":{"cluster_id":123,"member_id":456,"revision":7},"dbSize":99}}]`,
			extraEnv:   []string{"EXPECTED_PPROF_DISABLED_CHECKS=true"},
			wantOutput: "EXPECTED_PPROF_DISABLED_CHECKS must be empty or 1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "kubectl"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s' "$FAKE_PODS_JSON"
`)
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "curl"), `#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" -X POST "* ]]; then
  target="${@: -1}"
  if [[ "$target" == "${READYZ_URL}" || "$target" == "${READYZ_URL%/readyz}/livez" || "$target" == "${READYZ_URL%/readyz}/debug/vars" || "$target" == "${ENDPOINT%/}/health" ]]; then
  code="${FAKE_HEALTH_METHOD_CODE:-405}"
  allow="${FAKE_HEALTH_METHOD_ALLOW:-GET}"
  printf 'HTTP/1.1 %s Method Not Allowed\r\nAllow: %s\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nMethod Not Allowed\n' "$code" "$allow"
  exit 0
  fi
fi
if [[ " $* " == *" -D - "* ]]; then
  target="${@: -1}"
  if [[ "$target" == "${READYZ_URL}" || "$target" == "${READYZ_URL%/readyz}/livez" ]]; then
    printf 'HTTP/1.1 200 OK\r\nContent-Type: %s\r\n' "$FAKE_HTTP_HEADER_CONTENT_TYPE"
    if [[ "$FAKE_HTTP_HEADER_NOSNIFF" != "missing" ]]; then
      printf 'X-Content-Type-Options: %s\r\n' "$FAKE_HTTP_HEADER_NOSNIFF"
    fi
    printf '\r\n'
    exit 0
  fi
  if [[ "$target" == "${ENDPOINT%/}/version" || "$target" == "${READYZ_URL%/readyz}/version" ]]; then
    printf 'HTTP/1.1 200 OK\r\nContent-Type: %s\r\n\r\n' "$FAKE_VERSION_HEADER_CONTENT_TYPE"
    exit 0
  fi
  if [[ "$target" == "${READYZ_URL%/readyz}/debug/vars" ]]; then
    printf 'HTTP/1.1 200 OK\r\nContent-Type: %s\r\n\r\n' "$FAKE_DEBUG_VARS_HEADER_CONTENT_TYPE"
    exit 0
  fi
fi
if [[ " $* " == *" -i "* ]]; then
  target="${@: -1}"
  if [[ "$target" == "${ENDPOINT%/}/metrics" ]]; then
    printf '%s' "$FAKE_CLIENT_METRICS_RESPONSE"
    exit 0
  fi
  if [[ "$target" == "${ENDPOINT%/}/debug/vars" ]]; then
    printf '%s' "$FAKE_CLIENT_DEBUG_VARS_RESPONSE"
    exit 0
  fi
  if [[ "$target" == "${ENDPOINT%/}/debug/pprof/" ]]; then
    printf '%s' "$FAKE_CLIENT_PPROF_RESPONSE"
    exit 0
  fi
  if [[ "$target" == "${READYZ_URL%/readyz}/debug/pprof/" ]]; then
    printf '%s' "$FAKE_INFO_PPROF_RESPONSE"
    exit 0
  fi
fi
for arg in "$@"; do
  if [[ "$arg" == "${READYZ_URL}?verbose" ]]; then
    printf '%s' "$FAKE_READYZ_VERBOSE"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL}?verbose&exclude=data_corruption" ]]; then
    printf '%s' "$FAKE_READYZ_EXCLUDE_DATA"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL}?verbose&exclude=unknown" ]]; then
    printf '%s' "$FAKE_READYZ_VERBOSE"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/}/data_corruption?verbose" ]]; then
    printf '[+]data_corruption ok\nok'
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/}/serializable_read?verbose" ]]; then
    printf '[+]serializable_read ok\nok'
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/}/linearizable_read?verbose" ]]; then
    printf '[+]linearizable_read ok\nok'
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/}/non_learner?verbose" ]]; then
    printf '[+]non_learner ok\nok'
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/livez?verbose" ]]; then
    printf '%s' "$FAKE_LIVEZ_VERBOSE"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/livez?verbose&exclude=serializable_read" ]]; then
    printf '%s' "$FAKE_LIVEZ_EXCLUDE"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/livez/serializable_read?verbose" ]]; then
    printf '%s' "$FAKE_LIVEZ_SERIALIZABLE_READ_VERBOSE"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/livez" ]]; then
    printf '%s' "$FAKE_LIVEZ"
    exit 0
  fi
  if [[ "$arg" == */health\?serializable=true ]]; then
    printf '%s' "$FAKE_SERIALIZABLE_HEALTH_JSON"
    exit 0
  fi
  if [[ "$arg" == */health ]]; then
    printf '%s' "$FAKE_HEALTH_JSON"
    exit 0
  fi
  if [[ "$arg" == */v3/maintenance/status ]]; then
    printf '%s' "$FAKE_GATEWAY_STATUS_JSON"
    exit 0
  fi
  if [[ "$arg" == */v3/maintenance/hashkv ]]; then
    printf '%s' "$FAKE_GATEWAY_HASHKV_JSON"
    exit 0
  fi
  if [[ "$arg" == */v3/maintenance/hash ]]; then
    printf '%s' "$FAKE_GATEWAY_HASH_JSON"
    exit 0
  fi
  if [[ "$arg" == */v3/auth/status ]]; then
    printf '%s' "$FAKE_GATEWAY_AUTH_STATUS_JSON"
    exit 0
  fi
  if [[ "$arg" == */v3/maintenance/alarm ]]; then
    printf '%s' "$FAKE_GATEWAY_ALARM_JSON"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/version" ]]; then
    printf '%s' "$FAKE_INFO_VERSION_JSON"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/metrics" ]]; then
    printf '%s' "$FAKE_INFO_METRICS"
    exit 0
  fi
  if [[ "$arg" == "${READYZ_URL%/readyz}/debug/vars" ]]; then
    printf '%s' "$FAKE_INFO_DEBUG_VARS"
    exit 0
  fi
  if [[ "$arg" == */version ]]; then
    printf '%s' "$FAKE_VERSION_JSON"
    exit 0
  fi
done
printf '%s' "$FAKE_READYZ"
`)
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "go"), `#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${FAKE_PREFIX_COUNTS:-}" ]]; then
  while IFS= read -r line; do
    endpoint="${line%=*}"
    count="${line##*=}"
    if [[ "$endpoint" == "$ENDPOINT" ]]; then
      printf '%s\n' "$count"
      exit 0
    fi
  done <<<"$FAKE_PREFIX_COUNTS"
  echo "no fake prefix count for endpoint ${ENDPOINT}" >&2
  exit 1
fi
printf '%s\n' "$FAKE_PREFIX_COUNT"
`)
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "etcdctl"), `#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"endpoint hashkv"* ]]; then
  printf '%s\n' "$FAKE_HASHKV_JSON"
  exit 0
fi
printf '%s\n' "$FAKE_STATUS_JSON"
`)
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "timeout"), `#!/usr/bin/env bash
set -euo pipefail
duration="$1"
shift
printf '%s %s\n' "$duration" "$(basename "$1")" >>"$FAKE_TIMEOUT_LOG"
exec "$@"
`)
			timeoutLog := filepath.Join(dir, "timeout.log")

			env := []string{
				"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
				"KUBECTL=kubectl",
				"CURL=curl",
				"GO=go",
				"ETCDCTL=etcdctl",
				"TIMEOUT_CMD=timeout",
				"JQ=jq",
				"KUBE_CONTEXT=kind-kubebrain-dbaas",
				"KUBEBRAIN_NAMESPACE=kubebrain-dev",
				"EXPECTED_READY_PODS=3",
				"ENDPOINT=http://127.0.0.1:2379",
				"READYZ_URL=http://172.18.0.3:32758/readyz",
				"PREFIX=/",
				"PROBE_TIMEOUT=10s",
				"FAKE_PODS_JSON=" + compactJSONString(tc.podsJSON),
				"FAKE_READYZ=" + tc.readyz,
				"FAKE_READYZ_VERBOSE=" + defaultReadyzVerbose(tc.readyzVerbose),
				"FAKE_READYZ_EXCLUDE_DATA=" + defaultReadyzExcludeData(tc.readyzExcludeData),
				"FAKE_LIVEZ=" + defaultLivez(tc.livez),
				"FAKE_LIVEZ_VERBOSE=" + defaultLivezVerbose(tc.livezVerbose),
				"FAKE_LIVEZ_EXCLUDE=" + defaultLivez(tc.livezExclude),
				"FAKE_LIVEZ_SERIALIZABLE_READ_VERBOSE=" + defaultLivezVerbose(tc.livezNamedVerbose),
				"FAKE_HEALTH_JSON=" + defaultHealthJSON(tc.healthJSON),
				"FAKE_HEALTH_METHOD_CODE=" + defaultHealthMethodCode(tc.healthMethodCode),
				"FAKE_HEALTH_METHOD_ALLOW=" + defaultHealthMethodAllow(tc.healthMethodAllow),
				"FAKE_HTTP_HEADER_CONTENT_TYPE=" + defaultHTTPHeaderContentType(tc.httpHeaderType),
				"FAKE_HTTP_HEADER_NOSNIFF=" + defaultHTTPHeaderNosniff(tc.httpHeaderNosniff),
				"FAKE_VERSION_HEADER_CONTENT_TYPE=" + defaultVersionHeaderContentType(tc.versionHeaderType),
				"FAKE_DEBUG_VARS_HEADER_CONTENT_TYPE=" + defaultDebugVarsHeaderContentType(tc.debugVarsHeader),
				"FAKE_SERIALIZABLE_HEALTH_JSON=" + defaultHealthJSON(tc.serialHealthJSON),
				"FAKE_PREFIX_COUNT=" + tc.count,
				"FAKE_STATUS_JSON=" + tc.statusJSON,
				"FAKE_GATEWAY_STATUS_JSON=" + defaultGatewayStatusJSON(tc.gatewayJSON),
				"FAKE_GATEWAY_AUTH_STATUS_JSON=" + defaultGatewayAuthStatusJSON(tc.authJSON),
				"FAKE_GATEWAY_ALARM_JSON=" + defaultGatewayAlarmJSON(tc.alarmJSON),
				"FAKE_VERSION_JSON=" + defaultVersionJSON(tc.versionJSON),
				"FAKE_INFO_VERSION_JSON=" + defaultVersionJSON(tc.infoVersionJSON),
				"FAKE_INFO_METRICS=" + defaultInfoMetrics(tc.infoMetrics),
				"FAKE_CLIENT_METRICS_RESPONSE=" + defaultClientMetricsResponse(tc.clientMetrics),
				"FAKE_INFO_DEBUG_VARS=" + defaultInfoDebugVars(tc.infoDebugVars),
				"FAKE_CLIENT_DEBUG_VARS_RESPONSE=" + defaultClientDebugVarsResponse(tc.clientDebugVars),
				"FAKE_INFO_PPROF_RESPONSE=" + defaultInfoPprofResponse(tc.infoPprof),
				"FAKE_CLIENT_PPROF_RESPONSE=" + defaultClientPprofResponse(tc.clientPprof),
				`FAKE_GATEWAY_HASH_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":222}`,
				`FAKE_GATEWAY_HASHKV_JSON={"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"hash":111,"compact_revision":"3","hash_revision":"7"}`,
				`FAKE_HASHKV_JSON=[{"Endpoint":"http://127.0.0.1:2379","HashKV":{"header":{"cluster_id":123,"member_id":456,"revision":7},"hash":111,"compact_revision":3}}]`,
				"FAKE_TIMEOUT_LOG=" + timeoutLog,
			}
			env = append(env, tc.extraEnv...)

			output, err := runProductionScriptCommand(t, "validate-dataplane-readonly.sh", env)
			if tc.wantOK {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.Contains(t, string(output), tc.wantOutput)
			if len(tc.wantTimeout) > 0 {
				timeoutBytes, readErr := os.ReadFile(timeoutLog)
				require.NoError(t, readErr)
				for _, want := range tc.wantTimeout {
					require.Contains(t, string(timeoutBytes), want)
				}
			}
		})
	}
}

func compactJSONString(value string) string {
	return strings.Join(strings.Fields(value), "")
}

func defaultReadyzVerbose(value string) string {
	if value != "" {
		return value
	}
	return "[+]data_corruption ok\n[+]serializable_read ok\n[+]linearizable_read ok\n[+]non_learner ok\nok"
}

func defaultReadyzExcludeData(value string) string {
	if value != "" {
		return value
	}
	return "[+]serializable_read ok\n[+]linearizable_read ok\n[+]non_learner ok\nok"
}

func defaultLivez(value string) string {
	if value != "" {
		return value
	}
	return "ok"
}

func defaultLivezVerbose(value string) string {
	if value != "" {
		return value
	}
	return "[+]serializable_read ok\nok"
}

func defaultHealthJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"health":"true","reason":""}`
}

func defaultHealthMethodCode(value string) string {
	if value != "" {
		return value
	}
	return "405"
}

func defaultHealthMethodAllow(value string) string {
	if value != "" {
		return value
	}
	return "GET"
}

func defaultHTTPHeaderContentType(value string) string {
	if value != "" {
		return value
	}
	return "text/plain; charset=utf-8"
}

func defaultHTTPHeaderNosniff(value string) string {
	if value != "" {
		return value
	}
	return "nosniff"
}

func defaultVersionHeaderContentType(value string) string {
	if value != "" {
		return value
	}
	return "application/json"
}

func defaultDebugVarsHeaderContentType(value string) string {
	if value != "" {
		return value
	}
	return "application/json; charset=utf-8"
}

func defaultGatewayStatusJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"version":"3.7.0","storageVersion":"3.7.0","dbSize":"99","dbSizeInUse":"88","dbSizeQuota":"2147483648","isLearner":false,"leader":"456","raftTerm":"8","raftIndex":"7","raftAppliedIndex":"7","downgradeInfo":{}}`
}

func defaultGatewayAuthStatusJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"},"authRevision":"5"}`
}

func defaultGatewayAlarmJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"header":{"cluster_id":"123","member_id":"456","revision":"7","raft_term":"8"}}`
}

func defaultVersionJSON(value string) string {
	if value != "" {
		return value
	}
	return `{"etcdserver":"3.7.0","etcdcluster":"3.7","storage":"3.7.0"}`
}

func defaultInfoMetrics(value string) string {
	if value != "" {
		return value
	}
	return strings.Join([]string{
		`# HELP etcd_server_version Which version is running. 1 for 'server_version' label with current version.`,
		`# TYPE etcd_server_version gauge`,
		`etcd_server_version{cluster="default",server_version="3.7.0"} 1`,
		`# HELP etcd_cluster_version Which version is running. 1 for 'cluster_version' label with current cluster version.`,
		`# TYPE etcd_cluster_version gauge`,
		`etcd_cluster_version{cluster="default",cluster_version="3.7"} 1`,
		`etcd_server_go_version{cluster="default",server_go_version="go1.26.5"} 1`,
		`etcd_server_id{cluster="default",server_id="e3f"} 1`,
		`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
		`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
		`grpc_server_msg_received_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
		`grpc_server_msg_sent_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
		`etcd_network_known_peers{Local="abc",Remote="abc"} 1`,
		`go_info{version="go1.26.5"} 1`,
		`go_goroutines 12`,
		`go_threads 7`,
		`go_gc_gogc_percent 100`,
		`go_gc_gomemlimit_bytes 9.223372036854776e+18`,
		`go_sched_gomaxprocs_threads 80`,
		`os_fd_used 64`,
		`os_fd_limit 1048576`,
		`etcd_server_has_leader{cluster="default"} 1`,
		`etcd_server_is_leader{cluster="default"} 1`,
		`etcd_server_leader_changes_seen_total{cluster="default"} 1`,
		`etcd_server_is_learner{cluster="default"} 0`,
		`etcd_server_health_success{cluster="default"} 0`,
		`etcd_server_health_failures{cluster="default"} 0`,
		`etcd_debugging_auth_revision{cluster="default"} 1`,
		`etcd_server_quota_backend_bytes{cluster="default"} 2147483648`,
		`etcd_mvcc_db_total_size_in_bytes{cluster="default"} 88`,
		`etcd_mvcc_db_total_size_in_use_in_bytes{cluster="default"} 88`,
		`etcd_debugging_mvcc_current_revision{cluster="default"} 7`,
		`etcd_debugging_mvcc_compact_revision{cluster="default"} 3`,
		`etcd_debugging_mvcc_watch_stream_total{cluster="default"} 0`,
		`etcd_debugging_mvcc_watcher_total{cluster="default"} 0`,
		`etcd_debugging_mvcc_slow_watcher_total{cluster="default"} 0`,
		`promhttp_metric_handler_requests_in_flight 1`,
		`promhttp_metric_handler_requests_total{code="200"} 1`,
	}, "\n") + "\n"
}

func defaultClientMetricsResponse(value string) string {
	if value != "" {
		return value
	}
	return "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n404 page not found\n"
}

func defaultInfoDebugVars(value string) string {
	if value != "" {
		return value
	}
	return `{"cmdline":["kubebrain"],"memstats":{"Alloc":1}}`
}

func defaultClientDebugVarsResponse(value string) string {
	if value != "" {
		return value
	}
	return "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n404 page not found\n"
}

func defaultInfoPprofResponse(value string) string {
	if value != "" {
		return value
	}
	return "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n404 page not found\n"
}

func defaultClientPprofResponse(value string) string {
	if value != "" {
		return value
	}
	return "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n404 page not found\n"
}

func TestProductionReadinessDataplaneReadonlyExampleIncludesReadonlyAuditFields(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "production_readiness_cn.md"))
	require.NoError(t, err)
	doc := string(data)

	end := strings.Index(doc, "hack/production/validate-dataplane-readonly.sh")
	require.NotEqual(t, -1, end, "dataplane readonly gate command is missing")
	start := strings.LastIndex(doc[:end], "```shell")
	require.NotEqual(t, -1, start, "dataplane readonly shell example is missing")
	example := doc[start:end]

	for _, required := range []string{
		"EXPECTED_STATUS_CLUSTER_ID=",
		"EXPECTED_STATUS_VERSION=",
		"EXPECTED_HASHKV_HASH=",
		"EXPECTED_READYZ_NAMED_CHECKS=1",
		"EXPECTED_LIVEZ_NAMED_CHECKS=1",
		"EXPECTED_HEALTH_EXCLUDE_CHECKS=1",
		"EXPECTED_HEALTH_METHOD_CHECKS=1",
		"EXPECTED_HTTP_HEADER_CHECKS=1",
		"EXPECTED_INFO_METRICS_CHECKS=1",
		"EXPECTED_DEBUG_VARS_CHECKS=1",
		"EXPECTED_PPROF_DISABLED_CHECKS=1",
	} {
		require.Contains(t, example, required)
	}
	require.Contains(t, doc, "所有 Status version 唯一且等于期望 semver")
	for _, required := range []string{
		"readyz_verbose=ok",
		"readyz_data_corruption=ok",
		"readyz_serializable_read=ok",
		"readyz_linearizable_read=ok",
		"readyz_non_learner=ok",
		"readyz_named_checks=ok",
		"health_exclude_checks=ok",
		"livez=ok",
		"livez_serializable_read=ok",
		"livez_named_checks=ok",
		"health_method_checks=ok",
		"http_header_checks=ok",
		"info_metrics=ok",
		"client_metrics=404",
		"server_identity_metrics=ok",
		"grpc_metrics=ok",
		"runtime_metrics=ok",
		"fd_metrics=ok",
		"server_state_metrics=ok",
		"health_metrics=ok",
		"quota_metrics=ok",
		"mvcc_db_size_metrics=ok",
		"mvcc_revision_metrics=ok",
		"promhttp_metrics=ok",
		"info_debug_vars=ok",
		"client_debug_vars=404",
		"debug_vars_method_headers=ok",
		"client_pprof=404",
		"info_pprof=404",
		"health=true",
		"serializable_health=true",
		"status_errors=empty",
		"raft_indexes_match_revision=true",
		"gateway_status_version=<semver>",
		"gateway_storage_version=<semver>",
		"version_etcdserver=<semver>",
		"version_storage=<semver>",
		"info_version_storage=<semver>",
		"gateway_auth_enabled=<bool>",
		"gateway_alarms=empty",
		"gateway_hashkv_hash=<n>",
		"gateway_hashkv_revisions_match=true",
		"revisions_match=true",
		"hashkv_raft_terms=<unique>",
		"raft_terms_match=true",
		"同一个静态 MVCC/Raft 观察边界",
	} {
		require.Contains(t, doc, required)
	}
}

func writeDataplaneProbeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o755))
}
