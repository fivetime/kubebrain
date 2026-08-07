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
		name        string
		podsJSON    string
		readyz      string
		count       string
		statusJSON  string
		extraEnv    []string
		wantTimeout []string
		wantOK      bool
		wantOutput  string
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
			wantOutput: "status cluster ID mismatch",
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
			wantOutput: "hashkv cluster ID mismatch",
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
			wantOutput: "status dbSize must be non-negative",
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "kubectl"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s' "$FAKE_PODS_JSON"
`)
			writeDataplaneProbeExecutable(t, filepath.Join(dir, "curl"), `#!/usr/bin/env bash
set -euo pipefail
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
				"FAKE_PREFIX_COUNT=" + tc.count,
				"FAKE_STATUS_JSON=" + tc.statusJSON,
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

func writeDataplaneProbeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o755))
}
