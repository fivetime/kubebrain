package production_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const fakeInitialCluster = "kb-0=peer-0,kb-1=peer-1,kb-2=peer-2"

func fakeKubeBrainArgs(advertisedURLs, initialCluster string) string {
	return "--port=3379\n" +
		"--keyspace=instance-a\n" +
		"--pd-addrs=kb-pd.storage.svc:2379\n" +
		"--quota-backend-bytes=429496729600\n" +
		"--advertise-client-urls=" + advertisedURLs + "\n" +
		"--initial-cluster=" + initialCluster + "\n" +
		"--max-request-rate=2000\n" +
		"--request-rate-burst=4000\n" +
		"--max-delete-range-keys=1024\n" +
		"--max-watches=10000"
}

func TestValidateInstanceReady(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		image                    string
		kubeStatus               string
		kubeArgs                 string
		topology                 string
		healthOK                 bool
		advertisedURLs           string
		unreachableAdvertisedURL string
		memberListJSON           string
		podsJSON                 string
		endpointSlicesJSON       string
		initialCluster           string
		etcdctlExecPod           string
		wantExec                 bool
		wantOK                   bool
		wantOutput               string
	}{
		{
			name:       "converged release",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			wantOK:     true,
			wantOutput: "release gate passed",
		},
		{
			name:           "runs etcdctl from selected cluster Pod",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			etcdctlExecPod: "release-gate-runner",
			wantExec:       true,
			wantOK:         true,
			wantOutput:     "release gate passed",
		},
		{
			name:           "multiple reachable advertised client URLs",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			advertisedURLs: "https://instance-a.example:2379,https://instance-b.example:2379",
			wantOK:         true,
			wantOutput:     "--endpoints=https://instance-b.example:2379",
		},
		{
			name:       "wrong quota",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--port=3379\n--quota-backend-bytes=1073741824\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "quota configuration mismatch",
		},
		{
			name:       "duplicate quota",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "quota configuration mismatch",
		},
		{
			name:       "missing advertised client URL argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--port=3379\n--quota-backend-bytes=429496729600",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "advertised client URL mismatch",
		},
		{
			name:       "wrong advertised client URL",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--advertise-client-urls=https://internal.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "advertised client URL mismatch",
		},
		{
			name:       "duplicate advertised client URL argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "advertised client URL mismatch",
		},
		{
			name:       "missing keyspace argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "keyspace configuration mismatch",
		},
		{
			name:       "wrong keyspace",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--keyspace=instance-b\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "keyspace configuration mismatch",
		},
		{
			name:       "duplicate keyspace argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--keyspace=instance-a\n--keyspace=instance-a\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "keyspace configuration mismatch",
		},
		{
			name:       "missing PD address argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--keyspace=instance-a\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "PD address configuration mismatch",
		},
		{
			name:       "wrong PD address",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--keyspace=instance-a\n--pd-addrs=wrong-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "PD address configuration mismatch",
		},
		{
			name:       "duplicate PD address argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--keyspace=instance-a\n--pd-addrs=kb-pd.storage.svc:2379\n--pd-addrs=kb-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "PD address configuration mismatch",
		},
		{
			name:       "wrong image",
			image:      "registry/kubebrain@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "not the expected converged release",
		},
		{
			name:       "stale rollout",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "9\t8\t3\t3\t2\tkb-old\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "not the expected converged release",
		},
		{
			name:       "wrong KubeBrain StatefulSet resource identity",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tuid-other",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "StatefulSet resource identity mismatch",
		},
		{
			name:       "Pod has stale StatefulSet owner",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON:   fakeKubeBrainPodsJSON("uid-old", "kb-new", "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true, false),
			wantOutput: "Pod set does not match",
		},
		{
			name:       "Pod has stale controller revision",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON:   fakeKubeBrainPodsJSON("uid-kubebrain", "kb-old", "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true, false),
			wantOutput: "Pod set does not match",
		},
		{
			name:       "Pod is terminating",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON:   fakeKubeBrainPodsJSON("uid-kubebrain", "kb-new", "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true, true),
			wantOutput: "Pod set does not match",
		},
		{
			name:       "Pod is not Ready",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON:   fakeKubeBrainPodsJSON("uid-kubebrain", "kb-new", "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false, false),
			wantOutput: "Pod set does not match",
		},
		{
			name:       "client Service has stale endpoint Pod identity",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			endpointSlicesJSON: fakeEndpointSlicesJSON([]string{
				"uid-kubebrain-0", "uid-kubebrain-1", "uid-old",
			}, true),
			wantOutput: "EndpointSlices do not match",
		},
		{
			name:       "wrong storage topology",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "1\t3",
			healthOK:   true,
			wantOutput: "topology mismatch",
		},
		{
			name:       "wrong storage cluster identity",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3\t2",
			healthOK:   true,
			wantOutput: "storage identity mismatch",
		},
		{
			name:       "wrong TidbCluster resource identity",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3\t1\tuid-other",
			healthOK:   true,
			wantOutput: "resource identity mismatch",
		},
		{
			name:       "unhealthy endpoint",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			wantOutput: "endpoint health failed",
		},
		{
			name:                     "unreachable advertised client URL",
			image:                    "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:               "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:                 "3\t3",
			healthOK:                 true,
			advertisedURLs:           "https://instance.example:2379,https://unreachable.example:2379",
			unreachableAdvertisedURL: "https://unreachable.example:2379",
			wantOutput:               "advertised client URL is unreachable",
		},
		{
			name:           "empty advertised client URL entry",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			advertisedURLs: "https://instance.example:2379,,https://other.example:2379",
			wantOutput:     "empty entry",
		},
		{
			name:           "duplicate advertised client URL entry",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			advertisedURLs: "https://instance.example:2379,https://instance.example:2379",
			wantOutput:     "duplicate entry",
		},
		{
			name:           "runtime MemberList has wrong advertised URL",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: fakeRuntimeMemberListJSON("https://internal.example:2379"),
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList repeats advertised URL",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: fakeRuntimeMemberListJSON("https://instance.example:2379,https://instance.example:2379"),
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList repeats peer URL",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["peer-0","peer-0"],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["peer-1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["peer-2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList has wrong storage cluster identity",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":2},"members":[]}`,
			wantOutput:     "runtime storage identity mismatch",
		},
		{
			name:           "runtime MemberList repeats member identity",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["p0"],"clientURLs":["https://instance.example:2379"]},{"ID":11,"name":"kb-1","peerURLs":["p1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["p2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList is missing a member",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["p0"],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["p1"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList has an incomplete member",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":[],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["p1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["p2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList has wrong peer mapping",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["wrong-peer"],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["peer-1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["peer-2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:       "missing initial cluster argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--port=3379\n--keyspace=instance-a\n--pd-addrs=kb-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "initial cluster configuration mismatch",
		},
		{
			name:           "wrong initial cluster argument",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			initialCluster: "kb-0=peer-0,kb-1=peer-1,kb-2=wrong-peer",
			kubeArgs:       "--port=3379\n--keyspace=instance-a\n--pd-addrs=kb-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379\n--initial-cluster=" + fakeInitialCluster,
			wantOutput:     "initial cluster configuration mismatch",
		},
		{
			name:           "repeated initial cluster member peer URLs",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			initialCluster: "kb-0=peer-0a,kb-0=peer-0b,kb-1=peer-1,kb-2=peer-2",
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["peer-0a","peer-0b"],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["peer-1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["peer-2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOK:         true,
			wantOutput:     "release gate passed",
		},
		{
			name:           "duplicate initial cluster peer URL",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			initialCluster: "kb-0=peer-0,kb-1=peer-0,kb-2=peer-2",
			wantOutput:     "duplicate peer URL",
		},
		{
			name:       "wrong max request rate",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--max-request-rate=2000", "--max-request-rate=100"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "max request rate configuration mismatch",
		},
		{
			name:       "missing request rate burst",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "\n--request-rate-burst=4000", ""),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "request rate burst configuration mismatch",
		},
		{
			name:       "duplicate max delete range keys",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--max-delete-range-keys=1024",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "max delete range keys configuration mismatch",
		},
		{
			name:       "wrong max watches",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--max-watches=10000", "--max-watches=1000"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "max watches configuration mismatch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *" exec "* ]]; then
  printf '%s\n' "$*" >>"${FAKE_EXEC_LOG:?}"
  while [[ "$1" != "--" ]]; do
    shift
  done
  shift
  exec "$@"
elif [[ "$*" == *"get tidbcluster"* && "$*" == *".status.conditions"* ]]; then
  printf 'True'
elif [[ "$*" == *"get statefulset kb-pd"* && "$*" == *"jsonpath="* ]]; then
  printf '5\t5\t3\t3\t3\tpd-new\tpd-new'
elif [[ "$*" == *"get statefulset kb-tikv"* && "$*" == *"jsonpath="* ]]; then
  printf '7\t7\t3\t3\t3\ttikv-new\ttikv-new'
elif [[ "$*" == *"get tidbcluster"* && "$*" == *".spec.pd.replicas"* ]]; then
  printf '%s' "$FAKE_TOPOLOGY"
elif [[ "$*" == *"get statefulset kubebrain"* && "$*" == *".args"* ]]; then
  printf '%s' "$FAKE_KUBEBRAIN_ARGS"
elif [[ "$*" == *"get statefulset kubebrain"* && "$*" == *"jsonpath="* ]]; then
  printf '%s' "$FAKE_KUBEBRAIN_STATUS"
elif [[ "$*" == *"get service kubebrain"* && "$*" == *"jsonpath="* ]]; then
  printf 'uid-client-service'
elif [[ "$*" == *"get endpointslice"* && "$*" == *"kubernetes.io/service-name=kubebrain"* ]]; then
  printf '%s' "$FAKE_ENDPOINT_SLICES_JSON"
elif [[ "$*" == *"get pods"* && "$*" == *"app.kubernetes.io/name=kubebrain"* ]]; then
  printf '%s' "$FAKE_KUBEBRAIN_PODS_JSON"
else
  printf 'diagnostic output\n'
fi
`), 0o755))
			fakeEtcdctl := filepath.Join(dir, "etcdctl")
			require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ -n "$FAKE_UNREACHABLE_ADVERTISED_URL" && "$*" == *"--endpoints=$FAKE_UNREACHABLE_ADVERTISED_URL"* ]]; then
  printf 'unreachable\n' >&2
  exit 1
fi
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' "$FAKE_MEMBER_LIST_JSON"
  exit 0
fi
if [[ "$FAKE_HEALTH_OK" == "true" && "$*" == *"endpoint health"* ]]; then
  printf 'healthy %s\n' "$*"
  exit 0
fi
printf 'unhealthy\n' >&2
exit 1
`), 0o755))

			kubeArgs := tc.kubeArgs
			advertisedURLs := tc.advertisedURLs
			if advertisedURLs == "" {
				advertisedURLs = "https://instance.example:2379"
			}
			topology := tc.topology
			switch strings.Count(topology, "\t") {
			case 1:
				topology += "\t1\tuid-tidb"
			case 2:
				topology += "\tuid-tidb"
			}
			initialCluster := tc.initialCluster
			if initialCluster == "" {
				initialCluster = fakeInitialCluster
			}
			if kubeArgs == "" {
				kubeArgs = fakeKubeBrainArgs(advertisedURLs, initialCluster)
			}
			memberListJSON := tc.memberListJSON
			if memberListJSON == "" {
				memberListJSON = fakeRuntimeMemberListJSON(advertisedURLs)
			}
			podsJSON := tc.podsJSON
			if podsJSON == "" {
				podsJSON = fakeKubeBrainPodsJSON("uid-kubebrain", "kb-new", tc.image, true, false)
			}
			endpointSlicesJSON := tc.endpointSlicesJSON
			if endpointSlicesJSON == "" {
				endpointSlicesJSON = fakeEndpointSlicesJSON([]string{"uid-kubebrain-0", "uid-kubebrain-1", "uid-kubebrain-2"}, true)
			}
			kubeStatus := tc.kubeStatus
			if strings.Count(kubeStatus, "\t") == 7 {
				kubeStatus += "\tuid-kubebrain"
			}
			command := exec.Command("bash", "validate-instance-ready.sh")
			command.Env = append(os.Environ(),
				"KUBECTL="+fakeKubectl,
				"ETCDCTL="+fakeEtcdctl,
				"EXPECTED_IMAGE="+tc.image,
				"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
				"EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID=uid-client-service",
				"EXPECTED_KEYSPACE=instance-a",
				"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
				"EXPECTED_CLUSTER_ID=1",
				"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
				"EXPECTED_INITIAL_CLUSTER="+initialCluster,
				"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
				"EXPECTED_ADVERTISE_CLIENT_URLS="+advertisedURLs,
				"EXPECTED_MAX_REQUEST_RATE=2000",
				"EXPECTED_REQUEST_RATE_BURST=4000",
				"EXPECTED_MAX_DELETE_RANGE_KEYS=1024",
				"EXPECTED_MAX_WATCHES=10000",
				"ENDPOINT=https://instance.example:2379",
				"TIMEOUT_SECONDS=1",
				"POLL_INTERVAL_SECONDS=0",
				"FAKE_KUBEBRAIN_STATUS="+kubeStatus,
				"FAKE_KUBEBRAIN_ARGS="+kubeArgs,
				"FAKE_KUBEBRAIN_PODS_JSON="+podsJSON,
				"FAKE_ENDPOINT_SLICES_JSON="+endpointSlicesJSON,
				"FAKE_TOPOLOGY="+topology,
				"FAKE_HEALTH_OK="+boolString(tc.healthOK),
				"FAKE_UNREACHABLE_ADVERTISED_URL="+tc.unreachableAdvertisedURL,
				"FAKE_MEMBER_LIST_JSON="+memberListJSON,
			)
			execLog := filepath.Join(dir, "etcdctl-exec.log")
			if tc.etcdctlExecPod != "" {
				command.Env = append(command.Env,
					"ETCDCTL_EXEC_POD="+tc.etcdctlExecPod,
					"FAKE_EXEC_LOG="+execLog,
				)
			}
			output, err := command.CombinedOutput()
			if tc.wantOK {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.Contains(t, strings.TrimSpace(string(output)), tc.wantOutput)
			if tc.wantExec {
				executions, readErr := os.ReadFile(execLog)
				require.NoError(t, readErr)
				require.Contains(t, string(executions), "exec "+tc.etcdctlExecPod+" -- "+fakeEtcdctl)
			}
		})
	}
}

func fakeKubeBrainPodsJSON(ownerUID, revision, image string, ready, terminating bool) string {
	items := make([]map[string]any, 3)
	for index := range items {
		metadata := map[string]any{
			"name":   "kubebrain-" + string(rune('0'+index)),
			"uid":    "uid-kubebrain-" + string(rune('0'+index)),
			"labels": map[string]any{"controller-revision-hash": revision},
			"ownerReferences": []map[string]any{{
				"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "kubebrain",
				"uid": ownerUID, "controller": true,
			}},
		}
		if terminating && index == 0 {
			metadata["deletionTimestamp"] = "2026-07-22T00:00:00Z"
		}
		readyStatus := "True"
		if !ready && index == 0 {
			readyStatus = "False"
		}
		items[index] = map[string]any{
			"metadata": metadata,
			"spec":     map[string]any{"containers": []map[string]any{{"name": "kubebrain", "image": image}}},
			"status": map[string]any{
				"phase":      "Running",
				"conditions": []map[string]any{{"type": "Ready", "status": readyStatus}},
			},
		}
	}
	encoded, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeEndpointSlicesJSON(podUIDs []string, ready bool) string {
	endpoints := make([]map[string]any, len(podUIDs))
	for index, podUID := range podUIDs {
		endpoints[index] = map[string]any{
			"conditions": map[string]any{"ready": ready, "serving": ready, "terminating": false},
			"targetRef":  map[string]any{"kind": "Pod", "uid": podUID},
		}
	}
	encoded, err := json.Marshal(map[string]any{"items": []map[string]any{{"endpoints": endpoints}}})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeRuntimeMemberListJSON(advertisedURLs string) string {
	clientURLs := strings.Split(advertisedURLs, ",")
	members := make([]map[string]any, 3)
	for index := range members {
		members[index] = map[string]any{
			"ID":         index + 11,
			"name":       "kb-" + string(rune('0'+index)),
			"peerURLs":   []string{"peer-" + string(rune('0'+index))},
			"clientURLs": clientURLs,
		}
	}
	encoded, err := json.Marshal(map[string]any{
		"header":  map[string]any{"cluster_id": 1},
		"members": members,
	})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func TestValidateInstanceReadyRequiresImmutableInputs(t *testing.T) {
	command := exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(), "EXPECTED_IMAGE=", "EXPECTED_KUBEBRAIN_STATEFULSET_UID=", "ENDPOINT=", "EXPECTED_KEYSPACE=", "EXPECTED_PD_ADDRS=", "EXPECTED_CLUSTER_ID=", "EXPECTED_TIDB_CLUSTER_UID=", "EXPECTED_INITIAL_CLUSTER=", "EXPECTED_QUOTA_BACKEND_BYTES=", "EXPECTED_ADVERTISE_CLIENT_URLS=")
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_IMAGE is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=kubebrain:dev",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_CLUSTER_ID=1",
		"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_IMAGE must be an immutable image reference")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_CLUSTER_ID=1",
		"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_QUOTA_BACKEND_BYTES is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_CLUSTER_ID=1",
		"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_ADVERTISE_CLIENT_URLS is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_CLUSTER_ID=1",
		"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_KEYSPACE is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=",
		"EXPECTED_CLUSTER_ID=1",
		"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_PD_ADDRS is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_CLUSTER_ID=1",
		"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
		"EXPECTED_INITIAL_CLUSTER=",
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_INITIAL_CLUSTER is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_CLUSTER_ID=",
		"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_CLUSTER_ID is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_CLUSTER_ID=1",
		"EXPECTED_TIDB_CLUSTER_UID=",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_TIDB_CLUSTER_UID is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_CLUSTER_ID=1",
		"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_KUBEBRAIN_STATEFULSET_UID is required")
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
