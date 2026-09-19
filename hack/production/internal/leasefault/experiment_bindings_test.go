package leasefault

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

func TestNativeExperimentBindings(t *testing.T) {
	for _, mode := range []string{"valid", "version", "owner", "term", "observer", "source", "image-tag", "hash", "ports-short", "ports-long", "ports-duplicate", "ports-privileged", "metric-scope", "minimum", "offset", "unknown", "duplicate", "prefilled-origin", "numeric-term", "wrong-digest", "public", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			n := networkPlan()
			minimum := uint64(1)
			p := NativeExperimentBindings{
				Version: 1, Network: n, Protocol: ProtocolRecovery{Owner: n.Owner, NamespaceUID: n.NamespaceUID, StatefulSetUID: n.StatefulSetUID, ClusterID: ^uint64(0), AlarmMemberID: ^uint64(0) - 1, LeaseID: 3, Key: "/acceptance/fixture"},
				InitialTerm: ^uint64(0) - 2, ObserverMemberID: 7, Source: strings.Repeat("a", 40), SourceHash: strings.Repeat("b", 64), Image: "ghcr.io/fivetime/kubebrain@sha256:" + strings.Repeat("c", 64), StackPorts: []int{18588, 18589, 18590, 18591},
				Metrics: []retirementmetrics.WorkerExpectation{{Binding: retirementmetrics.CaptureBinding{NamespaceUID: n.NamespaceUID, StatefulSetUID: n.StatefulSetUID, PodUID: n.PodUID, SpecSHA256: strings.Repeat("d", 64), Cluster: "test"}, Key: retirementmetrics.Key{Stage: "local", Outcome: "confirmed"}, MinimumCount: &minimum, RequireDuration: true}},
			}
			switch mode {
			case "version":
				p.Version = 2
			case "owner":
				p.Protocol.Owner = "other"
			case "term":
				p.InitialTerm = 0
			case "observer":
				p.ObserverMemberID = p.Protocol.AlarmMemberID
			case "source":
				p.Source = "short"
			case "image-tag":
				p.Image = "ghcr.io/fivetime/kubebrain:dbaas"
			case "hash":
				p.SourceHash = strings.Repeat("G", 64)
			case "ports-short":
				p.StackPorts = p.StackPorts[:3]
			case "ports-long":
				p.StackPorts = append(p.StackPorts, 18592)
			case "ports-duplicate":
				p.StackPorts[3] = p.StackPorts[0]
			case "ports-privileged":
				p.StackPorts[3] = 80
			case "metric-scope":
				p.Metrics[0].Binding.NamespaceUID = "other"
			case "minimum":
				p.Metrics[0].MinimumCount = nil
			case "offset":
				p.Metrics[0].Offset = 30 * time.Second
			}
			data, err := json.Marshal(p)
			require.NoError(t, err)
			switch mode {
			case "unknown":
				data = append(data[:len(data)-1], []byte(`,"unexpected":true}`)...)
			case "duplicate":
				data = bytes.Replace(data, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			case "prefilled-origin":
				data = append(data[:len(data)-1], []byte(`,"origin":"2026-09-19T00:00:00Z"}`)...)
			case "numeric-term":
				data = bytes.Replace(data, []byte(`"initial_term":"18446744073709551613"`), []byte(`"initial_term":18446744073709551613`), 1)
			}
			path := filepath.Join(t.TempDir(), "bindings.json")
			require.NoError(t, os.WriteFile(path, data, 0600))
			approved := planinput.SHA256(data)
			switch mode {
			case "wrong-digest":
				approved = strings.Repeat("e", 64)
			case "public":
				require.NoError(t, os.Chmod(path, 0644))
			case "symlink":
				require.NoError(t, os.Symlink(path, path+".link"))
				path += ".link"
			}
			got, err := LoadNativeExperimentBindings(path, approved)
			if mode != "valid" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			var compact bytes.Buffer
			require.NoError(t, json.Compact(&compact, p.Network.PodBefore))
			p.Network.PodBefore = compact.Bytes() // Marshal compacts RawMessage whitespace, not numbers.
			require.Equal(t, p, got)
			require.Equal(t, p.Protocol.ClusterID, got.Initial().ClusterID)
			require.Equal(t, p.Protocol.AlarmMemberID, got.Initial().InitialMemberID)
			require.Equal(t, p.InitialTerm, got.Successor().OldTerm)
			require.Equal(t, p.ObserverMemberID, got.Successor().ObserverMemberID)
			require.True(t, got.Initial().Origin.IsZero())
			require.True(t, got.Successor().Origin.IsZero())
			require.Zero(t, got.Initial().SuccessorTerm)
		})
	}
}
