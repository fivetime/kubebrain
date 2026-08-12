package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestOptionalEnvoyProfileIsHardenedAndCapacityAligned(t *testing.T) {
	objects := decodeManifest(t, filepath.Join("envoy", "resources.yaml"))
	require.Len(t, objects, 7)

	serviceAccount := objectByKindAndName(t, objects, "ServiceAccount", "kubebrain-envoy")
	require.False(t, nestedBool(t, serviceAccount, "automountServiceAccountToken"))
	pdb := objectByKindAndName(t, objects, "PodDisruptionBudget", "kubebrain-envoy")
	require.EqualValues(t, 2, nestedInt64(t, pdb, "spec", "minAvailable"))
	require.Equal(t, "AlwaysAllow", nestedString(t, pdb, "spec", "unhealthyPodEvictionPolicy"))

	deployment := objectByKindAndName(t, objects, "Deployment", "kubebrain-envoy")
	require.EqualValues(t, 3, nestedInt64(t, deployment, "spec", "replicas"))
	require.Equal(t, "kubebrain-envoy", nestedString(t, deployment, "spec", "template", "spec", "serviceAccountName"))
	require.False(t, nestedBool(t, deployment, "spec", "template", "spec", "automountServiceAccountToken"))
	require.EqualValues(t, 60, nestedInt64(t, deployment, "spec", "template", "spec", "terminationGracePeriodSeconds"))
	require.True(t, nestedBool(t, deployment, "spec", "template", "spec", "securityContext", "runAsNonRoot"))
	require.EqualValues(t, 65532, nestedInt64(t, deployment, "spec", "template", "spec", "securityContext", "runAsUser"))

	containers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
	require.Equal(t,
		"envoyproxy/envoy:v1.39.0@sha256:d59f7f5fa10cff6d5892b6c5e7df5c9297ddfb2c3683e33fbfb82da24de4fa66",
		nestedString(t, container, "image"))
	require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
	require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
	require.Equal(t, "/ready", nestedString(t, container, "readinessProbe", "httpGet", "path"))
	require.EqualValues(t, 1, nestedInt64(t, container, "readinessProbe", "failureThreshold"))
	preStop, found, err := unstructured.NestedStringSlice(container.Object, "lifecycle", "preStop", "exec", "command")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{
		"/usr/bin/timeout", "12", "/bin/bash", "-ec",
		"exec 3<>/dev/tcp/127.0.0.1/9901; printf 'POST /healthcheck/fail HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\nContent-Length: 0\\r\\n\\r\\n' >&3; IFS= read -r status <&3; [[ \"$status\" == $'HTTP/1.1 200 OK\\r' ]]; /bin/sleep 10",
	}, preStop)
	ports, found, err := unstructured.NestedSlice(container.Object, "ports")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, ports, 3)
	for index, want := range []struct {
		name string
		port int64
	}{{"client", 2379}, {"client-tls", 2380}, {"admin", 9901}} {
		port := &unstructured.Unstructured{Object: ports[index].(map[string]any)}
		require.Equal(t, want.name, nestedString(t, port, "name"))
		require.EqualValues(t, want.port, nestedInt64(t, port, "containerPort"))
	}

	upstream := objectByKindAndName(t, objects, "Service", "kubebrain-envoy-upstream")
	require.Equal(t, "None", nestedString(t, upstream, "spec", "clusterIP"))
	require.Equal(t, "kubebrain", nestedString(t, upstream, "spec", "selector", "app.kubernetes.io/name"))
	_, publishNotReady, err := unstructured.NestedBool(upstream.Object, "spec", "publishNotReadyAddresses")
	require.NoError(t, err)
	require.False(t, publishNotReady, "Envoy upstream discovery must exclude NotReady pods")
	client := objectByKindAndName(t, objects, "Service", "kubebrain-envoy")
	require.Equal(t, "ClusterIP", nestedString(t, client, "spec", "type"))
	clientPorts, found, err := unstructured.NestedSlice(client.Object, "spec", "ports")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, clientPorts, 2)

	for _, name := range []string{"kubebrain-envoy-ingress", "kubebrain-envoy-egress"} {
		policy := objectByKindAndName(t, objects, "NetworkPolicy", name)
		require.Equal(t, "kubebrain-envoy", nestedString(t, policy,
			"spec", "podSelector", "matchLabels", "app.kubernetes.io/name"))
	}

	kustomization, err := os.ReadFile(filepath.Join("envoy", "kustomization.yaml"))
	require.NoError(t, err)
	require.Contains(t, string(kustomization), "bootstrap.yaml")
	require.Contains(t, string(kustomization), "resources.yaml")
}

func TestOptionalEnvoyBootstrapPreservesEtcdLongStreams(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("envoy", "bootstrap.yaml"))
	require.NoError(t, err)
	var config map[string]any
	require.NoError(t, yaml.Unmarshal(data, &config))
	require.NotEmpty(t, config["static_resources"])
	require.NotEmpty(t, config["admin"])

	text := string(data)
	for _, required := range []string{
		"type: STRICT_DNS",
		"kubebrain-envoy-upstream.kubebrain-system.svc.cluster.local",
		"envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
		"http2_protocol_options:",
		"stream_idle_timeout: 0s",
		"request_timeout: 0s",
		"timeout: 0s",
		"idle_timeout: 0s",
		"grpc_timeout_header_max: 0s",
		"max_concurrent_streams: 2147483647",
		"max_requests: 20000",
		"split_external_local_origin_errors: true",
	} {
		require.Contains(t, text, required)
	}
	for _, forbidden := range []string{
		"respect_dns_ttl:",
		"access_log_path:",
		"max_concurrent_streams: 4294967295",
	} {
		require.NotContains(t, text, forbidden)
	}
	require.Equal(t, 1, strings.Count(text, "address: kubebrain-envoy-upstream.kubebrain-system.svc.cluster.local"))
}

func TestOptionalEnvoyBootstrapPreservesEndToEndTLSIdentity(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("envoy", "bootstrap.yaml"))
	require.NoError(t, err)
	text := string(data)
	for _, required := range []string{
		"name: kubebrain-client-tls-passthrough",
		"port_value: 2380",
		"envoy.filters.network.tcp_proxy",
		"stat_prefix: kubebrain_tls_passthrough",
		"cluster: kubebrain",
		"idle_timeout: 0s",
		"max_connect_attempts: 3",
	} {
		require.Contains(t, text, required)
	}
	for _, forbidden := range []string{
		"transport_socket:",
		"tls_context:",
		"forward_client_cert_details:",
	} {
		require.NotContains(t, text, forbidden, "passthrough profile must not terminate or synthesize TLS identity")
	}
}
