#!/usr/bin/env bash
# Copyright 2026 ByteDance and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dbaas}"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
WORKLOAD="${WORKLOAD:-statefulset/kubebrain}"
POD_BASENAME="${POD_BASENAME:-kubebrain}"
VICTIM_POD="${VICTIM_POD:-kubebrain-0}"
PEER_SERVICE="${PEER_SERVICE:-kubebrain-peer}"
RUNTIME_IMAGE="${RUNTIME_IMAGE:-}"
PROBE_IMAGE="${PROBE_IMAGE:-}"
JOB_NAME="${JOB_NAME:-kubebrain-balancer-smoke-$(date +%s)}"
JOB_TIMEOUT_SECONDS="${JOB_TIMEOUT_SECONDS:-300}"
EXPECTED_ADVERTISED_ENDPOINTS="${EXPECTED_ADVERTISED_ENDPOINTS:-}"
GO_IMAGE="${GO_IMAGE:-golang:1.26.8-bookworm@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81}"

for command in docker jq kind kubectl sha256sum; do
  command -v "$command" >/dev/null || { echo "missing required command: ${command}" >&2; exit 1; }
done
[[ "$JOB_NAME" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#JOB_NAME} -le 63 ]] ||
  { echo "JOB_NAME must be a DNS label" >&2; exit 2; }
for value in "$CLUSTER_NAME" "$NAMESPACE" "$POD_BASENAME" "$PEER_SERVICE"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#value} -le 63 ]] ||
    { echo "cluster, namespace, Pod basename, and peer service must be DNS labels" >&2; exit 2; }
done
[[ "$WORKLOAD" =~ ^(statefulset|deployment)/[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] ||
  { echo "WORKLOAD must be a statefulset/name or deployment/name" >&2; exit 2; }
[[ "$VICTIM_POD" =~ ^${POD_BASENAME}-[012]$ ]] ||
  { echo "VICTIM_POD must be one of ${POD_BASENAME}-0, -1, or -2" >&2; exit 2; }
[[ "$JOB_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
  { echo "JOB_TIMEOUT_SECONDS must be a positive integer" >&2; exit 2; }
if [[ -n "$EXPECTED_ADVERTISED_ENDPOINTS" ]]; then
  IFS=',' read -r -a expected_advertised_endpoints <<<"$EXPECTED_ADVERTISED_ENDPOINTS"
  (( ${#expected_advertised_endpoints[@]} == 3 )) ||
    { echo "EXPECTED_ADVERTISED_ENDPOINTS must contain exactly three comma-separated URLs" >&2; exit 2; }
  for endpoint in "${expected_advertised_endpoints[@]}"; do
    [[ "$endpoint" =~ ^https?://[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?(:[1-9][0-9]{0,4})?$ ]] ||
      { echo "EXPECTED_ADVERTISED_ENDPOINTS contains an invalid URL: ${endpoint}" >&2; exit 2; }
  done
fi

if [[ -z "$RUNTIME_IMAGE" ]]; then
  RUNTIME_IMAGE="$(kubectl -n "$NAMESPACE" get "$WORKLOAD" \
    -o jsonpath='{.spec.template.spec.containers[0].image}')"
fi
[[ -n "$RUNTIME_IMAGE" ]] || { echo "RUNTIME_IMAGE is empty" >&2; exit 2; }
[[ "$RUNTIME_IMAGE" =~ ^[A-Za-z0-9][A-Za-z0-9._/:@-]{0,255}$ ]] ||
  { echo "RUNTIME_IMAGE is not a safe image reference" >&2; exit 2; }

victim_uid="$(kubectl -n "$NAMESPACE" get pod "$VICTIM_POD" -o jsonpath='{.metadata.uid}')"
[[ "$victim_uid" =~ ^[A-Za-z0-9-]{1,64}$ ]] || { echo "victim Pod UID is invalid" >&2; exit 1; }
victim_node="$(kubectl -n "$NAMESPACE" get pod "$VICTIM_POD" -o jsonpath='{.spec.nodeName}')"
[[ -n "$victim_node" ]] || { echo "victim Pod is not assigned to a node" >&2; exit 1; }
probe_goos="$(kubectl get node "$victim_node" -o jsonpath='{.status.nodeInfo.operatingSystem}')"
probe_goarch="$(kubectl get node "$victim_node" -o jsonpath='{.status.nodeInfo.architecture}')"
[[ "$probe_goos" == linux ]] || { echo "victim node operating system must be linux, got ${probe_goos}" >&2; exit 2; }
case "$probe_goarch" in
  amd64|arm64) ;;
  *) echo "victim node architecture must be amd64 or arm64, got ${probe_goarch}" >&2; exit 2 ;;
esac

if [[ -z "$PROBE_IMAGE" ]]; then
  probe_source_digest="$({
    printf '%s\0' "$GO_IMAGE" "$RUNTIME_IMAGE" "$probe_goos" "$probe_goarch"
    sha256sum "$ROOT_DIR/hack/dev/incluster-balancer-smoke.sh" \
      "$ROOT_DIR/hack/dev/cmd/balancer-smoke/main.go" "$ROOT_DIR/go.mod" "$ROOT_DIR/go.sum"
  } | sha256sum | cut -c1-20)"
  [[ "$probe_source_digest" =~ ^[a-f0-9]{20}$ ]] || { echo "cannot digest probe image inputs" >&2; exit 1; }
  PROBE_IMAGE="kubebrain:balancer-smoke-${probe_goarch}-${probe_source_digest}"
fi
[[ "$PROBE_IMAGE" =~ ^[A-Za-z0-9][A-Za-z0-9._/:-]{0,255}$ ]] ||
  { echo "PROBE_IMAGE must be a safe tag reference (digests are not accepted)" >&2; exit 2; }

workdir="$(mktemp -d)"
cleanup() {
  kubectl -n "$NAMESPACE" delete job "$JOB_NAME" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete rolebinding "$JOB_NAME" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete role "$JOB_NAME" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete serviceaccount "$JOB_NAME" --ignore-not-found >/dev/null 2>&1 || true
  rm -rf "$workdir"
}
trap cleanup EXIT

cat >"$workdir/Dockerfile" <<EOF
ARG GO_IMAGE=${GO_IMAGE}
ARG RUNTIME_IMAGE=${RUNTIME_IMAGE}
FROM --platform=\$BUILDPLATFORM \${GO_IMAGE} AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY hack/dev/cmd/balancer-smoke/main.go ./hack/dev/cmd/balancer-smoke/main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=\${TARGETARCH} go build -trimpath -o /balancer-smoke ./hack/dev/cmd/balancer-smoke
FROM \${RUNTIME_IMAGE}
COPY --from=build /balancer-smoke /usr/local/bin/kubebrain-balancer-smoke
EOF

docker build -f "$workdir/Dockerfile" \
  --platform "linux/$probe_goarch" \
  --build-arg "GO_IMAGE=$GO_IMAGE" --build-arg "RUNTIME_IMAGE=$RUNTIME_IMAGE" \
  -t "$PROBE_IMAGE" "$ROOT_DIR"
built_platform="$(docker image inspect "$PROBE_IMAGE" --format '{{.Os}}/{{.Architecture}}')"
[[ "$built_platform" == "$probe_goos/$probe_goarch" ]] ||
  { echo "probe image platform mismatch: got ${built_platform}, want ${probe_goos}/${probe_goarch}" >&2; exit 1; }
kind load docker-image "$PROBE_IMAGE" --name "$CLUSTER_NAME"

endpoints=""
for ordinal in 0 1 2; do
  endpoint="http://${POD_BASENAME}-${ordinal}.${PEER_SERVICE}.${NAMESPACE}.svc.cluster.local:3379"
  endpoints="${endpoints:+${endpoints},}${endpoint}"
done
victim_endpoint="http://${VICTIM_POD}.${PEER_SERVICE}.${NAMESPACE}.svc.cluster.local:3379"

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
rules:
- apiGroups: [""]
  resources: ["pods"]
  resourceNames: ["${VICTIM_POD}"]
  verbs: ["get", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
subjects:
- kind: ServiceAccount
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ${JOB_NAME}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: ${JOB_TIMEOUT_SECONDS}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: kubebrain-balancer-smoke
    spec:
      serviceAccountName: ${JOB_NAME}
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/os: ${probe_goos}
        kubernetes.io/arch: ${probe_goarch}
      securityContext:
        fsGroup: 65532
      containers:
      - name: client
        image: ${PROBE_IMAGE}
        imagePullPolicy: Never
        command:
        - /bin/bash
        - -ec
        - |
          trap 'status=\$?; printf "%s\\n" "\$status" >/state/client-exited; exit "\$status"' EXIT
          /usr/local/bin/kubebrain-balancer-smoke
        env:
        - name: ENDPOINTS
          value: "${endpoints}"
        - name: VICTIM_ENDPOINT
          value: "${victim_endpoint}"
        - name: EXPECTED_ADVERTISED_ENDPOINTS
          value: "${EXPECTED_ADVERTISED_ENDPOINTS}"
        - name: STATE_DIR
          value: /state
        volumeMounts:
        - name: state
          mountPath: /state
      - name: fault
        image: ${PROBE_IMAGE}
        imagePullPolicy: Never
        command:
        - /bin/bash
        - -ec
        - |
          while [[ ! -f /state/ready ]]; do
            if [[ -f /state/client-exited ]]; then
              status=\$(cat /state/client-exited)
              echo "client exited before fault injection: status=\$status" >&2
              exit 1
            fi
            sleep 0.1
          done
          old_uid='${victim_uid}'
          kubectl -n '${NAMESPACE}' delete pod '${VICTIM_POD}' --wait=false
          printf 'ok\n' >/state/deleted
          for _ in \$(seq 1 180); do
            new_uid=\$(kubectl -n '${NAMESPACE}' get pod '${VICTIM_POD}' -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
            ready=\$(kubectl -n '${NAMESPACE}' get pod '${VICTIM_POD}' -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)
            if [[ -n "\$new_uid" && "\$new_uid" != "\$old_uid" && "\$ready" == true ]]; then
              printf 'ok\n' >/state/replaced
              exit 0
            fi
            sleep 1
          done
          echo 'replacement Pod did not become Ready with a new UID' >&2
          exit 1
        volumeMounts:
        - name: state
          mountPath: /state
      volumes:
      - name: state
        emptyDir: {}
EOF

deadline=$((SECONDS + JOB_TIMEOUT_SECONDS))
job_result=""
while (( SECONDS < deadline )); do
  job_result="$(kubectl -n "$NAMESPACE" get job "$JOB_NAME" -o json | jq -r '
    if any(.status.conditions[]?; .type == "Complete" and .status == "True") then "complete"
    elif any(.status.conditions[]?; .type == "Failed" and .status == "True") then "failed"
    else "running" end')"
  [[ "$job_result" == running ]] || break
  sleep 1
done
if [[ "$job_result" != complete ]]; then
  kubectl -n "$NAMESPACE" describe "job/${JOB_NAME}" >&2 || true
  kubectl -n "$NAMESPACE" logs "job/${JOB_NAME}" --all-containers --tail=300 >&2 || true
  exit 1
fi
kubectl -n "$NAMESPACE" logs "job/${JOB_NAME}" -c client
new_uid="$(kubectl -n "$NAMESPACE" get pod "$VICTIM_POD" -o jsonpath='{.metadata.uid}')"
[[ -n "$new_uid" && "$new_uid" != "$victim_uid" ]] ||
  { echo "victim Pod UID did not change" >&2; exit 1; }
kubectl -n "$NAMESPACE" rollout status "$WORKLOAD" --timeout=120s
echo "In-cluster balancer smoke completed: victim=${VICTIM_POD} old_uid=${victim_uid} new_uid=${new_uid} node_os=${probe_goos} node_arch=${probe_goarch} probe_image=${PROBE_IMAGE}"
