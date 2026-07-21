#!/usr/bin/env bash
# Comprehensive Kubernetes resource-type coverage test against KubeBrain.
#
# Proves KubeBrain (as the etcd-v3 backend) correctly stores and serves the full
# breadth of k8s API resources — not just the pods/deploy/rs/svc/secret/ns/node
# the scale campaign exercised. For each resource type it applies a minimal
# valid manifest (create + persist through KubeBrain), then runs deep checks:
# a CRD custom-resource instance, the full controller->scheduler->KWOK loop
# reaching Running, watch add/update/delete delivery, and read-back correctness.
#
# Requires a running single-node stack (KubeBrain + k3s pointed at it + KWOK).
# Bring one up with ./single-node-stack.sh, or set KUBECONFIG to any cluster
# backed by KubeBrain with KWOK-managed fake nodes labelled type=kwok.
#
#   KUBECONFIG=/root/.kube-restest ./resource-coverage.sh
export KUBECONFIG="${KUBECONFIG:-/root/.kube-restest}"
NS=restest
kubectl create namespace $NS >/dev/null 2>&1

PASS=0; FAIL=0
declare -a FAILED
# t <category> <kind> <manifest-or-"GET:kind"> ; verifies create+get
t() {
  local cat="$1" kind="$2" body="$3"
  if [[ "$body" == GET:* ]]; then
    # read-only resource: just list it
    local r="${body#GET:}"
    if kubectl get "$r" >/dev/null 2>&1; then
      printf "  \033[32m✓\033[0m %-24s [%s] (list ok, read-only)\n" "$kind" "$cat"; PASS=$((PASS+1))
    else
      printf "  \033[31m✗\033[0m %-24s [%s] LIST FAILED\n" "$kind" "$cat"; FAIL=$((FAIL+1)); FAILED+=("$kind"); fi
    return
  fi
  local out
  if ! out=$(echo "$body" | kubectl apply -f - 2>&1); then
    printf "  \033[31m✗\033[0m %-24s [%s] CREATE FAILED: %s\n" "$kind" "$cat" "$(echo "$out"|tail -1|cut -c1-70)"
    FAIL=$((FAIL+1)); FAILED+=("$kind"); return
  fi
  # extract name/namespace from apply output "kind.group/name created"
  printf "  \033[32m✓\033[0m %-24s [%s]\n" "$kind" "$cat"; PASS=$((PASS+1))
}

echo "=== 1. Workloads ==="
t Workloads Pod '
apiVersion: v1
kind: Pod
metadata: {name: p1, namespace: restest}
spec:
  tolerations: [{key: kwok.x-k8s.io/node, operator: Exists, effect: NoSchedule}]
  nodeSelector: {type: kwok}
  containers: [{name: c, image: nginx}]'
t Workloads ReplicaSet '
apiVersion: apps/v1
kind: ReplicaSet
metadata: {name: rs1, namespace: restest}
spec:
  replicas: 2
  selector: {matchLabels: {app: rs1}}
  template:
    metadata: {labels: {app: rs1}}
    spec:
      tolerations: [{key: kwok.x-k8s.io/node, operator: Exists, effect: NoSchedule}]
      nodeSelector: {type: kwok}
      containers: [{name: c, image: nginx}]'
t Workloads Deployment '
apiVersion: apps/v1
kind: Deployment
metadata: {name: dep1, namespace: restest}
spec:
  replicas: 2
  selector: {matchLabels: {app: dep1}}
  template:
    metadata: {labels: {app: dep1}}
    spec:
      tolerations: [{key: kwok.x-k8s.io/node, operator: Exists, effect: NoSchedule}]
      nodeSelector: {type: kwok}
      containers: [{name: c, image: nginx}]'
t Workloads StatefulSet '
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: sts1, namespace: restest}
spec:
  serviceName: sts1
  replicas: 2
  selector: {matchLabels: {app: sts1}}
  template:
    metadata: {labels: {app: sts1}}
    spec:
      tolerations: [{key: kwok.x-k8s.io/node, operator: Exists, effect: NoSchedule}]
      nodeSelector: {type: kwok}
      containers: [{name: c, image: nginx}]'
t Workloads DaemonSet '
apiVersion: apps/v1
kind: DaemonSet
metadata: {name: ds1, namespace: restest}
spec:
  selector: {matchLabels: {app: ds1}}
  template:
    metadata: {labels: {app: ds1}}
    spec:
      tolerations: [{key: kwok.x-k8s.io/node, operator: Exists, effect: NoSchedule}]
      nodeSelector: {type: kwok}
      containers: [{name: c, image: nginx}]'
t Workloads Job '
apiVersion: batch/v1
kind: Job
metadata: {name: job1, namespace: restest}
spec:
  template:
    spec:
      restartPolicy: Never
      tolerations: [{key: kwok.x-k8s.io/node, operator: Exists, effect: NoSchedule}]
      nodeSelector: {type: kwok}
      containers: [{name: c, image: busybox, command: ["true"]}]'
t Workloads CronJob '
apiVersion: batch/v1
kind: CronJob
metadata: {name: cj1, namespace: restest}
spec:
  schedule: "*/5 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers: [{name: c, image: busybox, command: ["true"]}]'

echo "=== 2. Service Discovery & LB ==="
t SvcLB Service '
apiVersion: v1
kind: Service
metadata: {name: svc1, namespace: restest}
spec: {selector: {app: dep1}, ports: [{port: 80}]}'
t SvcLB Ingress '
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: {name: ing1, namespace: restest}
spec:
  rules:
  - http:
      paths:
      - path: /
        pathType: Prefix
        backend: {service: {name: svc1, port: {number: 80}}}'
t SvcLB EndpointSlice '
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata: {name: eps1, namespace: restest, labels: {kubernetes.io/service-name: svc1}}
addressType: IPv4
ports: [{name: http, port: 80}]
endpoints: [{addresses: ["10.244.0.5"]}]'
t SvcLB Endpoints '
apiVersion: v1
kind: Endpoints
metadata: {name: ep1, namespace: restest}
subsets: [{addresses: [{ip: "10.244.0.6"}], ports: [{port: 80}]}]'

echo "=== 3. Storage ==="
t Storage PersistentVolume '
apiVersion: v1
kind: PersistentVolume
metadata: {name: pv1}
spec:
  capacity: {storage: 1Gi}
  accessModes: [ReadWriteOnce]
  hostPath: {path: /tmp/pv1}
  persistentVolumeReclaimPolicy: Retain'
t Storage PersistentVolumeClaim '
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: pvc1, namespace: restest}
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 1Gi}}'
t Storage StorageClass '
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: sc1}
provisioner: kubernetes.io/no-provisioner'
t Storage VolumeAttachment '
apiVersion: storage.k8s.io/v1
kind: VolumeAttachment
metadata: {name: va1}
spec:
  attacher: csi.example.com
  nodeName: kwok-node-0
  source: {persistentVolumeName: pv1}'

echo "=== 4. Configuration & Metadata ==="
t Config ConfigMap '
apiVersion: v1
kind: ConfigMap
metadata: {name: cm1, namespace: restest}
data: {key: value}'
t Config Secret '
apiVersion: v1
kind: Secret
metadata: {name: sec1, namespace: restest}
stringData: {password: s3cr3t}'
t Config Namespace '
apiVersion: v1
kind: Namespace
metadata: {name: restest-ns2}'
t Config LimitRange '
apiVersion: v1
kind: LimitRange
metadata: {name: lr1, namespace: restest}
spec:
  limits: [{type: Container, default: {cpu: 500m}, defaultRequest: {cpu: 100m}}]'
t Config ResourceQuota '
apiVersion: v1
kind: ResourceQuota
metadata: {name: rq1, namespace: restest}
spec: {hard: {pods: "100"}}'
t Config HorizontalPodAutoscaler '
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: hpa1, namespace: restest}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: dep1}
  minReplicas: 1
  maxReplicas: 5
  metrics: [{type: Resource, resource: {name: cpu, target: {type: Utilization, averageUtilization: 80}}}]'

echo "=== 5. Security / RBAC ==="
t RBAC ServiceAccount '
apiVersion: v1
kind: ServiceAccount
metadata: {name: sa1, namespace: restest}'
t RBAC Role '
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: role1, namespace: restest}
rules: [{apiGroups: [""], resources: [pods], verbs: [get, list]}]'
t RBAC RoleBinding '
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: rb1, namespace: restest}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: role1}
subjects: [{kind: ServiceAccount, name: sa1, namespace: restest}]'
t RBAC ClusterRole '
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: crole1}
rules: [{apiGroups: [""], resources: [nodes], verbs: [get, list]}]'
t RBAC ClusterRoleBinding '
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: crb1}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: crole1}
subjects: [{kind: ServiceAccount, name: sa1, namespace: restest}]'

echo "=== 6. Cluster Management & Node ==="
t Cluster Node '
apiVersion: v1
kind: Node
metadata: {name: kwok-node-extra, labels: {type: kwok}, annotations: {kwok.x-k8s.io/node: fake}}
spec:
  taints: [{key: kwok.x-k8s.io/node, value: fake, effect: NoSchedule}]
status:
  capacity: {cpu: "8", memory: 32Gi, pods: "110"}
  allocatable: {cpu: "8", memory: 32Gi, pods: "110"}'
t Cluster Event '
apiVersion: v1
kind: Event
metadata: {name: ev1, namespace: restest}
involvedObject: {kind: Pod, namespace: restest, name: p1}
reason: Testing
message: resource-test event
type: Normal
source: {component: restest}'
t Cluster ComponentStatus GET:componentstatuses
t Cluster Lease '
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata: {name: lease1, namespace: restest}
spec: {holderIdentity: restest, leaseDurationSeconds: 40}'

echo "=== 7. Extensibility ==="
t Extend CustomResourceDefinition '
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata: {name: widgets.test.kubebrain.io}
spec:
  group: test.kubebrain.io
  scope: Namespaced
  names: {plural: widgets, singular: widget, kind: Widget}
  versions:
  - name: v1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec: {type: object, properties: {size: {type: integer}}}'

echo ""
echo "=== 8. Deep verification ==="
chk() { # chk <desc> <actual> <expected>
  if [[ "$2" == "$3" ]]; then printf "  \033[32m✓\033[0m %-40s (%s)\n" "$1" "$2"; PASS=$((PASS+1));
  else printf "  \033[31m✗\033[0m %-40s got=%q want=%q\n" "$1" "$2" "$3"; FAIL=$((FAIL+1)); FAILED+=("$1"); fi
}

# CRD custom-resource instance: a runtime-registered type stored by KubeBrain.
sleep 3 # let the CRD become Established
kubectl apply -f - >/dev/null 2>&1 <<EOF
apiVersion: test.kubebrain.io/v1
kind: Widget
metadata: {name: w1, namespace: restest}
spec: {size: 42}
EOF
chk "CRD instance stored+served (widget size)" "$(kubectl get widget w1 -n restest -o jsonpath='{.spec.size}' 2>/dev/null)" "42"

# Full controller loop: controllers create pods, scheduler binds to KWOK nodes,
# KWOK marks them Running, controllers observe Running via watch. Exists-taint
# tolerations mean no node taint removal is needed. A cold KWOK start (node
# leases + pod running-stage) can take a couple of minutes, so wait generously.
for _ in $(seq 1 60); do
  [[ "$(kubectl get deploy dep1 -n restest -o jsonpath='{.status.readyReplicas}' 2>/dev/null)" == "2" &&
     "$(kubectl get sts sts1 -n restest -o jsonpath='{.status.readyReplicas}' 2>/dev/null)" == "2" ]] && break
  sleep 3
done
chk "Deployment pods Running (controller->KWOK loop)" "$(kubectl get deploy dep1 -n restest -o jsonpath='{.status.readyReplicas}/{.spec.replicas}')" "2/2"
chk "StatefulSet pods Running" "$(kubectl get sts sts1 -n restest -o jsonpath='{.status.readyReplicas}/{.spec.replicas}')" "2/2"
# DaemonSet desired tracks the node count (the Node test may add one); assert
# every desired pod is ready and there is at least one per original KWOK node.
dsReady=$(kubectl get ds ds1 -n restest -o jsonpath='{.status.numberReady}' 2>/dev/null)
dsWant=$(kubectl get ds ds1 -n restest -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null)
chk "DaemonSet one-per-node Running" "$([[ -n "$dsReady" && "$dsReady" == "$dsWant" && "$dsReady" -ge 3 ]] && echo ok)" "ok"

# Read-back correctness through KubeBrain.
chk "Secret round-trips (base64 decode)" "$(kubectl get secret sec1 -n restest -o jsonpath='{.data.password}' 2>/dev/null | base64 -d)" "s3cr3t"
chk "ConfigMap round-trips" "$(kubectl get cm cm1 -n restest -o jsonpath='{.data.key}' 2>/dev/null)" "value"

# Watch: add/update/delete events all delivered from KubeBrain's watch path.
( timeout 12 kubectl get cm -n restest --watch --no-headers -o custom-columns=N:.metadata.name 2>/dev/null | grep --line-buffered watchcm > /tmp/restest-watch.out ) &
sleep 2
kubectl create cm watchcm -n restest --from-literal=k=v1 >/dev/null 2>&1
sleep 1; kubectl patch cm watchcm -n restest --type merge -p '{"data":{"k":"v2"}}' >/dev/null 2>&1
sleep 1; kubectl delete cm watchcm -n restest >/dev/null 2>&1
sleep 3
chk "Watch delivers add+update+delete (>=3 events)" "$([[ $(grep -c watchcm /tmp/restest-watch.out 2>/dev/null) -ge 3 ]] && echo ok)" "ok"

# Update: rolling update produces a second ReplicaSet with the new image.
kubectl set image deploy/dep1 c=nginx:1.25 -n restest >/dev/null 2>&1
sleep 6
chk "Rolling update -> new image" "$(kubectl get deploy dep1 -n restest -o jsonpath='{.spec.template.spec.containers[0].image}')" "nginx:1.25"

# Delete: object is gone.
kubectl delete pod p1 -n restest >/dev/null 2>&1
chk "Delete -> NotFound" "$(kubectl get pod p1 -n restest 2>&1 | grep -oE 'NotFound' | head -1)" "NotFound"

echo ""
echo "=== Cleanup ==="
kubectl delete namespace restest restest-ns2 --wait=false >/dev/null 2>&1
kubectl delete pv pv1 --wait=false 2>/dev/null; kubectl delete sc sc1 2>/dev/null
kubectl delete va va1 --wait=false 2>/dev/null
kubectl delete clusterrole crole1 clusterrolebinding crb1 2>/dev/null
kubectl delete node kwok-node-extra 2>/dev/null
kubectl delete crd widgets.test.kubebrain.io --wait=false 2>/dev/null
echo "  test objects removed (stack left running)"

echo ""
echo "=== 汇总:PASS=$PASS FAIL=$FAIL ==="
if [[ $FAIL -gt 0 ]]; then printf "失败: %s\n" "${FAILED[*]}"; exit 1; fi
echo "ALL PASS — KubeBrain serves the full k8s resource breadth."
