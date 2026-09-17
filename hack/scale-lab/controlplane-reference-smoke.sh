#!/usr/bin/env bash
# Isolated reference-etcd control-plane check. Never uses a supplied kubeconfig,
# shared datastore, systemd, or a cluster-wide process-name kill.
set -euo pipefail
umask 077
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
[[ ${ALLOW_LOCAL_CONTROLPLANE_TEST:-false} == true ]] || { echo 'requires ALLOW_LOCAL_CONTROLPLANE_TEST=true' >&2; exit 2; }
for name in APISERVER_BIN CONTROLLER_MANAGER_BIN SCHEDULER_BIN REFERENCE_ETCD_BIN; do
  value=${!name:-}
  checksum_name=${name}_SHA256
  checksum=${!checksum_name:-}
  [[ $value == /* && -x $value && $checksum =~ ^[a-f0-9]{64}$ ]] || { echo "invalid $name or $checksum_name" >&2; exit 2; }
  actual=$(sha256sum "$value")
  [[ ${actual%% *} == "$checksum" ]] || { echo "$name checksum mismatch" >&2; exit 2; }
done
for tool in jq kubectl curl openssl timeout flock ss; do command -v "$tool" >/dev/null; done
api_port=${API_PORT:-18453}
client_port=${ETCD_CLIENT_PORT:-13579}
peer_port=${ETCD_PEER_PORT:-13580}
for port in "$api_port" "$client_port" "$peer_port"; do
  [[ $port =~ ^[1-9][0-9]{3,4}$ ]] && ((port >= 1024 && port <= 65535 && port != 6443)) || exit 2
done
[[ $api_port != "$client_port" && $api_port != "$peer_port" && $client_port != "$peer_port" ]] || exit 2
[[ ${WORK_PARENT:-} == /* && -d $WORK_PARENT ]] || { echo 'WORK_PARENT must be an existing absolute private directory' >&2; exit 2; }
work=$(mktemp -d "$WORK_PARENT/controlplane-reference.XXXXXXXX")
echo "CONTROLPLANE_EVIDENCE=$work"
sha256sum "${BASH_SOURCE[0]}" "$root/hack/scale-lab/verify-controlplane-audit.jq" > "$work/runner.sha256"
pids=()
finish() {
  result=$?
  trap - EXIT
  # Children are timeout supervisors owned by this shell, with bounded kill-after.
  for ((index=${#pids[@]}-1; index>=0; index--)); do
    pid=${pids[index]}
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  printf '%s\n' "$result" > "$work/result.exit"
  exit "$result"
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
lock_fds=()
for port in "$api_port" "$client_port" "$peer_port"; do
  # Cooperating instances share a host lock; actual listener checks also catch
  # unrelated processes. A later bind race must fail startup, never kill its owner.
  exec {fd}>"$WORK_PARENT/controlplane-port-$port.lock"
  flock -n "$fd" || { echo "port $port reserved" >&2; exit 1; }
  lock_fds+=("$fd")
  [[ -z $(ss -H -ltn "sport = :$port") ]] || { echo "port $port already listening" >&2; exit 1; }
done
bash "$root/hack/etcd-client-compat/verify-reference-etcd-provenance.sh" > "$work/etcd-provenance.log"
for name in APISERVER_BIN CONTROLLER_MANAGER_BIN SCHEDULER_BIN; do
  "${!name}" --version > "$work/$name.version"
  [[ $(<"$work/$name.version") == 'Kubernetes v1.36.1' ]] || exit 2
done
bash "$root/hack/dev/create-apiserver-test-pki.sh" "$work/pki" --controlplane
for identity in admin controller-manager scheduler; do
  jq -n --arg server "https://127.0.0.1:$api_port" --arg pki "$work/pki" --arg identity "$identity" '
    {apiVersion:"v1",kind:"Config",clusters:[{name:"isolated",cluster:{server:$server,"certificate-authority":($pki+"/ca.crt")}}],
     users:[{name:$identity,user:{"client-certificate":($pki+"/"+$identity+".crt"),"client-key":($pki+"/"+$identity+".key")}}],
     contexts:[{name:"isolated",context:{cluster:"isolated",user:$identity}}],"current-context":"isolated"}' > "$work/$identity.conf"
done
start() {
  local name=$1; shift
  timeout --signal=TERM --kill-after=5s 300s "$@" > "$work/$name.log" 2>&1 &
  pids+=("$!")
}
alive() { for pid in "${pids[@]}"; do kill -0 "$pid" || return 1; done; }
start etcd "$REFERENCE_ETCD_BIN" --name=reference --data-dir="$work/etcd-data" \
  --listen-client-urls="http://127.0.0.1:$client_port" --advertise-client-urls="http://127.0.0.1:$client_port" \
  --listen-peer-urls="http://127.0.0.1:$peer_port" --initial-advertise-peer-urls="http://127.0.0.1:$peer_port" \
  --initial-cluster="reference=http://127.0.0.1:$peer_port" --initial-cluster-token="${work##*/}"
deadline=$((SECONDS+30))
until curl --fail --silent --max-time 2 "http://127.0.0.1:$client_port/health" > "$work/etcd-health.json"; do
  alive; ((SECONDS < deadline)); sleep 1
done
jq -n '{apiVersion:"audit.k8s.io/v1",kind:"Policy",omitStages:["RequestReceived"],rules:[
  {level:"RequestResponse",verbs:["create"],namespaces:["controlplane-smoke"],resources:[{group:"apps",resources:["replicasets"]},{group:"",resources:["pods"]}]},
  {level:"Metadata"}]}' > "$work/audit-policy.json"
start apiserver "$APISERVER_BIN" --bind-address=127.0.0.1 --advertise-address=127.0.0.1 \
  --secure-port="$api_port" --authorization-mode=Node,RBAC --anonymous-auth=false \
  --client-ca-file="$work/pki/ca.crt" --tls-cert-file="$work/pki/apiserver.crt" --tls-private-key-file="$work/pki/apiserver.key" \
  --etcd-servers="http://127.0.0.1:$client_port" --etcd-prefix=/registry-isolated-controlplane \
  --endpoint-reconciler-type=none --service-cluster-ip-range=10.97.0.0/16 \
  --service-account-issuer=https://kubernetes.default.svc.cluster.local \
  --service-account-key-file="$work/pki/sa.pub" --service-account-signing-key-file="$work/pki/sa.key" \
  --audit-policy-file="$work/audit-policy.json" --audit-log-path="$work/audit.jsonl" --audit-log-mode=blocking --v=2
kctl() { kubectl --kubeconfig="$work/admin.conf" --context=isolated --request-timeout=5s "$@"; }
deadline=$((SECONDS+90))
until kctl get --raw=/readyz > "$work/readyz" 2> "$work/readyz.err"; do
  alive; ((SECONDS < deadline)); sleep 1
done
# Exercise the component's own certificate; an administrator impersonating it
# would not verify that the new certificate/config is usable.
for identity in controller-manager scheduler; do
  kubectl --kubeconfig="$work/$identity.conf" --context=isolated --request-timeout=5s auth whoami -o json > "$work/$identity.identity.json"
  jq -e --arg identity "system:kube-$identity" '.status.userInfo.username==$identity and ((.status.userInfo.groups|index("system:masters"))==null)' "$work/$identity.identity.json" >/dev/null
  denied=0
  kubectl --kubeconfig="$work/$identity.conf" --context=isolated --request-timeout=5s auth can-i create deployments.apps -n default > "$work/$identity.denied" || denied=$?
  [[ $denied == 1 && $(<"$work/$identity.denied") == no ]]
done
start controller-manager "$CONTROLLER_MANAGER_BIN" --kubeconfig="$work/controller-manager.conf" \
  --bind-address=127.0.0.1 --secure-port=0 --leader-elect=true --use-service-account-credentials=true \
  --controllers=deployment-controller,replicaset-controller,serviceaccount-controller,serviceaccount-token-controller \
  --service-account-private-key-file="$work/pki/sa.key" --root-ca-file="$work/pki/ca.crt" --v=2
start scheduler "$SCHEDULER_BIN" --kubeconfig="$work/scheduler.conf" --bind-address=127.0.0.1 --secure-port=0 --leader-elect=true --v=2
kctl create namespace controlplane-smoke -o json > "$work/namespace.json"
# A fake Ready node exercises the real scheduler only. No kubelet or KWOK runs,
# so this test must not claim containers started or Deployment availability.
jq -n '{apiVersion:"v1",kind:"Node",metadata:{name:"reference-node",labels:{"kubernetes.io/hostname":"reference-node"}},spec:{}}' | kctl create -f - -o json > "$work/node.json"
kctl patch node reference-node --subresource=status --type=merge -p '{"status":{"capacity":{"cpu":"4","memory":"8Gi","pods":"32"},"allocatable":{"cpu":"4","memory":"8Gi","pods":"32"},"conditions":[{"type":"Ready","status":"True","reason":"IsolatedSchedulingFixture","message":"No kubelet; scheduling only"}]}}' > "$work/node-status.log"
# Node admission adds not-ready even though this fixture later patches Ready.
# No node-lifecycle controller is enabled here. Remove only that exact initial
# taint, with UID/RV tests; never broadly clear taints or bind Pods ourselves.
kctl get node reference-node -o json > "$work/node-ready.json"
node_patch=$(jq -ce '
  select(.spec.taints==[{effect:"NoSchedule",key:"node.kubernetes.io/not-ready"}]) |
  [{op:"test",path:"/metadata/uid",value:.metadata.uid},
   {op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},
   {op:"test",path:"/spec/taints",value:.spec.taints}, {op:"remove",path:"/spec/taints"}]' "$work/node-ready.json")
kctl patch node reference-node --type=json -p "$node_patch" -o json > "$work/node-schedulable.json"
jq -n '{apiVersion:"apps/v1",kind:"Deployment",metadata:{name:"chain",namespace:"controlplane-smoke"},spec:{replicas:3,selector:{matchLabels:{app:"chain"}},template:{metadata:{labels:{app:"chain"}},spec:{automountServiceAccountToken:false,containers:[{name:"pause",image:"registry.k8s.io/pause:3.10",resources:{requests:{cpu:"10m",memory:"8Mi"}}}]}}}}' | kctl create -f - -o json > "$work/deployment-created.json"
deadline=$((SECONDS+120))
until (
  kctl -n controlplane-smoke get deployment chain -o json > "$work/deployment.json" &&
  kctl -n controlplane-smoke get replicasets -o json > "$work/replicasets.json" &&
  kctl -n controlplane-smoke get pods -o json > "$work/pods.json" &&
  jq -e -n --slurpfile created "$work/deployment-created.json" --slurpfile deployment "$work/deployment.json" --slurpfile rs "$work/replicasets.json" --slurpfile pods "$work/pods.json" '
    $created[0].metadata.uid as $duid |
    $deployment[0].metadata.uid==$duid and $deployment[0].status.observedGeneration==$deployment[0].metadata.generation and
    ($rs[0].items|length)==1 and ($rs[0].items[0].spec.replicas==3) and
    ([$rs[0].items[0].metadata.ownerReferences[]|select(.controller==true and .uid==$duid and .kind=="Deployment")]|length)==1 and
    ($rs[0].items[0].metadata.uid as $ruid | ($pods[0].items|length)==3 and
      ([$pods[0].items[].metadata.uid]|unique|length)==3 and
      all($pods[0].items[]; .metadata.deletionTimestamp==null and .spec.nodeName=="reference-node" and
        ([.metadata.ownerReferences[]|select(.controller==true and .uid==$ruid and .kind=="ReplicaSet")]|length)==1 and
        any(.status.conditions[]?; .type=="PodScheduled" and .status=="True")))' > "$work/chain-check.json"
); do
  alive; ((SECONDS < deadline)); sleep 1
done
alive
kctl -n kube-system get leases kube-controller-manager kube-scheduler -o json > "$work/component-leases.json"
jq -e '.items|length==2 and all(.[]; (.spec.holderIdentity|type)=="string" and (.spec.holderIdentity|length)>0)' "$work/component-leases.json" >/dev/null
kctl -n controlplane-smoke get events -o json > "$work/events.json"
# Bodies are captured only for fixture RS/Pod creates, never token responses.
jq -s -e --slurpfile rs "$work/replicasets.json" --slurpfile pods "$work/pods.json" \
  -f "$root/hack/scale-lab/verify-controlplane-audit.jq" "$work/audit.jsonl" > "$work/audit-check.json"
echo 'REFERENCE_CONTROLPLANE_SCHEDULING_PASS: real controllers and scheduler; fake node, no Running/HA/KubeBrain claim'
