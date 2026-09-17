#!/usr/bin/env bash
# Sourced only by the admitted disposable control-plane harness.
kwok_client() { kubectl --kubeconfig="$work/kwok.conf" --context=isolated --request-timeout=5s "$@"; }
kwok_permission() {
  local expected=$1 name=$2 status=0; shift 2
  kwok_client auth can-i "$@" > "$work/kwok-rbac-$name.txt" || status=$?
  if [[ $expected == yes ]]; then
    [[ $status == 0 && $(<"$work/kwok-rbac-$name.txt") == yes ]]
  else
    [[ $status == 1 && $(<"$work/kwok-rbac-$name.txt") == no ]]
  fi
}
controlplane_kwok_start() {
  mkdir -m 0700 "$work/kwok-workdir"
  kctl create -f "$root/hack/scale-lab/config/controlplane-kwok-rbac.json" > "$work/kwok-rbac-created.txt"
  kwok_client auth whoami -o json > "$work/kwok.identity.json"
  jq -e '.status.userInfo.username=="kubebrain-test-kwok" and ((.status.userInfo.groups|index("system:masters"))==null)' "$work/kwok.identity.json" >/dev/null
  kwok_permission yes node-status patch nodes/reference-node --subresource=status
  kwok_permission yes pod-status patch pods --subresource=status -n controlplane-smoke
  kwok_permission yes node-lease update leases.coordination.k8s.io/reference-node -n kube-node-lease
  kwok_permission no other-node patch nodes/other-node --subresource=status
  kwok_permission no other-namespace patch pods --subresource=status -n default
  kwok_permission no deployment create deployments.apps -n controlplane-smoke
  kwok_permission no pod-create create pods -n controlplane-smoke
  kwok_permission no binding create pods --subresource=binding -n controlplane-smoke
  kwok_permission no secret get secrets -n controlplane-smoke
  kwok_permission no role create clusterroles.rbac.authorization.k8s.io
  kwok_permission no lease-create create leases.coordination.k8s.io -n kube-node-lease
  # Create only the test fixture; KWOK must acquire and renew it itself.
  jq -n --slurpfile node "$work/node.json" '{apiVersion:"coordination.k8s.io/v1",kind:"Lease",metadata:{name:"reference-node",namespace:"kube-node-lease",ownerReferences:[{apiVersion:"v1",kind:"Node",name:"reference-node",uid:$node[0].metadata.uid}]},spec:{}}' | kctl create -f - -o json > "$work/node-lease-created.json"
  start kwok env KWOK_WORKDIR="$work/kwok-workdir" "$KWOK_BIN" \
    --config="$root/hack/scale-lab/config/controlplane-kwok-stages.yaml" \
    --kubeconfig="$work/kwok.conf" --manage-all-nodes=false --manage-single-node=reference-node \
    --node-lease-duration-seconds=40 --server-address= --enable-crds=
  deadline=$((SECONDS+60))
  until (
    kctl get node reference-node -o json > "$work/node-ready.json" &&
    jq -e 'any(.status.conditions[]?;.type=="Ready" and .status=="True") and all(.spec.taints[]?;.effect!="NoSchedule" and .effect!="NoExecute")' "$work/node-ready.json" >/dev/null &&
    kctl -n kube-node-lease get lease reference-node -o json > "$work/node-lease-first.json" &&
    jq -e --slurpfile created "$work/node-lease-created.json" '.metadata.uid==$created[0].metadata.uid and .spec.leaseDurationSeconds==40 and (.spec.holderIdentity|type)=="string" and (.spec.holderIdentity|length)>0 and (.spec.renewTime|type)=="string"' "$work/node-lease-first.json" >/dev/null
  ); do alive; ((SECONDS < deadline)); sleep 1; done
}
controlplane_kwok_verify() {
  deadline=$((SECONDS+60))
  until (
    kctl -n controlplane-smoke get deployment chain -o json > "$work/kwok-deployment.json" &&
    kctl -n controlplane-smoke get pods -o json > "$work/kwok-pods.json" &&
    kctl -n kube-node-lease get lease reference-node -o json > "$work/node-lease-last.json" &&
    jq -e --slurpfile created "$work/deployment-created.json" '.metadata.uid==$created[0].metadata.uid and .status.observedGeneration==.metadata.generation and .status.availableReplicas==3 and .status.readyReplicas==3' "$work/kwok-deployment.json" >/dev/null &&
    jq -e --slurpfile before "$work/pods.json" '(.items|length)==3 and ([.items[].metadata.uid]|sort)==([$before[0].items[].metadata.uid]|sort) and all(.items[];.spec.nodeName=="reference-node" and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True"))' "$work/kwok-pods.json" >/dev/null &&
    jq -e --slurpfile first "$work/node-lease-first.json" '.metadata.uid==$first[0].metadata.uid and .spec.holderIdentity==$first[0].spec.holderIdentity and .spec.leaseDurationSeconds==40 and .spec.renewTime>$first[0].spec.renewTime' "$work/node-lease-last.json" >/dev/null
  ); do alive; ((SECONDS < deadline)); sleep 1; done
  kctl get node reference-node -o json > "$work/kwok-node.json"
  jq -e --slurpfile created "$work/node.json" '.metadata.uid==$created[0].metadata.uid and any(.status.conditions[]?;.type=="Ready" and .status=="True")' "$work/kwok-node.json" >/dev/null
  jq -s -e --slurpfile created "$work/node-lease-created.json" --slurpfile node "$work/kwok-node.json" --slurpfile pods "$work/kwok-pods.json" --slurpfile first "$work/node-lease-first.json" --slurpfile last "$work/node-lease-last.json" \
    -f "$root/hack/scale-lab/verify-controlplane-kwok-audit.jq" "$work/audit.jsonl" > "$work/kwok-audit-check.json"
}
