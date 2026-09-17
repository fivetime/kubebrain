#!/usr/bin/env bash
# Sourced by the disposable control-plane harness, never a management kubeconfig.
controlplane_replace_pod() {
  jq -e '.items|length==3' "$work/kwok-pods.json" >/dev/null
  jq '.items|sort_by(.metadata.name)|first' "$work/kwok-pods.json" > "$work/replacement-victim.json"
  local name
  name=$(jq -er '.metadata.name|select(test("^[a-z0-9][a-z0-9-]*$"))' "$work/replacement-victim.json")
  jq '{apiVersion:"v1",kind:"DeleteOptions",gracePeriodSeconds:0,
    preconditions:{uid:.metadata.uid,resourceVersion:.metadata.resourceVersion}}' \
    "$work/replacement-victim.json" > "$work/replacement-delete-options.json"
  # No kubelet exists here. Remove only this exact disposable UID/RV; a
  # concurrent change must fail, not silently select a different object.
  curl --fail-with-body --silent --show-error --max-time 10 \
    --cacert "$work/pki/ca.crt" --cert "$work/pki/admin.crt" --key "$work/pki/admin.key" \
    -X DELETE -H 'Content-Type: application/json' --data-binary "@$work/replacement-delete-options.json" \
    "https://127.0.0.1:$api_port/api/v1/namespaces/controlplane-smoke/pods/$name" > "$work/replacement-delete-response.json"
  local deadline=$((SECONDS+60))
  until (
    kctl -n controlplane-smoke get deployment chain -o json > "$work/replacement-deployment.json" &&
    kctl -n controlplane-smoke get replicasets -o json > "$work/replacement-replicasets.json" &&
    kctl -n controlplane-smoke get pods -o json > "$work/replacement-pods.json" &&
    jq -n -e --slurpfile before "$work/kwok-pods.json" --slurpfile victim "$work/replacement-victim.json" \
      --slurpfile pods "$work/replacement-pods.json" --slurpfile created "$work/deployment-created.json" \
      --slurpfile deployment "$work/replacement-deployment.json" --slurpfile originalRS "$work/replicasets.json" \
      --slurpfile rs "$work/replacement-replicasets.json" \
      -f "$root/hack/scale-lab/verify-controlplane-replacement.jq" > "$work/replacement-state-check.json"
  ); do alive; ((SECONDS < deadline)); sleep 1; done
  alive
  jq -s -e --slurpfile before "$work/kwok-pods.json" --slurpfile victim "$work/replacement-victim.json" \
    --slurpfile pods "$work/replacement-pods.json" \
    -f "$root/hack/scale-lab/verify-controlplane-replacement-audit.jq" "$work/audit.jsonl" > "$work/replacement-audit-check.json"
}
