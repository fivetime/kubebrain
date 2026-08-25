#!/usr/bin/env bash
set -euo pipefail

OPERATION_ID="${OPERATION_ID:-}"; KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"; KEY_SECRET="${KEY_SECRET:-}"

die() { echo "$*" >&2; exit 2; }
dns_id='^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
[[ "$OPERATION_ID" =~ ^jwt-key-rotate-([a-f0-9]{20})$ ]] || die "OPERATION_ID must be a deterministic JWTKeyRotation identity"
suffix="${BASH_REMATCH[1]}"; expected_secret="${OPERATION_ID}-keys"
[[ "$KUBEBRAIN_NAMESPACE" =~ $dns_id && "$KUBEBRAIN_STATEFULSET" =~ $dns_id ]] || die "KubeBrain namespace or StatefulSet identity is invalid"
[[ -z "$KEY_SECRET" || "$KEY_SECRET" == "$expected_secret" ]] || die "KEY_SECRET must equal the operation-bound key Secret"
role="kubebrain-jwt-publisher-${suffix}"

printf '%s\n' "apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${role}
  namespace: ${KUBEBRAIN_NAMESPACE}
  labels:
    app.kubernetes.io/part-of: kubebrain
    dbaas.kubebrain.io/jwt-key-rotation-operation: ${OPERATION_ID}
rules:
  - apiGroups: [apps]
    resources: [statefulsets]
    resourceNames: [${KUBEBRAIN_STATEFULSET}]
    verbs: [get, patch]
  - apiGroups: [\"\"]
    resources: [secrets]
    resourceNames: [${expected_secret}]
    verbs: [get]
  - apiGroups: [\"\"]
    resources: [secrets]
    verbs: [create]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ${role}
  namespace: ${KUBEBRAIN_NAMESPACE}
  labels:
    app.kubernetes.io/part-of: kubebrain
    dbaas.kubebrain.io/jwt-key-rotation-operation: ${OPERATION_ID}
subjects:
  - kind: ServiceAccount
    name: kubebrain-jwt-key-rotation-executor
    namespace: kubebrain-operations
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ${role}"
