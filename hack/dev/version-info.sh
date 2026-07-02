#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dev}"
NODE_NAME="${NODE_NAME:-${CLUSTER_NAME}-control-plane}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need docker
need kind
need kubectl

echo "kind:"
kind version

echo
echo "kind clusters:"
kind get clusters

echo
echo "kind node image:"
if docker inspect "$NODE_NAME" >/dev/null 2>&1; then
  docker inspect "$NODE_NAME" --format '{{.Config.Image}}'
else
  echo "node container ${NODE_NAME} not found"
fi

echo
echo "kubernetes:"
kubectl version -o yaml
