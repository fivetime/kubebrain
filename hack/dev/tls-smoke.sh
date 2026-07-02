#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-tls-smoke}"
IMAGE_NAME="${IMAGE_NAME:-kubebrain:dev}"
PD_ADDRS="${PD_ADDRS:-kb-pd.tidb-cluster.svc:2379}"
LOCAL_PORT="${LOCAL_PORT:-12379}"
REPLICAS="${REPLICAS:-3}"
RUN_APISERVER_SMOKE="${RUN_APISERVER_SMOKE:-true}"
RUN_BACKUP_DRILL="${RUN_BACKUP_DRILL:-true}"
BACKUP_PREFIX="${BACKUP_PREFIX:-/registry/tls-smoke}"
BACKUP_BATCH_SIZE="${BACKUP_BATCH_SIZE:-100}"
APISERVER_SECURE_PORT="${APISERVER_SECURE_PORT:-16444}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need kubectl
need openssl
need go

workdir="$(mktemp -d)"
cleanup() {
  if [ -n "${pf_pid:-}" ]; then
    kill "$pf_pid" >/dev/null 2>&1 || true
  fi
  kubectl delete namespace "$NAMESPACE" --wait=false >/dev/null 2>&1 || true
  rm -rf "$workdir"
}
trap cleanup EXIT

cd "$workdir"

cat > ca.conf <<'EOF'
[req]
distinguished_name = dn
x509_extensions = v3_ca
prompt = no
[dn]
CN = kubebrain-smoke-ca
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout ca.key -out ca.crt -config ca.conf >/dev/null 2>&1

cat > server.conf <<EOF
[req]
distinguished_name = dn
req_extensions = v3_req
prompt = no
[dn]
CN = kubebrain-client.${NAMESPACE}.svc
[v3_req]
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = kubebrain-client.${NAMESPACE}.svc
DNS.2 = kubebrain-peer.${NAMESPACE}.svc
DNS.3 = kubebrain-peer.kubebrain-system.svc
IP.1 = 127.0.0.1
EOF

openssl req -newkey rsa:2048 -nodes \
  -keyout tls.key -out tls.csr -config server.conf >/dev/null 2>&1
openssl x509 -req -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 1 \
  -out tls.crt -extensions v3_req -extfile server.conf >/dev/null 2>&1

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" create secret generic kubebrain-client-tls \
  --from-file=tls.crt=tls.crt \
  --from-file=tls.key=tls.key \
  --from-file=ca.crt=ca.crt >/dev/null
kubectl -n "$NAMESPACE" create secret generic kubebrain-peer-tls \
  --from-file=tls.crt=tls.crt \
  --from-file=tls.key=tls.key \
  --from-file=ca.crt=ca.crt >/dev/null

sed \
  -e "s/namespace: kubebrain-system/namespace: ${NAMESPACE}/g" \
  -e "s/name: kubebrain-system/name: ${NAMESPACE}/g" \
  -e "s/replicas: 3/replicas: ${REPLICAS}/" \
  -e "s#image: kubebrain:dev#image: ${IMAGE_NAME}#" \
  -e "s#--pd-addrs=kb-pd.tidb-cluster.svc:2379#--pd-addrs=${PD_ADDRS}#" \
  -e "s#--peer-tls-server-name=kubebrain-peer.kubebrain-system.svc#--peer-tls-server-name=kubebrain-peer.${NAMESPACE}.svc#" \
  "$ROOT_DIR/deploy/production/kubebrain-tls.yaml" > manifest.yaml

python3 - <<'PY'
from pathlib import Path
p = Path("manifest.yaml")
s = p.read_text()
s = s.replace("            - --compatible-with-etcd=true\n", "            - --compatible-with-etcd=true\n            - --key-prefix=/tls-smoke\n")
p.write_text(s)
PY

kubectl apply -f manifest.yaml >/dev/null

wait_ready() {
  kubectl -n "$NAMESPACE" rollout status deployment/kubebrain --timeout=180s
  kubectl -n "$NAMESPACE" wait --for=condition=ready pod \
    -l app.kubernetes.io/name=kubebrain --timeout=180s >/dev/null
}

start_port_forward() {
  if [ -n "${pf_pid:-}" ]; then
    kill "$pf_pid" >/dev/null 2>&1 || true
    wait "$pf_pid" >/dev/null 2>&1 || true
  fi
  kubectl -n "$NAMESPACE" port-forward service/kubebrain-client "${LOCAL_PORT}:3379" >/tmp/kubebrain-tls-smoke-port-forward.log 2>&1 &
  pf_pid=$!
  sleep 2
}

run_client_smoke() {
  start_port_forward
  LOCAL_PORT="$LOCAL_PORT" go run main.go
}

run_apiserver_smoke() {
  start_port_forward
  ENDPOINT="https://127.0.0.1:${LOCAL_PORT}" \
    ETCD_CAFILE="${workdir}/ca.crt" \
    ETCD_CERTFILE="${workdir}/tls.crt" \
    ETCD_KEYFILE="${workdir}/tls.key" \
    SECURE_PORT="$APISERVER_SECURE_PORT" \
    WORK_DIR="${ROOT_DIR}/.dev/apiserver-tls-smoke" \
    "$ROOT_DIR/hack/dev/apiserver-smoke.sh"
}

run_backup_drill() {
  start_port_forward
  ENDPOINT="https://127.0.0.1:${LOCAL_PORT}" \
    ETCDCTL_CACERT="${workdir}/ca.crt" \
    ETCDCTL_CERT="${workdir}/tls.crt" \
    ETCDCTL_KEY="${workdir}/tls.key" \
    PREFIX="$BACKUP_PREFIX" \
    BATCH_SIZE="$BACKUP_BATCH_SIZE" \
    REQUIRE_RECORDS=true \
    "$ROOT_DIR/hack/backup/logical-drill.sh"
}

wait_ready

go mod init kubebrain-tls-smoke >/dev/null
go get go.etcd.io/etcd/client/v3@v3.5.2 >/dev/null

cat > main.go <<'EOF'
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	ca, err := os.ReadFile("ca.crt")
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		log.Fatal("failed to load ca")
	}
	cert, err := tls.LoadX509KeyPair("tls.crt", "tls.key")
	if err != nil {
		log.Fatal(err)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"https://127.0.0.1:" + os.Getenv("LOCAL_PORT")},
		DialTimeout: 10 * time.Second,
		TLS: &tls.Config{
			RootCAs:      pool,
			Certificates: []tls.Certificate{cert},
			ServerName:   "127.0.0.1",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := "/registry/tls-smoke/key"
	if _, err := cli.Put(ctx, key, "value"); err != nil {
		log.Fatal(err)
	}
	resp, err := cli.Get(ctx, key)
	if err != nil {
		log.Fatal(err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "value" {
		log.Fatalf("unexpected get response: %+v", resp.Kvs)
	}
	fmt.Println("TLS smoke completed")
}
EOF

echo "Running TLS smoke against ${REPLICAS} replica(s)"
run_client_smoke

if [ "$RUN_APISERVER_SMOKE" = "true" ]; then
  echo "Running standalone kube-apiserver smoke through TLS endpoint"
  run_apiserver_smoke
fi

if [ "$RUN_BACKUP_DRILL" = "true" ]; then
  echo "Running logical backup restore drill through TLS endpoint"
  run_backup_drill
fi

if [ "$REPLICAS" -ge 3 ]; then
  mapfile -t pods < <(kubectl -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/name=kubebrain \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')

  for pod in "${pods[@]}"; do
    echo "Deleting pod ${pod} and re-running TLS smoke"
    kubectl -n "$NAMESPACE" delete pod "$pod" --wait=false >/dev/null
    wait_ready
    run_client_smoke
  done
fi

echo "TLS HA smoke completed"
