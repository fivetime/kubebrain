#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-tls-smoke}"
IMAGE_NAME="${IMAGE_NAME:-kubebrain:dev}"
PD_ADDRS="${PD_ADDRS:-kb-pd.tidb-cluster.svc:2379}"
LOCAL_PORT="${LOCAL_PORT:-12379}"
SOAK_LOCAL_PORT="${SOAK_LOCAL_PORT:-22379}"
REPLICAS="${REPLICAS:-3}"
RUN_APISERVER_SMOKE="${RUN_APISERVER_SMOKE:-true}"
RUN_BACKUP_DRILL="${RUN_BACKUP_DRILL:-true}"
RUN_AUTH_CERT_SMOKE="${RUN_AUTH_CERT_SMOKE:-true}"
RUN_CERT_ROTATION_SMOKE="${RUN_CERT_ROTATION_SMOKE:-true}"
GRPC_MAX_CONNECTION_AGE="${GRPC_MAX_CONNECTION_AGE:-5s}"
GRPC_MAX_CONNECTION_AGE_GRACE="${GRPC_MAX_CONNECTION_AGE_GRACE:-2s}"
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
  if [ -n "${soak_pf_pid:-}" ]; then
    kill "$soak_pf_pid" >/dev/null 2>&1 || true
  fi
  if [ -n "${soak_pid:-}" ]; then
    kill "$soak_pid" >/dev/null 2>&1 || true
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

cat > auth-client.conf <<'EOF'
[req]
distinguished_name = dn
req_extensions = v3_req
prompt = no
[dn]
CN = cert-root
[v3_req]
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = clientAuth
EOF

openssl req -newkey rsa:2048 -nodes \
  -keyout auth-client.key -out auth-client.csr -config auth-client.conf >/dev/null 2>&1
openssl x509 -req -in auth-client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 1 \
  -out auth-client.crt -extensions v3_req -extfile auth-client.conf >/dev/null 2>&1

cp ca.crt old-ca.crt
cp tls.crt old-tls.crt
cp tls.key old-tls.key

sed 's/kubebrain-smoke-ca/kubebrain-smoke-ca-next/' ca.conf > ca-next.conf
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout ca-next.key -out ca-next.crt -config ca-next.conf >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -keyout tls-next.key -out tls-next.csr -config server.conf >/dev/null 2>&1
openssl x509 -req -in tls-next.csr -CA ca-next.crt -CAkey ca-next.key -CAcreateserial -days 1 \
  -out tls-next.crt -extensions v3_req -extfile server.conf >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -keyout auth-client-next.key -out auth-client-next.csr -config auth-client.conf >/dev/null 2>&1
openssl x509 -req -in auth-client-next.csr -CA ca-next.crt -CAkey ca-next.key -CAcreateserial -days 1 \
  -out auth-client-next.crt -extensions v3_req -extfile auth-client.conf >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -keyout auth-client-good.key -out auth-client-good.csr -config auth-client.conf >/dev/null 2>&1
openssl x509 -req -in auth-client-good.csr -CA ca-next.crt -CAkey ca-next.key -CAcreateserial -days 1 \
  -out auth-client-good.crt -extensions v3_req -extfile auth-client.conf >/dev/null 2>&1
cat old-ca.crt ca-next.crt > ca-overlap.crt

make_crl_config() {
  local name="$1"
  local ca_cert="$2"
  local ca_key="$3"
  : > "index-${name}.txt"
  echo 1000 > "crlnumber-${name}"
  echo 1000 > "serial-${name}"
  mkdir -p "newcerts-${name}"
  cat > "crl-${name}.conf" <<EOF
[ca]
default_ca = CA_default
[CA_default]
database = ${workdir}/index-${name}.txt
new_certs_dir = ${workdir}/newcerts-${name}
serial = ${workdir}/serial-${name}
private_key = ${workdir}/${ca_key}
certificate = ${workdir}/${ca_cert}
default_md = sha256
default_crl_days = 1
crlnumber = ${workdir}/crlnumber-${name}
EOF
}

make_crl_config old ca.crt ca.key
make_crl_config next ca-next.crt ca-next.key
openssl ca -gencrl -config crl-old.conf -out revoked.pem -batch >/dev/null 2>&1
openssl crl -in revoked.pem -outform DER -out revoked.crl
openssl ca -gencrl -config crl-next.conf -out revoked-next.pem -batch >/dev/null 2>&1
openssl crl -in revoked-next.pem -outform DER -out revoked-next.crl

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" create secret generic kubebrain-client-tls \
  --from-file=tls.crt=tls.crt \
  --from-file=tls.key=tls.key \
  --from-file=ca.crt=ca.crt \
  --from-file=revoked.crl=revoked.crl >/dev/null
kubectl -n "$NAMESPACE" create secret generic kubebrain-peer-tls \
  --from-file=tls.crt=tls.crt \
  --from-file=tls.key=tls.key \
  --from-file=ca.crt=ca.crt \
  --from-file=revoked.crl=revoked.crl >/dev/null

sed \
  -e "s/namespace: kubebrain-system/namespace: ${NAMESPACE}/g" \
  -e "s/name: kubebrain-system/name: ${NAMESPACE}/g" \
  -e "s/replicas: 3/replicas: ${REPLICAS}/" \
  -e "s#image: kubebrain:dev#image: ${IMAGE_NAME}#" \
  -e "s#--pd-addrs=kb-pd.tidb-cluster.svc:2379#--pd-addrs=${PD_ADDRS}#" \
  -e "s#--peer-tls-server-name=kubebrain-peer.kubebrain-system.svc#--peer-tls-server-name=kubebrain-peer.${NAMESPACE}.svc#" \
  -e "s#--tls-server-name=kubebrain-client.kubebrain-system.svc#--tls-server-name=kubebrain-client.${NAMESPACE}.svc#" \
  -e "s#--grpc-max-connection-age=1h#--grpc-max-connection-age=${GRPC_MAX_CONNECTION_AGE}#" \
  -e "s#--grpc-max-connection-age-grace=5m#--grpc-max-connection-age-grace=${GRPC_MAX_CONNECTION_AGE_GRACE}#" \
  "$ROOT_DIR/deploy/production/kubebrain-tls.yaml" > manifest.yaml

python3 - <<PY
from pathlib import Path
p = Path("manifest.yaml")
s = p.read_text()
s = s.replace("            - --compatible-with-etcd=true\n", "            - --compatible-with-etcd=true\n            - --keyspace=${NAMESPACE}\n")
s = s.replace("            - --trusted-ca-file=/etc/kubebrain/client-tls/ca.crt\n", "            - --trusted-ca-file=/etc/kubebrain/client-tls/ca.crt\n            - --client-crl-file=/etc/kubebrain/client-tls/revoked.crl\n")
s = s.replace("            - --peer-trusted-ca-file=/etc/kubebrain/peer-tls/ca.crt\n", "            - --peer-trusted-ca-file=/etc/kubebrain/peer-tls/ca.crt\n            - --peer-crl-file=/etc/kubebrain/peer-tls/revoked.crl\n")
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
  for _ in $(seq 1 50); do
    if ! kill -0 "$pf_pid" >/dev/null 2>&1; then
      cat /tmp/kubebrain-tls-smoke-port-forward.log >&2
      echo "service port-forward exited before becoming ready" >&2
      exit 1
    fi
    if grep -q "Forwarding from 127.0.0.1:${LOCAL_PORT}" /tmp/kubebrain-tls-smoke-port-forward.log; then
      return
    fi
    sleep 0.2
  done
  cat /tmp/kubebrain-tls-smoke-port-forward.log >&2
  echo "timed out waiting for service port-forward" >&2
  exit 1
}

run_client_smoke() {
  start_port_forward
  LOCAL_PORT="$LOCAL_PORT" go run main.go
}

assert_client_rejected() {
  local pod
  pod="$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=kubebrain \
    -o jsonpath='{.items[0].metadata.name}')"
  if [ -n "${pf_pid:-}" ]; then
    kill "$pf_pid" >/dev/null 2>&1 || true
    wait "$pf_pid" >/dev/null 2>&1 || true
  fi
  : > /tmp/kubebrain-tls-smoke-port-forward.log
  kubectl -n "$NAMESPACE" port-forward "pod/${pod}" "${LOCAL_PORT}:3379" \
    >/tmp/kubebrain-tls-smoke-port-forward.log 2>&1 &
  pf_pid=$!
  for _ in $(seq 1 50); do
    if grep -q "Forwarding from 127.0.0.1:${LOCAL_PORT}" /tmp/kubebrain-tls-smoke-port-forward.log; then
      break
    fi
    sleep 0.2
  done
  if ! grep -q "Forwarding from 127.0.0.1:${LOCAL_PORT}" /tmp/kubebrain-tls-smoke-port-forward.log; then
    cat /tmp/kubebrain-tls-smoke-port-forward.log >&2
    echo "timed out waiting for retired-client port-forward" >&2
    exit 1
  fi
  local since_time
  since_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  local output
  local status
  set +e
  output="$(LOCAL_PORT="$LOCAL_PORT" go run main.go 2>&1)"
  status=$?
  set -e
  if [ "$status" -eq 0 ]; then
    echo "client unexpectedly remained trusted after trust-policy update" >&2
    exit 1
  fi
  local server_logs
  server_logs="$(kubectl -n "$NAMESPACE" logs "$pod" --since-time="$since_time" 2>/dev/null || true)"
  if ! grep -Ei 'rejected TLS connection' <<<"$server_logs" | \
      grep -Eqi "unknown authority|bad certificate|client didn't provide a certificate|certificate serial .* revoked"; then
    echo "client failed without a certificate rejection in server logs:" >&2
    echo "$output" >&2
    cat /tmp/kubebrain-tls-smoke-port-forward.log >&2
    echo "$server_logs" >&2
    exit 1
  fi
  echo "Client certificate rejected by the active trust policy"
}

apply_tls_secrets() {
  local cert_file="$1"
  local key_file="$2"
  local ca_file="$3"
  local crl_file="${4:-revoked.crl}"
  for secret in kubebrain-client-tls kubebrain-peer-tls; do
    kubectl -n "$NAMESPACE" create secret generic "$secret" \
      --from-file="tls.crt=${cert_file}" \
      --from-file="tls.key=${key_file}" \
      --from-file="ca.crt=${ca_file}" \
      --from-file="revoked.crl=${crl_file}" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  done
}

wait_for_projected_file() {
  local source_file="$1"
  local mounted_file="$2"
  local expected_hash
  expected_hash="$(sha256sum "$source_file" | awk '{print $1}')"
  local deadline=$((SECONDS + 150))
  while [ "$SECONDS" -lt "$deadline" ]; do
    local all_current=true
    mapfile -t projected_pods < <(kubectl -n "$NAMESPACE" get pods \
      -l app.kubernetes.io/name=kubebrain \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
    if [ "${#projected_pods[@]}" -ne "$REPLICAS" ]; then
      all_current=false
    fi
    for pod in "${projected_pods[@]}"; do
      actual_hash="$(kubectl -n "$NAMESPACE" exec "$pod" -- sha256sum "$mounted_file" 2>/dev/null | awk '{print $1}' || true)"
      if [ "$actual_hash" != "$expected_hash" ]; then
        all_current=false
        break
      fi
    done
    if [ "$all_current" = "true" ]; then
      return
    fi
    sleep 2
  done
  echo "timed out waiting for ${mounted_file} projection" >&2
  exit 1
}

run_direct_client_smoke() {
  local pod="$1"
  if [ -n "${pf_pid:-}" ]; then
    kill "$pf_pid" >/dev/null 2>&1 || true
    wait "$pf_pid" >/dev/null 2>&1 || true
  fi
  kubectl -n "$NAMESPACE" port-forward "pod/${pod}" "${LOCAL_PORT}:3379" \
    >/tmp/kubebrain-tls-rotation-port-forward.log 2>&1 &
  pf_pid=$!
  sleep 2
  LOCAL_PORT="$LOCAL_PORT" go run main.go
}

start_soak_port_forward() {
  if [ -n "${soak_pf_pid:-}" ]; then
    kill "$soak_pf_pid" >/dev/null 2>&1 || true
    wait "$soak_pf_pid" >/dev/null 2>&1 || true
  fi
  : > /tmp/kubebrain-tls-soak-port-forward.log
  kubectl -n "$NAMESPACE" port-forward service/kubebrain-client "${SOAK_LOCAL_PORT}:3379" \
    >/tmp/kubebrain-tls-soak-port-forward.log 2>&1 &
  soak_pf_pid=$!
  for _ in $(seq 1 50); do
    if ! kill -0 "$soak_pf_pid" >/dev/null 2>&1; then
      cat /tmp/kubebrain-tls-soak-port-forward.log >&2
      echo "soak port-forward exited before becoming ready" >&2
      exit 1
    fi
    if grep -q "Forwarding from 127.0.0.1:${SOAK_LOCAL_PORT}" /tmp/kubebrain-tls-soak-port-forward.log; then
      return
    fi
    sleep 0.2
  done
  echo "timed out waiting for soak port-forward" >&2
  exit 1
}

soak_progress() {
  grep -c '^SOAK_HEALTH ' /tmp/kubebrain-tls-connection-soak.log 2>/dev/null || true
}

wait_for_soak_progress() {
  local previous="$1"
  local deadline=$((SECONDS + 30))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if ! kill -0 "$soak_pid" >/dev/null 2>&1; then
      cat /tmp/kubebrain-tls-connection-soak.log >&2
      echo "long-lived TLS client exited" >&2
      exit 1
    fi
    if [ "$(soak_progress)" -gt "$previous" ]; then
      return
    fi
    sleep 1
  done
  cat /tmp/kubebrain-tls-connection-soak.log >&2
  echo "long-lived TLS client made no progress after reconnect" >&2
  exit 1
}

run_auth_cert_smoke() {
  local bootstrap="${1:-false}"
  LOCAL_PORT="$LOCAL_PORT" BOOTSTRAP_AUTH="$bootstrap" go run auth-main.go
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
	caFile, certFile, keyFile := os.Getenv("CA_FILE"), os.Getenv("CERT_FILE"), os.Getenv("KEY_FILE")
	if caFile == "" { caFile = "ca.crt" }
	if certFile == "" { certFile = "tls.crt" }
	if keyFile == "" { keyFile = "tls.key" }
	ca, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		log.Fatal("failed to load ca")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
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

cat > soak-main.go <<'EOF'
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
	ca, err := os.ReadFile(os.Getenv("CA_FILE"))
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		log.Fatal("failed to load CA")
	}
	cert, err := tls.LoadX509KeyPair(os.Getenv("CERT_FILE"), os.Getenv("KEY_FILE"))
	if err != nil {
		log.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"https://127.0.0.1:" + os.Getenv("LOCAL_PORT")},
		DialTimeout: 10 * time.Second,
		TLS: &tls.Config{
			RootCAs: pool, Certificates: []tls.Certificate{cert}, ServerName: "127.0.0.1",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	lease, err := client.Grant(ctx, 15)
	if err != nil {
		log.Fatal(err)
	}
	keepAlive, err := client.KeepAlive(ctx, lease.ID)
	if err != nil {
		log.Fatal(err)
	}
	const key = "/registry/tls-smoke/connection-soak"
	watch := client.Watch(ctx, key)
	var nextRevision int64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var writes, events, keepAlives int
	for {
		select {
		case response, ok := <-watch:
			if !ok || response.Err() != nil {
				log.Printf("watch stream reconnect: open=%v err=%v revision=%d", ok, response.Err(), nextRevision)
				watch = client.Watch(ctx, key, clientv3.WithRev(nextRevision))
				continue
			}
			events += len(response.Events)
			for _, event := range response.Events {
				if event.Kv.ModRevision >= nextRevision {
					nextRevision = event.Kv.ModRevision + 1
				}
			}
		case response, ok := <-keepAlive:
			if !ok || response == nil {
				log.Printf("lease keepalive stream reconnect")
				keepAlive, err = client.KeepAlive(ctx, lease.ID)
				if err != nil {
					log.Printf("lease keepalive reconnect failed: %v", err)
					keepAlive = nil
					time.Sleep(time.Second)
				}
				continue
			}
			keepAlives++
		case <-ticker.C:
			if keepAlive == nil {
				keepAlive, err = client.KeepAlive(ctx, lease.ID)
				if err != nil {
					log.Printf("lease keepalive reconnect retry failed: %v", err)
					continue
				}
			}
			requestCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
			_, err = client.Put(requestCtx, key, fmt.Sprintf("%d", writes+1), clientv3.WithLease(lease.ID))
			cancel()
			if err != nil {
				log.Printf("transient put failure: %v", err)
				continue
			}
			writes++
			fmt.Printf("SOAK_HEALTH writes=%d events=%d keepalives=%d\n", writes, events, keepAlives)
		}
	}
}
EOF

cat > auth-main.go <<'EOF'
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
	cert, err := tls.LoadX509KeyPair("auth-client.crt", "auth-client.key")
	if err != nil {
		log.Fatal(err)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"https://127.0.0.1:" + os.Getenv("LOCAL_PORT")},
		DialTimeout: 10 * time.Second,
		TLS: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, ServerName: "127.0.0.1"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if os.Getenv("BOOTSTRAP_AUTH") == "true" {
		if _, err = cli.UserAdd(ctx, "root", "unused"); err != nil {
			log.Fatalf("add root user: %v", err)
		}
		if _, err = cli.UserGrantRole(ctx, "root", "root"); err != nil {
			log.Fatalf("grant root role to root: %v", err)
		}
		if _, err = cli.UserAdd(ctx, "cert-root", "unused"); err != nil {
			log.Fatalf("add cert-root user: %v", err)
		}
		if _, err = cli.UserGrantRole(ctx, "cert-root", "root"); err != nil {
			log.Fatalf("grant root role to cert-root: %v", err)
		}
		if _, err = cli.AuthEnable(ctx); err != nil {
			log.Fatalf("enable auth: %v", err)
		}
	}
	key := "/registry/tls-smoke/auth-cert/" + os.Getenv("POD_NAME")
	if _, err = cli.Put(ctx, key, "certificate-identity"); err != nil {
		log.Fatalf("certificate-auth put: %v", err)
	}
	resp, err := cli.Get(ctx, key)
	if err != nil {
		log.Fatal(err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "certificate-identity" {
		log.Fatalf("unexpected certificate-auth get response: %+v", resp.Kvs)
	}
	fmt.Println("certificate auth smoke completed")
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

if [ "$RUN_CERT_ROTATION_SMOKE" = "true" ]; then
  echo "Publishing old+new CA overlap bundle"
  apply_tls_secrets old-tls.crt old-tls.key ca-overlap.crt
  wait_for_projected_file ca-overlap.crt /etc/kubebrain/client-tls/ca.crt
  wait_for_projected_file ca-overlap.crt /etc/kubebrain/peer-tls/ca.crt
  CA_FILE=ca-overlap.crt CERT_FILE=tls-next.crt KEY_FILE=tls-next.key run_client_smoke

  go build -o soak-client soak-main.go
  start_soak_port_forward
  : > /tmp/kubebrain-tls-connection-soak.log
  CA_FILE=ca-overlap.crt CERT_FILE=tls-next.crt KEY_FILE=tls-next.key \
    LOCAL_PORT="$SOAK_LOCAL_PORT" ./soak-client >/tmp/kubebrain-tls-connection-soak.log 2>&1 &
  soak_pid=$!
  wait_for_soak_progress 0
  echo "Holding watch and lease across repeated gRPC max-age reconnects"
  sleep 12
  soak_before="$(soak_progress)"
  wait_for_soak_progress "$soak_before"

  echo "Rotating client and peer leaf certificates without a rollout"
  soak_before="$(soak_progress)"
  apply_tls_secrets tls-next.crt tls-next.key ca-overlap.crt
  wait_for_projected_file tls-next.crt /etc/kubebrain/client-tls/tls.crt
  wait_for_projected_file tls-next.crt /etc/kubebrain/peer-tls/tls.crt
  CA_FILE=ca-overlap.crt CERT_FILE=tls-next.crt KEY_FILE=tls-next.key run_client_smoke
  wait_for_soak_progress "$soak_before"

  mapfile -t rotation_pods < <(kubectl -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/name=kubebrain \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  for pod in "${rotation_pods[@]}"; do
    echo "Running rotated certificate smoke directly against ${pod}"
    CA_FILE=ca-overlap.crt CERT_FILE=tls-next.crt KEY_FILE=tls-next.key run_direct_client_smoke "$pod"
  done

  echo "Removing the old CA from client and peer trust bundles"
  soak_before="$(soak_progress)"
  apply_tls_secrets tls-next.crt tls-next.key ca-next.crt revoked-next.crl
  wait_for_projected_file ca-next.crt /etc/kubebrain/client-tls/ca.crt
  wait_for_projected_file ca-next.crt /etc/kubebrain/peer-tls/ca.crt
  CA_FILE=ca-next.crt CERT_FILE=tls-next.crt KEY_FILE=tls-next.key run_client_smoke
  wait_for_soak_progress "$soak_before"
  CA_FILE=ca-next.crt CERT_FILE=old-tls.crt KEY_FILE=old-tls.key assert_client_rejected
  CA_FILE=ca-next.crt CERT_FILE=tls-next.crt KEY_FILE=tls-next.key run_client_smoke

  if [ "$REPLICAS" -ge 3 ]; then
    for pod in "${rotation_pods[@]}"; do
      echo "Replacing ${pod} after CA cutover and verifying follower reconnect"
      soak_before="$(soak_progress)"
      kubectl -n "$NAMESPACE" delete pod "$pod" --wait=false >/dev/null
      wait_ready
      start_soak_port_forward
      CA_FILE=ca-next.crt CERT_FILE=tls-next.crt KEY_FILE=tls-next.key run_client_smoke
      wait_for_soak_progress "$soak_before"
    done
  fi

  echo "Long-lived watch/lease reconnect soak completed with $(soak_progress) successful writes"
  kill "$soak_pid" >/dev/null 2>&1 || true
  wait "$soak_pid" >/dev/null 2>&1 || true
  unset soak_pid
  kill "$soak_pf_pid" >/dev/null 2>&1 || true
  wait "$soak_pf_pid" >/dev/null 2>&1 || true
  unset soak_pf_pid

  cp ca-next.crt ca.crt
  cp tls-next.crt tls.crt
  cp tls-next.key tls.key
  cp auth-client-next.crt auth-client.crt
  cp auth-client-next.key auth-client.key
fi

if [ "$RUN_AUTH_CERT_SMOKE" = "true" ]; then
  echo "Enabling auth with a dedicated cert-root client identity"
  start_port_forward
  POD_NAME=bootstrap run_auth_cert_smoke true

  mapfile -t auth_pods < <(kubectl -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/name=kubebrain \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  for pod in "${auth_pods[@]}"; do
    echo "Running certificate auth smoke directly against ${pod}"
    if [ -n "${pf_pid:-}" ]; then
      kill "$pf_pid" >/dev/null 2>&1 || true
      wait "$pf_pid" >/dev/null 2>&1 || true
    fi
    kubectl -n "$NAMESPACE" port-forward "pod/${pod}" "${LOCAL_PORT}:3379" \
      >/tmp/kubebrain-auth-cert-smoke-port-forward.log 2>&1 &
    pf_pid=$!
    sleep 2
    POD_NAME="$pod" run_auth_cert_smoke false
  done

  echo "Revoking the active cert-root certificate without restarting Pods"
  openssl ca -config crl-next.conf -revoke auth-client-next.crt -batch >/dev/null 2>&1
  openssl ca -gencrl -config crl-next.conf -out revoked-next.pem -batch >/dev/null 2>&1
  openssl crl -in revoked-next.pem -outform DER -out revoked-next.crl
  apply_tls_secrets tls-next.crt tls-next.key ca-next.crt revoked-next.crl
  wait_for_projected_file revoked-next.crl /etc/kubebrain/client-tls/revoked.crl
  wait_for_projected_file revoked-next.crl /etc/kubebrain/peer-tls/revoked.crl
  CA_FILE=ca-next.crt CERT_FILE=auth-client-next.crt KEY_FILE=auth-client-next.key assert_client_rejected
  CA_FILE=ca-next.crt CERT_FILE=auth-client-good.crt KEY_FILE=auth-client-good.key run_client_smoke
  echo "CRL revocation smoke completed"
fi

echo "TLS HA smoke completed"
