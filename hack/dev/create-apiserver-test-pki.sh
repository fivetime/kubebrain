#!/usr/bin/env bash
set -euo pipefail
umask 077

# Disposable loopback-only apiserver PKI, never copied from a real cluster.
# The caller owns the parent directory and must remove this directory afterwards.
pki_dir="${1:-}"
controlplane=false
kwok=false
if [[ "$#" == 2 ]]; then
  case "$2" in
    --controlplane) controlplane=true ;;
    --controlplane-kwok) controlplane=true; kwok=true ;;
  esac
fi
if [[ ( "$#" != 1 && "$controlplane" != true ) || "$pki_dir" != /* || -e "$pki_dir" || -L "$pki_dir" ]]; then
  echo "usage: create-apiserver-test-pki.sh /absolute/new/pki-directory [--controlplane|--controlplane-kwok]" >&2
  exit 2
fi
command -v openssl >/dev/null
mkdir -m 0700 -- "$pki_dir"

create_ca() {
  local name="$1"
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
    -subj "/CN=kubebrain-test-${name}" \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign' \
    -keyout "$pki_dir/$name.key" -out "$pki_dir/$name.crt" >/dev/null 2>&1
}
create_leaf() {
  local name="$1" ca="$2" subject="$3" extensions="$4"
  openssl req -new -newkey rsa:2048 -nodes -sha256 -subj "$subject" \
    -keyout "$pki_dir/$name.key" -out "$pki_dir/$name.csr" >/dev/null 2>&1
  openssl x509 -req -sha256 -days 1 -in "$pki_dir/$name.csr" \
    -CA "$pki_dir/$ca.crt" -CAkey "$pki_dir/$ca.key" \
    -set_serial "0x$(openssl rand -hex 16)" \
    -extfile <(printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n%s\n' "$extensions") \
    -out "$pki_dir/$name.crt" >/dev/null 2>&1
}
create_ca ca
create_ca front-proxy-ca
create_leaf apiserver ca /CN=kubebrain-test-apiserver \
  $'extendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1,DNS:localhost'
create_leaf apiserver-kubelet-client ca /CN=kubebrain-test-client \
  'extendedKeyUsage=clientAuth'
create_leaf front-proxy-client front-proxy-ca /CN=front-proxy-client \
  'extendedKeyUsage=clientAuth'
if [[ "$controlplane" == true ]]; then
  # Only for this disposable CA. Components use the upstream bootstrap RBAC
  # user identities, not the privileged test administrator's credentials.
  create_leaf admin ca /CN=kubebrain-test-admin/O=system:masters \
    'extendedKeyUsage=clientAuth'
  create_leaf controller-manager ca /CN=system:kube-controller-manager \
    'extendedKeyUsage=clientAuth'
  create_leaf scheduler ca /CN=system:kube-scheduler \
    'extendedKeyUsage=clientAuth'
fi
if [[ "$kwok" == true ]]; then
  # No privileged group or upstream bootstrap identity. The isolated harness
  # must explicitly bind the fixture's limited RBAC permissions to this user.
  create_leaf kwok ca /CN=kubebrain-test-kwok \
    'extendedKeyUsage=clientAuth'
fi
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -out "$pki_dir/sa.key" >/dev/null 2>&1
openssl pkey -in "$pki_dir/sa.key" -pubout -out "$pki_dir/sa.pub" >/dev/null 2>&1
