#!/usr/bin/env bash
# Offline first-stage material check. Inputs must be trusted private local files.
# No Kubernetes access, Secret mutation, key output, or claimed runtime evidence.
set -euo pipefail
if [[ $# != 4 ]]; then
 echo 'Usage: bash verify-peer-trust-material.sh ORIGINAL_DIR EXPANDED_DIR NEW_MEMBER_DIR PEER_DNS' >&2
 exit 2
fi
original=$1; expanded=$2; member=$3; peer_dns=$4
[[ $original == /* && $expanded == /* && $member == /* && $peer_dns =~ ^[a-z0-9][a-z0-9.-]*$ ]]
for dir in "$original" "$expanded" "$member"; do
 for file in ca.crt tls.crt tls.key; do
  [[ -f $dir/$file && ! -L $dir/$file ]]
  size=$(stat -c %s "$dir/$file")
  [[ $size -gt 0 && $size -le 1048576 ]]
 done
done
# Only this disposable single-root layout is admitted. Do not silently broaden
# an operator-approved trust set or accept concatenated private keys in CA data.
cert_blocks() {
 local file=$1 count=$2
 [[ $(grep -c '^-----BEGIN CERTIFICATE-----$' "$file") == "$count" ]]
 [[ $(grep -c '^-----END CERTIFICATE-----$' "$file") == "$count" ]]
 [[ $(grep -c '^-----BEGIN ' "$file") == "$count" ]]
}
cert_blocks "$original/ca.crt" 1
cert_blocks "$expanded/ca.crt" 2
cert_blocks "$member/ca.crt" 1
for dir in "$original" "$expanded" "$member"; do cert_blocks "$dir/tls.crt" 1; done
cmp -s "$original/tls.crt" "$expanded/tls.crt"
cmp -s "$original/tls.key" "$expanded/tls.key"
# Only blank-line differences are allowed between the exact approved PEM roots.
# No comment, extra block or changed certificate is silently normalized away.
cmp -s <(sed '/^[[:space:]]*$/d' "$expanded/ca.crt") \
       <(cat "$original/ca.crt" "$member/ca.crt" | sed '/^[[:space:]]*$/d')
old_fingerprint=$(openssl x509 -in "$original/ca.crt" -noout -fingerprint -sha256)
new_fingerprint=$(openssl x509 -in "$member/ca.crt" -noout -fingerprint -sha256)
[[ $old_fingerprint != "$new_fingerprint" ]]
old_spki=$(openssl x509 -in "$original/tls.crt" -pubkey -noout | openssl pkey -pubin -outform DER | sha256sum)
new_spki=$(openssl x509 -in "$member/tls.crt" -pubkey -noout | openssl pkey -pubin -outform DER | sha256sum)
[[ $old_spki != "$new_spki" ]]
for dir in "$original" "$member"; do
 openssl x509 -in "$dir/ca.crt" -noout -checkend 3600 >/dev/null
 openssl x509 -in "$dir/tls.crt" -noout -checkend 3600 >/dev/null
 openssl verify -no-CApath -no-CAstore -check_ss_sig -CAfile "$dir/ca.crt" "$dir/ca.crt" >/dev/null
 cmp -s <(openssl x509 -in "$dir/tls.crt" -pubkey -noout | openssl pkey -pubin -outform DER) \
        <(openssl pkey -in "$dir/tls.key" -pubout -outform DER)
 for purpose in sslserver sslclient; do
  openssl verify -no-CApath -no-CAstore -purpose "$purpose" -verify_hostname "$peer_dns" \
   -CAfile "$dir/ca.crt" "$dir/tls.crt" >/dev/null
  openssl verify -no-CApath -no-CAstore -purpose "$purpose" -verify_hostname "$peer_dns" \
   -CAfile "$expanded/ca.crt" "$dir/tls.crt" >/dev/null
 done
done
# Prevent a differently encoded copy of the same CA/leaf authority from posing
# as a distinct trust phase. Both negative verifications must actually fail.
if openssl verify -no-CApath -no-CAstore -CAfile "$original/ca.crt" "$member/tls.crt" >/dev/null 2>&1; then exit 1; fi
if openssl verify -no-CApath -no-CAstore -CAfile "$member/ca.crt" "$original/tls.crt" >/dev/null 2>&1; then exit 1; fi
echo OFFLINE_PEER_TRUST_MATERIAL_OK_NOT_RUNTIME_ACCEPTANCE
