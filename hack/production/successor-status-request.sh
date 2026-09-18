#!/usr/bin/env bash
# Callback for observe-successor.sh. Config is operator-audited, hash-bound and
# contains paths, never embedded credentials. No Kubernetes writes or retries.
set -euo pipefail
umask 077
[[ $# == 2 && $1 =~ ^[0-5][.][0-9]{9}$ && $2 == /* ]]
budget=$1; output=$2
[[ $budget != 0.000000000 && ( $budget < 5.000000000 || $budget == 5.000000000 ) ]]
config=${KB_SUCCESSOR_STATUS_CONFIG:?}; hash=${KB_SUCCESSOR_STATUS_CONFIG_SHA256:?}
[[ $config == /* && -f $config && ! -L $config && $hash =~ ^[a-f0-9]{64}$ ]]
[[ ! -e $output && ! -L $output ]]
dir=${output%/*}
[[ -d $dir && ! -e $dir/request-config.json ]]
cp -- "$config" "$dir/request-config.json"
[[ $(sha256sum "$dir/request-config.json" | cut -d ' ' -f1) == "$hash" ]]
jq -e '
 (keys)==["ca","cert","key","port","server_name"] and
 (.server_name|type=="string" and test("^[a-z0-9][a-z0-9.-]*$")) and
 (.port|type=="string" and test("^[1-9][0-9]{0,4}$") and (tonumber)<=65535) and
 all(.ca,.cert,.key;type=="string" and startswith("/") and (contains("\r")|not) and (contains("\n")|not))
' "$dir/request-config.json" >/dev/null
get() { jq -er --arg name "$1" '.[$name]' "$dir/request-config.json"; }
name=$(get server_name); port=$(get port); ca=$(get ca); cert=$(get cert); key=$(get key)
for file in "$ca" "$cert" "$key"; do [[ -f $file && ! -L $file ]]; done
# --disable must be first: a user's .curlrc must not enable redirects/retries or
# disable verification. The endpoint is always the operator-owned loopback tunnel.
curl --disable --silent --show-error --fail-with-body --http1.1 --retry 0 \
 --max-time "$budget" --connect-timeout "$budget" --max-filesize 65536 \
 --noproxy '*' --proto '=https' --tlsv1.2 \
 --resolve "$name:$port:127.0.0.1" --cacert "$ca" --cert "$cert" --key "$key" \
 -H 'Content-Type: application/json' --request POST --data '{}' \
 --output "$output" --write-out '%{http_code}' \
 "https://$name:$port/v3/maintenance/status" > "$dir/http-status"
[[ $(<"$dir/http-status") == 200 ]]
