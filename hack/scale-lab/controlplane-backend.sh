#!/usr/bin/env bash
# Source-only shared-backend lifecycle. The caller must stop and reap every
# apiserver/controller process before cleanup. This helper never revokes leases.

controlplane_backend_identity() {
  local output cluster member
  output=$("${controlplane_etcdctl[@]}" endpoint status --write-out=fields) || return 1
  # Preserve uint64 IDs as decimal strings, not jq floating-point numbers.
  cluster=$(sed -n 's/^"ClusterID" : \([0-9][0-9]*\)$/\1/p' <<< "$output")
  member=$(sed -n 's/^"MemberID" : \([0-9][0-9]*\)$/\1/p' <<< "$output")
  [[ "$cluster" == "$CONTROLPLANE_CLUSTER_ID" && "$member" == "$CONTROLPLANE_MEMBER_ID" ]] || {
    echo 'control-plane backend identity mismatch or ambiguous status' >&2; return 1;
  }
  printf '%s\n' "$output"
}

controlplane_backend_empty_prefix() {
  local response
  response=$("${controlplane_etcdctl[@]}" get "$controlplane_prefix" --prefix --limit=1 --write-out=json) || return 1
  jq -se 'length==1 and (.[0] | (.header|type)=="object" and
    (.count // 0)==0 and (.kvs // [] | type)=="array" and (.kvs // [] | length)==0)' <<< "$response" >/dev/null
}

controlplane_backend_prepare() {
  local directory=$1 run_id=$2 actual
  [[ ${ALLOW_MUTATING_CONTROLPLANE_BACKEND:-false} == true ]] || {
    echo 'requires ALLOW_MUTATING_CONTROLPLANE_BACKEND=true' >&2; return 2;
  }
  [[ $directory == /* && -d $directory && ! -e $directory/backend-prefix &&
     $run_id =~ ^[a-z0-9][a-z0-9-]{15,79}$ ]] || return 2
  [[ ${CONTROLPLANE_ENDPOINT:-} =~ ^https://[a-zA-Z0-9.-]+:[1-9][0-9]{0,4}$ &&
     ${CONTROLPLANE_CLUSTER_ID:-} =~ ^[1-9][0-9]{0,19}$ &&
     ${CONTROLPLANE_MEMBER_ID:-} =~ ^[1-9][0-9]{0,19}$ ]] || return 2
  local name
  for name in CONTROLPLANE_CA CONTROLPLANE_CERT CONTROLPLANE_KEY; do
    [[ ${!name:-} == /* && -f ${!name} ]] || return 2
  done
  [[ ${CONTROLPLANE_ETCDCTL:-} == /* && -x $CONTROLPLANE_ETCDCTL &&
     ${CONTROLPLANE_ETCDCTL_SHA256:-} =~ ^[a-f0-9]{64}$ ]] || return 2
  actual=$(sha256sum "$CONTROLPLANE_ETCDCTL") || return 1
  [[ ${actual%% *} == "$CONTROLPLANE_ETCDCTL_SHA256" ]] || return 2
  # Fixed full TLS verification and bounded CLI calls. Explicit conflicting
  # ETCDCTL_* environment options fail in etcdctl rather than weakening TLS.
  controlplane_etcdctl=(timeout --signal=TERM --kill-after=1s 10s "$CONTROLPLANE_ETCDCTL"
    --endpoints="$CONTROLPLANE_ENDPOINT" --cacert="$CONTROLPLANE_CA"
    --cert="$CONTROLPLANE_CERT" --key="$CONTROLPLANE_KEY"
    --insecure-transport=false --insecure-skip-tls-verify=false --dial-timeout=3s --command-timeout=5s)
  controlplane_prefix="/registry-kubebrain-controlplane-$run_id/"
  controlplane_backend_identity > "$directory/backend-admission.fields" || return 1
  controlplane_backend_empty_prefix || { echo 'control-plane prefix is not empty' >&2; return 1; }
  [[ $("${controlplane_etcdctl[@]}" lease list) == 'found 0 leases' ]] || {
    echo 'control-plane backend requires an empty lease baseline' >&2; return 1;
  }
  # A second caller cannot claim the same evidence directory. This receipt is
  # not a cross-host lock; the dedicated test backend must have one test owner.
  (set -o noclobber; printf '%s\n' "$controlplane_prefix" > "$directory/backend-prefix") || return 1
  controlplane_backend_owned=true
  controlplane_backend_directory=$directory
  controlplane_storage_args=(--etcd-servers="$CONTROLPLANE_ENDPOINT" --etcd-prefix="$controlplane_prefix"
    --etcd-cafile="$CONTROLPLANE_CA" --etcd-certfile="$CONTROLPLANE_CERT" --etcd-keyfile="$CONTROLPLANE_KEY")
}

controlplane_backend_cleanup() {
  [[ ${controlplane_backend_owned:-false} == true ]] || return 0
  # Call only after owned writers have stopped. Refuse identity drift rather
  # than deleting from a different backend, even when that leaves test residue.
  controlplane_backend_identity > "$controlplane_backend_directory/backend-cleanup.fields" || return 1
  "${controlplane_etcdctl[@]}" del "$controlplane_prefix" --prefix --write-out=json > "$controlplane_backend_directory/backend-delete.json" || return 1
  controlplane_backend_empty_prefix || return 1
  # Empty leases remain valid after their last key is deleted. Freeze the
  # observation budget once: granted lifetime plus the existing 60s cleanup
  # allowance. Never refresh this deadline on renewal or leadership changes.
  local started=$SECONDS deadline leases lease response ttl granted max_ttl=0 initial=true
  local -a ids=()
  local -A admitted=()
  deadline=$((started+60))
  while true; do
    controlplane_backend_identity > "$controlplane_backend_directory/backend-cleanup-current.fields" || return 1
    leases=$("${controlplane_etcdctl[@]}" lease list) || return 1
    printf '%s\n' "$leases" > "$controlplane_backend_directory/backend-final-leases.txt"
    ((SECONDS <= deadline)) || { echo 'leases did not naturally expire; none revoked' >&2; return 1; }
    if [[ $leases == 'found 0 leases' ]]; then
      controlplane_backend_empty_prefix || return 1
      controlplane_backend_owned=false
      return 0
    fi
    mapfile -t ids <<< "${leases#*$'\n'}"
    [[ ${leases%%$'\n'*} == "found ${#ids[@]} leases" ]] || return 1
    for lease in "${ids[@]}"; do
      [[ $lease =~ ^[a-f0-9]{1,16}$ ]] || return 1
      if [[ $initial == true ]]; then
        [[ ! -v admitted[$lease] ]] || return 1
        admitted[$lease]=true
      else
        [[ -v admitted[$lease] ]] || { echo 'new lease appeared after writers stopped' >&2; return 1; }
      fi
      response=$("${controlplane_etcdctl[@]}" lease timetolive "$lease" --keys --write-out=json) || return 1
      printf '%s\n' "$response" > "$controlplane_backend_directory/backend-lease-$lease.json"
      # A lease may expire between List and TTL. Live leases must have no keys.
      ttl=$(jq -er 'select((.keys // [] | type)=="array" and (.keys // [] | length)==0)
        | .ttl | select(type=="number" and .==floor and .>= -1 and .<=2147483647)' <<< "$response") || return 1
      if ((ttl >= 0)); then
        granted=$(jq -er '.["granted-ttl"] | select(type=="number" and .==floor and .>0 and .<=2147483647)' <<< "$response") || return 1
        ((ttl <= granted)) || return 1
        if [[ $initial == true ]] && ((granted > max_ttl)); then max_ttl=$granted; fi
      fi
    done
    if [[ $initial == true ]]; then
      deadline=$((started+max_ttl+60))
      printf 'granted_ttl_max=%s\ncleanup_allowance=60\n' "$max_ttl" > "$controlplane_backend_directory/backend-cleanup-budget.txt"
      initial=false
    fi
    sleep 1
  done
}
