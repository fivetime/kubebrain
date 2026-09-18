# Offline planner for adjacent trust phases only. No API calls.
# Invoke jq -er -f peer-trust-expand-plan.jq (output is encoded JSON text).
# Input: baseline, current, namespace, namespace_uid, original_secret,
# expanded_secret, live_original_secret, live_expanded_secret, mode.
# Optional phase="members" additionally requires member_secret/live_member_secret.
# Default phase="roots": original <-> shared-old-leaf/dual-CA.
# Members phase: shared-old-leaf/dual-CA <-> distinct-member-leaves/dual-CA.
# Protocol phase additionally requires an independently audited candidate_image:
# member layout <-> same layout plus pinned image and explicit opt-in argument.
# This planner validates a digest reference, not CI provenance or running scope.
# Secret inputs contain private data: pipe from restricted files, never log them.
# The caller must independently verify X.509 roots/purposes/expiry, authenticate
# receipts, recheck namespace/Secret identities before patch, and bound rollout.
# No-op [] is not a readiness or successful migration receipt.
def need($ok; $message): if $ok then . else error($message) end;
def nonempty: type == "string" and length > 0;
def active: .metadata.deletionTimestamp == null;
def same_secret($saved; $live):
  ($saved.kind == "Secret" and $live.kind == "Secret") and
  ($saved.metadata.uid | nonempty) and
  ($saved.metadata.resourceVersion | nonempty) and
  ($saved.metadata.name | nonempty) and
  ($saved.metadata.namespace | nonempty) and
  ($saved | active) and ($live | active) and
  $saved.metadata.uid == $live.metadata.uid and
  $saved.metadata.resourceVersion == $live.metadata.resourceVersion and
  $saved.metadata.name == $live.metadata.name and
  $saved.metadata.namespace == $live.metadata.namespace and
  $saved.type == $live.type and $saved.immutable == $live.immutable and
  $saved.data == $live.data;

. as $in |
need(.mode == "expand" or .mode == "restore"; "unsupported phase") |
need(.phase == null or .phase == "roots" or .phase == "members" or .phase == "protocol"; "unsupported trust phase") |
need((.namespace_uid | nonempty) and .namespace.kind == "Namespace" and
     .namespace.metadata.uid == .namespace_uid and (.namespace | active);
     "namespace identity changed") |
.baseline as $base | .current as $live |
need($base.kind == "StatefulSet" and $live.kind == "StatefulSet" and
     ($base.metadata.uid | nonempty) and ($live.metadata.resourceVersion | nonempty) and
     ($base.metadata.name | nonempty) and
     $base.metadata.uid == $live.metadata.uid and
     $base.metadata.name == $live.metadata.name and
     $base.metadata.namespace == .namespace.metadata.name and
     $live.metadata.namespace == .namespace.metadata.name and
     ($base | active) and ($live | active); "StatefulSet identity changed") |
need(same_secret(.original_secret; .live_original_secret) and
     same_secret(.expanded_secret; .live_expanded_secret); "Secret receipt changed") |
need(.original_secret.metadata.namespace == $base.metadata.namespace and
     .expanded_secret.metadata.namespace == $base.metadata.namespace and
     .original_secret.metadata.name != .expanded_secret.metadata.name and
     .original_secret.metadata.uid != .expanded_secret.metadata.uid and
     .expanded_secret.immutable == true and
     (.original_secret.data | keys) == ["ca.crt", "tls.crt", "tls.key"] and
     (.expanded_secret.data | keys) == ["ca.crt", "tls.crt", "tls.key"] and
     all(.original_secret.data[]; nonempty) and
     all(.expanded_secret.data[]; nonempty) and
     .original_secret.data["tls.crt"] == .expanded_secret.data["tls.crt"] and
     .original_secret.data["tls.key"] == .expanded_secret.data["tls.key"] and
     .original_secret.data["ca.crt"] != .expanded_secret.data["ca.crt"];
     "expansion must retain original leaf/key and use immutable different roots") |
[$base.spec.template.spec.volumes | to_entries[] | select(.value.name == "peer-tls")] as $volumes |
need(($volumes | length) == 1; "expected exactly one peer volume") |
$volumes[0].key as $index |
need($volumes[0].value.secret.secretName == .original_secret.metadata.name and
     ($base.spec.template.spec.containers | length) == 1 and
     $base.spec.template.spec.containers[0].name == "kubebrain" and
     all($base.spec.template.spec.containers[0].args[];
         (startswith("--experimental-peer-retirement-config") | not)) and
     $base.spec.replicas == 3 and $base.spec.updateStrategy.type == "RollingUpdate";
     "baseline is not original three-member nonexperimental layout") |
($base.spec | .template.spec.volumes[$index].secret.secretName = $in.expanded_secret.metadata.name) as $expanded |
(if .phase == "members" or .phase == "protocol" then
  need(same_secret(.member_secret; .live_member_secret) and .member_secret.immutable == true and
       .member_secret.metadata.namespace == $base.metadata.namespace and
       .member_secret.metadata.name != .original_secret.metadata.name and
       .member_secret.metadata.name != .expanded_secret.metadata.name and
       .member_secret.metadata.uid != .original_secret.metadata.uid and
       .member_secret.metadata.uid != .expanded_secret.metadata.uid;
       "member Secret receipt changed") |
  [range(0;3) | "\($base.metadata.name)-\(.)"] as $members |
  [ $members[] as $member | ["tls.crt","tls.key","ca.crt","policy.json"][] as $file |
    {key:"\($member).\($file)",path:"\($member)/\($file)"} ] as $items |
  need((.member_secret.data | keys) == ($items | map(.key) | sort) and
       all(.member_secret.data[]; nonempty) and
       all($members[]; . as $member |
           $in.member_secret.data["\($member).ca.crt"] == $in.expanded_secret.data["ca.crt"] and
           $in.member_secret.data["\($member).tls.crt"] != $in.original_secret.data["tls.crt"] and
           $in.member_secret.data["\($member).tls.key"] != $in.original_secret.data["tls.key"]) and
       ([$members[] as $m | $in.member_secret.data["\($m).tls.crt"]] | unique | length) == 3 and
       ([$members[] as $m | $in.member_secret.data["\($m).tls.key"]] | unique | length) == 3;
       "member layout, unchanged dual roots or distinct material violated") |
  [$base.spec.template.spec.containers[0].volumeMounts | to_entries[] | select(.value.name == "peer-tls")] as $mounts |
  [$base.spec.template.spec.containers[0].env[] | select(.name == "POD_NAME")] as $env |
  need(($mounts | length) == 1 and $mounts[0].value.mountPath == "/etc/kubebrain/peer-tls" and
       $mounts[0].value.readOnly == true and ($mounts[0].value.subPath // "") == "" and
       ($mounts[0].value.subPathExpr // "") == "" and
       ($base.spec.template.spec.initContainers // [] | length) == 0 and
       ($base.spec.template.spec.ephemeralContainers // [] | length) == 0 and
       ($env | length) == 1 and ($env[0].value // "") == "" and
       $env[0].valueFrom.fieldRef.fieldPath == "metadata.name" and
       ($base.spec.updateStrategy.rollingUpdate.partition // 0) == 0;
       "member mount isolation prerequisites missing") |
  ($expanded |
   .template.spec.volumes[$index].secret = {secretName:$in.member_secret.metadata.name,defaultMode:288,optional:false,items:$items} |
   .template.spec.containers[0].volumeMounts[$mounts[0].key].subPathExpr = "$(POD_NAME)") as $member_spec |
  if $in.phase == "protocol" then
    need(($in.candidate_image | type) == "string";
         "protocol phase requires audited digest reference") |
    need(($in.candidate_image | test("^ghcr.io/fivetime/kubebrain@sha256:[0-9a-f]{64}$")) and
         $in.candidate_image != $member_spec.template.spec.containers[0].image;
         "protocol image must be a distinct fixed digest") |
    ($member_spec |
      .template.spec.containers[0].image = $in.candidate_image |
      .template.spec.containers[0].args += ["--experimental-peer-retirement-config=/etc/kubebrain/peer-tls/policy.json"]
    ) as $protocol_spec |
    {from:$member_spec,to:$protocol_spec}
  else {from:$expanded,to:$member_spec} end
 else {from:$base.spec,to:$expanded} end) as $transition |
need($live.spec == $transition.from or $live.spec == $transition.to;
     "unknown spec: refusing drift or nonadjacent trust transition") |
(if .mode == "expand" then $transition.to else $transition.from end) as $desired |
if $live.spec == $desired then []
else
  # Restoration must remain possible after a failed first-stage rollout.
  need(.mode == "restore" or
       ($live.status.observedGeneration == $live.metadata.generation and
        $live.status.readyReplicas == 3 and $live.status.updatedReplicas == 3 and
        ($live.status.currentRevision | nonempty) and
        $live.status.currentRevision == $live.status.updateRevision);
       "expansion requires fully observed ready baseline") |
  [{op:"test",path:"/metadata/uid",value:$live.metadata.uid},
   {op:"test",path:"/metadata/resourceVersion",value:$live.metadata.resourceVersion},
   {op:"test",path:"/spec",value:$live.spec}] +
  (if .phase == "members" or .phase == "protocol" then
    [{op:"replace",path:"/spec/template",value:$desired.template}]
   else
    [{op:"replace",path:"/spec/template/spec/volumes/\($index)/secret/secretName",
      value:$desired.template.spec.volumes[$index].secret.secretName}]
   end)
end |
# Kubernetes' json-patch v4 compares raw scalar encodings. Match Go's default
# HTML-safe JSON encoder for existing command strings such as "sleep && curl".
tojson | gsub("&"; "\\u0026") | gsub("<"; "\\u003c") | gsub(">"; "\\u003e") |
gsub("\u2028"; "\\u2028") | gsub("\u2029"; "\\u2029")
