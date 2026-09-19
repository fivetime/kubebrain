# Pure snapshot validator, not a collector or an atomic cluster-wide proof.
# Input: {expected_agents:[{uid,node}], agents:[{uid,node,endpoints:[]}],
# target:{node,endpoint_id,namespace,pod,ipv4}}. Expected membership and target
# must be independently bound to live Node/Pod/CEP identities by the collector.
# Args: active, reserved. Reserved must occur nowhere; active only on target.
def need($ok; $why): if $ok then . else error($why) end;
def text: type=="string" and length>0;
def endpoint_id: type=="number" and .>0 and .<65536 and floor==.;
def labels: type=="array" and all(.[];text) and (unique|length)==length;
def optional_labels: .==null or labels;
need(($active|test("^term-[a-z0-9]+$")) and
 ($reserved|test("^[a-z0-9][a-z0-9-]*$")) and $active!=$reserved;
 "invalid nonce binding") |
need((.expected_agents|type)=="array" and (.expected_agents|length)>0 and
 all(.expected_agents[];(.uid|text) and (.node|text)) and
 (.expected_agents|map(.uid)|unique|length)==(.expected_agents|length) and
 (.expected_agents|map(.node)|unique|length)==(.expected_agents|length);
 "invalid independent agent membership") |
. as $in |
need((.agents|type)=="array" and
 (.agents|map({uid,node})|sort_by(.uid))==(.expected_agents|map({uid,node})|sort_by(.uid));
 "incomplete or changed agent membership") |
need((.target.node|text) and (.target.endpoint_id|endpoint_id) and
 .target.namespace=="kubebrain-dbaas-test" and
 (.target.pod|test("^kubebrain-local-[012]$")) and (.target.ipv4|text) and
 any(.expected_agents[];.node==$in.target.node); "invalid target binding") |
# The policy's unqualified selector key matches any label source, not just k8s.
"kubebrain.io/fault-owner=" as $prefix |
[$in.agents[] | . as $agent |
 need((.endpoints|type)=="array" and
  all(.endpoints[];(.id|endpoint_id)) and
  (.endpoints|map(.id)|unique|length)==(.endpoints|length);
  "invalid or duplicate agent endpoints") |
 .endpoints[] |
 need((.status.identity.labels|labels) and (.status.identity.labels|length)>0;
  "missing endpoint identity labels") |
 [.status.identity.labels, .status.labels["security-relevant"],
  .status.labels.derived, .status.labels.disabled,
  .status.labels.realized.user, .spec["label-configuration"].user,
  .status.realized["label-configuration"].user] as $sets |
 need(all($sets[];optional_labels); "malformed endpoint label set") |
 ([$sets[] | select(.!=null) | .[] | sub("^[^:]+:"; "") |
   select(startswith($prefix))] | unique) as $owners |
 ($agent.node==$in.target.node and .id==$in.target.endpoint_id) as $target |
 need(all($owners[];.!=($prefix+$reserved)); "reserved nonce occurs on endpoint") |
 need(all($owners[];if .==($prefix+$active) then $target else true end);
  "active nonce occurs on foreign endpoint") |
 if $target then
  need(.status["external-identifiers"]["k8s-namespace"]==$in.target.namespace and
   .status["external-identifiers"]["k8s-pod-name"]==$in.target.pod and
   (.status.networking.addressing|type)=="array" and
   any(.status.networking.addressing[];.ipv4==$in.target.ipv4) and
   all($owners[];.==($prefix+$active)); "target endpoint binding or owner changed") |
  {target:true}
 else {target:false} end
] as $checked |
need(([$checked[]|select(.target)]|length)==1; "target endpoint missing") |
{scope:"nonce_snapshot_only_not_enforcement",agents:(.agents|length),
 endpoints:($checked|length),reserved_absent:true,active_confined_to_target:true}
