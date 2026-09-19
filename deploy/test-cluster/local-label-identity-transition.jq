# Input: {cep_before, endpoint, cep_after}; endpoint is one cilium-dbg object.
# Args: mode=present|absent, token=term-... . This classifies label convergence
# only; caller must independently bind the original Pod and agent processes.
def need($ok; $why): if $ok then . else error($why) end;
def positive_integer: type=="number" and .>0 and .<=9007199254740991 and floor==.;
def valid_identity:
  (.id|positive_integer) and (.labels|type)=="array" and
  (.labels|length)>0 and all(.labels[];type=="string" and length>0) and
  (.labels|unique|length)==(.labels|length);
need(($mode=="present" or $mode=="absent") and ($token|test("^term-[a-z0-9]+$")); "invalid label scope") |
. as $in |
"k8s:kubebrain.io/fault-owner=" as $prefix |
($prefix+$token) as $own |
need((.cep_before.metadata.uid|type)=="string" and (.cep_before.metadata.uid|length)>0 and
 .cep_before.metadata.uid==.cep_after.metadata.uid and
 (.cep_before.status.id|positive_integer) and
 .cep_before.status.id==.endpoint.id and .cep_before.status.id==.cep_after.status.id and
 (.cep_before.status.networking|type)=="object" and
 .cep_before.status.networking==.cep_after.status.networking and
 (.cep_before.metadata.ownerReferences|type)=="array" and
 (.cep_before.metadata.ownerReferences|length)>0 and
 .cep_before.metadata.ownerReferences==.cep_after.metadata.ownerReferences;
 "endpoint structure changed") |
need((["ready","waiting-for-identity","regenerating"]|index($in.endpoint.status.state))!=null;
 "unknown endpoint state") |
[.cep_before.status.identity,.endpoint.status.identity,.cep_after.status.identity] as $identities |
need(all($identities[];valid_identity); "malformed identity") |
need(all($identities[]; [.labels[]|select(startswith($prefix))] as $owners |
 ($owners|length)<=1 and all($owners[];.==$own)); "foreign fault owner") |
($identities|map([.labels[]|select(startswith($prefix)|not)]|sort)) as $bases |
need(($bases[0]|length)>0 and all($bases[];.==$bases[0]); "non-owner identity labels changed") |
# A numeric identity cannot denote two label sets within a coherent sample.
need(all($identities[]; . as $a | all($identities[];
 if .id==$a.id then (.labels|sort)==($a.labels|sort)
 else (.labels|sort)!=($a.labels|sort) end)); "inconsistent identity mapping") |
{state:(if $in.endpoint.status.state=="ready" and
 all($identities[]; .id==$identities[0].id and
 ((.labels|index($own))!=null)==($mode=="present")) then "matched" else "pending" end),
 mode:$mode,scope:"label_identity_transition_only_not_enforcement"}
