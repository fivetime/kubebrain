# Requires a separately proven Pod/CEP/agent binding. Never a packet/RPC verdict.
def need($ok; $why): if $ok then . else error($why) end;
def revision: type=="number" and .>=0 and .<=999999999999999 and floor==.;
need(($mode=="present" or $mode=="absent" or $mode=="absent-unlabelled") and
 ($policy_uid|type=="string" and length>0) and
 ($policy_name|test("^kb-term-[a-z0-9]+$")); "invalid observation scope") |
need(type=="array" and length==1; "one endpoint required") |
.[0].status as $s |
need($s.state=="ready" and ($s.identity.labels|type)=="array" and
 all($s.identity.labels[];type=="string") and
 ($s.policy.spec|type)=="object" and ($s.policy.realized|type)=="object" and
 ($s.policy.realized.l4|type)=="object" and
 ($s.policy.realized.l4.ingress|type)=="array" and ($s.policy.realized.l4.egress|type)=="array" and
 ($s.policy.spec["policy-revision"]|revision) and
 ($s.policy.realized["policy-revision"]|revision); "invalid endpoint policy evidence") |
("k8s:kubebrain.io/fault-owner="+($policy_name|ltrimstr("kb-"))) as $nonce |
need((if $mode=="absent-unlabelled" then
 all($s.identity.labels[]; startswith("k8s:kubebrain.io/fault-owner=")|not)
 else ($s.identity.labels|index($nonce))!=null end); "target identity label mismatch") |
($s.policy.spec["policy-revision"]) as $desired |
($s.policy.realized["policy-revision"]) as $actual |
need($desired >= $actual; "policy revision moved backwards") |
[$s.policy.realized|..|strings] as $labels |
($labels|index("k8s:io.cilium.k8s.policy.uid="+$policy_uid)!=null) as $uid_present |
($labels|index("k8s:io.cilium.k8s.policy.name="+$policy_name)!=null) as $name_present |
need($uid_present==$name_present; "policy name/UID evidence conflicts") |
{state:(if $desired!=$actual then "pending"
 elif ($mode=="present" and $uid_present) or ($mode!="present" and ($uid_present|not)) then "matched"
 else "pending" end), mode:$mode,policy_uid:$policy_uid,policy_name:$policy_name,
 desired_revision:$desired,realized_revision:$actual,
 scope:"same_process_policy_realization_only",packet_enforcement_proven:false}
