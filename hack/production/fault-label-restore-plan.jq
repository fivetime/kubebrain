# Pure JSON Patch construction. This is not a policy withdrawal or Cilium
# identity-convergence proof. Caller must own the exclusive recovery lifecycle.
def nonempty: type == "string" and length > 0;
if type != "object" or (has("binding") and has("namespace") and has("expected") and has("current") and has("policies") | not)
then error("refuse label cleanup: incomplete observations") else . end |
.binding as $b | .namespace as $n | .expected as $e | .current as $p | .policies as $policies |
if (($b.namespace | nonempty) != true or ($b.namespaceUID | nonempty) != true or
    ($b.name | nonempty) != true or ($b.uid | nonempty) != true or
    ($b.nonce | nonempty) != true or ($b.policyName | nonempty) != true or
    $n.apiVersion != "v1" or $n.kind != "Namespace" or
    $n.metadata.name != $b.namespace or $n.metadata.uid != $b.namespaceUID or $n.metadata.deletionTimestamp != null or
    $e.apiVersion != "v1" or $e.kind != "Pod" or $e.metadata.namespace != $b.namespace or
    $e.metadata.name != $b.name or $e.metadata.uid != $b.uid or $e.metadata.deletionTimestamp != null or
    (($e.metadata.labels | type) != "object" and $e.metadata.labels != null) or
    (($e.metadata.labels // {}) | has("kubebrain.io/fault-owner")) or
    $p.apiVersion != "v1" or $p.kind != "Pod" or $p.metadata.namespace != $b.namespace or
    $p.metadata.name != $b.name or $p.metadata.uid != $b.uid or $p.metadata.deletionTimestamp != null or
    ($p.metadata.resourceVersion | nonempty) != true or
    (($p.metadata.labels | type) != "object" and $p.metadata.labels != null))
then error("refuse label cleanup: original/current identity or label provenance mismatch")
elif ((($policies.apiVersion == "cilium.io/v2" and $policies.kind == "CiliumNetworkPolicyList") or
       ($policies.apiVersion == "v1" and $policies.kind == "List") | not) or
      ($policies.items | type) != "array" or
      ($policies.metadata.continue != null and $policies.metadata.continue != "") or
      any($policies.items[]; .apiVersion != "cilium.io/v2" or .kind != "CiliumNetworkPolicy" or
          .metadata.namespace != $b.namespace or .metadata.name == $b.policyName or
          ([.spec,.specs] | [.. | strings] | index($b.nonce)) != null))
then error("refuse label cleanup: incomplete policy list or fault policy remains")
elif (($p.metadata.labels // {}) | has("kubebrain.io/fault-owner") | not) then []
elif $p.metadata.labels["kubebrain.io/fault-owner"] != $b.nonce
then error("refuse label cleanup: label ownership changed")
else [
  {op:"test",path:"/metadata/uid",value:$b.uid},
  {op:"test",path:"/metadata/resourceVersion",value:$p.metadata.resourceVersion},
  {op:"test",path:"/metadata/labels/kubebrain.io~1fault-owner",value:$b.nonce},
  {op:"remove",path:"/metadata/labels/kubebrain.io~1fault-owner"}
] end
