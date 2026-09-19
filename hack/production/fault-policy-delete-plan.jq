# Pure plan construction. Binding and approved schema must come from independent
# admission; expected is the actual inactive create receipt, not a dry-run UID.
# This makes no API calls and proves neither policy withdrawal nor dataplane recovery.
def nonempty: type == "string" and length > 0;
if type != "object" or (has("binding") and has("namespace") and has("approved") and has("expected") and has("current") | not)
then error("refuse policy cleanup: incomplete observations") else . end |
.binding as $b | .namespace as $n | .approved as $a |
.expected as $e | .current as $c |
if (($b.namespace | nonempty) != true or ($b.namespaceUID | nonempty) != true or
    ($b.name | nonempty) != true or ($b.nonce | nonempty) != true or
    ($b.reservedNonce | nonempty) != true or $b.nonce == $b.reservedNonce or
    $n.apiVersion != "v1" or $n.kind != "Namespace" or
    $n.metadata.name != $b.namespace or $n.metadata.uid != $b.namespaceUID or
    $n.metadata.deletionTimestamp != null or
    $a.apiVersion != "cilium.io/v2" or $a.kind != "CiliumNetworkPolicy" or
    $a.metadata.namespace != $b.namespace or $a.metadata.name != $b.name or
    ($a.spec | type) != "object" or $a.specs != null or
    ($a.spec.endpointSelector.matchLabels | type) != "object" or
    $a.spec.endpointSelector.matchLabels["kubebrain.io/fault-owner"] != $b.nonce)
then error("refuse policy cleanup: invalid independent admission")
else
  $a.spec as $active |
  ($active | .endpointSelector.matchLabels["kubebrain.io/fault-owner"]=$b.reservedNonce) as $inactive |
  if ($e.apiVersion != "cilium.io/v2" or $e.kind != "CiliumNetworkPolicy" or
      $e.metadata.namespace != $b.namespace or $e.metadata.name != $b.name or
      ($e.metadata.uid | nonempty) != true or ($e.metadata.resourceVersion | nonempty) != true or
      $e.metadata.deletionTimestamp != null or $e.spec != $inactive or $e.specs != null)
  then error("refuse policy cleanup: invalid actual create receipt")
  elif $c == null then {absent:true}
  elif ($c.apiVersion != $e.apiVersion or $c.kind != $e.kind or
        $c.metadata.namespace != $e.metadata.namespace or $c.metadata.name != $e.metadata.name or
        $c.metadata.uid != $e.metadata.uid or ($c.metadata.resourceVersion | nonempty) != true or
        ($c.spec != $active and $c.spec != $inactive) or $c.specs != null)
  then error("refuse policy cleanup: replacement or concurrent policy change")
  else {apiVersion:$c.apiVersion,resource:"ciliumnetworkpolicies",namespace:$c.metadata.namespace,
        name:$c.metadata.name,uid:$c.metadata.uid,resourceVersion:$c.metadata.resourceVersion}
  end
end
