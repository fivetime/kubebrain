def successful: .stage=="ResponseComplete" and .responseStatus.code>=200 and .responseStatus.code<300;
def pod: .objectRef.resource=="pods" and (.objectRef.apiGroup//"")=="" and .objectRef.namespace=="controlplane-smoke";
($before[0].items + $pods[0].items | unique_by(.metadata.uid) | map([.metadata.name,.metadata.uid]) | sort) as $all |
([$before[0].items[].metadata.uid]) as $old |
([$pods[0].items[]|select(.metadata.uid as $u|($old|index($u))==null)]) as $replacement |
if ($all|length)!=4 or ($replacement|length)!=1 then error("invalid replacement identities") else
  ([.[]|select(successful and pod and .verb=="create" and .objectRef.subresource==null)|
    [.user.username,.responseObject.metadata.name,.responseObject.metadata.uid]]|sort) as $creates |
  ([.[]|select(successful and pod and .verb=="create" and .objectRef.subresource=="binding")|
    [.user.username,.objectRef.name,.objectRef.uid]]|sort) as $bindings |
  ([.[]|select(successful and pod and .verb=="delete" and .objectRef.subresource==null)|
    [.user.username,.objectRef.name,.requestObject.preconditions.uid,.requestObject.preconditions.resourceVersion,.requestObject.gracePeriodSeconds]]) as $deletes |
  $creates==($all|map(["system:serviceaccount:kube-system:replicaset-controller"]+.)) and
  $bindings==($all|map(["system:kube-scheduler"]+.)) and
  $deletes==[["kubebrain-test-admin",$victim[0].metadata.name,$victim[0].metadata.uid,$victim[0].metadata.resourceVersion,0]] and
  any(.[];successful and pod and (.verb=="patch" or .verb=="update") and .objectRef.subresource=="status" and
    .user.username=="kubebrain-test-kwok" and .responseObject.metadata.uid==$replacement[0].metadata.uid and
    .responseObject.status.phase=="Running" and any(.responseObject.status.conditions[]?;.type=="Ready" and .status=="True"))
end
