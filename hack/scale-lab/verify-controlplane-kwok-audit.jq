def successful:
  .stage=="ResponseComplete" and (.verb=="patch" or .verb=="update") and
  .responseStatus.code>=200 and .responseStatus.code<300 and
  .user.username=="kubebrain-test-kwok";
def ready: any(.status.conditions[]?; .type=="Ready" and .status=="True");
map(select(successful)) as $events |
($node[0].metadata.uid|type)=="string" and ($node[0].metadata.uid|length)>0 and
($first[0].metadata.uid|type)=="string" and ($first[0].metadata.uid|length)>0 and
([$pods[0].items[].metadata.uid]|unique|length)==3 and
all($pods[0].items[]; (.metadata.uid|type)=="string" and (.metadata.uid|length)>0) and
any($events[]; .objectRef.resource=="nodes" and .objectRef.subresource=="status" and
  .responseObject.metadata.uid==$node[0].metadata.uid and (.responseObject|ready)) and
($pods[0].items|length)==3 and all($pods[0].items[]; . as $pod |
  any($events[]; .objectRef.namespace=="controlplane-smoke" and .objectRef.resource=="pods" and
    .objectRef.subresource=="status" and .responseObject.metadata.uid==$pod.metadata.uid and
    .responseObject.status.phase=="Running" and (.responseObject|ready))) and
$first[0].metadata.uid==$last[0].metadata.uid and
$last[0].spec.renewTime>$first[0].spec.renewTime and
all([$first[0],$last[0]][]; . as $lease |
  any($events[]; .objectRef.namespace=="kube-node-lease" and .objectRef.resource=="leases" and
    (.objectRef.subresource//"")=="" and .responseObject.metadata.uid==$lease.metadata.uid and
    .responseObject.spec.renewTime==$lease.spec.renewTime and
    .responseObject.spec.holderIdentity==$lease.spec.holderIdentity))
