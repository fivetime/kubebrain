# Same Deployment/ReplicaSet; exactly two survivors and one new ready Pod.
def uid: .metadata.uid;
def nonempty: type=="string" and length>0;
($before[0].items|map(uid)|sort) as $old |
($pods[0].items|map(uid)|sort) as $new |
$victim[0].metadata.uid as $deleted |
$created[0].metadata.uid as $duid |
$originalRS[0].items[0].metadata.uid as $ruid |
($old|length)==3 and ($old|unique|length)==3 and all($old[];nonempty) and
($old|index($deleted))!=null and ($new|length)==3 and ($new|unique|length)==3 and all($new[];nonempty) and
($new|index($deleted))==null and (($new-$old)|length)==1 and (($old-$new)==[$deleted]) and
($duid|nonempty) and ($ruid|nonempty) and ($rs[0].items|length)==1 and
$rs[0].items[0].metadata.uid==$ruid and $rs[0].items[0].spec.replicas==3 and
any($rs[0].items[0].metadata.ownerReferences[]?;.controller==true and .kind=="Deployment" and .uid==$duid) and
$deployment[0].metadata.uid==$duid and $deployment[0].spec.replicas==3 and
$deployment[0].status.observedGeneration==$deployment[0].metadata.generation and
$deployment[0].status.readyReplicas==3 and $deployment[0].status.availableReplicas==3 and
all($pods[0].items[];.metadata.deletionTimestamp==null and .spec.nodeName=="reference-node" and
  any(.metadata.ownerReferences[]?;.controller==true and .kind=="ReplicaSet" and .uid==$ruid) and
  .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True") and
  any(.status.conditions[]?;.type=="PodScheduled" and .status=="True"))
