# Input: slurped audit events; snapshots passed with --slurpfile rs/pods.
# generateName POSTs have no objectRef.name; use the successful response identity.
def successful: .stage=="ResponseComplete" and .verb=="create" and
  .objectRef.namespace=="controlplane-smoke" and
  .responseStatus.code>=200 and .responseStatus.code<300;
def creates($resource): [.[] | select(successful and .objectRef.resource==$resource and .objectRef.subresource==null) |
  [.user.username,.responseObject.metadata.name,.responseObject.metadata.uid]] | sort;
([ $rs[0].items[] | ["system:serviceaccount:kube-system:deployment-controller",.metadata.name,.metadata.uid]]|sort) as $expected_rs |
([ $pods[0].items[] | ["system:serviceaccount:kube-system:replicaset-controller",.metadata.name,.metadata.uid]]|sort) as $expected_pods |
([ $pods[0].items[] | ["system:kube-scheduler",.metadata.name,.metadata.uid]]|sort) as $expected_bindings |
if ($expected_rs|length)!=1 or ($expected_pods|length)!=3 or
   any(($expected_rs+$expected_pods)[]; any(.[]; type!="string" or length==0)) then
  error("invalid expected object identities")
elif creates("replicasets")!=$expected_rs or creates("pods")!=$expected_pods or
  ([.[]|select(successful and .objectRef.resource=="pods" and .objectRef.subresource=="binding")|
    [.user.username,.objectRef.name,.objectRef.uid]]|sort)!=$expected_bindings then
  error("controller or scheduler audit does not match observed objects")
else {controller_creation_and_binding:"passed", replicasets:1, pods:3} end
