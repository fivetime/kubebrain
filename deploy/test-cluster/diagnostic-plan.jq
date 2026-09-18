# Offline only: jq -er -f diagnostic-plan.jq.
# Input: mode enable|restore, baseline, current, namespace, namespace_uid, info_dns.
# Baseline is an operator-audited full spec (possibly with peer protocol enabled).
# No image, peer, election, storage, Service or NetworkPolicy changes are allowed.
# Caller must verify certificate contents and unauthenticated pprof rejection.
def need($ok;$why): if $ok then . else error($why) end;
def text: type=="string" and length>0;
def active: .metadata.deletionTimestamp==null;
def protected_probe($path;$seconds;$dns):
 need(.httpGet=={path:$path,port:"info",scheme:"HTTPS"} and
      .exec==null and .tcpSocket==null and .grpc==null; "unexpected health probe") |
 del(.httpGet) |
 .exec={command:["curl","--disable","--silent","--show-error","--fail","--retry","0",
  "--max-time",$seconds,"--noproxy","*","--proto","=https","--resolve",
  ($dns+":8080:127.0.0.1"),"--cacert","/etc/kubebrain/info-tls/ca.crt",
  "--cert","/etc/kubebrain/client-tls/tls.crt","--key","/etc/kubebrain/client-tls/tls.key",
  ("https://"+$dns+":8080"+$path)]};
. as $in |
need(.mode=="enable" or .mode=="restore";"unknown diagnostic mode") |
need((.namespace_uid|text) and .namespace.kind=="Namespace" and (.namespace|active) and
     .namespace.metadata.uid==.namespace_uid;"namespace identity changed") |
need((.info_dns|type)=="string" and (.info_dns|test("^[a-z0-9][a-z0-9.-]*$"));"invalid info DNS") |
.baseline as $base | .current as $live |
need(all([$base,$live][]; .apiVersion=="apps/v1" and .kind=="StatefulSet" and (.|active) and
     (.metadata.uid|text) and (.metadata.resourceVersion|text)) and
     $base.metadata.uid==$live.metadata.uid and $base.metadata.name==$live.metadata.name and
     $base.metadata.namespace==$in.namespace.metadata.name and
     $live.metadata.namespace==$in.namespace.metadata.name;"StatefulSet identity changed") |
need($base.spec.replicas==3 and $base.spec.updateStrategy.type=="RollingUpdate" and
     ($base.spec.updateStrategy.rollingUpdate.partition//0)==0 and
     ($base.spec.template.spec.containers|length)==1;"unsupported rollout layout") |
$base.spec.template.spec.containers[0] as $c |
need($c.name=="kubebrain" and
     ([$c.args[]|select(startswith("--enable-pprof") or startswith("--info-client-cert-auth") or
      startswith("--info-trusted-ca-file") or startswith("--info-allow-insecure"))])==["--enable-pprof=false"] and
     ([$c.args[]|select(startswith("--info-port") or startswith("--info-cert-file") or startswith("--info-key-file"))]|sort)==
       (["--info-port=8080","--info-cert-file=/etc/kubebrain/info-tls/tls.crt","--info-key-file=/etc/kubebrain/info-tls/tls.key"]|sort);
     "unreviewed diagnostic or info TLS arguments") |
need($c.livenessProbe.timeoutSeconds==2 and $c.startupProbe.timeoutSeconds==2 and
     $c.readinessProbe.timeoutSeconds==6;"unexpected health timeout budgets") |
need(all(["info-tls","client-tls"][]; . as $name |
     ([$c.volumeMounts[]|select(.name==$name and .mountPath==("/etc/kubebrain/"+$name) and .readOnly==true)]|length)==1);
     "missing read-only certificate mounts") |
($base.spec | .template.spec.containers[0] |= (
 .args |= (map(if .=="--enable-pprof=false" then "--enable-pprof=true" else . end)+
  ["--info-client-cert-auth=true","--info-trusted-ca-file=/etc/kubebrain/info-tls/ca.crt"]) |
 .livenessProbe |= protected_probe("/ping";"1";$in.info_dns) |
 .startupProbe |= protected_probe("/ping";"1";$in.info_dns) |
 .readinessProbe |= protected_probe("/ready";"5";$in.info_dns))) as $enabled |
need($live.spec==$base.spec or $live.spec==$enabled;"refusing unknown diagnostic spec drift") |
(if .mode=="enable" then $enabled else $base.spec end) as $desired |
(if $live.spec==$desired then [] else
 need(.mode=="restore" or ($live.status.observedGeneration==$live.metadata.generation and
      $live.status.readyReplicas==3 and $live.status.updatedReplicas==3 and
      ($live.status.currentRevision|text) and $live.status.currentRevision==$live.status.updateRevision);
      "diagnostic enable requires observed ready baseline") |
 [{op:"test",path:"/metadata/uid",value:$live.metadata.uid},
  {op:"test",path:"/metadata/resourceVersion",value:$live.metadata.resourceVersion},
  {op:"test",path:"/spec",value:$live.spec},
  {op:"replace",path:"/spec/template",value:$desired.template}]
 end) |
tojson | gsub("&";"\\u0026") | gsub("<";"\\u003c") | gsub(">";"\\u003e") |
gsub("\u2028";"\\u2028") | gsub("\u2029";"\\u2029")
