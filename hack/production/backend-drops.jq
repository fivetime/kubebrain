# Reviewed Cilium v1.19.4 capture schema. Input: one parsed drop event.
# Caller binds interval, agent/Pod/process, policy realization and backend IPs.
# Args: endpoint (number), source_ip (string), backends [{ip,port}] (strings).
# Matching packets do not prove all connections denied, term loss or RPC outcome.
if ($endpoint|type)!="number" or $endpoint<1 or $endpoint>65535 or
   ($endpoint|floor)!=$endpoint or ($source_ip|type)!="string" or $source_ip=="" or
   ($backends|type)!="array" or ($backends|length)==0 or
   (all($backends[]; (.ip|type)=="string" and .ip!="" and
      (.port=="2379" or .port=="20160"))|not)
then error("invalid endpoint/backend binding") else . end |
select(type=="object") |
select(.type=="drop" and .source==$endpoint and
       (.reason=="Policy denied" or .reason=="Policy denied by denylist")) |
select((.summary|type)=="object" and (.summary.tcp|type)=="string" and
       (.summary.tcp|length)>0 and .summary.udp==null and .summary.sctp==null) |
select(.summary.l3.src==$source_ip) |
. as $event |
select(any($backends[]; .ip==$event.summary.l3.dst and .port==$event.summary.l4.dst)) |
{scope:"matching_backend_policy_drop_only", endpoint:.source, reason,
 source_ip:.summary.l3.src, destination_ip:.summary.l3.dst,
 source_port:.summary.l4.src, destination_port:.summary.l4.dst,
 all_connections_denied_proven:false, term_loss_proven:false}
