# jq -Rn. Preserve parsed Cilium drop events and the recognized startup record.
# Framing only: caller bounds input size/runtime and authenticates capture.
def startup:
 test("^time=[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,9})?Z level=info msg=\"Initializing dissection cache\\.\\.\\.\"$");
reduce inputs as $line ({events:[],startup_logs:[],lines:0};
 .lines += 1 |
 if ($line|startup) then
  if .lines==1 then .startup_logs += [$line]
  else error("unexpected repeated or late startup record") end
 else
  ($line|fromjson) as $event |
  if ($event|type)=="object" and $event.type=="drop" and
     ($event.source|type)=="number" and $event.source>=0 and
     $event.source<=65535 and ($event.source|floor)==$event.source and
     ($event.reason|type)=="string" and ($event.reason|length)>0 and
     ($event.summary|type)=="object" then
   .events += [$event]
  else error("invalid drop event envelope") end
 end
) |
if (.events|length)==0 then error("no drop events captured")
else . + {scope:"monitor_stream_framing_only",packet_enforcement_proven:false} end
