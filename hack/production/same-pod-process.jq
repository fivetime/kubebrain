# Input: {expected: Pod, current: Pod}. Identity only, NOT readiness, TLS,
# fault enforcement or RPC acceptance. Use jq -e; false/errors must reject.
# Readiness/startup-probe flags can change while the same process is isolated.
def nonempty: type == "string" and length > 0;
def valid:
 .apiVersion == "v1" and .kind == "Pod" and
 (.metadata.uid | nonempty) and (.metadata.name | nonempty) and
 (.metadata.namespace | nonempty) and .metadata.deletionTimestamp == null and
 (.spec | type == "object") and (.spec.nodeName | nonempty) and
 (.status.podIP | nonempty) and
 (.spec.containers | type == "array" and length > 0) and
 (.status.containerStatuses | type == "array" and length > 0) and
 ([.spec.containers[].name] | all(.[]; nonempty)) and
 ([.spec.containers[].name] | length == (unique | length)) and
 ([.spec.containers[].name] | sort) == ([.status.containerStatuses[].name] | sort) and
 all(.status.containerStatuses[];
  (.containerID | nonempty) and (.imageID | nonempty) and
  (.restartCount | type == "number" and . >= 0 and floor == .) and
  (.state | keys) == ["running"] and (.state.running.startedAt | nonempty));
def processes:
 [.status.containerStatuses[] | {name, containerID, imageID, restartCount,
   startedAt: .state.running.startedAt}] | sort_by(.name);
.expected as $before | .current as $after |
($before | valid) and ($after | valid) and
$before.metadata.uid == $after.metadata.uid and
$before.metadata.name == $after.metadata.name and
$before.metadata.namespace == $after.metadata.namespace and
$before.spec == $after.spec and $before.status.podIP == $after.status.podIP and
($before | processes) == ($after | processes)
