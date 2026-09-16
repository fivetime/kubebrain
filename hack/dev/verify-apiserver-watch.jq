# Applied only after the watch writer has stopped. Waiting-loop event counts are
# progress hints, not integrity evidence. Compare decimal revisions as strings
# to avoid rounding revisions above 2^53 in jq's number representation.
def valid_revision: type == "string" and test("^[1-9][0-9]*$");
def revision_after($a; $b):
  ($a|length) > ($b|length) or (($a|length) == ($b|length) and $a > $b);
if ($objects < 1 or $updates < 1 or $namespace == "") then
  error("invalid expected watch dimensions")
else . end |
map(select(.type == "MODIFIED")) as $modified |
([range(1; $objects+1) as $id | range(1; $updates+1) as $v |
  ["soak-\($id)", ($v|tostring)]] | sort) as $expected |
($expected | map(.[0]) | unique) as $names |
([$modified[] | [.object.metadata.name, .object.data.version]] | sort) as $actual |
([$modified[] | .object.metadata.resourceVersion]) as $revisions |
map(select(.type == "ADDED" or .type == "MODIFIED")) as $data |
if any(.[]; .type != "ADDED" and .type != "MODIFIED" and .type != "BOOKMARK") then
  error("unexpected watch event type")
elif $actual != $expected then
  error("missing, duplicate, or unexpected object/version pair")
elif any($data[]; .object.metadata.name as $name | ($names | index($name)) == null) then
  error("unexpected watched object name")
elif any($data[]; .object.kind != "ConfigMap" or .object.metadata.namespace != $namespace
  or (.object.metadata.uid | type != "string" or length == 0)
  or (.object.metadata.resourceVersion | valid_revision | not)) then
  error("invalid watched object identity or revision")
elif any(($data | group_by(.object.metadata.name))[];
  ([.[].object.metadata.uid] | unique | length) != 1) then
  error("object UID changed during watch")
elif (all(range(1; ($revisions|length)); revision_after($revisions[.]; $revisions[.-1])) | not) then
  error("MODIFIED revisions are not strictly increasing")
else {objects: $objects, updates: $updates, modified_events: ($modified|length), integrity: "passed"}
end
