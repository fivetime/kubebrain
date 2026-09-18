# jq -Rs --arg source IMAGE_GIT_SHA --arg source_file_sha256 FROZEN_LEASE_GO_SHA
# Classifies candidate frames only. Caller must establish complete HTTP capture,
# Pod/container identity and one original pending RPC; this cannot identify a lease.
# Source approval is not image/CI admission; those are separate caller gates.
def approved:
 {"02786d91ff406fe50d72e9baebdf2963b74b702c":
   {line:1645,sha256:"3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88"},
  "da303fdbd460ee42f3aa158fac27c396faf6b58f":
   {line:1645,sha256:"3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88"},
  "2ad79751ebc35291ed8caac144a6e73442b927a6":
   {line:1645,sha256:"3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88"}};
(approved[$source] // error("unapproved image source for wait-site classification")) as $binding |
if $binding.sha256 != $source_file_sha256 then
 error("frozen lease source hash mismatch")
elif length==0 or length>=8388608 or (startswith("goroutine ") and endswith("\n"))!=true then
 error("invalid or potentially truncated debug2 capture")
else
 [split("\n\n")[] | select(length>0) | split("\n") |
  select(length>=3) |
  select(.[0]|test("^goroutine [0-9]+ \\[select(, [^]]+)?\\]:$")) |
  select(.[1]|startswith("github.com/kubewharf/kubebrain/pkg/server/etcd.(*leaseManager).refreshLeaseHoldingLocks(")) |
  select(.[2]|test("/pkg/server/etcd/lease[.]go:"+($binding.line|tostring)+"( |$)")) |
  {goroutine_id:(.[0]|capture("^goroutine (?<id>[0-9]+) ").id),state:.[0],frame:.[1],location:.[2],source:$source,source_file_sha256:$source_file_sha256}]
end
