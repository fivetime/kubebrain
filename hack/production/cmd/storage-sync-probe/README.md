# Bounded storage synchronization diagnostic

This Linux-only command measures sequential append + `fdatasync` on a **dedicated
test volume**. It does not measure TiKV transactions, prove power-loss durability,
or pass a KubeBrain acceptance gate. It does not deploy a Pod or choose a
StorageClass for you.

For the `tk-001-003` environment use an isolated PVC from the consumer
`rook-ceph` StorageClass, never `rook-ceph-secondary`. Do not mount a TiKV/PD PVC
or use a database data directory. Validate the namespace, PVC/PV/StorageClass
identities and actual mounted volume before running. Compare identical block
sizes and sample counts on the relevant worker nodes; retain node, filesystem,
volume and image identity alongside JSON output. Do not compare this isolated
workload directly with a concurrent TiKV histogram population.

Build locally:

```sh
go build -o /absolute/private/tool-directory/storage-sync-probe ./hack/production/cmd/storage-sync-probe
```

Run **inside the prepared test-volume environment**, using its mount path:

```sh
storage-sync-probe --directory=/test-volume --allow-test-writes --count=256 --block-size=4096 --duration=30s
```

Both the explicit directory and write acknowledgement are mandatory. Defaults
append 1 MiB total; hard limits are 4096 pairs, 64 KiB per block, 64 MiB total,
and a soft deadline of at most one minute. Each block contains fresh random data
to avoid zero-fill compression. Random generation is outside write/sync timers.
Buffered write and Linux `fdatasync` are measured separately. There is no warm-up
or preallocation; initial allocation and filesystem metadata work can therefore
influence observations. JSON contains every successful pair and the nearest-rank
p99 of sync samples. This is **not** a Prometheus interpolated histogram p99.

The deadline and cancellation are checked between operations and after the final
sync. They cannot interrupt a blocked kernel I/O call: use an externally bounded
test Pod and retain evidence if it cannot terminate. Timeout/error is a nonzero
exit even when some samples completed. Never treat partial JSON as a passed run.

A new private subdirectory and exclusive file prevent overwriting existing
files. Normal/error cleanup removes that file and empty directory, never
recursively deletes a tree. Unexpected extra entries cause a cleanup error and
remain on disk. Forced termination may leave the private subdirectory behind;
identify the exact owned directory before removing it. Delete the temporary
test Pod/PVC using their recorded UIDs after collecting results, and remove the
local binary when it is no longer needed.

Tests exercise real local Linux `fdatasync`, rejected options, cancellation,
injected sync errors and non-recursive cleanup. Local test timing is not evidence
about a Kubernetes PVC or Ceph performance.
