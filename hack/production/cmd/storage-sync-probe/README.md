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
storage-sync-probe --directory=/test-volume --allow-test-writes --mode=append --count=256 --block-size=4096 --duration=30s
```

Both the explicit directory and write acknowledgement are mandatory. Defaults
append 1 MiB total; hard limits are 4096 pairs, 64 KiB per block, 64 MiB total (including preparation),
and a soft deadline of at most one minute. Each block contains fresh random data
to avoid zero-fill compression. Random generation is outside write/sync timers.
Buffered write and Linux `fdatasync` are measured separately. Default `append`
has no warm-up or preallocation; initial allocation and filesystem metadata work
can therefore influence observations. JSON contains every successful pair and the nearest-rank
p99 of sync samples. This is **not** a Prometheus interpolated histogram p99.

For a controlled no-file-growth comparison, select `--mode=overwrite` with the
same count and block size on the same isolated volume. This first writes fresh
random data over the full file, performs one preparation `fdatasync`, seeks to
offset zero, then measures fresh writes over those initialized blocks. Default
overwrite mode writes 1 MiB during preparation and 1 MiB during measurement.
Preparation bytes/time are reported separately and never included in per-pair
samples or the sync mean/p99. Preparation consumes the same overall soft deadline
and 64 MiB write budget; a failed or cancelled preparation produces no measured
samples. Each invocation still creates its own exclusive private file.

This removes file growth from the measured overwrite operations, not all metadata
work, caching effects or shared storage contention. It is not TiKV's precise log
allocation algorithm. For actual comparisons record ordering and repeat both
modes with a balanced order if permitted; do not attribute an unpaired change
entirely to allocation or compare different nodes as an allocation experiment.

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
