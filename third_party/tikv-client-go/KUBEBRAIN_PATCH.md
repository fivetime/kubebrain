# KubeBrain client-go patch

This directory is an otherwise unchanged copy of
`github.com/tikv/client-go/v2@v2.0.7`.

The module also pins `toolchain go1.26.5` so its standalone CI build, vet,
test, and vulnerability scan use the same audited compiler as KubeBrain.

KubeBrain changes `internal/unionstore.memdbNode.klen` from `uint16` to
`uint32`, and adjusts the arena allocation header size accordingly. The
upstream type silently wraps keys larger than 65535 bytes: a transaction can
report a successful commit while persisting only `len(key) mod 65536` bytes.
KubeBrain embeds etcd user keys in several physical TiKV keys, while etcd's
default request limit permits substantially larger keys.

The storage adapter retains an independent 2 MiB preflight guard matching the
managed TiKV configuration, so deployment/configuration drift fails before a
transaction reaches TiKV.
