// Package restorationfence defines the persistent coordination keys shared by
// KubeBrain writers and native-restore tooling.
package restorationfence

import "fmt"

const (
	Open       = "open"
	ShardCount = 256
)

func ControlKey(prefix string) []byte {
	return []byte(fmt.Sprintf("%s/restoration-fence", prefix))
}

func ShardKey(prefix string, shard uint64) []byte {
	return []byte(fmt.Sprintf("%s/restoration-fence-shard/%02x", prefix, shard%ShardCount))
}

func AllKeys(prefix string) [][]byte {
	keys := make([][]byte, 0, ShardCount+1)
	keys = append(keys, ControlKey(prefix))
	for shard := uint64(0); shard < ShardCount; shard++ {
		keys = append(keys, ShardKey(prefix, shard))
	}
	return keys
}
