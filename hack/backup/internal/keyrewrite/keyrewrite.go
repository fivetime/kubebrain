package keyrewrite

import (
	"errors"
	"fmt"
	"strings"
)

func Rewrite(key []byte, from, to string) []byte {
	if from == "" {
		return key
	}
	keyText := string(key)
	if !strings.HasPrefix(keyText, from) {
		return key
	}
	return []byte(to + strings.TrimPrefix(keyText, from))
}

func RewriteUnique(key []byte, from, to string, seen map[string]struct{}) ([]byte, error) {
	target := Rewrite(key, from, to)
	if len(target) == 0 {
		return nil, errors.New("key rewrite produced an empty target key")
	}
	if _, exists := seen[string(target)]; exists {
		return nil, fmt.Errorf("key rewrite produced duplicate target key %q", target)
	}
	seen[string(target)] = struct{}{}
	return target, nil
}
