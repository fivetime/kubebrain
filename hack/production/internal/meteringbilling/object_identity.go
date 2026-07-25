package meteringbilling

import (
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

func validObjectScopeValue(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	return strings.IndexFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) == -1
}

func validRelativeObjectPrefix(value string) bool {
	if !validObjectScopeValue(value) || strings.HasPrefix(value, "/") {
		return false
	}
	trimmed := strings.TrimSuffix(value, "/")
	if trimmed == "" || trimmed == "." || trimmed == ".." || strings.HasPrefix(trimmed, "../") {
		return false
	}
	return path.Clean(trimmed) == trimmed
}

func validObjectIdentity(objectStoreID, bucket string, prefixes ...string) bool {
	if !validObjectScopeValue(objectStoreID) || !validObjectScopeValue(bucket) {
		return false
	}
	for _, prefix := range prefixes {
		if !validRelativeObjectPrefix(prefix) {
			return false
		}
	}
	return true
}
