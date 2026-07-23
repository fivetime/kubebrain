package namespaceinventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
)

var ConfigMapResource = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

const DefaultKey = "namespaces.json"
const MaxNamespaces = 256

func Load(
	ctx context.Context,
	client dynamic.Interface,
	namespace, name, key string,
) ([]string, error) {
	if err := ValidateOne(namespace); err != nil {
		return nil, fmt.Errorf("inventory namespace: %w", err)
	}
	if key == "" {
		key = DefaultKey
	}
	inventory, err := client.Resource(ConfigMapResource).Namespace(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read namespace inventory: %w", err)
	}
	data, found, err := unstructured.NestedStringMap(inventory.Object, "data")
	if err != nil {
		return nil, fmt.Errorf("read namespace inventory data: %w", err)
	}
	raw, foundKey := data[key]
	if !found || !foundKey {
		return nil, fmt.Errorf("namespace inventory is missing data key %q", key)
	}
	namespaces, err := parseJSONStringArray(raw)
	if err != nil {
		return nil, fmt.Errorf("decode namespace inventory: %w", err)
	}
	namespaces, err = Validate(namespaces)
	if err != nil {
		return nil, fmt.Errorf("validate namespace inventory: %w", err)
	}
	sort.Strings(namespaces)
	return namespaces, nil
}

func parseJSONStringArray(raw string) ([]string, error) {
	var values []string
	decoder := json.NewDecoder(strings.NewReader(raw))
	if err := decoder.Decode(&values); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("contains trailing JSON")
	}
	if values == nil {
		return nil, errors.New("must be a JSON string array, not null")
	}
	return values, nil
}

func ValidateOne(namespace string) error {
	if namespace == "" {
		return errors.New("namespace must not be empty")
	}
	if problems := validation.IsDNS1123Label(namespace); len(problems) != 0 {
		return errors.New("invalid namespace " + namespace + ": " + problems[0])
	}
	return nil
}

func Validate(namespaces []string) ([]string, error) {
	if len(namespaces) == 0 {
		return nil, errors.New("namespace allowlist must not be empty")
	}
	if len(namespaces) > MaxNamespaces {
		return nil, fmt.Errorf("namespace allowlist exceeds %d entries", MaxNamespaces)
	}
	result := make([]string, 0, len(namespaces))
	seen := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		if err := ValidateOne(namespace); err != nil {
			if namespace == "" {
				return nil, errors.New("namespace allowlist contains an empty value")
			}
			return nil, err
		}
		if _, duplicate := seen[namespace]; duplicate {
			return nil, errors.New("namespace allowlist contains duplicate " + namespace)
		}
		seen[namespace] = struct{}{}
		result = append(result, namespace)
	}
	return result, nil
}
