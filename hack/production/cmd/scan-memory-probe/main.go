package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/dynamicpagination"
	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const (
	formatVersion = "kubebrain.scan-memory-probe.v1"
	cgroupRoot    = "/sys/fs/cgroup"
)

type namespaceFlags []string

func (f *namespaceFlags) String() string { return strings.Join(*f, ",") }
func (f *namespaceFlags) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type cgroupSnapshot struct {
	Current  uint64 `json:"current_bytes"`
	Peak     uint64 `json:"peak_bytes"`
	Limit    uint64 `json:"limit_bytes"`
	OOM      uint64 `json:"oom_events"`
	OOMKill  uint64 `json:"oom_kill_events"`
	OOMGroup uint64 `json:"oom_group_kill_events"`
}

type result struct {
	Format           string `json:"format"`
	Namespaces       int    `json:"namespaces"`
	Items            int    `json:"items"`
	ChargedBytes     int64  `json:"charged_bytes"`
	PageLimit        int64  `json:"page_limit"`
	MaxItems         int64  `json:"max_items"`
	MaxBytes         int64  `json:"max_bytes"`
	LabelSelector    string `json:"label_selector"`
	MemoryLimitBytes uint64 `json:"memory_limit_bytes"`
	MemoryPeakBytes  uint64 `json:"memory_peak_bytes"`
	MemoryEndBytes   uint64 `json:"memory_end_bytes"`
	HeapAllocBytes   uint64 `json:"heap_alloc_bytes"`
	HeapSysBytes     uint64 `json:"heap_sys_bytes"`
	NumGC            uint32 `json:"num_gc"`
	PauseTotalNS     uint64 `json:"pause_total_ns"`
	OOMEvents        uint64 `json:"oom_events_delta"`
	OOMKillEvents    uint64 `json:"oom_kill_events_delta"`
	OOMGroupEvents   uint64 `json:"oom_group_kill_events_delta"`
	ElapsedMillis    int64  `json:"elapsed_millis"`
}

var retained []*unstructured.Unstructured

func main() {
	var namespaces namespaceFlags
	var pageLimit, maxItems, maxBytes, expectedLimit int64
	var labelSelector string
	var timeout time.Duration
	flag.Var(&namespaces, "namespace", "namespace to scan; repeat for multiple namespaces")
	flag.Int64Var(&pageLimit, "page-limit", 500, "Kubernetes objects requested per page")
	flag.Int64Var(&maxItems, "max-items", 10_000, "aggregate object count budget")
	flag.Int64Var(&maxBytes, "max-bytes", 64<<20, "aggregate serialized object byte budget")
	flag.Int64Var(&expectedLimit, "expected-memory-limit-bytes", 512<<20, "required cgroup v2 memory.max")
	flag.StringVar(&labelSelector, "label-selector", "", "required selector limiting the synthetic scan set")
	flag.DurationVar(&timeout, "timeout", 2*time.Minute, "overall scan deadline")
	flag.Parse()
	if err := validate(namespaces, labelSelector, pageLimit, maxItems, maxBytes, expectedLimit, timeout); err != nil {
		log.Fatal(err)
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	before, err := readCgroup(cgroupRoot)
	if err != nil {
		log.Fatal(err)
	}
	if before.Limit != uint64(expectedLimit) {
		log.Fatalf("cgroup memory.max=%d, expected %d", before.Limit, expectedLimit)
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	items, charged, err := scan(ctx, client, namespaces, labelSelector, pageLimit, maxItems, maxBytes)
	if err != nil {
		log.Fatal(err)
	}
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	after, err := readCgroup(cgroupRoot)
	if err != nil {
		log.Fatal(err)
	}
	if after.OOM != before.OOM || after.OOMKill != before.OOMKill || after.OOMGroup != before.OOMGroup {
		log.Fatalf("cgroup OOM counters changed: before=%+v after=%+v", before, after)
	}
	output := result{
		Format: formatVersion, Namespaces: len(namespaces), Items: items, ChargedBytes: charged,
		PageLimit: pageLimit, MaxItems: maxItems, MaxBytes: maxBytes,
		LabelSelector:    labelSelector,
		MemoryLimitBytes: after.Limit, MemoryPeakBytes: after.Peak, MemoryEndBytes: after.Current,
		HeapAllocBytes: memory.HeapAlloc, HeapSysBytes: memory.HeapSys, NumGC: memory.NumGC,
		PauseTotalNS: memory.PauseTotalNs, OOMEvents: after.OOM - before.OOM,
		OOMKillEvents: after.OOMKill - before.OOMKill, OOMGroupEvents: after.OOMGroup - before.OOMGroup,
		ElapsedMillis: time.Since(started).Milliseconds(),
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(output); err != nil {
		log.Fatal(err)
	}
}

func validate(namespaces []string, labelSelector string, pageLimit, maxItems, maxBytes, expectedLimit int64, timeout time.Duration) error {
	if len(namespaces) < 2 || len(namespaces) > 256 {
		return errors.New("2..256 --namespace values are required")
	}
	if labelSelector == "" {
		return errors.New("--label-selector is required")
	}
	if _, err := labels.Parse(labelSelector); err != nil {
		return fmt.Errorf("label selector: %w", err)
	}
	seen := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		if err := namespaceinventory.ValidateOne(namespace); err != nil {
			return err
		}
		if _, exists := seen[namespace]; exists {
			return fmt.Errorf("duplicate namespace %s", namespace)
		}
		seen[namespace] = struct{}{}
	}
	if pageLimit <= 0 || maxItems <= 0 || maxBytes <= 0 || expectedLimit <= 0 || timeout <= 0 || pageLimit >= maxItems {
		return errors.New("positive page/max/memory/time budgets with page-limit < max-items are required")
	}
	return nil
}

func scan(ctx context.Context, client dynamic.Interface, namespaces []string, labelSelector string, pageLimit, maxItems, maxBytes int64) (int, int64, error) {
	resource := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	retained = make([]*unstructured.Unstructured, 0)
	var charged int64
	for _, namespace := range namespaces {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		items, err := dynamicpagination.All(ctx, client.Resource(resource).Namespace(namespace), metav1.ListOptions{LabelSelector: labelSelector}, pageLimit, maxItems, maxBytes)
		if err != nil {
			return 0, 0, fmt.Errorf("scan namespace %s: %w", namespace, err)
		}
		for i := range items {
			if int64(len(retained)) >= maxItems {
				return 0, 0, fmt.Errorf("aggregate objects exceed %d items", maxItems)
			}
			charged, err = dynamicpagination.Charge(charged, maxBytes, &items[i])
			if err != nil {
				return 0, 0, fmt.Errorf("charge namespace %s object %s: %w", namespace, items[i].GetName(), err)
			}
			retained = append(retained, items[i].DeepCopy())
		}
	}
	return len(retained), charged, nil
}

func readCgroup(root string) (cgroupSnapshot, error) {
	readValue := func(name string) (uint64, error) {
		raw, err := os.ReadFile(root + "/" + name)
		if err != nil {
			return 0, err
		}
		value := strings.TrimSpace(string(raw))
		if value == "max" {
			return 0, fmt.Errorf("%s is unlimited", name)
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %s: %w", name, err)
		}
		return parsed, nil
	}
	current, err := readValue("memory.current")
	if err != nil {
		return cgroupSnapshot{}, err
	}
	peak, err := readValue("memory.peak")
	if err != nil {
		return cgroupSnapshot{}, err
	}
	limit, err := readValue("memory.max")
	if err != nil {
		return cgroupSnapshot{}, err
	}
	raw, err := os.ReadFile(root + "/memory.events")
	if err != nil {
		return cgroupSnapshot{}, err
	}
	events := map[string]uint64{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return cgroupSnapshot{}, errors.New("invalid cgroup memory.events")
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return cgroupSnapshot{}, fmt.Errorf("parse memory.events %s: %w", fields[0], err)
		}
		events[fields[0]] = value
	}
	for _, name := range []string{"oom", "oom_kill", "oom_group_kill"} {
		if _, exists := events[name]; !exists {
			return cgroupSnapshot{}, fmt.Errorf("memory.events lacks %s", name)
		}
	}
	return cgroupSnapshot{Current: current, Peak: peak, Limit: limit, OOM: events["oom"], OOMKill: events["oom_kill"], OOMGroup: events["oom_group_kill"]}, nil
}

func init() { log.SetFlags(0) }
