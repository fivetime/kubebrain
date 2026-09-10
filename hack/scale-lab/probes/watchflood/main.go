// watchflood drives read-only Kubernetes list/watch load, not a correctness proof.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type page struct{ revision, next string }
type listFunc func(context.Context, metav1.ListOptions) (page, error)
type watchFunc func(context.Context, metav1.ListOptions) (watch.Interface, error)
type counters struct{ events, bookmarks, reconnects, lists, listMS, failures atomic.Int64 }

// Even pod watchers consume every page; odd watchers and node watchers only
// obtain a starting revision. Neither mode is described as an informer cache.
func listRevision(ctx context.Context, list listFunc, full bool) (string, error) {
	options := metav1.ListOptions{Limit: 1}
	if full {
		options.Limit = 5000
	}
	var revision string
	for {
		p, err := list(ctx, options)
		if err != nil {
			return "", err
		}
		if p.revision == "" {
			return "", errors.New("list has no resourceVersion")
		}
		if revision != "" && revision != p.revision {
			return "", errors.New("list resourceVersion changed across pages")
		}
		revision = p.revision
		if !full || p.next == "" {
			return revision, nil
		}
		if p.next == options.Continue {
			return "", errors.New("list continuation did not advance")
		}
		options.Continue = p.next
	}
}

func drain(ctx context.Context, stream watch.Interface, c *counters) error {
	defer stream.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-stream.ResultChan():
			if !ok {
				return nil
			}
			switch event.Type {
			case watch.Error:
				return errors.New("watch error event (including expiration/compaction)")
			case watch.Bookmark:
				c.bookmarks.Add(1)
			case watch.Added, watch.Modified, watch.Deleted:
				c.events.Add(1)
			default:
				return fmt.Errorf("unexpected watch event type %q", event.Type)
			}
		}
	}
}

func watchLoop(ctx context.Context, list listFunc, open watchFunc, full bool, c *counters) {
	for ctx.Err() == nil {
		started := time.Now()
		listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		rv, err := listRevision(listCtx, list, full)
		cancel()
		if err == nil {
			c.lists.Add(1)
			c.listMS.Add(time.Since(started).Milliseconds())
			var stream watch.Interface
			stream, err = open(ctx, metav1.ListOptions{ResourceVersion: rv, AllowWatchBookmarks: true})
			if err == nil {
				err = drain(ctx, stream, c)
			}
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.failures.Add(1)
		}
		c.reconnects.Add(1)
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func run() error {
	kubeconfig := flag.String("kubeconfig", "", "required explicit kubeconfig file (no ambient fallback)")
	contextName := flag.String("context", "", "required kubeconfig context")
	namespace := flag.String("namespace", "", "namespace for pod list/watch")
	all := flag.Bool("all-namespaces", false, "explicitly permit cluster-wide pod list/watch")
	allowed := flag.Bool("allow-load", false, "confirm read-only load against the selected test cluster")
	pods := flag.Int("watchers", 50, "concurrent pod watchers")
	nodes := flag.Int("node-watchers", 10, "concurrent cluster-scoped node watchers")
	duration := flag.Duration("duration", 5*time.Minute, "bounded run duration")
	flag.Parse()
	if flag.NArg() != 0 || !*allowed || *kubeconfig == "" || *contextName == "" || *duration <= 0 || *pods < 0 || *nodes < 0 || *pods > 10000 || *nodes > 10000 || *pods+*nodes == 0 {
		return errors.New("require allow-load, kubeconfig, context, positive duration and 1..20000 total watchers (<=10000 each)")
	}
	if *pods > 0 && ((*namespace == "" && !*all) || (*namespace != "" && *all)) {
		return errors.New("choose exactly one of namespace or all-namespaces for pod watches")
	}
	loading := &clientcmd.ClientConfigLoadingRules{ExplicitPath: *kubeconfig}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, &clientcmd.ConfigOverrides{CurrentContext: *contextName}).ClientConfig()
	if err != nil {
		return err
	}
	config.QPS = 5000
	config.Burst = 10000
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(parent, *duration)
	defer cancel()
	var stats counters
	var workers sync.WaitGroup
	podClient := client.CoreV1().Pods(*namespace)
	nodeClient := client.CoreV1().Nodes()
	podList := func(ctx context.Context, opts metav1.ListOptions) (page, error) {
		r, err := podClient.List(ctx, opts)
		if err != nil {
			return page{}, err
		}
		return page{r.ResourceVersion, r.Continue}, nil
	}
	nodeList := func(ctx context.Context, opts metav1.ListOptions) (page, error) {
		r, err := nodeClient.List(ctx, opts)
		if err != nil {
			return page{}, err
		}
		return page{r.ResourceVersion, r.Continue}, nil
	}
	for i := 0; i < *pods+*nodes; i++ {
		list, open, full := podList, watchFunc(podClient.Watch), i%2 == 0
		if i >= *pods {
			list, open, full = nodeList, nodeClient.Watch, false
		}
		workers.Add(1)
		go func() { defer workers.Done(); watchLoop(ctx, list, open, full, &stats) }()
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
waiting:
	for {
		select {
		case <-done:
			break waiting
		case <-tick.C:
			fmt.Printf("PROGRESS events=%d bookmarks=%d lists=%d reconnects=%d errs=%d\n", stats.events.Load(), stats.bookmarks.Load(), stats.lists.Load(), stats.reconnects.Load(), stats.failures.Load())
		}
	}
	// All watches have stopped before taking the final snapshot.
	lists := stats.lists.Load()
	var average int64
	if lists > 0 {
		average = stats.listMS.Load() / lists
	}
	fmt.Printf("FINAL events=%d bookmarks=%d reconnects=%d lists=%d avgListMs=%d errs=%d completeness_proven=false\n", stats.events.Load(), stats.bookmarks.Load(), stats.reconnects.Load(), lists, average, stats.failures.Load())
	if parent.Err() != nil {
		return parent.Err()
	}
	if lists == 0 || stats.failures.Load() != 0 {
		return errors.New("list/watch load encountered errors or completed no lists")
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "watchflood:", err)
		os.Exit(1)
	}
}
