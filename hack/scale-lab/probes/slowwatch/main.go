// slowwatch deliberately consumes etcd watch batches slowly. It checks observed
// revision ordering, not event completeness (which requires a known writer).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type observations struct{ events, batches, lastRevision int64 }

func (s *observations) record(r clientv3.WatchResponse) error {
	if err := r.Err(); err != nil {
		return err
	}
	if r.Canceled {
		return errors.New("watch canceled")
	}
	for _, event := range r.Events {
		if event == nil || event.Kv == nil || event.Kv.ModRevision <= 0 {
			return errors.New("invalid watch event")
		}
		if event.Kv.ModRevision < s.lastRevision {
			return fmt.Errorf("revision regression: %d -> %d", s.lastRevision, event.Kv.ModRevision)
		}
		s.lastRevision = event.Kv.ModRevision
		s.events++
	}
	if len(r.Events) > 0 {
		s.batches++
	}
	return nil
}

func consume(ctx context.Context, ch clientv3.WatchChan, delay time.Duration, s *observations) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case response, ok := <-ch:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return errors.New("watch closed before deadline")
			}
			if err := s.record(response); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			if len(response.Events) > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
		}
	}
}

func run() error {
	ep := flag.String("ep", "", "explicit http(s) KubeBrain endpoint")
	prefix := flag.String("prefix", "", "explicit key prefix to observe (read-only)")
	delay := flag.Duration("delay", 50*time.Millisecond, "delay per nonempty watch batch")
	duration := flag.Duration("duration", 3*time.Minute, "observation duration")
	minimum := flag.Int64("min-events", 1, "minimum observed events; zero explicitly allows idle observations")
	ca := flag.String("cacert", "", "trusted CA file for HTTPS")
	cert := flag.String("cert", "", "client certificate for mTLS")
	key := flag.String("key", "", "client private key for mTLS")
	flag.Parse()
	u, err := url.Parse(*ep)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("ep must be an explicit http(s) endpoint without credentials, query, or path")
	}
	if flag.NArg() != 0 || *prefix == "" || *duration <= 0 || *delay < 0 || *minimum < 0 {
		return errors.New("require prefix, positive duration, nonnegative delay and min-events")
	}
	if (*cert == "") != (*key == "") {
		return errors.New("cert and key must be provided together")
	}
	if u.Scheme != "https" && (*ca != "" || *cert != "") {
		return errors.New("TLS files require HTTPS")
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(parent, *duration)
	defer cancel()
	config := clientv3.Config{Endpoints: []string{*ep}, DialTimeout: 5 * time.Second, Context: ctx}
	if u.Scheme == "https" {
		config.TLS, err = (transport.TLSInfo{TrustedCAFile: *ca, CertFile: *cert, KeyFile: *key}).ClientConfig()
		if err != nil {
			return err
		}
	}
	client, err := clientv3.New(config)
	if err != nil {
		return err
	}
	defer client.Close()
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	response, err := client.Get(readCtx, *prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	readCancel()
	if err != nil {
		return err
	}
	if response.Header == nil {
		return errors.New("missing response header")
	}
	var stats observations
	err = consume(ctx, client.Watch(ctx, *prefix, clientv3.WithPrefix(), clientv3.WithRev(response.Header.Revision+1)), *delay, &stats)
	fmt.Printf("RESULT events=%d batches=%d last_revision=%d completeness_proven=false\n", stats.events, stats.batches, stats.lastRevision)
	if parent.Err() != nil {
		return parent.Err()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if stats.events < *minimum {
		return fmt.Errorf("insufficient events: got %d, require %d", stats.events, *minimum)
	}
	fmt.Println("OBSERVATION_OK ordering=nondecreasing completeness_proven=false")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "slowwatch:", err)
		os.Exit(1)
	}
}
