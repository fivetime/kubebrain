// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

type namedHealthCheck struct {
	name  string
	check func(context.Context) error
}

func (s *server) addEtcdHealthCheckHandlers(handlers map[string]http.Handler) {
	livez := []namedHealthCheck{
		{name: "serializable_read", check: func(ctx context.Context) error {
			return s.readHealthCheck(ctx, true)
		}},
	}
	readyz := []namedHealthCheck{
		// KubeBrain has no member-local CORRUPT alarm. TiKV owns replicated
		// storage integrity, while KubeBrain's hash and GC checks report their
		// failures directly. This check is therefore the platform equivalent
		// of "no active CORRUPT alarm".
		{name: "data_corruption", check: func(context.Context) error { return nil }},
		{name: "serializable_read", check: func(ctx context.Context) error {
			return s.readHealthCheck(ctx, true)
		}},
		{name: "linearizable_read", check: func(ctx context.Context) error {
			return s.readHealthCheck(ctx, false)
		}},
		// KubeBrain service replicas do not participate in TiKV's Raft group
		// and consequently have no learner member role.
		{name: "non_learner", check: func(context.Context) error { return nil }},
	}
	s.addHealthCheckGroup(handlers, "/livez", "livez", livez)
	s.addHealthCheckGroup(handlers, "/readyz", "readyz", readyz)
}

func (s *server) addHealthCheckGroup(handlers map[string]http.Handler, root, checkType string, checks []namedHealthCheck) {
	handlers[root] = s.healthCheckHandler(checkType, checks, true)
	for _, check := range checks {
		check := check
		handlers[root+"/"+check.name] = s.healthCheckHandler(checkType, []namedHealthCheck{check}, false)
	}
}

func (s *server) healthCheckHandler(checkType string, checks []namedHealthCheck, allowExclude bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		excluded := make(map[string]struct{})
		if allowExclude {
			for _, name := range req.URL.Query()["exclude"] {
				if name != "" {
					excluded[name] = struct{}{}
				}
			}
		}

		result := ""
		failed := false
		for _, check := range checks {
			if _, ok := excluded[check.name]; ok {
				continue
			}
			if err := check.check(req.Context()); err != nil {
				result += fmt.Sprintf("[-]%s failed: %v\n", check.name, err)
				failed = true
				s.recordHealthCheck(checkType, check.name, false)
			} else {
				result += fmt.Sprintf("[+]%s ok\n", check.name)
				s.recordHealthCheck(checkType, check.name, true)
			}
		}
		if failed {
			http.Error(w, result, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if _, verbose := req.URL.Query()["verbose"]; verbose {
			_, _ = fmt.Fprint(w, result)
		}
		_, _ = fmt.Fprint(w, "ok\n")
	})
}

func (s *server) recordHealthCheck(checkType, name string, success bool) {
	if s.metricCli == nil {
		return
	}
	status := "error"
	value := 0
	if success {
		status = "success"
		value = 1
	}
	tags := []metrics.T{
		metrics.Tag("type", checkType),
		metrics.Tag("name", name),
	}
	_ = s.metricCli.EmitGauge("etcd.server.healthcheck", value, tags...)
	_ = s.metricCli.EmitCounter(
		"etcd.server.healthchecks_total",
		1,
		append(tags, metrics.Tag("status", status))...,
	)
}
