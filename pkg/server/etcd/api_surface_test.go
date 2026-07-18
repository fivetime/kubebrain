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

package etcd

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// TestEtcdAPISurfaceIsExplicit prevents an API dependency upgrade from making a
// new RPC appear to be served only because RPCServer embeds grpc's forward-
// compatibility shims. Every public etcd RPC must be implemented or explicitly
// rejected by KubeBrain so its product classification cannot be accidental.
func TestEtcdAPISurfaceIsExplicit(t *testing.T) {
	methods := productionReceiverMethods(t)
	services := []struct {
		desc  grpc.ServiceDesc
		owner string
	}{
		{desc: etcdserverpb.KV_ServiceDesc, owner: "RPCServer"},
		{desc: etcdserverpb.Watch_ServiceDesc, owner: "RPCServer"},
		{desc: etcdserverpb.Lease_ServiceDesc, owner: "leaseManager"},
		{desc: etcdserverpb.Cluster_ServiceDesc, owner: "RPCServer"},
		{desc: etcdserverpb.Maintenance_ServiceDesc, owner: "RPCServer"},
		{desc: etcdserverpb.Auth_ServiceDesc, owner: "RPCServer"},
	}

	total := 0
	for _, service := range services {
		t.Run(service.desc.ServiceName, func(t *testing.T) {
			for _, method := range service.desc.Methods {
				total++
				require.Containsf(t, methods[service.owner], method.MethodName,
					"%s is only inherited from an Unimplemented*Server; classify and implement it explicitly",
					method.MethodName)
			}
			for _, stream := range service.desc.Streams {
				total++
				require.Containsf(t, methods[service.owner], stream.StreamName,
					"%s is only inherited from an Unimplemented*Server; classify and implement it explicitly",
					stream.StreamName)
			}
		})
	}
	require.Equal(t, 42, total, "review and classify every public RPC when the etcd API surface changes")
}

func productionReceiverMethods(t *testing.T) map[string]map[string]struct{} {
	t.Helper()
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)

	methods := make(map[string]map[string]struct{})
	fileset := token.NewFileSet()
	for _, path := range files {
		if filepath.Ext(path) != ".go" || len(path) >= len("_test.go") && path[len(path)-len("_test.go"):] == "_test.go" {
			continue
		}
		file, err := parser.ParseFile(fileset, path, nil, 0)
		require.NoError(t, err, path)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil || len(function.Recv.List) != 1 {
				continue
			}
			owner := receiverTypeName(function.Recv.List[0].Type)
			if owner == "" {
				continue
			}
			if methods[owner] == nil {
				methods[owner] = make(map[string]struct{})
			}
			methods[owner][function.Name.Name] = struct{}{}
		}
	}
	return methods
}

func receiverTypeName(expression ast.Expr) string {
	if pointer, ok := expression.(*ast.StarExpr); ok {
		expression = pointer.X
	}
	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return identifier.Name
}
