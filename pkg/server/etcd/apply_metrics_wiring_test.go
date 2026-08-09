// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package etcd

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is an intentional structural guard: upstream's apply histogram is
// emitted from one switch, while KubeBrain's DBaaS apply boundary is distributed
// across KV, lease, alarm and auth implementations. Keep every upstream v3 op
// visibly wired when those methods are refactored.
func TestApplyDurationWiresEverySupportedUpstreamV3Op(t *testing.T) {
	want := []string{
		"Put", "DeleteRange", "Txn", "Compaction",
		"LeaseGrant", "LeaseRevoke", "LeaseCheckpoint", "Alarm", "Authenticate",
		"AuthEnable", "AuthDisable", "unknown",
		"AuthUserAdd", "AuthUserDelete", "AuthUserChangePassword", "AuthUserGrantRole",
		"AuthUserGet", "AuthUserRevokeRole", "AuthUserList",
		"AuthRoleAdd", "AuthRoleGrantPermission", "AuthRoleGet",
		"AuthRoleRevokePermission", "AuthRoleDelete", "AuthRoleList",
	}

	fset := token.NewFileSet()
	packages, err := parser.ParseDir(fset, ".", nil, 0)
	require.NoError(t, err)
	pkg := packages["etcd"]
	require.NotNil(t, pkg)
	wired := map[string]bool{}
	for filename, file := range pkg.Files {
		if len(filename) >= len("_test.go") && filename[len(filename)-len("_test.go"):] == "_test.go" {
			continue
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) < 2 {
				return true
			}
			ident, ok := call.Fun.(*ast.Ident)
			if !ok || (ident.Name != "beginEtcdApply" && ident.Name != "emitEtcdApplyDuration") {
				return true
			}
			literal, ok := call.Args[1].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			op, unquoteErr := strconv.Unquote(literal.Value)
			require.NoError(t, unquoteErr)
			wired[op] = true
			return true
		})
	}
	for _, op := range want {
		require.Truef(t, wired[op], "upstream apply op %q is not wired", op)
	}
}
