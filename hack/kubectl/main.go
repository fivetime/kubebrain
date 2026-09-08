// Copyright 2014 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This entry point follows kubernetes/kubernetes v1.36.4 cmd/kubectl/kubectl.go.
// All commands remain upstream imports; only the build and dependency graph
// are maintained here so security fixes do not drop Kubernetes 1.35 support.
package main

import (
	"os"

	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/cmd"
	"k8s.io/kubectl/pkg/cmd/util"
)

func main() {
	// Upstream parses verbosity before constructing commands/plugins and kuberc.
	_, _ = logs.GlogSetter(cmd.GetLogVerbosity(os.Args))
	command := cmd.NewDefaultKubectlCommand()
	if err := cli.RunNoErrOutput(command); err != nil {
		util.CheckErr(err)
	}
}
