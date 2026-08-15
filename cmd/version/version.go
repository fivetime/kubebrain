// Copyright 2022 ByteDance and/or its affiliates
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

package version

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

// values will be injected while building
var (
	// Version is version of binary
	Version string

	// Storage is the storage engine used by this binary
	Storage string

	// GitSHA is the SHA of commit
	GitSHA string

	// GoVersion is the version of go compiler
	GoVersion string

	// GoOsArch is the os and arch of the binary
	GoOsArch string

	// Date is the time when binary is build
	Date string
)

// VersionCmd is the cobra.Command for print version info
var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "show version",
	Run: func(cmd *cobra.Command, args []string) {
		_, _ = io.WriteString(cmd.OutOrStdout(), Text())
	},
}

// Text returns the complete build identity used by both the version subcommand
// and Cobra's conventional --version flag. Keeping one renderer prevents the
// operational entrypoints from reporting different provenance.
func Text() string {
	return fmt.Sprintf(
		"KubeBrain\nVersion:   \t %s\nStorage:   \t %s\nGit SHA:   \t %s\nGo Version:\t %s\nGo OS/Arch:\t %s\nBuildTime: \t %s\n",
		Version, Storage, GitSHA, GoVersion, GoOsArch, Date,
	)
}
