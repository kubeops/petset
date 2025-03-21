/*
Copyright AppsCode Inc. and Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmds

import (
	api "kubeops.dev/petset/apis/apps/v1"

	"github.com/spf13/cobra"
	v "gomodules.xyz/x/version"
	genericapiserver "k8s.io/apiserver/pkg/server"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
)

func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:               "petset",
		DisableAutoGenTag: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return api.AddToScheme(clientscheme.Scheme)
		},
	}
	rootCmd.AddCommand(v.NewCmdVersion())

	ctx := genericapiserver.SetupSignalContext()
	rootCmd.AddCommand(NewCmdRun(ctx))

	return rootCmd
}
