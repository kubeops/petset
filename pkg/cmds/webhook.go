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
	"context"
	"os"
	"path/filepath"

	appsv1 "kubeops.dev/petset/apis/apps/v1"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	v1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/types"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/klog/v2/klogr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var setupLog = ctrl.Log.WithName("setup")

func NewCmdWebhook(ctx context.Context) *cobra.Command {
	certDir := "/var/serving-cert"
	var webhookName string
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	flag.StringVar(&webhookName, "webhook-name", "", "The name of mutating and validating webhook configuration")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")

	cmd := &cobra.Command{
		Use:               "webhook",
		Short:             "Launch webhook server",
		Long:              "Launch webhook server",
		DisableAutoGenTag: true,
		Run: func(cmd *cobra.Command, args []string) {
			ctrl.SetLogger(klogr.New()) // nolint: staticcheck

			mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
				Scheme:                 clientscheme.Scheme,
				Metrics:                metricsserver.Options{BindAddress: metricsAddr},
				HealthProbeBindAddress: probeAddr,
				WebhookServer: webhook.NewServer(webhook.Options{
					Port:    9443,
					CertDir: certDir,
				}),
				LeaderElection:   enableLeaderElection,
				LeaderElectionID: "0b66efc1.k8s.appscode.com",
				// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
				// when the Manager ends. This requires the binary to immediately end when the
				// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
				// speeds up voluntary leader transitions as the new leader don't have to wait
				// LeaseDuration time first.
				//
				// In the default scaffold provided, the program ends immediately after
				// the manager stops, so would be fine to enable this option. However,
				// if you are doing or is intended to do any operation such as perform cleanups
				// after the manager stops then its usage might be unsafe.
				// LeaderElectionReleaseOnCancel: true,
			})
			if err != nil {
				setupLog.Error(err, "unable to start manager")
				os.Exit(1)
			}

			if err = (&appsv1.PetSet{}).SetupWebhookWithManager(mgr); err != nil {
				setupLog.Error(err, "unable to create webhook", "webhook", "PetSet")
				os.Exit(1)
			}

			if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
				setupLog.Error(err, "unable to set up health check")
				os.Exit(1)
			}
			if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
				setupLog.Error(err, "unable to set up ready check")
				os.Exit(1)
			}

			if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
				if mgr.GetCache().WaitForCacheSync(context.TODO()) {
					if err := updateMutatingWebhookCABundle(mgr, webhookName, certDir); err != nil {
						setupLog.Error(err, "unable to update caBundle for MutatingWebhookConfiguration")
						os.Exit(1)
					}
					if err := updateValidatingWebhookCABundle(mgr, webhookName, certDir); err != nil {
						setupLog.Error(err, "unable to update caBundle for ValidatingWebhookConfiguration")
						os.Exit(1)
					}
				}
				return nil
			})); err != nil {
				setupLog.Error(err, "unable to setup webhook configuration updater")
				os.Exit(1)
			}

			setupLog.Info("starting manager")
			if err := mgr.Start(ctx); err != nil {
				setupLog.Error(err, "problem running manager")
				os.Exit(1)
			}
		},
	}

	return cmd
}

func updateMutatingWebhookCABundle(mgr ctrl.Manager, name, certDir string) error {
	webhook := &v1.MutatingWebhookConfiguration{}
	err := mgr.GetClient().Get(context.TODO(), types.NamespacedName{
		Name: name,
	}, webhook)
	if err != nil {
		return err
	}

	caBundle, err := os.ReadFile(filepath.Join(certDir, "ca.crt"))
	if err != nil {
		return err
	}
	for i := range webhook.Webhooks {
		webhook.Webhooks[i].ClientConfig.CABundle = caBundle
	}
	return mgr.GetClient().Update(context.TODO(), webhook, &client.UpdateOptions{})
}

func updateValidatingWebhookCABundle(mgr ctrl.Manager, name, certDir string) error {
	webhook := &v1.ValidatingWebhookConfiguration{}
	err := mgr.GetClient().Get(context.TODO(), types.NamespacedName{
		Name: name,
	}, webhook)
	if err != nil {
		return err
	}

	caBundle, err := os.ReadFile(filepath.Join(certDir, "ca.crt"))
	if err != nil {
		return err
	}
	for i := range webhook.Webhooks {
		webhook.Webhooks[i].ClientConfig.CABundle = caBundle
	}
	return mgr.GetClient().Update(context.TODO(), webhook, &client.UpdateOptions{})
}
