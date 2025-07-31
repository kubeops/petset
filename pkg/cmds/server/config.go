/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"time"

	"kubeops.dev/petset/client/clientset/versioned"
	apiinformers "kubeops.dev/petset/client/informers/externalversions"
	"kubeops.dev/petset/pkg/controller/petset"
	webhooks "kubeops.dev/petset/pkg/webhooks/apps/v1"

	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"kmodules.xyz/client-go/apiextensions"
	ocmclient "open-cluster-management.io/api/client/work/clientset/versioned"
	manifestinformers "open-cluster-management.io/api/client/work/informers/externalversions"
	apiworkv1 "open-cluster-management.io/api/work/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var setupLog = ctrl.Log.WithName("setup")

type OperatorConfig struct {
	ClientConfig *rest.Config

	ocmClient               ocmclient.Interface
	KubeClient              kubernetes.Interface
	Client                  versioned.Interface
	KubeInformerFactory     informers.SharedInformerFactory
	InformerFactory         apiinformers.SharedInformerFactory
	ManifestInformerFactory manifestinformers.SharedInformerFactory

	ResyncPeriod   time.Duration
	MaxNumRequeues int
	NumThreads     int

	MetricsAddr          string
	CertDir              string
	EnableLeaderElection bool
	ProbeAddr            string
	SecureMetrics        bool
	EnableHTTP2          bool
}

// New returns a new instance of KubeDBWebhookServer from the given config.
func (c *OperatorConfig) New(ctx context.Context) (manager.Manager, error) {
	ctrl.SetLogger(klog.NewKlogr())
	var tlsOpts []func(*tls.Config)

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !c.EnableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Create watchers for certificates
	var certWatcher *certwatcher.CertWatcher

	// Initial TLS options
	webhookTLSOpts := tlsOpts
	metricsTLSOpts := tlsOpts

	if len(c.CertDir) > 0 {
		setupLog.Info("Initializing certificate watcher using provided certificates",
			"cert-dir", c.CertDir, "cert-name", core.TLSCertKey, "cert-key", core.TLSPrivateKeyKey)

		var err error
		certWatcher, err = certwatcher.New(
			filepath.Join(c.CertDir, core.TLSCertKey),
			filepath.Join(c.CertDir, core.TLSPrivateKeyKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}

		webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = certWatcher.GetCertificate
		})

		metricsTLSOpts = append(metricsTLSOpts, func(config *tls.Config) {
			config.GetCertificate = certWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: webhookTLSOpts,
	})

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.2/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   c.MetricsAddr,
		SecureServing: c.SecureMetrics,
		TLSOpts:       metricsTLSOpts,
	}

	if c.SecureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.2/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	mgr, err := ctrl.NewManager(c.ClientConfig, ctrl.Options{
		Scheme:                 Scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: c.ProbeAddr,
		LeaderElection:         c.EnableLeaderElection,
		LeaderElectionID:       "0d1fb029.k8s.appscode.com",
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
		Cache: cache.Options{
			SyncPeriod: &c.ResyncPeriod, // Default SyncPeriod is 10 Hours
		},
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		err = apiextensions.NewReconciler(ctx, mgr).SetupWithManager(mgr)
		if err != nil {
			return errors.Wrap(err, "unable to create controller controller CustomResourceReconciler")
		}

		apiextensions.RegisterSetup(schema.GroupKind{
			Group: apiworkv1.GroupName,
			Kind:  "ManifestWork",
		}, func(ctx context.Context, mgr manager.Manager) {
			c.ManifestInformerFactory.Start(ctx.Done())
		})

		return nil
	})); err != nil {
		setupLog.Error(err, "unable to set manifestwork informer factory")
		os.Exit(1)
	}

	if err = webhooks.SetupPlacementPolicyWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "PlacementPolicy")
		os.Exit(1)
	}

	if err = webhooks.SetupPetSetWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "PetSet")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if certWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(certWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	psCtrl := petset.NewPetSetController(
		ctx,
		c.KubeInformerFactory.Core().V1().Pods(),
		c.InformerFactory.Apps().V1().PetSets(),
		c.InformerFactory.Apps().V1().PlacementPolicies(),
		c.KubeInformerFactory.Core().V1().PersistentVolumeClaims(),
		c.KubeInformerFactory.Apps().V1().ControllerRevisions(),
		c.ManifestInformerFactory.Work().V1().ManifestWorks(),
		c.KubeClient,
		c.Client,
		c.ocmClient,
	)

	c.KubeInformerFactory.Start(ctx.Done())
	c.InformerFactory.Start(ctx.Done())

	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		psCtrl.Run(ctx, c.NumThreads)
		return nil
	})); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}
	return mgr, nil
}
