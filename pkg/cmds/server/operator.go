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

package server

import (
	"context"
	"flag"
	"time"

	api "kubeops.dev/petset/apis/apps/v1"
	"kubeops.dev/petset/client/clientset/versioned"
	apiinformers "kubeops.dev/petset/client/informers/externalversions"
	"kubeops.dev/petset/pkg/features"

	kubesliceapi "github.com/kubeslice/kubeslice-controller/apis/controller/v1alpha1"
	"github.com/spf13/pflag"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	manifestclient "open-cluster-management.io/api/client/work/clientset/versioned"
	manifestinformers "open-cluster-management.io/api/client/work/informers/externalversions"
	apiworkv1 "open-cluster-management.io/api/work/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var Scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientsetscheme.AddToScheme(Scheme))
	utilruntime.Must(apiworkv1.AddToScheme(Scheme))
	utilruntime.Must(api.AddToScheme(Scheme))
	utilruntime.Must(apiextensionsv1.AddToScheme(Scheme))
	utilruntime.Must(kubesliceapi.AddToScheme(Scheme))
}

type OperatorOptions struct {
	QPS   float64
	Burst int

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

func NewOperatorOptions() *OperatorOptions {
	return &OperatorOptions{
		// ref: https://github.com/kubernetes/ingress-nginx/blob/e4d53786e771cc6bdd55f180674b79f5b692e552/pkg/ingress/controller/launch.go#L252-L259
		// High enough QPS to fit all expected use cases. QPS=0 is not set here, because client code is overriding it.
		QPS: 1e6,
		// High enough Burst to fit all expected use cases. Burst=0 is not set here, because client code is overriding it.
		Burst:                1e6,
		ResyncPeriod:         10 * time.Minute,
		MaxNumRequeues:       5,
		NumThreads:           5,
		MetricsAddr:          "0",
		ProbeAddr:            ":8081",
		EnableLeaderElection: false,
		SecureMetrics:        true,
		CertDir:              "",
		EnableHTTP2:          false,
	}
}

func (s *OperatorOptions) AddGoFlags(fs *flag.FlagSet) {
	fs.Float64Var(&s.QPS, "qps", s.QPS, "The maximum QPS to the master from this client")
	fs.IntVar(&s.Burst, "burst", s.Burst, "The maximum burst for throttle")

	fs.DurationVar(&s.ResyncPeriod, "resync-period", s.ResyncPeriod, "If non-zero, will re-list this often. Otherwise, re-list will be delayed aslong as possible (until the upstream source closes the watch or times out.")
	fs.IntVar(&s.NumThreads, "max-concurrent-reconciles", s.NumThreads, "The maximum number of concurrent reconciles which can be run")

	fs.StringVar(&s.MetricsAddr, "metrics-bind-address", s.MetricsAddr, "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	fs.StringVar(&s.ProbeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	fs.BoolVar(&s.EnableLeaderElection, "leader-elect", s.EnableLeaderElection,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	fs.BoolVar(&s.SecureMetrics, "metrics-secure", s.SecureMetrics,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	fs.StringVar(&s.CertDir, "cert-dir", s.CertDir, "The directory that contains the webhook and metrics server certificate.")
	fs.BoolVar(&s.EnableHTTP2, "enable-http2", s.EnableHTTP2,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
}

func (s *OperatorOptions) AddFlags(fs *pflag.FlagSet) {
	pfs := flag.NewFlagSet("extra-flags", flag.ExitOnError)
	s.AddGoFlags(pfs)
	fs.AddGoFlagSet(pfs)
	features.DefaultMutableFeatureGate.AddFlag(fs)
}

func (s *OperatorOptions) ApplyTo(cfg *OperatorConfig) error {
	var err error

	cfg.ClientConfig.QPS = float32(s.QPS)
	cfg.ClientConfig.Burst = s.Burst
	cfg.ResyncPeriod = s.ResyncPeriod
	cfg.MaxNumRequeues = s.MaxNumRequeues
	cfg.NumThreads = s.NumThreads

	if cfg.KubeClient, err = kubernetes.NewForConfig(cfg.ClientConfig); err != nil {
		return err
	}
	if cfg.Client, err = versioned.NewForConfig(cfg.ClientConfig); err != nil {
		return err
	}
	if cfg.ocmClient, err = manifestclient.NewForConfig(cfg.ClientConfig); err != nil {
		return err
	}
	cfg.KubeInformerFactory = informers.NewSharedInformerFactory(cfg.KubeClient, cfg.ResyncPeriod)
	cfg.InformerFactory = apiinformers.NewSharedInformerFactory(cfg.Client, cfg.ResyncPeriod)
	cfg.ManifestInformerFactory = manifestinformers.NewSharedInformerFactory(cfg.ocmClient, cfg.ResyncPeriod)
	cfg.MetricsAddr = s.MetricsAddr
	cfg.ProbeAddr = s.ProbeAddr
	cfg.EnableLeaderElection = s.EnableLeaderElection
	cfg.SecureMetrics = s.SecureMetrics
	cfg.CertDir = s.CertDir
	cfg.EnableHTTP2 = s.EnableHTTP2

	return nil
}

func (s *OperatorOptions) Validate() []error {
	return nil
}

func (s *OperatorOptions) Complete() error {
	return nil
}

func (s OperatorOptions) Config() (*OperatorConfig, error) {
	clientConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}

	cfg := &OperatorConfig{
		ClientConfig: clientConfig,
	}
	if err := s.ApplyTo(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (s OperatorOptions) Run(ctx context.Context) error {
	cfg, err := s.Config()
	if err != nil {
		return err
	}

	mgr, err := cfg.New(ctx)
	if err != nil {
		return err
	}
	return mgr.Start(ctx)
}
