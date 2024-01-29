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

	"kubeops.dev/statefulset/client/clientset/versioned"
	apiinformers "kubeops.dev/statefulset/client/informers/externalversions"
	"kubeops.dev/statefulset/pkg/controller"
	"kubeops.dev/statefulset/pkg/controller/statefulset"

	"github.com/spf13/pflag"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"kmodules.xyz/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
)

type OperatorOptions struct {
	QPS            float64
	Burst          int
	ResyncPeriod   time.Duration
	MaxNumRequeues int
	NumThreads     int
}

func NewOperatorOptions() *OperatorOptions {
	return &OperatorOptions{
		ResyncPeriod:   10 * time.Minute,
		MaxNumRequeues: 5,
		NumThreads:     5,
		// ref: https://github.com/kubernetes/ingress-nginx/blob/e4d53786e771cc6bdd55f180674b79f5b692e552/pkg/ingress/controller/launch.go#L252-L259
		// High enough QPS to fit all expected use cases. QPS=0 is not set here, because client code is overriding it.
		QPS: 1e6,
		// High enough Burst to fit all expected use cases. Burst=0 is not set here, because client code is overriding it.
		Burst: 1e6,
	}
}

func (s *OperatorOptions) AddGoFlags(fs *flag.FlagSet) {
	fs.Float64Var(&s.QPS, "qps", s.QPS, "The maximum QPS to the master from this client")
	fs.IntVar(&s.Burst, "burst", s.Burst, "The maximum burst for throttle")
	fs.DurationVar(&s.ResyncPeriod, "resync-period", s.ResyncPeriod, "If non-zero, will re-list this often. Otherwise, re-list will be delayed aslong as possible (until the upstream source closes the watch or times out.")
}

func (s *OperatorOptions) AddFlags(fs *pflag.FlagSet) {
	pfs := flag.NewFlagSet("extra-flags", flag.ExitOnError)
	s.AddGoFlags(pfs)
	fs.AddGoFlagSet(pfs)
}

func (s *OperatorOptions) ApplyTo(cfg *controller.OperatorConfig) error {
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
	cfg.KubeInformerFactory = informers.NewSharedInformerFactory(cfg.KubeClient, cfg.ResyncPeriod)
	cfg.InformerFactory = apiinformers.NewSharedInformerFactory(cfg.Client, cfg.ResyncPeriod)

	return nil
}

func (s *OperatorOptions) Validate() []error {
	return nil
}

func (s *OperatorOptions) Complete() error {
	return nil
}

func (s OperatorOptions) Config() (*controller.OperatorConfig, error) {
	clientConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}

	// Fixes https://github.com/Azure/AKS/issues/522
	clientcmd.Fix(clientConfig)

	cfg := controller.NewOperatorConfig(clientConfig)
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

	ctrl := statefulset.NewStatefulSetController(
		ctx,
		cfg.KubeInformerFactory.Core().V1().Pods(),
		cfg.InformerFactory.Apps().V1().StatefulSets(),
		cfg.KubeInformerFactory.Core().V1().PersistentVolumeClaims(),
		cfg.KubeInformerFactory.Apps().V1().ControllerRevisions(),
		cfg.KubeClient,
		cfg.Client,
	)

	cfg.KubeInformerFactory.Start(ctx.Done())
	cfg.InformerFactory.Start(ctx.Done())

	ctrl.Run(ctx, cfg.NumThreads)
	return nil
}
