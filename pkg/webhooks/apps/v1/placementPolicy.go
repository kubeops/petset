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

package v1

import (
	"context"
	"fmt"
	"sort"

	api "kubeops.dev/petset/apis/apps/v1"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// SetupPlacementPolicyWebhookWithManager registers the webhook for PlacementPolicy in the manager.
func SetupPlacementPolicyWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&api.PlacementPolicy{}).
		WithValidator(&PlacementPolicyCustomWebhook{mgr.GetClient()}).
		WithDefaulter(&PlacementPolicyCustomWebhook{mgr.GetClient()}).
		Complete()
}

// +kubebuilder:object:generate=false
type PlacementPolicyCustomWebhook struct {
	DefaultClient client.Client
}

// log is for logging in this package.
var pplog = logf.Log.WithName("placementPolicy-resource")

//+kubebuilder:webhook:path=/mutate-apps-k8s-appscode-com-v1-placementpolicy,mutating=true,failurePolicy=fail,sideEffects=None,groups=apps.k8s.appscode.com,resources=placementpolicies,verbs=create;update,versions=v1,name=mplacementpolicy.kb.io,admissionReviewVersions=v1

var _ webhook.CustomDefaulter = &PlacementPolicyCustomWebhook{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (w *PlacementPolicyCustomWebhook) Default(ctx context.Context, obj runtime.Object) error {
	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
//+kubebuilder:webhook:path=/validate-apps-k8s-appscode-com-v1-placementpolicy,mutating=false,failurePolicy=fail,sideEffects=None,groups=apps.k8s.appscode.com,resources=placementpolicies,verbs=create;update,versions=v1,name=vplacementpolicy.kb.io,admissionReviewVersions=v1

var _ webhook.CustomValidator = &PlacementPolicyCustomWebhook{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (w *PlacementPolicyCustomWebhook) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	ps, ok := obj.(*api.PlacementPolicy)
	if !ok {
		return nil, fmt.Errorf("expected an PlacementPolicy object but got %T", obj)
	}
	pplog.Info("validate create", "name", ps.Name)

	// TODO(user): fill in your validation logic upon object creation.
	return nil, w.validateCreatePlacementPolicy(ps)
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (w *PlacementPolicyCustomWebhook) ValidateUpdate(ctx context.Context, old, newObj runtime.Object) (admission.Warnings, error) {
	pp, ok := newObj.(*api.PlacementPolicy)
	if !ok {
		return nil, fmt.Errorf("expected an PlacementPolicy object but got %T", newObj)
	}
	oldPP, ok := old.(*api.PlacementPolicy)
	if !ok {
		return nil, fmt.Errorf("expected an PlacementPolicy object but got %T", oldPP)
	}
	pplog.Info("validate update", "name", pp.Name)

	// TODO(user): fill in your validation logic upon object update.
	return nil, w.validateUpdatePlacementPolicy(ctx, oldPP, pp)
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (w *PlacementPolicyCustomWebhook) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	ps, ok := obj.(*api.PlacementPolicy)
	if !ok {
		return nil, fmt.Errorf("expected an PlacementPolicy object but got %T", obj)
	}
	pplog.Info("validate delete", "name", ps.Name)

	// TODO(user): fill in your validation logic upon object deletion.
	return nil, nil
}

func (w *PlacementPolicyCustomWebhook) validateCreatePlacementPolicy(pp *api.PlacementPolicy) error {
	if pp.Spec.OCM == nil || pp.Spec.OCM.DistributionRules == nil {
		return nil
	}
	allReplicas := flattenReplicas(pp.Spec.OCM.DistributionRules)
	sort.Slice(allReplicas, func(i, j int) bool {
		return allReplicas[i] < allReplicas[j]
	})

	for i, replica := range allReplicas {
		if int32(i) != replica {
			return fmt.Errorf("invalid replica set: indices must be a contiguous block starting from 0. Found %d, expected %d", replica, i)
		}
	}
	return nil
}

func (w *PlacementPolicyCustomWebhook) validateUpdatePlacementPolicy(_ context.Context, oldPP *api.PlacementPolicy, newPP *api.PlacementPolicy) error {
	if oldPP.Spec.OCM == nil || newPP.Spec.OCM == nil {
		return nil
	}

	if err := w.validateCreatePlacementPolicy(newPP); err != nil {
		return err
	}

	oldMap := buildReplicaClusterMap(oldPP.Spec.OCM.DistributionRules)
	newMap := buildReplicaClusterMap(newPP.Spec.OCM.DistributionRules)

	commonLen := min(len(oldMap), len(newMap))

	for i := 0; i < commonLen; i++ {
		idx := int32(i)
		if oldMap[idx] != newMap[idx] {
			return fmt.Errorf("cannot modify existing replica assignments. Replica %d was moved from cluster %q to %q", idx, oldMap[idx], newMap[idx])
		}
	}
	return nil
}

// flattenReplicas takes the cluster spec and returns a single slice of all replica indices.
func flattenReplicas(spec []api.DistributionRule) []int32 {
	var allReplicas []int32
	for _, cluster := range spec {
		allReplicas = append(allReplicas, cluster.Replicas...)
	}
	return allReplicas
}

// buildReplicaClusterMap creates a map of replica index to its assigned cluster name.
func buildReplicaClusterMap(spec []api.DistributionRule) map[int32]string {
	replicaMap := make(map[int32]string)
	for _, cluster := range spec {
		for _, replica := range cluster.Replicas {
			replicaMap[replica] = cluster.ClusterName
		}
	}
	return replicaMap
}
