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

	api "kubeops.dev/petset/apis/apps/v1"
	"kubeops.dev/petset/pkg/features"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// SetupPetSetWebhookWithManager registers the webhook for PetSet in the manager.
func SetupPetSetWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&api.PetSet{}).
		WithValidator(&PetSetCustomWebhook{mgr.GetClient()}).
		WithDefaulter(&PetSetCustomWebhook{mgr.GetClient()}).
		Complete()
}

// +kubebuilder:object:generate=false
type PetSetCustomWebhook struct {
	DefaultClient client.Client
}

// log is for logging in this package.
var petsetlog = logf.Log.WithName("petset-resource")

//+kubebuilder:webhook:path=/mutate-apps-k8s-appscode-com-v1-petset,mutating=true,failurePolicy=fail,sideEffects=None,groups=apps.k8s.appscode.com,resources=petsets,verbs=create;update,versions=v1,name=mpetset.kb.io,admissionReviewVersions=v1

var _ webhook.CustomDefaulter = &PetSetCustomWebhook{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (w *PetSetCustomWebhook) Default(ctx context.Context, obj runtime.Object) error {
	ps, ok := obj.(*api.PetSet)
	if !ok {
		return fmt.Errorf("expected an PetSet object but got %T", obj)
	}

	if len(ps.Spec.PodManagementPolicy) == 0 {
		ps.Spec.PodManagementPolicy = appsv1.OrderedReadyPodManagement
	}

	if ps.Spec.UpdateStrategy.Type == "" {
		ps.Spec.UpdateStrategy.Type = appsv1.RollingUpdateStatefulSetStrategyType

		if ps.Spec.UpdateStrategy.RollingUpdate == nil {
			// UpdateStrategy.RollingUpdate will take default values below.
			ps.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
		}
	}

	if ps.Spec.UpdateStrategy.Type == appsv1.RollingUpdateStatefulSetStrategyType &&
		ps.Spec.UpdateStrategy.RollingUpdate != nil {

		if ps.Spec.UpdateStrategy.RollingUpdate.Partition == nil {
			ps.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To[int32](0)
		}
		if features.DefaultFeatureGate.Enabled(features.MaxUnavailablePetSet) {
			if ps.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable == nil {
				ps.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.FromInt32(1))
			}
		}
	}

	if features.DefaultFeatureGate.Enabled(features.PetSetAutoDeletePVC) {
		if ps.Spec.PersistentVolumeClaimRetentionPolicy == nil {
			ps.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{}
		}
		if len(ps.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted) == 0 {
			ps.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
		}
		if len(ps.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled) == 0 {
			ps.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
		}
	}

	if ps.Spec.Replicas == nil {
		ps.Spec.Replicas = new(int32)
		*ps.Spec.Replicas = 1
	}
	if ps.Spec.RevisionHistoryLimit == nil {
		ps.Spec.RevisionHistoryLimit = new(int32)
		*ps.Spec.RevisionHistoryLimit = 10
	}
	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
//+kubebuilder:webhook:path=/validate-apps-k8s-appscode-com-v1-petset,mutating=false,failurePolicy=fail,sideEffects=None,groups=apps.k8s.appscode.com,resources=petsets,verbs=create;update,versions=v1,name=vpetset.kb.io,admissionReviewVersions=v1

var _ webhook.CustomValidator = &PetSetCustomWebhook{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (w *PetSetCustomWebhook) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	ps, ok := obj.(*api.PetSet)
	if !ok {
		return nil, fmt.Errorf("expected an PetSet object but got %T", obj)
	}
	petsetlog.Info("validate create", "name", ps.Name)

	// TODO(user): fill in your validation logic upon object creation.
	return nil, w.validatePlacementPolicy(ctx, ps)
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (w *PetSetCustomWebhook) ValidateUpdate(ctx context.Context, old, newObj runtime.Object) (admission.Warnings, error) {
	ps, ok := newObj.(*api.PetSet)
	if !ok {
		return nil, fmt.Errorf("expected an PetSet object but got %T", newObj)
	}
	petsetlog.Info("validate update", "name", ps.Name)

	// TODO(user): fill in your validation logic upon object update.
	return nil, w.validatePlacementPolicy(ctx, ps)
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (w *PetSetCustomWebhook) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	ps, ok := obj.(*api.PetSet)
	if !ok {
		return nil, fmt.Errorf("expected an PetSet object but got %T", obj)
	}
	petsetlog.Info("validate delete", "name", ps.Name)

	// TODO(user): fill in your validation logic upon object deletion.
	return nil, nil
}

func (w *PetSetCustomWebhook) validatePlacementPolicy(ctx context.Context, set *api.PetSet) error {
	if !set.Spec.Distributed || set.Spec.PodPlacementPolicy.Name == "" || set.Spec.Replicas == nil {
		return nil
	}
	pp := &api.PlacementPolicy{}
	err := w.DefaultClient.Get(ctx, types.NamespacedName{
		Namespace: "",
		Name:      set.Spec.PodPlacementPolicy.Name,
	}, pp)
	if err != nil {
		return err
	}
	if pp.Spec.OCM == nil || pp.Spec.OCM.ClusterSpec == nil {
		return fmt.Errorf("expected an OCM cluster spec for distributed petset in the %v/%v: %v, but got none", api.GroupName, api.ResourceKindPlacementPolicy, pp.Name)
	}
	cSum := 0
	m := make(map[int32]struct{})
	for i := 0; i < len(pp.Spec.OCM.ClusterSpec); i++ {
		cSum += len(pp.Spec.OCM.ClusterSpec[i].Replicas)
		for _, replica := range pp.Spec.OCM.ClusterSpec[i].Replicas {
			m[replica] = struct{}{}
		}
	}
	if cSum < int(*set.Spec.Replicas) {
		return fmt.Errorf("expected at least %d replicas in the placementPolicy %v, but got totalReplicas = %d", set.Spec.Replicas, pp.Name, cSum)
	}
	missing := make([]int32, 0)
	for i := 0; i < int(*set.Spec.Replicas); i++ {
		if _, ok := m[int32(i)]; !ok {
			missing = append(missing, int32(i))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing replica index %v at placementpolicy: %v", missing, pp.Name)
	}
	return nil
}
