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
	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (w *PetSetCustomWebhook) ValidateUpdate(ctx context.Context, old, newObj runtime.Object) (admission.Warnings, error) {
	ps, ok := newObj.(*api.PetSet)
	if !ok {
		return nil, fmt.Errorf("expected an PetSet object but got %T", newObj)
	}
	petsetlog.Info("validate update", "name", ps.Name)

	// TODO(user): fill in your validation logic upon object update.
	return nil, nil
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
