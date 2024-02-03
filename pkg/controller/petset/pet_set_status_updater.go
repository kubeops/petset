/*
Copyright 2017 The Kubernetes Authors.

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

package petset

import (
	"context"
	"fmt"

	api "kubeops.dev/petset/apis/apps/v1"
	"kubeops.dev/petset/client/clientset/versioned"
	apilisters "kubeops.dev/petset/client/listers/apps/v1"

	apps "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/retry"
)

// StatefulSetStatusUpdaterInterface is an interface used to update the StatefulSetStatus associated with a PetSet.
// For any use other than testing, clients should create an instance using NewRealStatefulSetStatusUpdater.
type StatefulSetStatusUpdaterInterface interface {
	// UpdateStatefulSetStatus sets the set's Status to status. Implementations are required to retry on conflicts,
	// but fail on other errors. If the returned error is nil set's Status has been successfully set to status.
	UpdateStatefulSetStatus(ctx context.Context, set *api.PetSet, status *apps.StatefulSetStatus) error
}

// NewRealStatefulSetStatusUpdater returns a StatefulSetStatusUpdaterInterface that updates the Status of a PetSet,
// using the supplied client and setLister.
func NewRealStatefulSetStatusUpdater(
	client versioned.Interface,
	setLister apilisters.PetSetLister,
) StatefulSetStatusUpdaterInterface {
	return &realStatefulSetStatusUpdater{client, setLister}
}

type realStatefulSetStatusUpdater struct {
	client    versioned.Interface
	setLister apilisters.PetSetLister
}

func (ssu *realStatefulSetStatusUpdater) UpdateStatefulSetStatus(
	ctx context.Context,
	set *api.PetSet,
	status *apps.StatefulSetStatus,
) error {
	// don't wait due to limited number of clients, but backoff after the default number of steps
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		set.Status = *status
		// TODO: This context.TODO should use a real context once we have RetryOnConflictWithContext
		_, updateErr := ssu.client.AppsV1().PetSets(set.Namespace).UpdateStatus(context.TODO(), set, metav1.UpdateOptions{})
		if updateErr == nil {
			return nil
		}
		if updated, err := ssu.setLister.PetSets(set.Namespace).Get(set.Name); err == nil {
			// make a copy so we don't mutate the shared cache
			set = updated.DeepCopy()
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated PetSet %s/%s from lister: %v", set.Namespace, set.Name, err))
		}

		return updateErr
	})
}

var _ StatefulSetStatusUpdaterInterface = &realStatefulSetStatusUpdater{}
