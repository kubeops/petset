/*
Copyright 2016 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	api "kubeops.dev/petset/apis/apps/v1"
	"kubeops.dev/petset/client/clientset/versioned"
	apifake "kubeops.dev/petset/client/clientset/versioned/fake"
	apiinformers "kubeops.dev/petset/client/informers/externalversions"
	stsinformers "kubeops.dev/petset/client/informers/externalversions/apps/v1"
	apilisters "kubeops.dev/petset/client/listers/apps/v1"
	podutil "kubeops.dev/petset/pkg/api/v1/pod"
	"kubeops.dev/petset/pkg/controller"
	"kubeops.dev/petset/pkg/controller/history"
	"kubeops.dev/petset/pkg/features"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

type invariantFunc func(set *api.PetSet, om *fakeObjectManager) error

func setupController(client clientset.Interface, apiclient versioned.Interface) (*fakeObjectManager, *fakeStatefulSetStatusUpdater, PetSetControlInterface) {
	informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	apiinformerFactory := apiinformers.NewSharedInformerFactory(apiclient, controller.NoResyncPeriodFunc())
	om := newFakeObjectManager(informerFactory, apiinformerFactory)
	spc := NewStatefulPodControlFromManager(om, &noopRecorder{})
	ssu := newFakeStatefulSetStatusUpdater(apiinformerFactory.Apps().V1().PetSets())
	recorder := &noopRecorder{}
	ssc := NewDefaultPetSetControl(spc, ssu, history.NewFakeHistory(informerFactory.Apps().V1().ControllerRevisions()), recorder)

	// The informer is not started. The tests here manipulate the local cache (indexers) directly, and there is no waiting
	// for client state to sync. In fact, because the client is not updated during tests, informer updates will break tests
	// by unexpectedly deleting objects.
	//
	// TODO: It might be better to rewrite all these tests manipulate the client an explicitly sync to ensure consistent
	// state, or to create a fake client that does not use a local cache.

	// The client is passed initial sets, so we have to put them in the local setsIndexer cache.
	if sets, err := client.AppsV1().PetSets("").List(context.TODO(), metav1.ListOptions{}); err != nil {
		panic(err)
	} else {
		for _, set := range sets.Items {
			if err := om.setsIndexer.Update(&set); err != nil {
				panic(err)
			}
		}
	}

	return om, ssu, ssc
}

func burst(set *api.PetSet) *api.PetSet {
	set.Spec.PodManagementPolicy = apps.ParallelPodManagement
	return set
}

func setMinReadySeconds(set *api.PetSet, minReadySeconds int32) *api.PetSet {
	set.Spec.MinReadySeconds = minReadySeconds
	return set
}

func runTestOverPVCRetentionPolicies(t *testing.T, testName string, testFn func(*testing.T, *apps.StatefulSetPersistentVolumeClaimRetentionPolicy)) {
	subtestName := "PetSetAutoDeletePVCDisabled"
	if testName != "" {
		subtestName = fmt.Sprintf("%s/%s", testName, subtestName)
	}
	t.Run(subtestName, func(t *testing.T) {
		defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PetSetAutoDeletePVC, false)()
		testFn(t, &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		})
	})

	for _, policy := range []*apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		{
			WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		},
		{
			WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		},
		{
			WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
		},
		{
			WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
		},
		// tests the case when no policy is set.
		nil,
	} {
		subtestName := pvcDeletePolicyString(policy) + "/PetSetAutoDeletePVCEnabled"
		if testName != "" {
			subtestName = fmt.Sprintf("%s/%s", testName, subtestName)
		}
		t.Run(subtestName, func(t *testing.T) {
			defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PetSetAutoDeletePVC, true)()
			testFn(t, policy)
		})
	}
}

func pvcDeletePolicyString(policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) string {
	if policy == nil {
		return "nullPolicy"
	}
	const retain = apps.RetainPersistentVolumeClaimRetentionPolicyType
	const delete = apps.DeletePersistentVolumeClaimRetentionPolicyType
	switch {
	case policy.WhenScaled == retain && policy.WhenDeleted == retain:
		return "Retain"
	case policy.WhenScaled == retain && policy.WhenDeleted == delete:
		return "SetDeleteOnly"
	case policy.WhenScaled == delete && policy.WhenDeleted == retain:
		return "ScaleDownOnly"
	case policy.WhenScaled == delete && policy.WhenDeleted == delete:
		return "Delete"
	}
	return "invalid"
}

func TestPetSetControl(t *testing.T) {
	simpleSetFn := func() *api.PetSet { return newPetSet(3) }
	largeSetFn := func() *api.PetSet { return newPetSet(5) }

	testCases := []struct {
		fn  func(*testing.T, *api.PetSet, invariantFunc)
		obj func() *api.PetSet
	}{
		{CreatesPods, simpleSetFn},
		{ScalesUp, simpleSetFn},
		{ScalesDown, simpleSetFn},
		{ReplacesPods, largeSetFn},
		{RecreatesFailedPod, simpleSetFn},
		{RecreatesSucceededPod, simpleSetFn},
		{CreatePodFailure, simpleSetFn},
		{UpdatePodFailure, simpleSetFn},
		{UpdateSetStatusFailure, simpleSetFn},
		{PodRecreateDeleteFailure, simpleSetFn},
		{NewRevisionDeletePodFailure, simpleSetFn},
		{RecreatesPVCForPendingPod, simpleSetFn},
	}

	for _, testCase := range testCases {
		fnName := runtime.FuncForPC(reflect.ValueOf(testCase.fn).Pointer()).Name()
		if i := strings.LastIndex(fnName, "."); i != -1 {
			fnName = fnName[i+1:]
		}
		testObj := testCase.obj
		testFn := testCase.fn
		runTestOverPVCRetentionPolicies(
			t,
			fmt.Sprintf("%s/Monotonic", fnName),
			func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
				set := testObj()
				set.Spec.PersistentVolumeClaimRetentionPolicy = policy
				testFn(t, set, assertMonotonicInvariants)
			},
		)
		runTestOverPVCRetentionPolicies(
			t,
			fmt.Sprintf("%s/Burst", fnName),
			func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
				set := burst(testObj())
				set.Spec.PersistentVolumeClaimRetentionPolicy = policy
				testFn(t, set, assertBurstInvariants)
			},
		)
	}
}

func CreatesPods(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, _, ssc := setupController(client, apiclient)

	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up PetSet : %s", err)
	}
	var err error
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale petset to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set ReadyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set UpdatedReplicas correctly")
	}
	// Check all pods have correct pod index label.
	if utilfeature.DefaultFeatureGate.Enabled(features.PodIndexLabel) {
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Error(err)
		}
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Error(err)
		}
		if len(pods) != 3 {
			t.Errorf("Expected 3 pods, got %d", len(pods))
		}
		for _, pod := range pods {
			podIndexFromLabel, exists := pod.Labels[apps.PodIndexLabel]
			if !exists {
				t.Errorf("Missing pod index label: %s", apps.PodIndexLabel)
				continue
			}
			podIndexFromName := strconv.Itoa(getOrdinal(pod))
			if podIndexFromLabel != podIndexFromName {
				t.Errorf("Pod index label value (%s) does not match pod index in pod name (%s)", podIndexFromLabel, podIndexFromName)
			}
		}
	}
}

func ScalesUp(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, _, ssc := setupController(client, apiclient)

	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up PetSet : %s", err)
	}
	*set.Spec.Replicas = 4
	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to scale PetSet : %s", err)
	}
	var err error
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if set.Status.Replicas != 4 {
		t.Error("Failed to scale petset to 4 replicas")
	}
	if set.Status.ReadyReplicas != 4 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 4 {
		t.Error("Failed to set updatedReplicas correctly")
	}
}

func ScalesDown(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, _, ssc := setupController(client, apiclient)

	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up PetSet : %s", err)
	}
	var err error
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	*set.Spec.Replicas = 0
	if err := scaleDownPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to scale PetSet : %s", err)
	}

	// Check updated set.
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if set.Status.Replicas != 0 {
		t.Error("Failed to scale petset to 0 replicas")
	}
	if set.Status.ReadyReplicas != 0 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 0 {
		t.Error("Failed to set updatedReplicas correctly")
	}
}

func ReplacesPods(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, _, ssc := setupController(client, apiclient)

	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up PetSet : %s", err)
	}
	var err error
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if set.Status.Replicas != 5 {
		t.Error("Failed to scale petset to 5 replicas")
	}
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	claims, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	for _, pod := range pods {
		podClaims := getPersistentVolumeClaims(set, pod)
		for _, claim := range claims {
			if _, found := podClaims[claim.Name]; found {
				if hasOwnerRef(claim, pod) {
					t.Errorf("Unexpected ownerRef on %s", claim.Name)
				}
			}
		}
	}
	sort.Sort(ascendingOrdinal(pods))
	om.podsIndexer.Delete(pods[0])
	om.podsIndexer.Delete(pods[2])
	om.podsIndexer.Delete(pods[4])
	for i := 0; i < 5; i += 2 {
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Error(err)
		}
		if _, err = ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
			t.Errorf("Failed to update PetSet : %s", err)
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("Error getting updated PetSet: %v", err)
		}
		if pods, err = om.setPodRunning(set, i); err != nil {
			t.Error(err)
		}
		if _, err = ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
			t.Errorf("Failed to update PetSet : %s", err)
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("Error getting updated PetSet: %v", err)
		}
		if _, err = om.setPodReady(set, i); err != nil {
			t.Error(err)
		}
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
		t.Errorf("Failed to update PetSet : %s", err)
	}
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if e, a := int32(5), set.Status.Replicas; e != a {
		t.Errorf("Expected to scale to %d, got %d", e, a)
	}
}

func recreatesPod(t *testing.T, set *api.PetSet, invariants invariantFunc, phase v1.PodPhase) {
	client := fake.NewSimpleClientset()
	apiclient := apifake.NewSimpleClientset()
	om, _, ssc := setupController(client, apiclient)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
		t.Errorf("Error updating PetSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	pods[0].Status.Phase = phase
	om.podsIndexer.Update(pods[0])
	if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
		t.Errorf("Error updating PetSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if isCreated(pods[0]) {
		t.Error("PetSet did not recreate failed Pod")
	}
}

func RecreatesFailedPod(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	recreatesPod(t, set, invariants, v1.PodFailed)
}

func RecreatesSucceededPod(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	recreatesPod(t, set, invariants, v1.PodSucceeded)
}

func CreatePodFailure(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, _, ssc := setupController(client, apiclient)
	om.SetCreateStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 2)

	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil && isOrHasInternalError(err) {
		t.Errorf("PetSetControl did not return InternalError found %s", err)
	}
	// Update so set.Status is set for the next scaleUpPetSetControl call.
	var err error
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up PetSet : %s", err)
	}
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale PetSet to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to updatedReplicas correctly")
	}
}

func UpdatePodFailure(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, _, ssc := setupController(client, apiclient)
	om.SetUpdateStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 0)

	// have to have 1 successful loop first
	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var err error
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale PetSet to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set updatedReplicas correctly")
	}

	// now mutate a pod's identity
	pods, err := om.podsLister.List(labels.Everything())
	if err != nil {
		t.Fatalf("Error listing pods: %v", err)
	}
	if len(pods) != 3 {
		t.Fatalf("Expected 3 pods, got %d", len(pods))
	}
	sort.Sort(ascendingOrdinal(pods))
	pods[0].Name = "goo-0"
	om.podsIndexer.Update(pods[0])

	// now it should fail
	if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil && isOrHasInternalError(err) {
		t.Errorf("PetSetControl did not return InternalError found %s", err)
	}
}

func UpdateSetStatusFailure(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, ssu, ssc := setupController(client, apiclient)
	ssu.SetUpdateStatefulSetStatusError(apierrors.NewInternalError(errors.New("API server failed")), 2)

	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil && isOrHasInternalError(err) {
		t.Errorf("PetSetControl did not return InternalError found %s", err)
	}
	// Update so set.Status is set for the next scaleUpPetSetControl call.
	var err error
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up PetSet : %s", err)
	}
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale PetSet to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set readyReplicas to 3")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set updatedReplicas to 3")
	}
}

func PodRecreateDeleteFailure(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, _, ssc := setupController(client, apiclient)

	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
		t.Errorf("Error updating PetSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	pods[0].Status.Phase = v1.PodFailed
	om.podsIndexer.Update(pods[0])
	om.SetDeleteStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 0)
	if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil && isOrHasInternalError(err) {
		t.Errorf("PetSet failed to %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
		t.Errorf("Error updating PetSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if isCreated(pods[0]) {
		t.Error("PetSet did not recreate failed Pod")
	}
}

func NewRevisionDeletePodFailure(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, _, ssc := setupController(client, apiclient)
	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up PetSet : %s", err)
	}
	var err error
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale PetSet to 3 replicas")
	}
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}

	// trigger a new revision
	updateSet := set.DeepCopy()
	updateSet.Spec.Template.Spec.Containers[0].Image = "nginx-new"
	if err := om.setsIndexer.Update(updateSet); err != nil {
		t.Error("Failed to update PetSet")
	}
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}

	// delete fails
	om.SetDeleteStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 0)
	_, err = ssc.UpdatePetSet(context.TODO(), set, pods)
	if err == nil {
		t.Error("Expected err in update PetSet when deleting a pod")
	}

	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	if set.Status.CurrentReplicas != 3 {
		t.Fatalf("Failed pod deletion should not update CurrentReplicas: want 3, got %d", set.Status.CurrentReplicas)
	}
	if set.Status.CurrentRevision == set.Status.UpdateRevision {
		t.Error("Failed to create new revision")
	}

	// delete works
	om.SetDeleteStatefulPodError(nil, 0)
	status, err := ssc.UpdatePetSet(context.TODO(), set, pods)
	if err != nil {
		t.Fatalf("Unexpected err in update PetSet: %v", err)
	}
	if status.CurrentReplicas != 2 {
		t.Fatalf("Pod deletion should update CurrentReplicas: want 2, got %d", status.CurrentReplicas)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
}

func emptyInvariants(set *api.PetSet, om *fakeObjectManager) error {
	return nil
}

func TestPetSetControlWithStartOrdinal(t *testing.T) {
	defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PetSetStartOrdinal, true)()

	simpleSetFn := func() *api.PetSet {
		statefulSet := newPetSet(3)
		statefulSet.Spec.Ordinals = &apps.StatefulSetOrdinals{Start: int32(2)}
		return statefulSet
	}

	testCases := []struct {
		fn  func(*testing.T, *api.PetSet, invariantFunc)
		obj func() *api.PetSet
	}{
		{CreatesPodsWithStartOrdinal, simpleSetFn},
	}

	for _, testCase := range testCases {
		testObj := testCase.obj
		testFn := testCase.fn

		set := testObj()
		testFn(t, set, emptyInvariants)
	}
}

func CreatesPodsWithStartOrdinal(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, _, ssc := setupController(client, apiclient)

	if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up PetSet : %s", err)
	}
	var err error
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale petset to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set ReadyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set UpdatedReplicas correctly")
	}
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	sort.Sort(ascendingOrdinal(pods))
	for i, pod := range pods {
		expectedOrdinal := 2 + i
		actualPodOrdinal := getOrdinal(pod)
		if actualPodOrdinal != expectedOrdinal {
			t.Errorf("Expected pod ordinal %d. Got %d", expectedOrdinal, actualPodOrdinal)
		}
	}
}

func RecreatesPVCForPendingPod(t *testing.T, set *api.PetSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset()
	apiclient := apifake.NewSimpleClientset()
	om, _, ssc := setupController(client, apiclient)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
		t.Errorf("Error updating PetSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	for _, claim := range getPersistentVolumeClaims(set, pods[0]) {
		om.claimsIndexer.Delete(&claim)
	}
	pods[0].Status.Phase = v1.PodPending
	om.podsIndexer.Update(pods[0])
	if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
		t.Errorf("Error updating PetSet %s", err)
	}
	// invariants check if there any missing PVCs for the Pods
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	_, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
}

func TestPetSetControlScaleDownDeleteError(t *testing.T) {
	runTestOverPVCRetentionPolicies(
		t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
			set := newPetSet(3)
			set.Spec.PersistentVolumeClaimRetentionPolicy = policy
			invariants := assertMonotonicInvariants
			client := fake.NewSimpleClientset(set)
			apiclient := apifake.NewSimpleClientset(set)
			om, _, ssc := setupController(client, apiclient)

			if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
				t.Errorf("Failed to turn up PetSet : %s", err)
			}
			var err error
			set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated PetSet: %v", err)
			}
			*set.Spec.Replicas = 0
			om.SetDeleteStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 2)
			if err := scaleDownPetSetControl(set, ssc, om, invariants); err != nil && isOrHasInternalError(err) {
				t.Errorf("PetSetControl failed to throw error on delete %s", err)
			}
			set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated PetSet: %v", err)
			}
			if err := scaleDownPetSetControl(set, ssc, om, invariants); err != nil {
				t.Errorf("Failed to turn down PetSet %s", err)
			}
			set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated PetSet: %v", err)
			}
			if set.Status.Replicas != 0 {
				t.Error("Failed to scale petset to 0 replicas")
			}
			if set.Status.ReadyReplicas != 0 {
				t.Error("Failed to set readyReplicas to 0")
			}
			if set.Status.UpdatedReplicas != 0 {
				t.Error("Failed to set updatedReplicas to 0")
			}
		})
}

func TestPetSetControl_getSetRevisions(t *testing.T) {
	type testcase struct {
		name            string
		existing        []*apps.ControllerRevision
		set             *api.PetSet
		expectedCount   int
		expectedCurrent *apps.ControllerRevision
		expectedUpdate  *apps.ControllerRevision
		err             bool
	}

	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		apiclient := apifake.NewSimpleClientset()
		apiinformerFactory := apiinformers.NewSharedInformerFactory(apiclient, controller.NoResyncPeriodFunc())
		spc := NewStatefulPodControlFromManager(newFakeObjectManager(informerFactory, apiinformerFactory), &noopRecorder{})
		ssu := newFakeStatefulSetStatusUpdater(apiinformerFactory.Apps().V1().PetSets())
		recorder := &noopRecorder{}
		ssc := defaultPetSetControl{spc, ssu, history.NewFakeHistory(informerFactory.Apps().V1().ControllerRevisions()), recorder}

		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		cache.WaitForCacheSync(
			stop,
			informerFactory.Apps().V1().PetSets().Informer().HasSynced,
			informerFactory.Core().V1().Pods().Informer().HasSynced,
			informerFactory.Apps().V1().ControllerRevisions().Informer().HasSynced,
		)
		test.set.Status.CollisionCount = new(int32)
		for i := range test.existing {
			ssc.controllerHistory.CreateControllerRevision(test.set, test.existing[i], test.set.Status.CollisionCount)
		}
		revisions, err := ssc.ListRevisions(test.set)
		if err != nil {
			t.Fatal(err)
		}
		current, update, _, err := ssc.getPetSetRevisions(test.set, revisions)
		if err != nil {
			t.Fatalf("error getting petset revisions:%v", err)
		}
		revisions, err = ssc.ListRevisions(test.set)
		if err != nil {
			t.Fatal(err)
		}
		if len(revisions) != test.expectedCount {
			t.Errorf("%s: want %d revisions got %d", test.name, test.expectedCount, len(revisions))
		}
		if test.err && err == nil {
			t.Errorf("%s: expected error", test.name)
		}
		if !test.err && !history.EqualRevision(current, test.expectedCurrent) {
			t.Errorf("%s: for current want %v got %v", test.name, test.expectedCurrent, current)
		}
		if !test.err && !history.EqualRevision(update, test.expectedUpdate) {
			t.Errorf("%s: for update want %v got %v", test.name, test.expectedUpdate, update)
		}
		if !test.err && test.expectedCurrent != nil && current != nil && test.expectedCurrent.Revision != current.Revision {
			t.Errorf("%s: for current revision want %d got %d", test.name, test.expectedCurrent.Revision, current.Revision)
		}
		if !test.err && test.expectedUpdate != nil && update != nil && test.expectedUpdate.Revision != update.Revision {
			t.Errorf("%s: for update revision want %d got %d", test.name, test.expectedUpdate.Revision, update.Revision)
		}
	}

	updateRevision := func(cr *apps.ControllerRevision, revision int64) *apps.ControllerRevision {
		clone := cr.DeepCopy()
		clone.Revision = revision
		return clone
	}

	runTestOverPVCRetentionPolicies(
		t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
			set := newPetSet(3)
			set.Spec.PersistentVolumeClaimRetentionPolicy = policy
			set.Status.CollisionCount = new(int32)
			rev0 := newRevisionOrDie(set, 1)
			set1 := set.DeepCopy()
			set1.Spec.Template.Spec.Containers[0].Image = "foo"
			set1.Status.CurrentRevision = rev0.Name
			set1.Status.CollisionCount = new(int32)
			rev1 := newRevisionOrDie(set1, 2)
			set2 := set1.DeepCopy()
			set2.Spec.Template.Labels["new"] = "label"
			set2.Status.CurrentRevision = rev0.Name
			set2.Status.CollisionCount = new(int32)
			rev2 := newRevisionOrDie(set2, 3)
			tests := []testcase{
				{
					name:            "creates initial revision",
					existing:        nil,
					set:             set,
					expectedCount:   1,
					expectedCurrent: rev0,
					expectedUpdate:  rev0,
					err:             false,
				},
				{
					name:            "creates revision on update",
					existing:        []*apps.ControllerRevision{rev0},
					set:             set1,
					expectedCount:   2,
					expectedCurrent: rev0,
					expectedUpdate:  rev1,
					err:             false,
				},
				{
					name:            "must not recreate a new revision of same set",
					existing:        []*apps.ControllerRevision{rev0, rev1},
					set:             set1,
					expectedCount:   2,
					expectedCurrent: rev0,
					expectedUpdate:  rev1,
					err:             false,
				},
				{
					name:            "must rollback to a previous revision",
					existing:        []*apps.ControllerRevision{rev0, rev1, rev2},
					set:             set1,
					expectedCount:   3,
					expectedCurrent: rev0,
					expectedUpdate:  updateRevision(rev1, 4),
					err:             false,
				},
			}
			for i := range tests {
				testFn(&tests[i], t)
			}
		})
}

func setupPodManagementPolicy(podManagementPolicy apps.PodManagementPolicyType, set *api.PetSet) *api.PetSet {
	set.Spec.PodManagementPolicy = podManagementPolicy
	return set
}

func TestPetSetControlRollingUpdateWithMaxUnavailable(t *testing.T) {
	defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MaxUnavailablePetSet, true)()

	simpleParallelVerificationFn := func(
		set *api.PetSet,
		spc *fakeObjectManager,
		ssc PetSetControlInterface,
		pods []*v1.Pod,
		totalPods int,
		selector labels.Selector,
	) []*v1.Pod {
		// in burst mode, 2 pods got deleted, so 2 new pods will be created at the same time
		if len(pods) != totalPods {
			t.Fatalf("Expected create pods 4/5, got pods %v", len(pods))
		}

		// if pod 4 ready, start to update pod 3, even though 5 is not ready
		spc.setPodRunning(set, 4)
		spc.setPodRunning(set, 5)
		originalPods, _ := spc.setPodReady(set, 4)
		sort.Sort(ascendingOrdinal(originalPods))
		if _, err := ssc.UpdatePetSet(context.TODO(), set, originalPods); err != nil {
			t.Fatal(err)
		}
		pods, err := spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}
		sort.Sort(ascendingOrdinal(pods))
		// pods 0, 1,2, 4,5 should be present(note 3 is missing)
		if !reflect.DeepEqual(pods, append(originalPods[:3], originalPods[4:]...)) {
			t.Fatalf("Expected pods %v, got pods %v", append(originalPods[:3], originalPods[4:]...), pods)
		}

		// create new pod 3
		if _, err = ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
			t.Fatal(err)
		}
		pods, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}
		if len(pods) != totalPods {
			t.Fatalf("Expected create pods 2/3, got pods %v", pods)
		}

		return pods
	}
	simpleOrderedVerificationFn := func(
		set *api.PetSet,
		spc *fakeObjectManager,
		ssc PetSetControlInterface,
		pods []*v1.Pod,
		totalPods int,
		selector labels.Selector,
	) []*v1.Pod {
		// only one pod gets created at a time due to OrderedReady
		if len(pods) != 5 {
			t.Fatalf("Expected create pods 5, got pods %v", len(pods))
		}
		spc.setPodRunning(set, 4)
		pods, _ = spc.setPodReady(set, 4)

		// create new pods 4(only one pod gets created at a time due to OrderedReady)
		if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
			t.Fatal(err)
		}
		pods, err := spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}

		if len(pods) != totalPods {
			t.Fatalf("Expected create pods 4, got pods %v", len(pods))
		}
		// if pod 4 ready, start to update pod 3
		spc.setPodRunning(set, 5)
		originalPods, _ := spc.setPodReady(set, 5)
		sort.Sort(ascendingOrdinal(originalPods))
		if _, err = ssc.UpdatePetSet(context.TODO(), set, originalPods); err != nil {
			t.Fatal(err)
		}
		pods, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}
		sort.Sort(ascendingOrdinal(pods))

		// verify the remaining pods are 0,1,2,4,5 (3 got deleted)
		if !reflect.DeepEqual(pods, append(originalPods[:3], originalPods[4:]...)) {
			t.Fatalf("Expected pods %v, got pods %v", append(originalPods[:3], originalPods[4:]...), pods)
		}

		// create new pod 3
		if _, err = ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
			t.Fatal(err)
		}
		pods, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}
		if len(pods) != totalPods {
			t.Fatalf("Expected create pods 2/3, got pods %v", pods)
		}

		return pods
	}
	testCases := []struct {
		policyType apps.PodManagementPolicyType
		verifyFn   func(
			set *api.PetSet,
			spc *fakeObjectManager,
			ssc PetSetControlInterface,
			pods []*v1.Pod,
			totalPods int,
			selector labels.Selector,
		) []*v1.Pod
	}{
		{apps.OrderedReadyPodManagement, simpleOrderedVerificationFn},
		{apps.ParallelPodManagement, simpleParallelVerificationFn},
	}
	for _, tc := range testCases {
		// Setup the statefulSet controller
		var totalPods int32 = 6
		var partition int32 = 3
		maxUnavailable := intstr.FromInt32(2)
		set := setupPodManagementPolicy(tc.policyType, newPetSet(totalPods))
		set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
			Type: apps.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: func() *apps.RollingUpdateStatefulSetStrategy {
				return &apps.RollingUpdateStatefulSetStrategy{
					Partition:      &partition,
					MaxUnavailable: &maxUnavailable,
				}
			}(),
		}

		client := fake.NewSimpleClientset()
		apiclient := apifake.NewSimpleClientset(set)
		spc, _, ssc := setupController(client, apiclient)
		if err := scaleUpPetSetControl(set, ssc, spc, assertBurstInvariants); err != nil {
			t.Fatal(err)
		}
		set, err := spc.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatal(err)
		}

		// Change the image to trigger an update
		set.Spec.Template.Spec.Containers[0].Image = "foo"

		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatal(err)
		}
		originalPods, err := spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}
		sort.Sort(ascendingOrdinal(originalPods))

		// since maxUnavailable is 2, update pods 4 and 5, this will delete the pod 4 and 5,
		if _, err = ssc.UpdatePetSet(context.TODO(), set, originalPods); err != nil {
			t.Fatal(err)
		}
		pods, err := spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}

		sort.Sort(ascendingOrdinal(pods))

		// expected number of pod is 0,1,2,3
		if !reflect.DeepEqual(pods, originalPods[:4]) {
			t.Fatalf("Expected pods %v, got pods %v", originalPods[:4], pods)
		}

		// create new pods
		if _, err = ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
			t.Fatal(err)
		}
		pods, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}

		tc.verifyFn(set, spc, ssc, pods, int(totalPods), selector)

		// pods 3/4/5 ready, should not update other pods
		spc.setPodRunning(set, 3)
		spc.setPodRunning(set, 5)
		spc.setPodReady(set, 5)
		originalPods, _ = spc.setPodReady(set, 3)
		sort.Sort(ascendingOrdinal(originalPods))
		if _, err = ssc.UpdatePetSet(context.TODO(), set, originalPods); err != nil {
			t.Fatal(err)
		}
		pods, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}
		sort.Sort(ascendingOrdinal(pods))
		if !reflect.DeepEqual(pods, originalPods) {
			t.Fatalf("Expected pods %v, got pods %v", originalPods, pods)
		}
	}
}

func setupForInvariant(t *testing.T) (*api.PetSet, *fakeObjectManager, PetSetControlInterface, intstr.IntOrString, int32) {
	var totalPods int32 = 6
	set := newPetSet(totalPods)
	// update all pods >=3(3,4,5)
	var partition int32 = 3
	maxUnavailable := intstr.FromInt32(2)
	set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
		Type: apps.RollingUpdateStatefulSetStrategyType,
		RollingUpdate: func() *apps.RollingUpdateStatefulSetStrategy {
			return &apps.RollingUpdateStatefulSetStrategy{
				Partition:      &partition,
				MaxUnavailable: &maxUnavailable,
			}
		}(),
	}

	client := fake.NewSimpleClientset()
	apiclient := apifake.NewSimpleClientset(set)
	spc, _, ssc := setupController(client, apiclient)
	if err := scaleUpPetSetControl(set, ssc, spc, assertBurstInvariants); err != nil {
		t.Fatal(err)
	}
	set, err := spc.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatal(err)
	}

	return set, spc, ssc, maxUnavailable, totalPods
}

func TestPetSetControlRollingUpdateWithMaxUnavailableInOrderedModeVerifyInvariant(t *testing.T) {
	// Make all pods in petset unavailable one by one
	// and verify that RollingUpdate doesnt proceed with maxUnavailable set
	// this could have been a simple loop, keeping it like this to be able
	// to add more params here.
	testCases := []struct {
		ordinalOfPodToTerminate []int
	}{
		{[]int{}},
		{[]int{5}},
		{[]int{3}},
		{[]int{4}},
		{[]int{5, 4}},
		{[]int{5, 3}},
		{[]int{4, 3}},
		{[]int{5, 4, 3}},
		{[]int{2}}, // note this is an ordinal greater than partition(3)
		{[]int{1}}, // note this is an ordinal greater than partition(3)
	}
	for _, tc := range testCases {
		defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MaxUnavailablePetSet, true)()
		set, spc, ssc, maxUnavailable, totalPods := setupForInvariant(t)
		t.Run(fmt.Sprintf("terminating pod at ordinal %d", tc.ordinalOfPodToTerminate), func(t *testing.T) {
			status := apps.StatefulSetStatus{Replicas: int32(totalPods)}
			updateRevision := &apps.ControllerRevision{}

			for i := 0; i < len(tc.ordinalOfPodToTerminate); i++ {
				// Ensure at least one pod is unavailable before trying to update
				_, err := spc.addTerminatingPod(set, tc.ordinalOfPodToTerminate[i])
				if err != nil {
					t.Fatal(err)
				}
			}

			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatal(err)
			}

			originalPods, err := spc.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatal(err)
			}

			sort.Sort(ascendingOrdinal(originalPods))

			// start to update
			set.Spec.Template.Spec.Containers[0].Image = "foo"

			// try to update the petset
			// this function is only called in main code when feature gate is enabled
			if _, err = updatePetSetAfterInvariantEstablished(context.TODO(), ssc.(*defaultPetSetControl), set, originalPods, updateRevision, status); err != nil {
				t.Fatal(err)
			}
			pods, err := spc.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatal(err)
			}

			sort.Sort(ascendingOrdinal(pods))

			expecteddPodsToBeDeleted := maxUnavailable.IntValue() - len(tc.ordinalOfPodToTerminate)
			if expecteddPodsToBeDeleted < 0 {
				expecteddPodsToBeDeleted = 0
			}

			expectedPodsAfterUpdate := int(totalPods) - expecteddPodsToBeDeleted

			if len(pods) != expectedPodsAfterUpdate {
				t.Errorf("Expected pods %v, got pods %v", expectedPodsAfterUpdate, len(pods))
			}
		})
	}
}

func TestPetSetControlRollingUpdate(t *testing.T) {
	type testcase struct {
		name       string
		invariants func(set *api.PetSet, om *fakeObjectManager) error
		initial    func() *api.PetSet
		update     func(set *api.PetSet) *api.PetSet
		validate   func(set *api.PetSet, pods []*v1.Pod) error
	}

	testFn := func(test *testcase, t *testing.T) {
		set := test.initial()
		client := fake.NewSimpleClientset(set)
		apiclient := apifake.NewSimpleClientset(set)
		om, _, ssc := setupController(client, apiclient)
		if err := scaleUpPetSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err := om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set = test.update(set)
		if err := updatePetSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validate(set, pods); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
	}

	tests := []testcase{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(3)
			},
			update: func(set *api.PetSet) *api.PetSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(3)
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale down",
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(5)
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(3))
			},
			update: func(set *api.PetSet) *api.PetSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(3))
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale down",
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(5))
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

func TestPetSetControlOnDeleteUpdate(t *testing.T) {
	type testcase struct {
		name            string
		invariants      func(set *api.PetSet, om *fakeObjectManager) error
		initial         func() *api.PetSet
		update          func(set *api.PetSet) *api.PetSet
		validateUpdate  func(set *api.PetSet, pods []*v1.Pod) error
		validateRestart func(set *api.PetSet, pods []*v1.Pod) error
	}

	originalImage := newPetSet(3).Spec.Template.Spec.Containers[0].Image

	testFn := func(t *testing.T, test *testcase, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
		set := test.initial()
		set.Spec.PersistentVolumeClaimRetentionPolicy = policy
		set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{Type: apps.OnDeleteStatefulSetStrategyType}
		client := fake.NewSimpleClientset(set)
		apiclient := apifake.NewSimpleClientset(set)
		om, _, ssc := setupController(client, apiclient)
		if err := scaleUpPetSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err := om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set = test.update(set)
		if err := updatePetSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}

		// Pods may have been deleted in the update. Delete any claims with a pod ownerRef.
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		claims, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		for _, claim := range claims {
			for _, ref := range claim.GetOwnerReferences() {
				if strings.HasPrefix(ref.Name, "foo-") {
					om.claimsIndexer.Delete(claim)
					break
				}
			}
		}

		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validateUpdate(set, pods); err != nil {
			for i := range pods {
				t.Log(pods[i].Name)
			}
			t.Fatalf("%s: %s", test.name, err)

		}
		claims, err = om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		for _, claim := range claims {
			for _, ref := range claim.GetOwnerReferences() {
				if strings.HasPrefix(ref.Name, "foo-") {
					t.Fatalf("Unexpected pod reference on %s: %v", claim.Name, claim.GetOwnerReferences())
				}
			}
		}

		replicas := *set.Spec.Replicas
		*set.Spec.Replicas = 0
		if err := scaleDownPetSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		*set.Spec.Replicas = replicas

		claims, err = om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		for _, claim := range claims {
			for _, ref := range claim.GetOwnerReferences() {
				if strings.HasPrefix(ref.Name, "foo-") {
					t.Fatalf("Unexpected pod reference on %s: %v", claim.Name, claim.GetOwnerReferences())
				}
			}
		}

		if err := scaleUpPetSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validateRestart(set, pods); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
	}

	tests := []testcase{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(3)
			},
			update: func(set *api.PetSet) *api.PetSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(3)
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 3 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 3 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale down",
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(5)
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(3))
			},
			update: func(set *api.PetSet) *api.PetSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(3))
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 3 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 3 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale down",
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(5))
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	runTestOverPVCRetentionPolicies(t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
		for i := range tests {
			testFn(t, &tests[i], policy)
		}
	})
}

func TestPetSetControlRollingUpdateWithPartition(t *testing.T) {
	type testcase struct {
		name       string
		partition  int32
		invariants func(set *api.PetSet, om *fakeObjectManager) error
		initial    func() *api.PetSet
		update     func(set *api.PetSet) *api.PetSet
		validate   func(set *api.PetSet, pods []*v1.Pod) error
	}

	testFn := func(t *testing.T, test *testcase, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
		set := test.initial()
		set.Spec.PersistentVolumeClaimRetentionPolicy = policy
		set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
			Type: apps.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: func() *apps.RollingUpdateStatefulSetStrategy {
				return &apps.RollingUpdateStatefulSetStrategy{Partition: &test.partition}
			}(),
		}
		client := fake.NewSimpleClientset(set)
		apiclient := apifake.NewSimpleClientset(set)
		om, _, ssc := setupController(client, apiclient)
		if err := scaleUpPetSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err := om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set = test.update(set)
		if err := updatePetSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validate(set, pods); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
	}

	originalImage := newPetSet(3).Spec.Template.Spec.Containers[0].Image

	tests := []testcase{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			partition:  2,
			initial: func() *api.PetSet {
				return newPetSet(3)
			},
			update: func(set *api.PetSet) *api.PetSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			partition:  2,
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(3)
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			partition:  2,
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(3))
			},
			update: func(set *api.PetSet) *api.PetSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			partition:  2,
			initial: func() *api.PetSet {
				return burst(newPetSet(3))
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	runTestOverPVCRetentionPolicies(t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
		for i := range tests {
			testFn(t, &tests[i], policy)
		}
	})
}

func TestPetSetHonorRevisionHistoryLimit(t *testing.T) {
	runTestOverPVCRetentionPolicies(t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
		invariants := assertMonotonicInvariants
		set := newPetSet(3)
		set.Spec.PersistentVolumeClaimRetentionPolicy = policy
		client := fake.NewSimpleClientset(set)
		apiclient := apifake.NewSimpleClientset(set)
		om, ssu, ssc := setupController(client, apiclient)

		if err := scaleUpPetSetControl(set, ssc, om, invariants); err != nil {
			t.Errorf("Failed to turn up PetSet : %s", err)
		}
		var err error
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("Error getting updated PetSet: %v", err)
		}

		for i := 0; i < int(*set.Spec.RevisionHistoryLimit)+5; i++ {
			set.Spec.Template.Spec.Containers[0].Image = fmt.Sprintf("foo-%d", i)
			ssu.SetUpdateStatefulSetStatusError(apierrors.NewInternalError(errors.New("API server failed")), 2)
			updatePetSetControl(set, ssc, om, assertUpdateInvariants)
			set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated PetSet: %v", err)
			}
			revisions, err := ssc.ListRevisions(set)
			if err != nil {
				t.Fatalf("Error listing revisions: %v", err)
			}
			// the extra 2 revisions are `currentRevision` and `updateRevision`
			// They're considered as `live`, and truncateHistory only cleans up non-live revisions
			if len(revisions) > int(*set.Spec.RevisionHistoryLimit)+2 {
				t.Fatalf("%s: %d greater than limit %d", "", len(revisions), *set.Spec.RevisionHistoryLimit)
			}
		}
	})
}

func TestPetSetControlLimitsHistory(t *testing.T) {
	type testcase struct {
		name       string
		invariants func(set *api.PetSet, om *fakeObjectManager) error
		initial    func() *api.PetSet
	}

	testFn := func(t *testing.T, test *testcase) {
		set := test.initial()
		client := fake.NewSimpleClientset(set)
		apiclient := apifake.NewSimpleClientset(set)
		om, _, ssc := setupController(client, apiclient)
		if err := scaleUpPetSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err := om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		for i := 0; i < 10; i++ {
			set.Spec.Template.Spec.Containers[0].Image = fmt.Sprintf("foo-%d", i)
			if err := updatePetSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			pods, err := om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			_, err = ssc.UpdatePetSet(context.TODO(), set, pods)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			revisions, err := ssc.ListRevisions(set)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if len(revisions) > int(*set.Spec.RevisionHistoryLimit)+2 {
				t.Fatalf("%s: %d greater than limit %d", test.name, len(revisions), *set.Spec.RevisionHistoryLimit)
			}
		}
	}

	tests := []testcase{
		{
			name:       "monotonic update",
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(3)
			},
		},
		{
			name:       "burst update",
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(3))
			},
		},
	}
	for i := range tests {
		testFn(t, &tests[i])
	}
}

func TestPetSetControlRollback(t *testing.T) {
	type testcase struct {
		name             string
		invariants       func(set *api.PetSet, om *fakeObjectManager) error
		initial          func() *api.PetSet
		update           func(set *api.PetSet) *api.PetSet
		validateUpdate   func(set *api.PetSet, pods []*v1.Pod) error
		validateRollback func(set *api.PetSet, pods []*v1.Pod) error
	}

	originalImage := newPetSet(3).Spec.Template.Spec.Containers[0].Image

	testFn := func(t *testing.T, test *testcase) {
		set := test.initial()
		client := fake.NewSimpleClientset(set)
		apiclient := apifake.NewSimpleClientset(set)
		om, _, ssc := setupController(client, apiclient)
		if err := scaleUpPetSetControl(set, ssc, om, test.invariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err := om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set = test.update(set)
		if err := updatePetSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validateUpdate(set, pods); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		revisions, err := ssc.ListRevisions(set)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		history.SortControllerRevisions(revisions)
		set, err = ApplyRevision(set, revisions[0])
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := updatePetSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if err := test.validateRollback(set, pods); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
	}

	tests := []testcase{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(3)
			},
			update: func(set *api.PetSet) *api.PetSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(3)
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale down",
			invariants: assertMonotonicInvariants,
			initial: func() *api.PetSet {
				return newPetSet(5)
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(3))
			},
			update: func(set *api.PetSet) *api.PetSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(3))
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale down",
			invariants: assertBurstInvariants,
			initial: func() *api.PetSet {
				return burst(newPetSet(5))
			},
			update: func(set *api.PetSet) *api.PetSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *api.PetSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	for i := range tests {
		testFn(t, &tests[i])
	}
}

func TestPetSetAvailability(t *testing.T) {
	tests := []struct {
		name                   string
		inputSTS               *api.PetSet
		expectedActiveReplicas int32
		readyDuration          time.Duration
	}{
		{
			name:                   "replicas running for required time, when minReadySeconds is enabled",
			inputSTS:               setMinReadySeconds(newPetSet(1), int32(3600)),
			readyDuration:          -120 * time.Minute,
			expectedActiveReplicas: int32(1),
		},
		{
			name:                   "replicas not running for required time, when minReadySeconds is enabled",
			inputSTS:               setMinReadySeconds(newPetSet(1), int32(3600)),
			readyDuration:          -30 * time.Minute,
			expectedActiveReplicas: int32(0),
		},
	}
	for _, test := range tests {
		set := test.inputSTS
		client := fake.NewSimpleClientset(set)
		apiclient := apifake.NewSimpleClientset(set)
		spc, _, ssc := setupController(client, apiclient)
		if err := scaleUpPetSetControl(set, ssc, spc, assertBurstInvariants); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		_, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = spc.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := spc.setPodAvailable(set, 0, time.Now().Add(test.readyDuration))
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		status, err := ssc.UpdatePetSet(context.TODO(), set, pods)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if status.AvailableReplicas != test.expectedActiveReplicas {
			t.Fatalf("expected %d active replicas got %d", test.expectedActiveReplicas, status.AvailableReplicas)
		}
	}
}

func TestStatefulSetStatusUpdate(t *testing.T) {
	var (
		syncErr   = fmt.Errorf("sync error")
		statusErr = fmt.Errorf("status error")
	)

	testCases := []struct {
		desc string

		hasSyncErr   bool
		hasStatusErr bool

		expectedErr error
	}{
		{
			desc:         "no error",
			hasSyncErr:   false,
			hasStatusErr: false,
			expectedErr:  nil,
		},
		{
			desc:         "sync error",
			hasSyncErr:   true,
			hasStatusErr: false,
			expectedErr:  syncErr,
		},
		{
			desc:         "status error",
			hasSyncErr:   false,
			hasStatusErr: true,
			expectedErr:  statusErr,
		},
		{
			desc:         "sync and status error",
			hasSyncErr:   true,
			hasStatusErr: true,
			expectedErr:  syncErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			set := newPetSet(3)
			client := fake.NewSimpleClientset(set)
			apiclient := apifake.NewSimpleClientset(set)
			om, ssu, ssc := setupController(client, apiclient)

			if tc.hasSyncErr {
				om.SetCreateStatefulPodError(syncErr, 0)
			}
			if tc.hasStatusErr {
				ssu.SetUpdateStatefulSetStatusError(statusErr, 0)
			}

			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Error(err)
			}
			pods, err := om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Error(err)
			}
			_, err = ssc.UpdatePetSet(context.TODO(), set, pods)
			if ssu.updateStatusTracker.requests != 1 {
				t.Errorf("Did not update status")
			}
			if !errors.Is(err, tc.expectedErr) {
				t.Errorf("Expected error: %v, got: %v", tc.expectedErr, err)
			}
		})
	}
}

type requestTracker struct {
	sync.Mutex
	requests int
	err      error
	after    int

	parallelLock sync.Mutex
	parallel     int
	maxParallel  int

	delay time.Duration
}

func (rt *requestTracker) errorReady() bool {
	rt.Lock()
	defer rt.Unlock()
	return rt.err != nil && rt.requests >= rt.after
}

func (rt *requestTracker) inc() {
	rt.parallelLock.Lock()
	rt.parallel++
	if rt.maxParallel < rt.parallel {
		rt.maxParallel = rt.parallel
	}
	rt.parallelLock.Unlock()

	rt.Lock()
	defer rt.Unlock()
	rt.requests++
	if rt.delay != 0 {
		time.Sleep(rt.delay)
	}
}

func (rt *requestTracker) reset() {
	rt.parallelLock.Lock()
	rt.parallel = 0
	rt.parallelLock.Unlock()

	rt.Lock()
	defer rt.Unlock()
	rt.err = nil
	rt.after = 0
	rt.delay = 0
}

func (rt *requestTracker) getErr() error {
	rt.Lock()
	defer rt.Unlock()
	return rt.err
}

func newRequestTracker(requests int, err error, after int) requestTracker {
	return requestTracker{
		requests: requests,
		err:      err,
		after:    after,
	}
}

type fakeObjectManager struct {
	podsLister       corelisters.PodLister
	claimsLister     corelisters.PersistentVolumeClaimLister
	setsLister       apilisters.PetSetLister
	podsIndexer      cache.Indexer
	claimsIndexer    cache.Indexer
	setsIndexer      cache.Indexer
	revisionsIndexer cache.Indexer
	createPodTracker requestTracker
	updatePodTracker requestTracker
	deletePodTracker requestTracker
}

func newFakeObjectManager(informerFactory informers.SharedInformerFactory, apiinformerFactory apiinformers.SharedInformerFactory) *fakeObjectManager {
	podInformer := informerFactory.Core().V1().Pods()
	claimInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	setInformer := apiinformerFactory.Apps().V1().PetSets()
	revisionInformer := informerFactory.Apps().V1().ControllerRevisions()

	return &fakeObjectManager{
		podInformer.Lister(),
		claimInformer.Lister(),
		setInformer.Lister(),
		podInformer.Informer().GetIndexer(),
		claimInformer.Informer().GetIndexer(),
		setInformer.Informer().GetIndexer(),
		revisionInformer.Informer().GetIndexer(),
		newRequestTracker(0, nil, 0),
		newRequestTracker(0, nil, 0),
		newRequestTracker(0, nil, 0),
	}
}

func (om *fakeObjectManager) CreatePod(ctx context.Context, pod *v1.Pod) error {
	defer om.createPodTracker.inc()
	if om.createPodTracker.errorReady() {
		defer om.createPodTracker.reset()
		return om.createPodTracker.getErr()
	}
	pod.SetUID(types.UID(pod.Name + "-uid"))
	return om.podsIndexer.Update(pod)
}

func (om *fakeObjectManager) GetPod(namespace, podName string) (*v1.Pod, error) {
	return om.podsLister.Pods(namespace).Get(podName)
}

func (om *fakeObjectManager) UpdatePod(pod *v1.Pod) error {
	return om.podsIndexer.Update(pod)
}

func (om *fakeObjectManager) DeletePod(pod *v1.Pod) error {
	defer om.deletePodTracker.inc()
	if om.deletePodTracker.errorReady() {
		defer om.deletePodTracker.reset()
		return om.deletePodTracker.getErr()
	}
	if key, err := controller.KeyFunc(pod); err != nil {
		return err
	} else if obj, found, err := om.podsIndexer.GetByKey(key); err != nil {
		return err
	} else if found {
		return om.podsIndexer.Delete(obj)
	}
	return nil // Not found, no error in deleting.
}

func (om *fakeObjectManager) CreateClaim(claim *v1.PersistentVolumeClaim) error {
	om.claimsIndexer.Update(claim)
	return nil
}

func (om *fakeObjectManager) GetClaim(namespace, claimName string) (*v1.PersistentVolumeClaim, error) {
	return om.claimsLister.PersistentVolumeClaims(namespace).Get(claimName)
}

func (om *fakeObjectManager) UpdateClaim(claim *v1.PersistentVolumeClaim) error {
	// Validate ownerRefs.
	refs := claim.GetOwnerReferences()
	for _, ref := range refs {
		if ref.APIVersion == "" || ref.Kind == "" || ref.Name == "" {
			return fmt.Errorf("invalid ownerRefs: %s %v", claim.Name, refs)
		}
	}
	om.claimsIndexer.Update(claim)
	return nil
}

func (om *fakeObjectManager) SetCreateStatefulPodError(err error, after int) {
	om.createPodTracker.err = err
	om.createPodTracker.after = after
}

func (om *fakeObjectManager) SetUpdateStatefulPodError(err error, after int) {
	om.updatePodTracker.err = err
	om.updatePodTracker.after = after
}

func (om *fakeObjectManager) SetDeleteStatefulPodError(err error, after int) {
	om.deletePodTracker.err = err
	om.deletePodTracker.after = after
}

func findPodByOrdinal(pods []*v1.Pod, ordinal int) *v1.Pod {
	for _, pod := range pods {
		if getOrdinal(pod) == ordinal {
			return pod.DeepCopy()
		}
	}

	return nil
}

func (om *fakeObjectManager) setPodPending(set *api.PetSet, ordinal int) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	pod := findPodByOrdinal(pods, ordinal)
	if pod == nil {
		return nil, fmt.Errorf("setPodPending: pod ordinal %d not found", ordinal)
	}
	pod.Status.Phase = v1.PodPending
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

func (om *fakeObjectManager) setPodRunning(set *api.PetSet, ordinal int) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	pod := findPodByOrdinal(pods, ordinal)
	if pod == nil {
		return nil, fmt.Errorf("setPodRunning: pod ordinal %d not found", ordinal)
	}
	pod.Status.Phase = v1.PodRunning
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

func (om *fakeObjectManager) setPodReady(set *api.PetSet, ordinal int) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	pod := findPodByOrdinal(pods, ordinal)
	if pod == nil {
		return nil, fmt.Errorf("setPodReady: pod ordinal %d not found", ordinal)
	}
	condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue}
	podutil.UpdatePodCondition(&pod.Status, &condition)
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

func (om *fakeObjectManager) setPodAvailable(set *api.PetSet, ordinal int, lastTransitionTime time.Time) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	pod := findPodByOrdinal(pods, ordinal)
	if pod == nil {
		return nil, fmt.Errorf("setPodAvailable: pod ordinal %d not found", ordinal)
	}
	condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: lastTransitionTime}}
	_, existingCondition := podutil.GetPodCondition(&pod.Status, condition.Type)
	if existingCondition != nil {
		existingCondition.Status = v1.ConditionTrue
		existingCondition.LastTransitionTime = metav1.Time{Time: lastTransitionTime}
	} else {
		existingCondition = &v1.PodCondition{
			Type:               v1.PodReady,
			Status:             v1.ConditionTrue,
			LastTransitionTime: metav1.Time{Time: lastTransitionTime},
		}
		pod.Status.Conditions = append(pod.Status.Conditions, *existingCondition)
	}
	podutil.UpdatePodCondition(&pod.Status, &condition)
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

func (om *fakeObjectManager) addTerminatingPod(set *api.PetSet, ordinal int) ([]*v1.Pod, error) {
	pod := newPetSetPod(set, ordinal)
	pod.SetUID(types.UID(pod.Name + "-uid")) // To match fakeObjectManager.CreatePod
	pod.Status.Phase = v1.PodRunning
	deleted := metav1.NewTime(time.Now())
	pod.DeletionTimestamp = &deleted
	condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue}
	fakeResourceVersion(pod)
	podutil.UpdatePodCondition(&pod.Status, &condition)
	om.podsIndexer.Update(pod)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	return om.podsLister.Pods(set.Namespace).List(selector)
}

func (om *fakeObjectManager) setPodTerminated(set *api.PetSet, ordinal int) ([]*v1.Pod, error) {
	pod := newPetSetPod(set, ordinal)
	deleted := metav1.NewTime(time.Now())
	pod.DeletionTimestamp = &deleted
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	return om.podsLister.Pods(set.Namespace).List(selector)
}

var _ StatefulPodControlObjectManager = &fakeObjectManager{}

type fakeStatefulSetStatusUpdater struct {
	setsLister          apilisters.PetSetLister
	setsIndexer         cache.Indexer
	updateStatusTracker requestTracker
}

func newFakeStatefulSetStatusUpdater(setInformer stsinformers.PetSetInformer) *fakeStatefulSetStatusUpdater {
	return &fakeStatefulSetStatusUpdater{
		setInformer.Lister(),
		setInformer.Informer().GetIndexer(),
		newRequestTracker(0, nil, 0),
	}
}

func (ssu *fakeStatefulSetStatusUpdater) UpdateStatefulSetStatus(ctx context.Context, set *api.PetSet, status *apps.StatefulSetStatus) error {
	defer ssu.updateStatusTracker.inc()
	if ssu.updateStatusTracker.errorReady() {
		defer ssu.updateStatusTracker.reset()
		return ssu.updateStatusTracker.err
	}
	set.Status = *status
	ssu.setsIndexer.Update(set)
	return nil
}

func (ssu *fakeStatefulSetStatusUpdater) SetUpdateStatefulSetStatusError(err error, after int) {
	ssu.updateStatusTracker.err = err
	ssu.updateStatusTracker.after = after
}

var _ StatefulSetStatusUpdaterInterface = &fakeStatefulSetStatusUpdater{}

func assertMonotonicInvariants(set *api.PetSet, om *fakeObjectManager) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	sort.Sort(ascendingOrdinal(pods))
	for idx := 0; idx < len(pods); idx++ {
		if idx > 0 && isRunningAndReady(pods[idx]) && !isRunningAndReady(pods[idx-1]) {
			return fmt.Errorf("successor %s is Running and Ready while %s is not", pods[idx].Name, pods[idx-1].Name)
		}

		if ord := idx + getStartOrdinal(set); getOrdinal(pods[idx]) != ord {
			return fmt.Errorf("pods %s deployed in the wrong order %d", pods[idx].Name, ord)
		}

		if !storageMatches(set, pods[idx]) {
			return fmt.Errorf("pods %s does not match the storage specification of PetSet %s ", pods[idx].Name, set.Name)
		}

		for _, claim := range getPersistentVolumeClaims(set, pods[idx]) {
			claim, _ := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
			if err := checkClaimInvarients(set, pods[idx], claim); err != nil {
				return err
			}
		}

		if !identityMatches(set, pods[idx]) {
			return fmt.Errorf("pods %s does not match the identity specification of PetSet %s ", pods[idx].Name, set.Name)
		}
	}
	return nil
}

func assertBurstInvariants(set *api.PetSet, om *fakeObjectManager) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	sort.Sort(ascendingOrdinal(pods))
	for _, pod := range pods {
		if !storageMatches(set, pod) {
			return fmt.Errorf("pods %s does not match the storage specification of PetSet %s ", pod.Name, set.Name)
		}

		for _, claim := range getPersistentVolumeClaims(set, pod) {
			claim, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
			if err != nil {
				return err
			}
			if err := checkClaimInvarients(set, pod, claim); err != nil {
				return err
			}
		}

		if !identityMatches(set, pod) {
			return fmt.Errorf("pods %s does not match the identity specification of PetSet %s ",
				pod.Name,
				set.Name)
		}
	}
	return nil
}

func assertUpdateInvariants(set *api.PetSet, om *fakeObjectManager) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	sort.Sort(ascendingOrdinal(pods))
	for _, pod := range pods {

		if !storageMatches(set, pod) {
			return fmt.Errorf("pod %s does not match the storage specification of PetSet %s ", pod.Name, set.Name)
		}

		for _, claim := range getPersistentVolumeClaims(set, pod) {
			claim, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
			if err != nil {
				return err
			}
			if err := checkClaimInvarients(set, pod, claim); err != nil {
				return err
			}
		}

		if !identityMatches(set, pod) {
			return fmt.Errorf("pod %s does not match the identity specification of PetSet %s ", pod.Name, set.Name)
		}
	}
	if set.Spec.UpdateStrategy.Type == apps.OnDeleteStatefulSetStrategyType {
		return nil
	}
	if set.Spec.UpdateStrategy.Type == apps.RollingUpdateStatefulSetStrategyType {
		for i := 0; i < int(set.Status.CurrentReplicas) && i < len(pods); i++ {
			if want, got := set.Status.CurrentRevision, getPodRevision(pods[i]); want != got {
				return fmt.Errorf("pod %s want current revision %s got %s", pods[i].Name, want, got)
			}
		}
		for i, j := len(pods)-1, 0; j < int(set.Status.UpdatedReplicas); i, j = i-1, j+1 {
			if want, got := set.Status.UpdateRevision, getPodRevision(pods[i]); want != got {
				return fmt.Errorf("pod %s want update revision %s got %s", pods[i].Name, want, got)
			}
		}
	}
	return nil
}

func checkClaimInvarients(set *api.PetSet, pod *v1.Pod, claim *v1.PersistentVolumeClaim) error {
	policy := apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
		WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	if set.Spec.PersistentVolumeClaimRetentionPolicy != nil && utilfeature.DefaultFeatureGate.Enabled(features.PetSetAutoDeletePVC) {
		policy = *set.Spec.PersistentVolumeClaimRetentionPolicy
	}
	claimShouldBeRetained := policy.WhenScaled == apps.RetainPersistentVolumeClaimRetentionPolicyType
	if claim == nil {
		if claimShouldBeRetained {
			return fmt.Errorf("claim for Pod %s was not created", pod.Name)
		}
		return nil // A non-retained claim has no invariants to satisfy.
	}

	if pod.Status.Phase != v1.PodRunning || !podutil.IsPodReady(pod) {
		// The pod has spun up yet, we do not expect the owner refs on the claim to have been set.
		return nil
	}

	const retain = apps.RetainPersistentVolumeClaimRetentionPolicyType
	const delete = apps.DeletePersistentVolumeClaimRetentionPolicyType
	switch {
	case policy.WhenScaled == retain && policy.WhenDeleted == retain:
		if hasOwnerRef(claim, set) {
			return fmt.Errorf("claim %s has unexpected owner ref on %s for PetSet retain", claim.Name, set.Name)
		}
		if hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s has unexpected owner ref on pod %s for PetSet retain", claim.Name, pod.Name)
		}
	case policy.WhenScaled == retain && policy.WhenDeleted == delete:
		if !hasOwnerRef(claim, set) {
			return fmt.Errorf("claim %s does not have owner ref on %s for PetSet deletion", claim.Name, set.Name)
		}
		if hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s has unexpected owner ref on pod %s for PetSet deletion", claim.Name, pod.Name)
		}
	case policy.WhenScaled == delete && policy.WhenDeleted == retain:
		if hasOwnerRef(claim, set) {
			return fmt.Errorf("claim %s has unexpected owner ref on %s for scaledown only", claim.Name, set.Name)
		}
		if !podInOrdinalRange(pod, set) && !hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s does not have owner ref on condemned pod %s for scaledown delete", claim.Name, pod.Name)
		}
		if podInOrdinalRange(pod, set) && hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s has unexpected owner ref on condemned pod %s for scaledown delete", claim.Name, pod.Name)
		}
	case policy.WhenScaled == delete && policy.WhenDeleted == delete:
		if !podInOrdinalRange(pod, set) {
			if !hasOwnerRef(claim, pod) || hasOwnerRef(claim, set) {
				return fmt.Errorf("condemned claim %s has bad owner refs: %v", claim.Name, claim.GetOwnerReferences())
			}
		} else {
			if hasOwnerRef(claim, pod) || !hasOwnerRef(claim, set) {
				return fmt.Errorf("live claim %s has bad owner refs: %v", claim.Name, claim.GetOwnerReferences())
			}
		}
	}
	return nil
}

func fakeResourceVersion(object interface{}) {
	obj, isObj := object.(metav1.Object)
	if !isObj {
		return
	}
	if version := obj.GetResourceVersion(); version == "" {
		obj.SetResourceVersion("1")
	} else if intValue, err := strconv.ParseInt(version, 10, 32); err == nil {
		obj.SetResourceVersion(strconv.FormatInt(intValue+1, 10))
	}
}

func TestParallelScale(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		replicas        int32
		desiredReplicas int32
	}{
		{
			desc:            "scale up from 3 to 30",
			replicas:        3,
			desiredReplicas: 30,
		},
		{
			desc:            "scale down from 10 to 1",
			replicas:        10,
			desiredReplicas: 1,
		},

		{
			desc:            "scale down to 0",
			replicas:        501,
			desiredReplicas: 0,
		},
		{
			desc:            "scale up from 0",
			replicas:        0,
			desiredReplicas: 1000,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			set := burst(newPetSet(0))
			set.Spec.VolumeClaimTemplates[0].ObjectMeta.Labels = map[string]string{"test": "test"}
			parallelScale(t, set, tc.replicas, tc.desiredReplicas, assertBurstInvariants)
		})
	}
}

func parallelScale(t *testing.T, set *api.PetSet, replicas, desiredReplicas int32, invariants invariantFunc) {
	var err error
	diff := desiredReplicas - replicas
	client := fake.NewSimpleClientset(set)
	apiclient := apifake.NewSimpleClientset(set)
	om, _, ssc := setupController(client, apiclient)
	om.createPodTracker.delay = time.Millisecond

	*set.Spec.Replicas = replicas
	if err := parallelScaleUpPetSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up PetSet : %s", err)
	}
	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}
	if set.Status.Replicas != replicas {
		t.Errorf("want %v, got %v replicas", replicas, set.Status.Replicas)
	}

	fn := parallelScaleUpPetSetControl
	if diff < 0 {
		fn = parallelScaleDownPetSetControl
	}
	*set.Spec.Replicas = desiredReplicas
	if err := fn(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to scale PetSet : %s", err)
	}

	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated PetSet: %v", err)
	}

	if set.Status.Replicas != desiredReplicas {
		t.Errorf("Failed to scale petset to %v replicas, got %v replicas", desiredReplicas, set.Status.Replicas)
	}

	if (diff < -1 || diff > 1) && om.createPodTracker.maxParallel <= 1 {
		t.Errorf("want max parallel requests > 1, got %v", om.createPodTracker.maxParallel)
	}
}

func parallelScaleUpPetSetControl(set *api.PetSet,
	ssc PetSetControlInterface,
	om *fakeObjectManager,
	invariants invariantFunc,
) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}

	// Give up after 2 loops.
	// 2 * 500 pods per loop = 1000 max pods <- this should be enough for all test cases.
	// Anything slower than that (requiring more iterations) indicates a problem and should fail the test.
	maxLoops := 2
	loops := maxLoops
	for set.Status.Replicas < *set.Spec.Replicas {
		if loops < 1 {
			return fmt.Errorf("after %v loops: want %v, got replicas %v", maxLoops, *set.Spec.Replicas, set.Status.Replicas)
		}
		loops--
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))

		ordinals := []int{}
		for _, pod := range pods {
			if pod.Status.Phase == "" {
				ordinals = append(ordinals, getOrdinal(pod))
			}
		}
		// ensure all pods are valid (have a phase)
		for _, ord := range ordinals {
			if pods, err = om.setPodPending(set, ord); err != nil {
				return err
			}
		}

		// run the controller once and check invariants
		_, err = ssc.UpdatePetSet(context.TODO(), set, pods)
		if err != nil {
			return err
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}
		if err := invariants(set, om); err != nil {
			return err
		}
	}
	return invariants(set, om)
}

func parallelScaleDownPetSetControl(set *api.PetSet, ssc PetSetControlInterface, om *fakeObjectManager, invariants invariantFunc) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}

	// Give up after 2 loops.
	// 2 * 500 pods per loop = 1000 max pods <- this should be enough for all test cases.
	// Anything slower than that (requiring more iterations) indicates a problem and should fail the test.
	maxLoops := 2
	loops := maxLoops
	for set.Status.Replicas > *set.Spec.Replicas {
		if loops < 1 {
			return fmt.Errorf("after %v loops: want %v replicas, got %v", maxLoops, *set.Spec.Replicas, set.Status.Replicas)
		}
		loops--
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))
		if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
			return err
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}
		if _, err = ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
			return err
		}
	}

	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		return err
	}
	if err := invariants(set, om); err != nil {
		return err
	}

	return nil
}

func scaleUpPetSetControl(set *api.PetSet,
	ssc PetSetControlInterface,
	om *fakeObjectManager,
	invariants invariantFunc,
) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	for set.Status.ReadyReplicas < *set.Spec.Replicas {
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))

		// ensure all pods are valid (have a phase)
		for _, pod := range pods {
			if pod.Status.Phase == "" {
				if pods, err = om.setPodPending(set, getOrdinal(pod)); err != nil {
					return err
				}
				break
			}
		}

		// select one of the pods and move it forward in status
		if len(pods) > 0 {
			idx := int(rand.Int63n(int64(len(pods))))
			pod := pods[idx]
			switch pod.Status.Phase {
			case v1.PodPending:
				if pods, err = om.setPodRunning(set, getOrdinal(pod)); err != nil {
					return err
				}
			case v1.PodRunning:
				if pods, err = om.setPodReady(set, getOrdinal(pod)); err != nil {
					return err
				}
			default:
				continue
			}
		}
		// run the controller once and check invariants
		_, err = ssc.UpdatePetSet(context.TODO(), set, pods)
		if err != nil {
			return err
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}
		if err := invariants(set, om); err != nil {
			return err
		}
	}
	return invariants(set, om)
}

func scaleDownPetSetControl(set *api.PetSet, ssc PetSetControlInterface, om *fakeObjectManager, invariants invariantFunc) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}

	for set.Status.Replicas > *set.Spec.Replicas {
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))
		if idx := len(pods) - 1; idx >= 0 {
			if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
				return err
			}
			set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
			if err != nil {
				return err
			}
			if pods, err = om.addTerminatingPod(set, getOrdinal(pods[idx])); err != nil {
				return err
			}
			if _, err = ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
				return err
			}
			set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
			if err != nil {
				return err
			}
			pods, err = om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				return err
			}
			sort.Sort(ascendingOrdinal(pods))

			if len(pods) > 0 {
				om.podsIndexer.Delete(pods[len(pods)-1])
			}
		}
		if _, err := ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
			return err
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}

		if err := invariants(set, om); err != nil {
			return err
		}
	}
	// If there are claims with ownerRefs on pods that have been deleted, delete them.
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	currentPods := map[string]bool{}
	for _, pod := range pods {
		currentPods[pod.Name] = true
	}
	claims, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	for _, claim := range claims {
		claimPodName := getClaimPodName(set, claim)
		if claimPodName == "" {
			continue // Skip claims not related to a stateful set pod.
		}
		if _, found := currentPods[claimPodName]; found {
			continue // Skip claims which still have a current pod.
		}
		for _, refs := range claim.GetOwnerReferences() {
			if refs.Name == claimPodName {
				om.claimsIndexer.Delete(claim)
				break
			}
		}
	}

	return invariants(set, om)
}

func updateComplete(set *api.PetSet, pods []*v1.Pod) bool {
	sort.Sort(ascendingOrdinal(pods))
	if len(pods) != int(*set.Spec.Replicas) {
		return false
	}
	if set.Status.ReadyReplicas != *set.Spec.Replicas {
		return false
	}

	switch set.Spec.UpdateStrategy.Type {
	case apps.OnDeleteStatefulSetStrategyType:
		return true
	case apps.RollingUpdateStatefulSetStrategyType:
		if set.Spec.UpdateStrategy.RollingUpdate == nil || *set.Spec.UpdateStrategy.RollingUpdate.Partition <= 0 {
			if set.Status.CurrentReplicas < *set.Spec.Replicas {
				return false
			}
			for i := range pods {
				if getPodRevision(pods[i]) != set.Status.CurrentRevision {
					return false
				}
			}
		} else {
			partition := int(*set.Spec.UpdateStrategy.RollingUpdate.Partition)
			if len(pods) < partition {
				return false
			}
			for i := partition; i < len(pods); i++ {
				if getPodRevision(pods[i]) != set.Status.UpdateRevision {
					return false
				}
			}
		}
	}
	return true
}

func updatePetSetControl(set *api.PetSet,
	ssc PetSetControlInterface,
	om *fakeObjectManager,
	invariants invariantFunc,
) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	if _, err = ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
		return err
	}

	set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
	if err != nil {
		return err
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	for !updateComplete(set, pods) {
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))
		initialized := false
		for _, pod := range pods {
			if pod.Status.Phase == "" {
				if pods, err = om.setPodPending(set, getOrdinal(pod)); err != nil {
					return err
				}
				break
			}
		}
		if initialized {
			continue
		}

		if len(pods) > 0 {
			idx := int(rand.Int63n(int64(len(pods))))
			pod := pods[idx]
			switch pod.Status.Phase {
			case v1.PodPending:
				if pods, err = om.setPodRunning(set, getOrdinal(pod)); err != nil {
					return err
				}
			case v1.PodRunning:
				if pods, err = om.setPodReady(set, getOrdinal(pod)); err != nil {
					return err
				}
			default:
				continue
			}
		}

		if _, err = ssc.UpdatePetSet(context.TODO(), set, pods); err != nil {
			return err
		}
		set, err = om.setsLister.PetSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}
		if err := invariants(set, om); err != nil {
			return err
		}
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
	}
	return invariants(set, om)
}

func newRevisionOrDie(set *api.PetSet, revision int64) *apps.ControllerRevision {
	rev, err := newRevision(set, revision, set.Status.CollisionCount)
	if err != nil {
		panic(err)
	}
	return rev
}

func isOrHasInternalError(err error) bool {
	agg, ok := err.(utilerrors.Aggregate)
	return !ok && !apierrors.IsInternalError(err) || ok && len(agg.Errors()) > 0 && !apierrors.IsInternalError(agg.Errors()[0])
}
