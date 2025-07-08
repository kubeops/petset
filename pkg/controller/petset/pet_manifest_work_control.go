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
	manifestclient "open-cluster-management.io/api/client/work/clientset/versioned"
	manifestlisters "open-cluster-management.io/api/client/work/listers/work/v1"

	api "kubeops.dev/petset/apis/apps/v1"
	appslisters "kubeops.dev/petset/client/listers/apps/v1"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	apiworkv1 "open-cluster-management.io/api/work/v1"
)

// realStatefulManifestWorkControlObjectManager uses a clientset.Interface and listers.
type realStatefulManifestWorkControlObjectManager struct {
	mClient         manifestclient.Interface
	client          clientset.Interface
	podLister       corelisters.PodLister
	manifestLister  manifestlisters.ManifestWorkLister
	placementLister appslisters.PlacementPolicyLister
	claimLister     corelisters.PersistentVolumeClaimLister
}

func (om *realStatefulManifestWorkControlObjectManager) CreatePod(ctx context.Context, pod *v1.Pod) error {
	placement, err :=
	mwc := &apiworkv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
	}

	_, err := om.client.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	return err
}

func (om *realStatefulManifestWorkControlObjectManager) GetPlacementPolicy(name string) (*api.PlacementPolicy, error) {
	return om.placementLister.Get(name)
}

func (om *realStatefulManifestWorkControlObjectManager) GetPod(namespace, podName string) (*v1.Pod, error) {
	return om.podLister.Pods(namespace).Get(podName)
}

func (om *realStatefulManifestWorkControlObjectManager) UpdatePod(pod *v1.Pod) error {
	_, err := om.client.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
	return err
}

func (om *realStatefulManifestWorkControlObjectManager) DeletePod(pod *v1.Pod) error {
	return om.client.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
}

func (om *realStatefulManifestWorkControlObjectManager) ListPods(ns, labels string) (*v1.PodList, error) {
	return om.client.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels,
	})
}

func (om *realStatefulManifestWorkControlObjectManager) CreateClaim(claim *v1.PersistentVolumeClaim) error {
	_, err := om.client.CoreV1().PersistentVolumeClaims(claim.Namespace).Create(context.TODO(), claim, metav1.CreateOptions{})
	return err
}

func (om *realStatefulManifestWorkControlObjectManager) GetClaim(namespace, claimName string) (*v1.PersistentVolumeClaim, error) {
	return om.claimLister.PersistentVolumeClaims(namespace).Get(claimName)
}

func (om *realStatefulManifestWorkControlObjectManager) UpdateClaim(claim *v1.PersistentVolumeClaim) error {
	_, err := om.client.CoreV1().PersistentVolumeClaims(claim.Namespace).Update(context.TODO(), claim, metav1.UpdateOptions{})
	return err
}
