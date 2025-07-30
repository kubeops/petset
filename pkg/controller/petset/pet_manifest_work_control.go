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
	"encoding/json"
	"fmt"
	"strconv"

	api "kubeops.dev/petset/apis/apps/v1"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	manifestlisters "open-cluster-management.io/api/client/work/listers/work/v1"
	apiworkv1 "open-cluster-management.io/api/work/v1"
)

func (om *realStatefulPodControlObjectManager) CreatePodManifestWork(ctx context.Context, pod *v1.Pod, set *api.PetSet) error {
	if set.Spec.PodPlacementPolicy == nil {
		return fmt.Errorf("pod placement policy can't be nil for distributed petset")
	}
	namespace := pod.Annotations[ManifestWorkClusterNameLabel]
	if namespace == "" {
		return fmt.Errorf("%v annotation is empty", ManifestWorkClusterNameLabel)
	}
	pod.APIVersion = "v1"
	pod.Kind = "Pod"
	pod.ObjectMeta.GenerateName = ""
	podUnstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		return fmt.Errorf("failed to convert pod to unstructured: %w", err)
	}

	// Adding an extra label to only delete the pod and ignore deleting pvc
	labels := DeepCopyLabel(pod.ObjectMeta.Labels)
	labels[ManifestWorkRoleLabel] = RolePod

	mw := &apiworkv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: apiworkv1.ManifestWorkSpec{
			Workload: apiworkv1.ManifestsTemplate{
				Manifests: []apiworkv1.Manifest{
					{
						RawExtension: runtime.RawExtension{
							Object: &unstructured.Unstructured{Object: podUnstructured},
						},
					},
				},
			},

			ManifestConfigs: []apiworkv1.ManifestConfigOption{
				{
					ResourceIdentifier: apiworkv1.ResourceIdentifier{
						Group:     "",
						Resource:  "pods",
						Name:      pod.Name,
						Namespace: pod.Namespace,
					},
					FeedbackRules: []apiworkv1.FeedbackRule{
						{
							Type: apiworkv1.JSONPathsType,
							JsonPaths: []apiworkv1.JsonPath{
								{
									Name: "PodPhase",
									Path: ".status.phase",
								},
								{
									Name: "PodIP",
									Path: ".status.podIP",
								},
								{
									Name: "ReadyCondition",
									Path: ".status.conditions[?(@.type=='Ready')]",
								},
								{
									Name: "PodRoleLabel",
									Path: ".metadata.labels.kubedb-role",
								},
							},
						},
					},
				},
			},
		},
	}
	_, err = om.mClient.WorkV1().ManifestWorks(namespace).Create(ctx, mw, metav1.CreateOptions{})
	return err
}

func (om *realStatefulPodControlObjectManager) GetPodFromManifestWork(set *api.PetSet, manifestWorkName string) (*v1.Pod, error) {
	ordinal, _ := strconv.Atoi(getOrdinalFromClaim(manifestWorkName))
	namespace, err := om.getOcmClusterName(set.Spec.PodPlacementPolicy.Name, ordinal)
	if namespace == "" {
		klog.Errorf("failed to get ocm clustername for %v, err : %v", manifestWorkName, err)
		return nil, nil
	}

	mw, err := om.manifestLister.ManifestWorks(namespace).Get(manifestWorkName)
	if err != nil {
		return nil, err
	}

	if len(mw.Spec.Workload.Manifests) == 0 {
		return nil, fmt.Errorf("manifestwork %s has no manifests", manifestWorkName)
	}

	manifest := mw.Spec.Workload.Manifests[0]
	pod := &v1.Pod{}

	if err := json.Unmarshal(manifest.Raw, pod); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest to pod: %w", err)
	}

	if len(mw.Status.ResourceStatus.Manifests) == 0 {
		return pod, nil
	}

	feedback := mw.Status.ResourceStatus.Manifests[0].StatusFeedbacks
	newStatus := v1.PodStatus{}
	for _, value := range feedback.Values {
		switch value.Name {
		case "PodPhase":
			if value.Value.String != nil {
				newStatus.Phase = v1.PodPhase(*value.Value.String)
			}
		case "PodIP":
			if value.Value.String != nil {
				newStatus.PodIP = *value.Value.String
			}
		case "ReadyCondition":
			if value.Value.JsonRaw != nil {
				var readyCondition v1.PodCondition
				if err := json.Unmarshal([]byte(*value.Value.JsonRaw), &readyCondition); err == nil {
					newStatus.Conditions = append(newStatus.Conditions, readyCondition)
				}
			}
		}
	}
	pod.Status = newStatus
	return pod, nil
}

// UpdatePodManifestWork handles updating a pod by updating its corresponding ManifestWork.
func (om *realStatefulPodControlObjectManager) UpdatePodManifestWork(ctx context.Context, pod *v1.Pod) error {
	namespace := pod.Annotations[ManifestWorkClusterNameLabel]
	if namespace == "" {
		return fmt.Errorf("%v annotation is empty for pod %s", ManifestWorkClusterNameLabel, pod.Name)
	}

	existingMW, err := om.mClient.WorkV1().ManifestWorks(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get existing manifestwork %s in namespace %s: %w", pod.Name, namespace, err)
	}
	pod.APIVersion = "v1"
	pod.Kind = "Pod"

	podUnstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		return fmt.Errorf("failed to convert pod to unstructured: %w", err)
	}

	existingMW.Spec.Workload.Manifests = []apiworkv1.Manifest{
		{
			RawExtension: runtime.RawExtension{
				Object: &unstructured.Unstructured{Object: podUnstructured},
			},
		},
	}

	_, err = om.mClient.WorkV1().ManifestWorks(namespace).Update(ctx, existingMW, metav1.UpdateOptions{})
	return err
}

// DeletePodManifestWork handles deleting a pod by deleting its corresponding ManifestWork.
func (om *realStatefulPodControlObjectManager) DeletePodManifestWork(ctx context.Context, pod *v1.Pod) error {
	namespace := pod.Annotations[ManifestWorkClusterNameLabel]
	if namespace == "" {
		return fmt.Errorf("%v annotation is empty for pod %s", ManifestWorkClusterNameLabel, pod.Name)
	}

	return om.mClient.WorkV1().ManifestWorks(namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
}

// ListPodsManifestWork lists all ManifestWorks matching the PetSet's selector and reconstructs
// the Pod objects from them. It safely handles ManifestWorks containing multiple or non-Pod resources.
func (om *realStatefulPodControlObjectManager) ListPodsManifestWork(set *api.PetSet) (*v1.PodList, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("could not convert PetSet selector to selector: %w", err)
	}

	mws, err := om.manifestLister.List(selector)
	if err != nil {
		return nil, fmt.Errorf("failed to list manifestworks with selector %s using lister: %w", selector.String(), err)
	}
	// TODO: list pods using PodRole label selector

	var podItems []v1.Pod
	for _, mw := range mws {
		for i, manifest := range mw.Spec.Workload.Manifests {
			unstructuredObj := &unstructured.Unstructured{}
			if err := unstructuredObj.UnmarshalJSON(manifest.Raw); err != nil {
				klog.Errorf("Failed to unmarshal manifest %d in ManifestWork %s/%s: %v", i, mw.Namespace, mw.Name, err)
				continue
			}

			if unstructuredObj.GetKind() != "Pod" {
				continue
			}

			pod := &v1.Pod{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, pod); err != nil {
				klog.Errorf("Failed to convert unstructured object to Pod for manifest %d in ManifestWork %s/%s: %v", i, mw.Namespace, mw.Name, err)
				continue
			}

			for _, manifestStatus := range mw.Status.ResourceStatus.Manifests {
				if int(manifestStatus.ResourceMeta.Ordinal) != i {
					continue
				}

				feedback := manifestStatus.StatusFeedbacks
				newStatus := v1.PodStatus{}

				for _, value := range feedback.Values {
					switch value.Name {
					case "PodPhase":
						if value.Value.String != nil {
							newStatus.Phase = v1.PodPhase(*value.Value.String)
						}
					case "PodIP":
						if value.Value.String != nil {
							newStatus.PodIP = *value.Value.String
						}
					case "ReadyCondition":
						if value.Value.JsonRaw != nil {
							var readyCondition v1.PodCondition
							if err := json.Unmarshal([]byte(*value.Value.JsonRaw), &readyCondition); err == nil {
								newStatus.Conditions = append(newStatus.Conditions, readyCondition)
							}
						}
					}
				}
				pod.Status = newStatus
				break
			}

			podItems = append(podItems, *pod)
		}
	}
	podList := &v1.PodList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodList",
			APIVersion: "v1",
		},
		Items: podItems,
	}

	return podList, nil
}

// CreateClaimManifestWork creates a ManifestWork on the hub to deploy a PersistentVolumeClaim to a managed cluster.
func (om *realStatefulPodControlObjectManager) CreateClaimManifestWork(set *api.PetSet, claim *v1.PersistentVolumeClaim) error {
	namespace := claim.Annotations[ManifestWorkClusterNameLabel]
	if namespace == "" {
		return fmt.Errorf("%v annotation is empty for pvc %s/%s", ManifestWorkClusterNameLabel, claim.Namespace, claim.Name)
	}
	claim.APIVersion = "v1"
	claim.Kind = "PersistentVolumeClaim"

	claimUnstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(claim)
	if err != nil {
		return fmt.Errorf("failed to convert claim to unstructured: %w", err)
	}
	labels := DeepCopyLabel(claim.ObjectMeta.Labels)
	labels[ManifestWorkRoleLabel] = RolePVC

	mw := &apiworkv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claim.Name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: apiworkv1.ManifestWorkSpec{
			Workload: apiworkv1.ManifestsTemplate{
				Manifests: []apiworkv1.Manifest{
					{
						RawExtension: runtime.RawExtension{
							Object: &unstructured.Unstructured{Object: claimUnstructured},
						},
					},
				},
			},
		},
	}

	_, err = om.mClient.WorkV1().ManifestWorks(namespace).Create(context.TODO(), mw, metav1.CreateOptions{})
	return err
}

// GetClaimFromManifestWork retrieves a PersistentVolumeClaim by getting its corresponding ManifestWork from the lister.
func (om *realStatefulPodControlObjectManager) GetClaimFromManifestWork(set *api.PetSet, claimName string) (*v1.PersistentVolumeClaim, error) {
	ordinal, _ := strconv.Atoi(getOrdinalFromClaim(claimName))
	namespace, err := om.getOcmClusterName(set.Spec.PodPlacementPolicy.Name, ordinal)

	if namespace == "" {
		klog.Errorf("failed to get ocm clustername for %v, err : %v", claimName, err)
		pvcResource := schema.GroupResource{Group: "", Resource: "persistentvolumeclaims"}
		return nil, errors.NewNotFound(pvcResource, claimName)
	}

	mw, err := om.manifestLister.ManifestWorks(namespace).Get(claimName)
	if err != nil {
		return nil, err
	}

	if len(mw.Spec.Workload.Manifests) == 0 {
		return nil, fmt.Errorf("manifestwork %s has no manifests", claimName)
	}

	manifest := mw.Spec.Workload.Manifests[0]
	claim := &v1.PersistentVolumeClaim{}

	if err := json.Unmarshal(manifest.Raw, claim); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest to pvc: %w", err)
	}

	return claim, nil
}

// UpdateClaimManifestWork handles updating a PVC by updating its corresponding ManifestWork.
func (om *realStatefulPodControlObjectManager) UpdateClaimManifestWork(set *api.PetSet, claim *v1.PersistentVolumeClaim) error {
	namespace := claim.Annotations[ManifestWorkClusterNameLabel]
	if namespace == "" {
		return fmt.Errorf("%v annotation is empty for pvc %s/%s", ManifestWorkClusterNameLabel, claim.Namespace, claim.Name)
	}

	existingMW, err := om.mClient.WorkV1().ManifestWorks(namespace).Get(context.TODO(), claim.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get existing manifestwork for claim %s: %w", claim.Name, err)
	}
	claim.APIVersion = "v1"
	claim.Kind = "PersistentVolumeClaim"
	claimUnstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(claim)
	if err != nil {
		return fmt.Errorf("failed to convert updated claim to unstructured: %w", err)
	}

	existingMW.Spec.Workload.Manifests = []apiworkv1.Manifest{
		{
			RawExtension: runtime.RawExtension{
				Object: &unstructured.Unstructured{Object: claimUnstructured},
			},
		},
	}

	_, err = om.mClient.WorkV1().ManifestWorks(namespace).Update(context.TODO(), existingMW, metav1.UpdateOptions{})
	return err
}

func (om *realStatefulPodControlObjectManager) getOcmClusterName(ppName string, ordinal int) (string, error) {
	pp, err := om.GetPlacementPolicy(ppName)
	if err != nil {
		return "", err
	}
	if pp == nil || pp.Spec.OCM == nil || pp.Spec.OCM.DistributionRules == nil {
		klog.Errorf("no OCM cluster spec found in pod placement policy")
		return "", nil
	}

	return getOcmClusterName(pp, ordinal), nil
}

func ListPodsFromManifestWork(manifestLister manifestlisters.ManifestWorkLister, set *api.PetSet) (*v1.PodList, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("could not convert PetSet selector to selector: %w", err)
	}

	mws, err := manifestLister.List(selector)
	if err != nil {
		return nil, fmt.Errorf("failed to list manifestworks with selector %s using lister: %w", selector.String(), err)
	}

	var podItems []v1.Pod
	for _, mw := range mws {
		for i, manifest := range mw.Spec.Workload.Manifests {
			unstructuredObj := &unstructured.Unstructured{}
			if err := unstructuredObj.UnmarshalJSON(manifest.Raw); err != nil {
				klog.Errorf("Failed to unmarshal manifest %d in ManifestWork %s/%s: %v", i, mw.Namespace, mw.Name, err)
				continue
			}

			if unstructuredObj.GetKind() != "Pod" {
				continue
			}

			pod := &v1.Pod{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, pod); err != nil {
				klog.Errorf("Failed to convert unstructured object to Pod for manifest %d in ManifestWork %s/%s: %v", i, mw.Namespace, mw.Name, err)
				continue
			}

			for _, manifestStatus := range mw.Status.ResourceStatus.Manifests {
				if int(manifestStatus.ResourceMeta.Ordinal) != i {
					continue
				}

				feedback := manifestStatus.StatusFeedbacks
				newStatus := v1.PodStatus{}

				for _, value := range feedback.Values {
					switch value.Name {
					case "PodPhase":
						if value.Value.String != nil {
							newStatus.Phase = v1.PodPhase(*value.Value.String)
						}
					case "PodIP":
						if value.Value.String != nil {
							newStatus.PodIP = *value.Value.String
						}
					case "ReadyCondition":
						if value.Value.JsonRaw != nil {
							var readyCondition v1.PodCondition
							if err := json.Unmarshal([]byte(*value.Value.JsonRaw), &readyCondition); err == nil {
								newStatus.Conditions = append(newStatus.Conditions, readyCondition)
							}
						}
					}
				}
				pod.Status = newStatus
				break
			}
			podItems = append(podItems, *pod)
		}
	}
	podList := &v1.PodList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodList",
			APIVersion: "v1",
		},
		Items: podItems,
	}

	return podList, nil
}

func getOcmClusterName(pp *api.PlacementPolicy, ordinal int) string {
	clusterName := ""
	if pp == nil || pp.Spec.OCM == nil || pp.Spec.OCM.DistributionRules == nil {
		klog.Errorf("no OCM cluster spec found in placement policy")
		return ""
	}
	for i := 0; i < len(pp.Spec.OCM.DistributionRules); i++ {
		for j := 0; j < len(pp.Spec.OCM.DistributionRules[i].Replicas); j++ {
			if ordinal == int(pp.Spec.OCM.DistributionRules[i].Replicas[j]) {
				clusterName = pp.Spec.OCM.DistributionRules[i].ClusterName
				return clusterName
			}
		}
	}
	//replicaCount := 0
	//for i := 0; i < len(pp.Spec.OCM.DistributionRules); i++ {
	//	replicaCount += int(pp.Spec.OCM.DistributionRules[i].Replicas)
	//	if ordinal < replicaCount {
	//		clusterName = pp.Spec.OCM.DistributionRules[i].ClusterName
	//		break
	//	}
	//}

	return clusterName
}

func DeepCopyLabel(label map[string]string) map[string]string {
	newLabel := make(map[string]string)
	for key, value := range label {
		newLabel[key] = value
	}
	return newLabel
}
