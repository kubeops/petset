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
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	api "kubeops.dev/petset/apis/apps/v1"
	podutil "kubeops.dev/petset/pkg/api/v1/pod"
	"kubeops.dev/petset/pkg/controller"
	"kubeops.dev/petset/pkg/controller/history"
	"kubeops.dev/petset/pkg/features"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
)

func init() {
	// patchCodec serializes PetSets into ControllerRevisions using the client-go
	// global scheme, so the PetSet/PlacementPolicy types must be registered there.
	// Register them here rather than relying on a caller (e.g. cmd wiring) to do
	// it; otherwise constructing the controller through any other path panics with
	// "no kind is registered for the type v1.PetSet" the first time a revision is
	// created.
	utilruntime.Must(api.AddToScheme(scheme.Scheme))
}

var patchCodec = scheme.Codecs.LegacyCodec(api.SchemeGroupVersion)

// statefulPodRegex is a regular expression that extracts the parent PetSet and ordinal from the Name of a Pod
var statefulPodRegex = regexp.MustCompile("(.*)-([0-9]+)$")

// getParentNameAndOrdinal gets the name of pod's parent PetSet and pod's ordinal as extracted from its Name. If
// the Pod was not created by a PetSet, its parent is considered to be empty string, and its ordinal is considered
// to be -1.
func getParentNameAndOrdinal(pod *v1.Pod) (string, int) {
	parent := ""
	ordinal := -1
	subMatches := statefulPodRegex.FindStringSubmatch(pod.Name)
	if len(subMatches) < 3 {
		return parent, ordinal
	}
	parent = subMatches[1]
	if i, err := strconv.ParseInt(subMatches[2], 10, 32); err == nil {
		ordinal = int(i)
	}
	return parent, ordinal
}

// getParentName gets the name of pod's parent PetSet. If pod has not parent, the empty string is returned.
func getParentName(pod *v1.Pod) string {
	parent, _ := getParentNameAndOrdinal(pod)
	return parent
}

// getOrdinal gets pod's ordinal. If pod has no ordinal, -1 is returned.
func getOrdinal(pod *v1.Pod) int {
	_, ordinal := getParentNameAndOrdinal(pod)
	return ordinal
}

// getStartOrdinal gets the first possible ordinal (inclusive).
// Returns spec.ordinals.start if spec.ordinals is set, otherwise returns 0.
func getStartOrdinal(set *api.PetSet) int {
	if features.DefaultFeatureGate.Enabled(features.PetSetStartOrdinal) {
		if set.Spec.Ordinals != nil {
			return int(set.Spec.Ordinals.Start)
		}
	}
	return 0
}

// getEndOrdinal gets the last possible ordinal (inclusive).
func getEndOrdinal(set *api.PetSet) int {
	return getStartOrdinal(set) + int(*set.Spec.Replicas) - 1
}

// podInOrdinalRange returns true if the pod ordinal is within the allowed
// range of ordinals that this PetSet is set to control.
func podInOrdinalRange(pod *v1.Pod, set *api.PetSet) bool {
	ordinal := getOrdinal(pod)
	return ordinal >= getStartOrdinal(set) && ordinal <= getEndOrdinal(set)
}

// getPodName gets the name of set's child Pod with an ordinal index of ordinal
func getPodName(set *api.PetSet, ordinal int) string {
	return fmt.Sprintf("%s-%d", set.Name, ordinal)
}

// getPersistentVolumeClaimName gets the name of PersistentVolumeClaim for a Pod with an ordinal index of ordinal. claim
// must be a PersistentVolumeClaim from set's VolumeClaims template.
func getPersistentVolumeClaimName(set *api.PetSet, claim *v1.PersistentVolumeClaim, ordinal int) string {
	// NOTE: This name format is used by the heuristics for zone spreading in ChooseZoneForVolume
	return fmt.Sprintf("%s-%s-%d", claim.Name, set.Name, ordinal)
}

// isMemberOf tests if pod is a member of set.
func isMemberOf(set *api.PetSet, pod *v1.Pod) bool {
	return getParentName(pod) == set.Name
}

// identityMatches returns true if pod has a valid identity and network identity for a member of set.
func identityMatches(set *api.PetSet, pod *v1.Pod) bool {
	parent, ordinal := getParentNameAndOrdinal(pod)
	return ordinal >= 0 &&
		set.Name == parent &&
		pod.Name == getPodName(set, ordinal) &&
		pod.Namespace == set.Namespace &&
		pod.Labels[apps.StatefulSetPodNameLabel] == pod.Name
}

// storageMatches returns true if pod's Volumes cover the set of PersistentVolumeClaims
func storageMatches(set *api.PetSet, pod *v1.Pod) bool {
	ordinal := getOrdinal(pod)
	if ordinal < 0 {
		return false
	}
	volumes := make(map[string]v1.Volume, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		volumes[volume.Name] = volume
	}
	for _, claim := range set.Spec.VolumeClaimTemplates {
		volume, found := volumes[claim.Name]
		if !found ||
			volume.PersistentVolumeClaim == nil ||
			volume.PersistentVolumeClaim.ClaimName !=
				getPersistentVolumeClaimName(set, &claim, ordinal) {
			return false
		}
	}
	return true
}

// getPersistentVolumeClaimPolicy returns the PVC policy for a PetSet, returning a retain policy if the set policy is nil.
func getPersistentVolumeClaimRetentionPolicy(set *api.PetSet) apps.StatefulSetPersistentVolumeClaimRetentionPolicy {
	policy := apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	if set.Spec.PersistentVolumeClaimRetentionPolicy != nil {
		policy = *set.Spec.PersistentVolumeClaimRetentionPolicy
	}
	return policy
}

// claimOwnerMatchesSetAndPod returns false if the ownerRefs of the claim are not set consistently with the
// PVC deletion policy for the PetSet.
func claimOwnerMatchesSetAndPod(logger klog.Logger, claim *v1.PersistentVolumeClaim, set *api.PetSet, pod *v1.Pod) bool {
	policy := getPersistentVolumeClaimRetentionPolicy(set)
	const retain = apps.RetainPersistentVolumeClaimRetentionPolicyType
	const delete = apps.DeletePersistentVolumeClaimRetentionPolicyType
	switch {
	default:
		logger.Error(nil, "Unknown policy, treating as Retain", "policy", set.Spec.PersistentVolumeClaimRetentionPolicy)
		fallthrough
	case policy.WhenScaled == retain && policy.WhenDeleted == retain:
		if hasOwnerRef(claim, set) ||
			hasOwnerRef(claim, pod) {
			return false
		}
	case policy.WhenScaled == retain && policy.WhenDeleted == delete:
		if !hasOwnerRef(claim, set) ||
			hasOwnerRef(claim, pod) {
			return false
		}
	case policy.WhenScaled == delete && policy.WhenDeleted == retain:
		if hasOwnerRef(claim, set) {
			return false
		}
		podScaledDown := !podInOrdinalRange(pod, set)
		if podScaledDown != hasOwnerRef(claim, pod) {
			return false
		}
	case policy.WhenScaled == delete && policy.WhenDeleted == delete:
		podScaledDown := !podInOrdinalRange(pod, set)
		// If a pod is scaled down, there should be no set ref and a pod ref;
		// if the pod is not scaled down it's the other way around.
		if podScaledDown == hasOwnerRef(claim, set) {
			return false
		}
		if podScaledDown != hasOwnerRef(claim, pod) {
			return false
		}
	}
	return true
}

// updateClaimOwnerRefForSetAndPod updates the ownerRefs for the claim according to the deletion policy of
// the PetSet. Returns true if the claim was changed and should be updated and false otherwise.
func updateClaimOwnerRefForSetAndPod(logger klog.Logger, claim *v1.PersistentVolumeClaim, set *api.PetSet, pod *v1.Pod) bool {
	needsUpdate := false
	// Sometimes the version and kind are not set {pod,set}.TypeMeta. These are necessary for the ownerRef.
	// This is the case both in real clusters and the unittests.
	// TODO: there must be a better way to do this other than hardcoding the pod version?
	updateMeta := func(tm *metav1.TypeMeta, kind string) {
		if tm.APIVersion == "" {
			if kind == "PetSet" {
				tm.APIVersion = "apps/v1"
			} else {
				tm.APIVersion = "v1"
			}
		}
		if tm.Kind == "" {
			tm.Kind = kind
		}
	}
	podMeta := pod.TypeMeta
	updateMeta(&podMeta, "Pod")
	setMeta := set.TypeMeta
	updateMeta(&setMeta, "PetSet")
	policy := getPersistentVolumeClaimRetentionPolicy(set)
	const retain = apps.RetainPersistentVolumeClaimRetentionPolicyType
	const delete = apps.DeletePersistentVolumeClaimRetentionPolicyType
	switch {
	default:
		logger.Error(nil, "Unknown policy, treating as Retain", "policy", set.Spec.PersistentVolumeClaimRetentionPolicy)
		fallthrough
	case policy.WhenScaled == retain && policy.WhenDeleted == retain:
		needsUpdate = removeOwnerRef(claim, set) || needsUpdate
		needsUpdate = removeOwnerRef(claim, pod) || needsUpdate
	case policy.WhenScaled == retain && policy.WhenDeleted == delete:
		needsUpdate = setOwnerRef(claim, set, &setMeta) || needsUpdate
		needsUpdate = removeOwnerRef(claim, pod) || needsUpdate
	case policy.WhenScaled == delete && policy.WhenDeleted == retain:
		needsUpdate = removeOwnerRef(claim, set) || needsUpdate
		podScaledDown := !podInOrdinalRange(pod, set)
		if podScaledDown {
			needsUpdate = setOwnerRef(claim, pod, &podMeta) || needsUpdate
		}
		if !podScaledDown {
			needsUpdate = removeOwnerRef(claim, pod) || needsUpdate
		}
	case policy.WhenScaled == delete && policy.WhenDeleted == delete:
		podScaledDown := !podInOrdinalRange(pod, set)
		if podScaledDown {
			needsUpdate = removeOwnerRef(claim, set) || needsUpdate
			needsUpdate = setOwnerRef(claim, pod, &podMeta) || needsUpdate
		}
		if !podScaledDown {
			needsUpdate = setOwnerRef(claim, set, &setMeta) || needsUpdate
			needsUpdate = removeOwnerRef(claim, pod) || needsUpdate
		}
	}
	return needsUpdate
}

// hasOwnerRef returns true if target has an ownerRef to owner.
func hasOwnerRef(target, owner metav1.Object) bool {
	ownerUID := owner.GetUID()
	for _, ownerRef := range target.GetOwnerReferences() {
		if ownerRef.UID == ownerUID {
			return true
		}
	}
	return false
}

// hasStaleOwnerRef returns true if target has a ref to owner that appears to be stale.
func hasStaleOwnerRef(target, owner metav1.Object) bool {
	for _, ownerRef := range target.GetOwnerReferences() {
		if ownerRef.Name == owner.GetName() && ownerRef.UID != owner.GetUID() {
			return true
		}
	}
	return false
}

// setOwnerRef adds owner to the ownerRefs of target, if necessary. Returns true if target needs to be
// updated and false otherwise.
func setOwnerRef(target, owner metav1.Object, ownerType *metav1.TypeMeta) bool {
	if hasOwnerRef(target, owner) {
		return false
	}
	ownerRefs := append(
		target.GetOwnerReferences(),
		metav1.OwnerReference{
			APIVersion: ownerType.APIVersion,
			Kind:       ownerType.Kind,
			Name:       owner.GetName(),
			UID:        owner.GetUID(),
		},
	)
	target.SetOwnerReferences(ownerRefs)
	return true
}

// removeOwnerRef removes owner from the ownerRefs of target, if necessary. Returns true if target needs
// to be updated and false otherwise.
func removeOwnerRef(target, owner metav1.Object) bool {
	if !hasOwnerRef(target, owner) {
		return false
	}
	ownerUID := owner.GetUID()
	oldRefs := target.GetOwnerReferences()
	newRefs := make([]metav1.OwnerReference, len(oldRefs)-1)
	skip := 0
	for i := range oldRefs {
		if oldRefs[i].UID == ownerUID {
			skip = -1
		} else {
			newRefs[i+skip] = oldRefs[i]
		}
	}
	target.SetOwnerReferences(newRefs)
	return true
}

// getPersistentVolumeClaims gets a map of PersistentVolumeClaims to their template names, as defined in set. The
// returned PersistentVolumeClaims are each constructed with a the name specific to the Pod. This name is determined
// by getPersistentVolumeClaimName.
func getPersistentVolumeClaims(set *api.PetSet, pod *v1.Pod) map[string]v1.PersistentVolumeClaim {
	ordinal := getOrdinal(pod)
	templates := set.Spec.VolumeClaimTemplates
	claims := make(map[string]v1.PersistentVolumeClaim, len(templates))
	for i := range templates {
		claim := templates[i].DeepCopy()
		claim.Name = getPersistentVolumeClaimName(set, claim, ordinal)
		claim.Namespace = set.Namespace
		if claim.Labels != nil {
			for key, value := range set.Spec.Selector.MatchLabels {
				claim.Labels[key] = value
			}
		} else {
			claim.Labels = set.Spec.Selector.MatchLabels
		}
		claims[templates[i].Name] = *claim
	}
	return claims
}

// updateStorage updates pod's Volumes to conform with the PersistentVolumeClaim of set's templates. If pod has
// conflicting local Volumes these are replaced with Volumes that conform to the set's templates.
func updateStorage(set *api.PetSet, pod *v1.Pod) {
	currentVolumes := pod.Spec.Volumes
	claims := getPersistentVolumeClaims(set, pod)
	newVolumes := make([]v1.Volume, 0, len(claims))
	for name, claim := range claims {
		newVolumes = append(newVolumes, v1.Volume{
			Name: name,
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: claim.Name,
					// TODO: Use source definition to set this value when we have one.
					ReadOnly: false,
				},
			},
		})
	}
	for i := range currentVolumes {
		if _, ok := claims[currentVolumes[i].Name]; !ok {
			newVolumes = append(newVolumes, currentVolumes[i])
		}
	}
	pod.Spec.Volumes = newVolumes
}

func initIdentity(set *api.PetSet, pod *v1.Pod) {
	updateIdentity(set, pod)
	// Set these immutable fields only on initial Pod creation, not updates.
	pod.Spec.Hostname = pod.Name
	pod.Spec.Subdomain = set.Spec.ServiceName
}

// updateIdentity updates pod's name, hostname, and subdomain, and PetSetPodNameLabel to conform to set's name
// and headless service.
func updateIdentity(set *api.PetSet, pod *v1.Pod) {
	ordinal := getOrdinal(pod)
	pod.Name = getPodName(set, ordinal)
	pod.Namespace = set.Namespace
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[apps.StatefulSetPodNameLabel] = pod.Name
	if features.DefaultFeatureGate.Enabled(features.PodIndexLabel) {
		pod.Labels[apps.PodIndexLabel] = strconv.Itoa(ordinal)
	}
}

// isRunningAndReady returns true if pod is in the PodRunning Phase, if it has a condition of PodReady.
func isRunningAndReady(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodRunning && podutil.IsPodReady(pod)
}

func isRunningAndAvailable(pod *v1.Pod, minReadySeconds int32) bool {
	return podutil.IsPodAvailable(pod, minReadySeconds, metav1.Now())
}

// isCreated returns true if pod has been created and is maintained by the API server
func isCreated(pod *v1.Pod) bool {
	return pod.Status.Phase != ""
}

// isPending returns true if pod has a Phase of PodPending
func isPending(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodPending
}

// isFailed returns true if pod has a Phase of PodFailed
func isFailed(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodFailed
}

// isSucceeded returns true if pod has a Phase of PodSucceeded
func isSucceeded(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodSucceeded
}

// isTerminating returns true if pod's DeletionTimestamp has been set
func isTerminating(pod *v1.Pod) bool {
	return pod.DeletionTimestamp != nil
}

// isHealthy returns true if pod is running and ready and has not been terminated
func isHealthy(pod *v1.Pod) bool {
	return isRunningAndReady(pod) && !isTerminating(pod)
}

// allowsBurst is true if the alpha burst annotation is set.
func allowsBurst(set *api.PetSet) bool {
	return set.Spec.PodManagementPolicy == apps.ParallelPodManagement
}

// setPodRevision sets the revision of Pod to revision by adding the PetSetRevisionLabel
func setPodRevision(pod *v1.Pod, revision string) {
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[apps.StatefulSetRevisionLabel] = revision
}

// getPodRevision gets the revision of Pod by inspecting the PetSetRevisionLabel. If pod has no revision the empty
// string is returned.
func getPodRevision(pod *v1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	return pod.Labels[apps.StatefulSetRevisionLabel]
}

// newPetSetPod returns a new Pod conforming to the set's Spec with an identity generated from ordinal.
func newPetSetPod(set *api.PetSet, placementPolicy *api.PlacementPolicy, ordinal int, podList *v1.PodList) (*v1.Pod, error) {
	pInfo := controller.NewPodInfo(set, &set.Spec.Template, placementPolicy, ordinal-getStartOrdinal(set), podList)
	pod, err := controller.GetPodFromTemplate(pInfo, set, metav1.NewControllerRef(set, controllerKind))
	if err != nil {
		return pod, err
	}
	pod.Name = getPodName(set, ordinal)
	initIdentity(set, pod)
	updateStorage(set, pod)
	setOCMPlacement(set, pInfo.PodIndex, pod, placementPolicy)
	return pod, nil
}

func setOCMPlacement(set *api.PetSet, ordinal int, pod *v1.Pod, pp *api.PlacementPolicy) {
	if pp == nil || pp.Spec.ClusterSpreadConstraint == nil || pp.Spec.ClusterSpreadConstraint.DistributionRules == nil {
		return
	}
	clusterName := ""
	for i := 0; i < len(pp.Spec.ClusterSpreadConstraint.DistributionRules); i++ {
		for j := 0; j < len(pp.Spec.ClusterSpreadConstraint.DistributionRules[i].ReplicaIndices); j++ {
			if ordinal == int(pp.Spec.ClusterSpreadConstraint.DistributionRules[i].ReplicaIndices[j]) {
				clusterName = pp.Spec.ClusterSpreadConstraint.DistributionRules[i].ClusterName
				break
			}
		}
		if clusterName != "" {
			break
		}
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[api.ManifestWorkClusterNameLabel] = clusterName
	if set.Annotations == nil {
		set.Annotations = make(map[string]string)
	}
}

func setOCMPlacementForPVC(ordinal int, pvc *v1.PersistentVolumeClaim, placementPolicy *api.PlacementPolicy) {
	if placementPolicy == nil || placementPolicy.Spec.ClusterSpreadConstraint == nil || placementPolicy.Spec.ClusterSpreadConstraint.DistributionRules == nil {
		return
	}
	clusterName := getOcmClusterName(placementPolicy, ordinal)
	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}
	pvc.Annotations[api.ManifestWorkClusterNameLabel] = clusterName
}

func setStorageClassNameForPVC(ordinal int, pvc *v1.PersistentVolumeClaim, placementPolicy *api.PlacementPolicy) {
	if placementPolicy == nil || placementPolicy.Spec.ClusterSpreadConstraint == nil || placementPolicy.Spec.ClusterSpreadConstraint.DistributionRules == nil {
		return
	}
	storageClassName := getStorageClassName(placementPolicy, ordinal)
	if storageClassName != "" {
		// overwrite the storageClassName only when it's set in placementPolicy
		pvc.Spec.StorageClassName = &storageClassName
	}
}

// getPatch returns a strategic merge patch that can be applied to restore a PetSet to a
// previous version. If the returned error is nil the patch is valid. The current state that we save is just the
// PodSpecTemplate. We can modify this later to encompass more state (or less) and remain compatible with previously
// recorded patches.
func getPatch(set *api.PetSet) ([]byte, error) {
	data, err := runtime.Encode(patchCodec, set)
	if err != nil {
		return nil, err
	}
	var raw map[string]any
	err = json.Unmarshal(data, &raw)
	if err != nil {
		return nil, err
	}
	objCopy := make(map[string]any)
	specCopy := make(map[string]any)
	spec := raw["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	specCopy["template"] = template
	template["$patch"] = "replace"
	objCopy["spec"] = specCopy
	patch, err := json.Marshal(objCopy)
	return patch, err
}

// newRevision creates a new ControllerRevision containing a patch that reapplies the target state of set.
// The Revision of the returned ControllerRevision is set to revision. If the returned error is nil, the returned
// ControllerRevision is valid. PetSet revisions are stored as patches that re-apply the current state of set
// to a new PetSet using a strategic merge patch to replace the saved state of the new PetSet.
func newRevision(set *api.PetSet, revision int64, collisionCount *int32) (*apps.ControllerRevision, error) {
	patch, err := getPatch(set)
	if err != nil {
		return nil, err
	}
	cr, err := history.NewControllerRevision(set,
		controllerKind,
		set.Spec.Template.Labels,
		runtime.RawExtension{Raw: patch},
		revision,
		collisionCount)
	if err != nil {
		return nil, err
	}
	if cr.Annotations == nil {
		cr.Annotations = make(map[string]string)
	}
	for key, value := range set.Annotations {
		cr.Annotations[key] = value
	}
	return cr, nil
}

// ApplyRevision returns a new PetSet constructed by restoring the state in revision to set. If the returned error
// is nil, the returned PetSet is valid.
func ApplyRevision(set *api.PetSet, revision *apps.ControllerRevision) (*api.PetSet, error) {
	clone := set.DeepCopy()
	patched, err := strategicpatch.StrategicMergePatch([]byte(runtime.EncodeOrDie(patchCodec, clone)), revision.Data.Raw, clone)
	if err != nil {
		return nil, err
	}
	restoredSet := &api.PetSet{}
	err = json.Unmarshal(patched, restoredSet)
	if err != nil {
		return nil, err
	}
	return restoredSet, nil
}

// nextRevision finds the next valid revision number based on revisions. If the length of revisions
// is 0 this is 1. Otherwise, it is 1 greater than the largest revision's Revision. This method
// assumes that revisions has been sorted by Revision.
func nextRevision(revisions []*apps.ControllerRevision) int64 {
	count := len(revisions)
	if count <= 0 {
		return 1
	}
	return revisions[count-1].Revision + 1
}

// inconsistentStatus returns true if the ObservedGeneration of status is greater than set's
// Generation or if any of the status's fields do not match those of set's status.
func inconsistentStatus(set *api.PetSet, status *apps.StatefulSetStatus) bool {
	return status.ObservedGeneration > set.Status.ObservedGeneration ||
		status.Replicas != set.Status.Replicas ||
		status.CurrentReplicas != set.Status.CurrentReplicas ||
		status.ReadyReplicas != set.Status.ReadyReplicas ||
		status.UpdatedReplicas != set.Status.UpdatedReplicas ||
		status.CurrentRevision != set.Status.CurrentRevision ||
		status.AvailableReplicas != set.Status.AvailableReplicas ||
		status.UpdateRevision != set.Status.UpdateRevision
}

// completeRollingUpdate completes a rolling update when all of set's replica Pods have been updated
// to the updateRevision. status's currentRevision is set to updateRevision and its' updateRevision
// is set to the empty string. status's currentReplicas is set to updateReplicas and its updateReplicas
// are set to 0.
func completeRollingUpdate(set *api.PetSet, status *apps.StatefulSetStatus) {
	if set.Spec.UpdateStrategy.Type == apps.RollingUpdateStatefulSetStrategyType &&
		status.UpdatedReplicas == *set.Spec.Replicas &&
		status.ReadyReplicas == *set.Spec.Replicas &&
		status.Replicas == *set.Spec.Replicas {
		status.CurrentReplicas = status.UpdatedReplicas
		status.CurrentRevision = status.UpdateRevision
	}
}

// descendingOrdinal is a sort.Interface that Sorts a list of Pods based on the ordinals extracted
// from the Pod. Pod's that have not been constructed by PetSet's have an ordinal of -1, and are therefore pushed
// to the end of the list.
type descendingOrdinal []*v1.Pod

func (do descendingOrdinal) Len() int {
	return len(do)
}

func (do descendingOrdinal) Swap(i, j int) {
	do[i], do[j] = do[j], do[i]
}

func (do descendingOrdinal) Less(i, j int) bool {
	return getOrdinal(do[i]) > getOrdinal(do[j])
}

// getPetSetMaxUnavailable calculates the real maxUnavailable number according to the replica count
// and maxUnavailable from rollingUpdateStrategy. The number defaults to 1 if the maxUnavailable field is
// not set, and it will be round down to at least 1 if the maxUnavailable value is a percentage.
// Note that API validation has already guaranteed the maxUnavailable field to be >1 if it is an integer
// or 0% < value <= 100% if it is a percentage, so we don't have to consider other cases.
func getPetSetMaxUnavailable(maxUnavailable *intstr.IntOrString, replicaCount int) (int, error) {
	maxUnavailableNum, err := intstr.GetScaledValueFromIntOrPercent(intstr.ValueOrDefault(maxUnavailable, intstr.FromInt32(1)), replicaCount, false)
	if err != nil {
		return 0, err
	}
	// maxUnavailable might be zero for small percentage with round down.
	// So we have to enforce it not to be less than 1.
	if maxUnavailableNum < 1 {
		maxUnavailableNum = 1
	}
	return maxUnavailableNum, nil
}

func getOrdinalFromResource(resourceName string) string {
	parts := strings.Split(resourceName, "-")
	return parts[len(parts)-1]
}

func DeepCopyLabel(label map[string]string) map[string]string {
	newLabel := make(map[string]string)
	for key, value := range label {
		newLabel[key] = value
	}
	return newLabel
}

// ----------------------------------------------------------------------------
// In-place vertical scaling (resource-only) helpers.
// ----------------------------------------------------------------------------

// resizePhase enumerates the kubelet-reported progress of an in-place pod resize.
type resizePhase int

const (
	// resizeInfeasible means the kubelet rejected the resize and will not retry it.
	resizeInfeasible resizePhase = iota
	// resizeDeferred means the resize is feasible in theory but cannot be actuated right now.
	resizeDeferred
	// resizeInProgress means the kubelet has accepted the resize and is actuating it.
	resizeInProgress
	// resizeDone means the kubelet has finished actuating the resize.
	resizeDone
)

// inPlaceVerticalScalingEnabled reports whether the InPlaceVerticalScaling feature
// gate is on. When off, in-place resize is never attempted and the controller keeps
// its byte-for-byte current delete-and-recreate behavior.
func inPlaceVerticalScalingEnabled() bool {
	return features.DefaultFeatureGate.Enabled(features.InPlaceVerticalScaling)
}

// updateRevisionPodSpec materializes the pod spec captured in updateRevision by
// re-applying the revision patch to set.
func updateRevisionPodSpec(set *api.PetSet, updateRevision *apps.ControllerRevision) (*v1.PodSpec, error) {
	updateSet, err := ApplyRevision(set, updateRevision)
	if err != nil {
		return nil, err
	}
	return &updateSet.Spec.Template.Spec, nil
}

// containerResourcesByName indexes a container slice by name -> resources.
func containerResourcesByName(containers []v1.Container) map[string]v1.ResourceRequirements {
	m := make(map[string]v1.ResourceRequirements, len(containers))
	for i := range containers {
		m[containers[i].Name] = containers[i].Resources
	}
	return m
}

// applyResources copies the per-container resources (and pod-level resources, when
// set) from desiredSpec onto the target pod, matching containers by name. Containers
// present on the pod but absent from desiredSpec are left untouched.
func applyResources(target *v1.Pod, desiredSpec *v1.PodSpec) {
	desired := containerResourcesByName(desiredSpec.Containers)
	for i := range target.Spec.Containers {
		if res, ok := desired[target.Spec.Containers[i].Name]; ok {
			target.Spec.Containers[i].Resources = res
		}
	}
	desiredInit := containerResourcesByName(desiredSpec.InitContainers)
	for i := range target.Spec.InitContainers {
		if res, ok := desiredInit[target.Spec.InitContainers[i].Name]; ok {
			target.Spec.InitContainers[i].Resources = res
		}
	}
	if desiredSpec.Resources != nil {
		target.Spec.Resources = desiredSpec.Resources.DeepCopy()
	}
}

// resourcesMatch reports whether the running pod's container resources (and
// pod-level resources, when set on updateRevisionPod) already equal the
// update-revision pod. Containers are matched by name.
func resourcesMatch(updateRevisionPod *v1.Pod, pod *v1.Pod) bool {
	live := containerResourcesByName(pod.Spec.Containers)
	for i := range updateRevisionPod.Spec.Containers {
		c := &updateRevisionPod.Spec.Containers[i]
		got, ok := live[c.Name]
		if !ok || !apiequality.Semantic.DeepEqual(got, c.Resources) {
			return false
		}
	}
	liveInit := containerResourcesByName(pod.Spec.InitContainers)
	for i := range updateRevisionPod.Spec.InitContainers {
		c := &updateRevisionPod.Spec.InitContainers[i]
		got, ok := liveInit[c.Name]
		if !ok || !apiequality.Semantic.DeepEqual(got, c.Resources) {
			return false
		}
	}
	return apiequality.Semantic.DeepEqual(pod.Spec.Resources, updateRevisionPod.Spec.Resources)
}

// zeroPodResources clears every container's (and the pod-level) Resources on a pod
// spec so that two specs can be compared for non-resource differences.
func zeroPodResources(spec *v1.PodSpec) {
	for i := range spec.Containers {
		spec.Containers[i].Resources = v1.ResourceRequirements{}
	}
	for i := range spec.InitContainers {
		spec.InitContainers[i].Resources = v1.ResourceRequirements{}
	}
	spec.Resources = nil
}

// onlyResourcesDiffer renders the pod's CURRENT revision and the update revision and
// compares the whole pod specs with resources zeroed. Comparing two rendered revisions
// (instead of the live pod against a template) cancels apiserver defaulting noise, and
// comparing the FULL PodSpec (not just containers) guarantees that any non-resource
// change (volumes, affinity, tolerations, nodeSelector, securityContext, template
// labels, ...) makes the pod ineligible, so an in-place resize can never silently drop
// such a change. Equal => the ONLY difference between the revisions is resources.
func onlyResourcesDiffer(set *api.PetSet, pod *v1.Pod, currentRevision, updateRevision *apps.ControllerRevision) (bool, error) {
	// The pod is being updated FROM currentRevision TO updateRevision. The callers only
	// reach this for pods not at updateRevision, which in the PetSet model are at
	// currentRevision; verify that, and otherwise fall back to delete-and-recreate.
	if currentRevision == nil || getPodRevision(pod) != currentRevision.Name {
		return false, nil
	}
	curSpec, err := updateRevisionPodSpec(set, currentRevision)
	if err != nil {
		return false, err
	}
	updSpec, err := updateRevisionPodSpec(set, updateRevision)
	if err != nil {
		return false, err
	}
	cur := curSpec.DeepCopy()
	upd := updSpec.DeepCopy()
	zeroPodResources(cur)
	zeroPodResources(upd)
	return apiequality.Semantic.DeepEqual(cur, upd), nil
}

// inPlaceResizeEligible reports whether pod can be resized in place to the update
// revision instead of being deleted and recreated.
func inPlaceResizeEligible(set *api.PetSet, pod *v1.Pod, currentRevision, updateRevision *apps.ControllerRevision) (bool, error) {
	if !inPlaceVerticalScalingEnabled() {
		return false, nil
	}
	if !identityMatches(set, pod) || !storageMatches(set, pod) {
		return false, nil
	}
	only, err := onlyResourcesDiffer(set, pod, currentRevision, updateRevision)
	if err != nil {
		return false, err
	}
	return only, nil
}

// podCondition returns the pod condition of the given type, or nil if absent.
func podCondition(pod *v1.Pod, condType v1.PodConditionType) *v1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == condType {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

// resizeState derives the kubelet-reported progress of an in-place resize from the
// pod's status. It relies on authoritative status signals (the PodResizePending /
// PodResizeInProgress conditions and the actuated ContainerStatuses[i].Resources),
// never on the pod spec, because the spec updates instantly while the cgroup change
// is what we wait on.
func resizeState(pod *v1.Pod) resizePhase {
	if c := podCondition(pod, v1.PodResizePending); c != nil && c.Status == v1.ConditionTrue {
		if c.Reason == v1.PodReasonInfeasible {
			return resizeInfeasible
		}
		return resizeDeferred
	}
	if c := podCondition(pod, v1.PodResizeInProgress); c != nil && c.Status == v1.ConditionTrue {
		return resizeInProgress
	}
	// No pending/in-progress condition: confirm the kubelet has actuated the desired
	// resources by comparing the per-container status resources to the (already
	// updated) spec resources.
	desired := containerResourcesByName(pod.Spec.Containers)
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		want, ok := desired[cs.Name]
		if !ok {
			continue
		}
		var got v1.ResourceRequirements
		if cs.Resources != nil {
			got = *cs.Resources
		}
		if !apiequality.Semantic.DeepEqual(got, want) {
			return resizeInProgress
		}
	}
	return resizeDone
}
