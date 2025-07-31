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
	"fmt"
	"reflect"
	"time"

	api "kubeops.dev/petset/apis/apps/v1"
	"kubeops.dev/petset/client/clientset/versioned"
	stsinformers "kubeops.dev/petset/client/informers/externalversions/apps/v1"
	apilisters "kubeops.dev/petset/client/listers/apps/v1"
	podutil "kubeops.dev/petset/pkg/api/v1/pod"
	"kubeops.dev/petset/pkg/controller"
	"kubeops.dev/petset/pkg/controller/history"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	core_util "kmodules.xyz/client-go/core/v1"
	manifestclient "open-cluster-management.io/api/client/work/clientset/versioned"
	manifestinformers "open-cluster-management.io/api/client/work/informers/externalversions/work/v1"
	manifestlisters "open-cluster-management.io/api/client/work/listers/work/v1"
	apiworkv1 "open-cluster-management.io/api/work/v1"
)

// controllerKind contains the schema.GroupVersionKind for this controller type.
var controllerKind = api.SchemeGroupVersion.WithKind("PetSet")

// PetSetController controls petsets.
type PetSetController struct {
	// client interface
	client clientset.Interface
	// client interface
	apiClient versioned.Interface
	// manifestwork client
	ocmClient manifestclient.Interface
	// control returns an interface capable of syncing a stateful set.
	// Abstracted out for testing.
	control PetSetControlInterface
	// podControl is used for patching pods.
	podControl controller.PodControlInterface
	// podLister is able to list/get pods from a shared informer's store
	podLister corelisters.PodLister
	// podListerSynced returns true if the pod shared informer has synced at least once
	podListerSynced cache.InformerSynced
	// setLister is able to list/get stateful sets from a shared informer's store
	setLister apilisters.PetSetLister
	// setListerSynced returns true if the stateful set shared informer has synced at least once
	setListerSynced cache.InformerSynced
	// pvcListerSynced returns true if the pvc shared informer has synced at least once
	pvcListerSynced cache.InformerSynced
	// placementListerSynced returns true if the placementPolicy shared informer has synced at least once
	placementListerSynced cache.InformerSynced
	// revListerSynced returns true if the rev shared informer has synced at least once
	revListerSynced cache.InformerSynced

	// manifestWorkerLister is able to list/get manifestWork from a shared informer's store
	manifestLister manifestlisters.ManifestWorkLister
	// manifestListerSynced returns true if the pvc shared informer has synced at least once
	manifestListerSynced cache.InformerSynced
	// PetSets that need to be synced.
	queue workqueue.RateLimitingInterface
	// eventBroadcaster is the core of event processing pipeline.
	eventBroadcaster record.EventBroadcaster
}

// NewPetSetController creates a new petset controller.
func NewPetSetController(
	ctx context.Context,
	podInformer coreinformers.PodInformer,
	setInformer stsinformers.PetSetInformer,
	placementInformer stsinformers.PlacementPolicyInformer,
	pvcInformer coreinformers.PersistentVolumeClaimInformer,
	revInformer appsinformers.ControllerRevisionInformer,
	manifestInformer manifestinformers.ManifestWorkInformer,
	kubeClient clientset.Interface,
	apiClient versioned.Interface,
	ocmClient manifestclient.Interface,
) *PetSetController {
	logger := klog.FromContext(ctx)
	eventBroadcaster := record.NewBroadcaster()
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "petset-controller"})
	ssc := &PetSetController{
		client:    kubeClient,
		apiClient: apiClient,
		ocmClient: ocmClient,
		control: NewDefaultPetSetControl(
			NewStatefulPodControl(
				kubeClient,
				ocmClient,
				podInformer.Lister(),
				manifestInformer.Lister(),
				placementInformer.Lister(),
				pvcInformer.Lister(),
				recorder),
			NewRealStatefulSetStatusUpdater(apiClient, setInformer.Lister()),
			history.NewHistory(kubeClient, revInformer.Lister()),
			recorder,
		),
		pvcListerSynced:       pvcInformer.Informer().HasSynced,
		placementListerSynced: placementInformer.Informer().HasSynced,
		revListerSynced:       revInformer.Informer().HasSynced,
		queue:                 workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "petset"),
		podControl:            controller.RealPodControl{KubeClient: kubeClient, Recorder: recorder},

		eventBroadcaster: eventBroadcaster,
	}

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ssc.addPod(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ssc.updatePod(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			ssc.deletePod(logger, obj)
		},
	})

	ssc.podLister = podInformer.Lister()
	ssc.podListerSynced = podInformer.Informer().HasSynced

	manifestInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ssc.addManifestWork(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ssc.updateManifestWork(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			ssc.deleteManifestWork(logger, obj)
		},
	})

	ssc.manifestLister = manifestInformer.Lister()
	ssc.manifestListerSynced = manifestInformer.Informer().HasSynced

	setInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: ssc.enqueuePetSet,
			UpdateFunc: func(old, cur interface{}) {
				oldPS := old.(*api.PetSet)
				curPS := cur.(*api.PetSet)
				if oldPS.Status.Replicas != curPS.Status.Replicas {
					logger.V(4).Info("Observed updated replica count for PetSet", "statefulSet", klog.KObj(curPS), "oldReplicas", oldPS.Status.Replicas, "newReplicas", curPS.Status.Replicas)
				}
				ssc.enqueuePetSet(cur)
			},
			DeleteFunc: ssc.enqueuePetSet,
		},
	)
	ssc.setLister = setInformer.Lister()
	ssc.setListerSynced = setInformer.Informer().HasSynced

	// TODO: Watch volumes
	return ssc
}

// Run runs the petset controller.
func (ssc *PetSetController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	// Start events processing pipeline.
	ssc.eventBroadcaster.StartStructuredLogging(0)
	ssc.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: ssc.client.CoreV1().Events("")})
	defer ssc.eventBroadcaster.Shutdown()

	defer ssc.queue.ShutDown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting stateful set controller")
	defer logger.Info("Shutting down petset controller")

	if !cache.WaitForNamedCacheSync("stateful set", ctx.Done(), ssc.podListerSynced, ssc.setListerSynced, ssc.placementListerSynced, ssc.pvcListerSynced, ssc.revListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, ssc.worker, time.Second)
	}

	<-ctx.Done()
}

// addPod adds the petset for the pod to the sync queue
func (ssc *PetSetController) addPod(logger klog.Logger, obj interface{}) {
	pod := obj.(*v1.Pod)

	if pod.DeletionTimestamp != nil {
		// on a restart of the controller manager, it's possible a new pod shows up in a state that
		// is already pending deletion. Prevent the pod from being a creation observation.
		ssc.deletePod(logger, pod)
		return
	}

	// If it has a ControllerRef, that's all that matters.
	if controllerRef := metav1.GetControllerOf(pod); controllerRef != nil {
		set := ssc.resolveControllerRef(pod.Namespace, controllerRef)
		if set == nil {
			return
		}
		logger.V(4).Info("Pod created with labels", "pod", klog.KObj(pod), "labels", pod.Labels)
		ssc.enqueuePetSet(set)
		return
	}

	// Otherwise, it's an orphan. Get a list of all matching controllers and sync
	// them to see if anyone wants to adopt it.
	sets := ssc.getPetSetsForPod(pod)
	if len(sets) == 0 {
		return
	}
	logger.V(4).Info("Orphan Pod created with labels", "pod", klog.KObj(pod), "labels", pod.Labels)
	for _, set := range sets {
		ssc.enqueuePetSet(set)
	}
}

// updatePod adds the petset for the current and old pods to the sync queue.
func (ssc *PetSetController) updatePod(logger klog.Logger, old, cur interface{}) {
	curPod := cur.(*v1.Pod)
	oldPod := old.(*v1.Pod)
	if curPod.ResourceVersion == oldPod.ResourceVersion {
		// In the event of a re-list we may receive update events for all known pods.
		// Two different versions of the same pod will always have different RVs.
		return
	}

	labelChanged := !reflect.DeepEqual(curPod.Labels, oldPod.Labels)

	curControllerRef := metav1.GetControllerOf(curPod)
	oldControllerRef := metav1.GetControllerOf(oldPod)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// The ControllerRef was changed. Sync the old controller, if any.
		if set := ssc.resolveControllerRef(oldPod.Namespace, oldControllerRef); set != nil {
			ssc.enqueuePetSet(set)
		}
	}

	// If it has a ControllerRef, that's all that matters.
	if curControllerRef != nil {
		set := ssc.resolveControllerRef(curPod.Namespace, curControllerRef)
		if set == nil {
			return
		}
		logger.V(4).Info("Pod objectMeta updated", "pod", klog.KObj(curPod), "oldObjectMeta", oldPod.ObjectMeta, "newObjectMeta", curPod.ObjectMeta)
		ssc.enqueuePetSet(set)
		// TODO: MinReadySeconds in the Pod will generate an Available condition to be added in
		// the Pod status which in turn will trigger a requeue of the owning replica set thus
		// having its status updated with the newly available replica.
		if !podutil.IsPodReady(oldPod) && podutil.IsPodReady(curPod) && set.Spec.MinReadySeconds > 0 {
			logger.V(2).Info("PetSet will be enqueued after minReadySeconds for availability check", "statefulSet", klog.KObj(set), "minReadySeconds", set.Spec.MinReadySeconds)
			// Add a second to avoid milliseconds skew in AddAfter.
			// See https://github.com/kubernetes/kubernetes/issues/39785#issuecomment-279959133 for more info.
			ssc.enqueueSSAfter(set, (time.Duration(set.Spec.MinReadySeconds)*time.Second)+time.Second)
		}
		return
	}

	// Otherwise, it's an orphan. If anything changed, sync matching controllers
	// to see if anyone wants to adopt it now.
	if labelChanged || controllerRefChanged {
		sets := ssc.getPetSetsForPod(curPod)
		if len(sets) == 0 {
			return
		}
		logger.V(4).Info("Orphan Pod objectMeta updated", "pod", klog.KObj(curPod), "oldObjectMeta", oldPod.ObjectMeta, "newObjectMeta", curPod.ObjectMeta)
		for _, set := range sets {
			ssc.enqueuePetSet(set)
		}
	}
}

// deletePod enqueues the petset for the pod accounting for deletion tombstones.
func (ssc *PetSetController) deletePod(logger klog.Logger, obj interface{}) {
	pod, ok := obj.(*v1.Pod)

	// When a delete is dropped, the relist will notice a pod in the store not
	// in the list, leading to the insertion of a tombstone object which contains
	// the deleted key/value. Note that this value might be stale.
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %+v", obj))
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a pod %+v", obj))
			return
		}
	}

	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil {
		// No controller should care about orphans being deleted.
		return
	}
	set := ssc.resolveControllerRef(pod.Namespace, controllerRef)
	if set == nil {
		return
	}
	logger.V(4).Info("Pod deleted.", "pod", klog.KObj(pod), "caller", utilruntime.GetCaller())
	ssc.enqueuePetSet(set)
}

// getPodsForPetSet returns the Pods that a given PetSet should manage.
// It also reconciles ControllerRef by adopting/orphaning.
//
// NOTE: Returned Pods are pointers to objects from the cache.
// If you need to modify one, you need to copy it first.
func (ssc *PetSetController) getPodsForPetSet(ctx context.Context, set *api.PetSet, selector labels.Selector) ([]*v1.Pod, error) {
	// List all pods to include the pods that don't match the selector anymore but
	// has a ControllerRef pointing to this PetSet.

	if set.Spec.Distributed {
		podLists, err := ListPodsFromManifestWork(ssc.manifestLister, set)
		if err != nil {
			return nil, err
		}
		var pods []*v1.Pod
		for _, pod := range podLists.Items {
			pods = append(pods, &pod)
		}
		return pods, nil
	}

	pods, err := ssc.podLister.Pods(set.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	filter := func(pod *v1.Pod) bool {
		// Only claim if it matches our PetSet name. Otherwise release/ignore.
		return isMemberOf(set, pod)
	}

	cm := controller.NewPodControllerRefManager(ssc.podControl, set, selector, controllerKind, ssc.canAdoptFunc(ctx, set))
	return cm.ClaimPods(ctx, pods, filter)
}

func (ssc *PetSetController) addManifestWork(logger klog.Logger, obj interface{}) {
	mw := obj.(*apiworkv1.ManifestWork)
	if mw.DeletionTimestamp != nil {
		// on a restart of the controller manager, it's possible a new pod shows up in a state that
		// is already pending deletion. Prevent the pod from being a creation observation.
		ssc.deleteManifestWork(logger, mw)
		return
	}
	sets := ssc.getPetSetsForManifestWorks(mw)
	if len(sets) == 0 {
		return
	}
	logger.V(4).Info("Orphan manifeswork created with labels", "manifeswork", klog.KObj(mw), "labels", mw.Labels)
	for _, set := range sets {
		ssc.enqueuePetSet(set)
	}
}

// updateManifestWork adds the petset for the current and ManifestWork pods to the sync queue.
func (ssc *PetSetController) updateManifestWork(logger klog.Logger, old, cur interface{}) {
	curMW := cur.(*apiworkv1.ManifestWork)
	oldMW := old.(*apiworkv1.ManifestWork)
	if curMW.ResourceVersion == oldMW.ResourceVersion {
		// In the event of a re-list we may receive update events for all known pods.
		// Two different versions of the same pod will always have different RVs.
		return
	}

	sets := ssc.getPetSetsForManifestWorks(curMW)
	if len(sets) == 0 {
		return
	}
	logger.V(4).Info("Orphan ManifestWork objectMeta updated", "manifest", klog.KObj(curMW), "oldObjectMeta", oldMW.ObjectMeta, "newObjectMeta", curMW.ObjectMeta)
	for _, set := range sets {
		ssc.enqueuePetSet(set)
	}
}

// deleteManifestWork enqueues the petset for the ManifestWork accounting for deletion tombstones.
func (ssc *PetSetController) deleteManifestWork(logger klog.Logger, obj interface{}) {
	mw, ok := obj.(*apiworkv1.ManifestWork)

	// When a delete is dropped, the relist will notice an object in the store not
	// in the list, leading to the insertion of a tombstone object which contains
	// the deleted key/value.
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %+v", obj))
			return
		}
		mw, ok = tombstone.Obj.(*apiworkv1.ManifestWork)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a manifestwork %+v", obj))
			return
		}
	}

	sets := ssc.getPetSetsForManifestWorks(mw)
	if len(sets) == 0 {
		return
	}

	for _, set := range sets {
		logger.V(4).Info("ManifestWork deleted, enqueuing owner PetSet", "manifestwork", klog.KObj(mw), "petset", klog.KObj(set))
		ssc.enqueuePetSet(set)
	}
}

// If any adoptions are attempted, we should first recheck for deletion with
// an uncached quorum read sometime after listing Pods/ControllerRevisions (see #42639).
func (ssc *PetSetController) canAdoptFunc(ctx context.Context, set *api.PetSet) func(ctx2 context.Context) error {
	return controller.RecheckDeletionTimestamp(func(ctx context.Context) (metav1.Object, error) {
		fresh, err := ssc.apiClient.AppsV1().PetSets(set.Namespace).Get(ctx, set.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != set.UID {
			return nil, fmt.Errorf("original PetSet %v/%v is gone: got uid %v, wanted %v", set.Namespace, set.Name, fresh.UID, set.UID)
		}
		return fresh, nil
	})
}

// adoptOrphanRevisions adopts any orphaned ControllerRevisions matched by set's Selector.
func (ssc *PetSetController) adoptOrphanRevisions(ctx context.Context, set *api.PetSet) error {
	revisions, err := ssc.control.ListRevisions(set)
	if err != nil {
		return err
	}
	orphanRevisions := make([]*apps.ControllerRevision, 0)
	for i := range revisions {
		if metav1.GetControllerOf(revisions[i]) == nil {
			orphanRevisions = append(orphanRevisions, revisions[i])
		}
	}
	if len(orphanRevisions) > 0 {
		canAdoptErr := ssc.canAdoptFunc(ctx, set)(ctx)
		if canAdoptErr != nil {
			return fmt.Errorf("can't adopt ControllerRevisions: %v", canAdoptErr)
		}
		return ssc.control.AdoptOrphanRevisions(set, orphanRevisions)
	}
	return nil
}

// getPetSetsForPod returns a list of PetSets that potentially match
// a given pod.
func (ssc *PetSetController) getPetSetsForPod(pod *v1.Pod) []*api.PetSet {
	sets, err := ssc.setLister.GetPodPetSets(pod)
	if err != nil {
		return nil
	}
	// More than one set is selecting the same Pod
	if len(sets) > 1 {
		// ControllerRef will ensure we don't do anything crazy, but more than one
		// item in this list nevertheless constitutes user error.
		setNames := []string{}
		for _, s := range sets {
			setNames = append(setNames, s.Name)
		}
		utilruntime.HandleError(
			fmt.Errorf(
				"user error: more than one PetSet is selecting pods with labels: %+v. Sets: %v",
				pod.Labels, setNames))
	}
	return sets
}

// getPetSetsForManifestWorks returns a list of PetSets that potentially match
// a given manifestwork.
func (ssc *PetSetController) getPetSetsForManifestWorks(mw *apiworkv1.ManifestWork) []*api.PetSet {
	sets, err := ssc.setLister.GetManifestWorkPetSets(mw)
	if err != nil {
		return nil
	}
	// More than one set is selecting the same Pod
	if len(sets) > 1 {
		// ControllerRef will ensure we don't do anything crazy, but more than one
		// item in this list nevertheless constitutes user error.
		setNames := []string{}
		for _, s := range sets {
			setNames = append(setNames, s.Name)
		}
		utilruntime.HandleError(
			fmt.Errorf(
				"user error: more than one PetSet is selecting manifeswork with labels: %+v. Sets: %v",
				mw.Labels, setNames))
	}
	return sets
}

// resolveControllerRef returns the controller referenced by a ControllerRef,
// or nil if the ControllerRef could not be resolved to a matching controller
// of the correct Kind.
func (ssc *PetSetController) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *api.PetSet {
	// We can't look up by UID, so look up by Name and then verify UID.
	// Don't even try to look up by Name if it's the wrong Kind.
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	set, err := ssc.setLister.PetSets(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if set.UID != controllerRef.UID {
		// The controller we found with this Name is not the same one that the
		// ControllerRef points to.
		return nil
	}
	return set
}

// enqueuePetSet enqueues the given petset in the work queue.
func (ssc *PetSetController) enqueuePetSet(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %v", obj, err))
		return
	}
	ssc.queue.Add(key)
}

// enqueuePetSet enqueues the given petset in the work queue after given time
func (ssc *PetSetController) enqueueSSAfter(ss *api.PetSet, duration time.Duration) {
	key, err := controller.KeyFunc(ss)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", ss, err))
		return
	}
	ssc.queue.AddAfter(key, duration)
}

// processNextWorkItem dequeues items, processes them, and marks them done. It enforces that the syncHandler is never
// invoked concurrently with the same key.
func (ssc *PetSetController) processNextWorkItem(ctx context.Context) bool {
	key, quit := ssc.queue.Get()
	if quit {
		return false
	}
	defer ssc.queue.Done(key)
	if err := ssc.sync(ctx, key.(string)); err != nil {
		utilruntime.HandleError(fmt.Errorf("error syncing PetSet %v, requeuing: %v", key.(string), err))
		ssc.queue.AddRateLimited(key)
	} else {
		ssc.queue.Forget(key)
	}
	return true
}

// worker runs a worker goroutine that invokes processNextWorkItem until the controller's queue is closed
func (ssc *PetSetController) worker(ctx context.Context) {
	for ssc.processNextWorkItem(ctx) {
	}
}

// sync syncs the given petset.
func (ssc *PetSetController) sync(ctx context.Context, key string) error {
	startTime := time.Now()
	logger := klog.FromContext(ctx)
	defer func() {
		logger.V(4).Info("Finished syncing petset", "key", key, "time", time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	set, err := ssc.setLister.PetSets(namespace).Get(name)
	if errors.IsNotFound(err) {
		logger.Info("PetSet has been deleted", "key", key)
		return nil
	}
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to retrieve PetSet %v from store: %v", key, err))
		return err
	}
	if set.Spec.Distributed {
		rq, err := ssc.handleDistributedPetset(ctx, set)
		if rq {
			return err
		}
	}

	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error converting PetSet %v selector: %v", key, err))
		// This is a non-transient error, so don't retry.
		return nil
	}

	if err := ssc.adoptOrphanRevisions(ctx, set); err != nil {
		return err
	}

	// TODO: check if any manifestwork object has to be removed
	// Use case: user set set.spec.distributed = false

	pods, err := ssc.getPodsForPetSet(ctx, set, selector)
	if err != nil {
		return err
	}

	return ssc.syncPetSet(ctx, set, pods)
}

// syncPetSet syncs a tuple of (petset, []*v1.Pod).
func (ssc *PetSetController) syncPetSet(ctx context.Context, set *api.PetSet, pods []*v1.Pod) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Syncing PetSet with pods", "statefulSet", klog.KObj(set), "pods", len(pods))
	var status *apps.StatefulSetStatus
	var err error
	status, err = ssc.control.UpdatePetSet(ctx, set, pods)
	if err != nil {
		return err
	}
	logger.V(4).Info("Successfully synced PetSet", "statefulSet", klog.KObj(set))
	// One more sync to handle the clock skew. This is also helping in requeuing right after status update
	if set.Spec.MinReadySeconds > 0 && status != nil && status.AvailableReplicas != *set.Spec.Replicas {
		ssc.enqueueSSAfter(set, time.Duration(set.Spec.MinReadySeconds)*time.Second)
	}

	return nil
}

func (ssc *PetSetController) handleFinalizerRemove(set *api.PetSet) error {
	// TODO: cc@Arnob vai. should Delete all the Manifestworks

	sel := set.Spec.Selector.DeepCopy()
	// This role is added during manifestwork deletion
	sel.MatchLabels[api.ManifestWorkRoleLabel] = api.RolePod

	selector, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		return err
	}
	mws, err := ssc.manifestLister.List(selector)
	if err != nil {
		return err
	}
	for _, mw := range mws {
		err := ssc.ocmClient.WorkV1().ManifestWorks(mw.Namespace).Delete(context.TODO(), mw.Name, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}
	setCopy := set.DeepCopy()
	setCopy.ObjectMeta = core_util.RemoveFinalizer(setCopy.ObjectMeta, api.GroupName)
	_, err = ssc.apiClient.AppsV1().PetSets(set.Namespace).Update(context.TODO(), setCopy, metav1.UpdateOptions{})
	return err
}

func (ssc *PetSetController) handleDistributedPetset(ctx context.Context, set *api.PetSet) (bool, error) {
	setCopy := set.DeepCopy()

	if set.DeletionTimestamp != nil {
		return true, ssc.handleFinalizerRemove(set)
	}
	rq := false

	if setCopy.DeletionTimestamp == nil && !core_util.HasFinalizer(setCopy.ObjectMeta, api.GroupName) {
		setCopy.ObjectMeta = core_util.AddFinalizer(setCopy.ObjectMeta, api.GroupName)
		rq = true
	}
	if !rq {
		return false, nil
	}
	_, err := ssc.apiClient.AppsV1().PetSets(set.Namespace).Update(ctx, setCopy, metav1.UpdateOptions{})
	return rq, err
}
