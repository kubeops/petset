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

package petset

import (
	"testing"

	api "kubeops.dev/petset/apis/apps/v1"
	"kubeops.dev/petset/pkg/features"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

// resList is a small helper to build a ResourceList.
func resList(cpu, mem string) v1.ResourceList {
	rl := v1.ResourceList{}
	if cpu != "" {
		rl[v1.ResourceCPU] = resource.MustParse(cpu)
	}
	if mem != "" {
		rl[v1.ResourceMemory] = resource.MustParse(mem)
	}
	return rl
}

// inPlaceTestSet returns a single-replica PetSet with a known container and
// resources that we can mutate to construct update revisions.
func inPlaceTestSet() *api.PetSet {
	set := newPetSet(1)
	set.Spec.Template.Spec.Containers[0].Resources = v1.ResourceRequirements{
		Requests: resList("100m", "256Mi"),
		Limits:   resList("100m", "256Mi"),
	}
	return set
}

func TestOnlyResourcesDiffer(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(set *api.PetSet)
		wantOnly bool
	}{
		{
			name:     "identical revision => no diff at all (still resource-only eligible)",
			mutate:   func(set *api.PetSet) {},
			wantOnly: true,
		},
		{
			name: "resources only differ",
			mutate: func(set *api.PetSet) {
				set.Spec.Template.Spec.Containers[0].Resources.Requests = resList("200m", "512Mi")
				set.Spec.Template.Spec.Containers[0].Resources.Limits = resList("200m", "512Mi")
			},
			wantOnly: true,
		},
		{
			name: "image change",
			mutate: func(set *api.PetSet) {
				set.Spec.Template.Spec.Containers[0].Image = "nginx:1.2.3"
			},
			wantOnly: false,
		},
		{
			name: "env change",
			mutate: func(set *api.PetSet) {
				set.Spec.Template.Spec.Containers[0].Env = []v1.EnvVar{{Name: "FOO", Value: "bar"}}
			},
			wantOnly: false,
		},
		{
			name: "probe change",
			mutate: func(set *api.PetSet) {
				set.Spec.Template.Spec.Containers[0].LivenessProbe = &v1.Probe{
					ProbeHandler: v1.ProbeHandler{
						Exec: &v1.ExecAction{Command: []string{"true"}},
					},
				}
			},
			wantOnly: false,
		},
		{
			name: "args change alongside resources",
			mutate: func(set *api.PetSet) {
				set.Spec.Template.Spec.Containers[0].Args = []string{"--flag"}
				set.Spec.Template.Spec.Containers[0].Resources.Requests = resList("200m", "512Mi")
			},
			wantOnly: false,
		},
		{
			name: "non-container pod-spec change (nodeSelector) is NOT resource-only",
			mutate: func(set *api.PetSet) {
				set.Spec.Template.Spec.NodeSelector = map[string]string{"disktype": "ssd"}
			},
			wantOnly: false,
		},
		{
			name: "non-container pod-spec change (volume) alongside resources is NOT resource-only",
			mutate: func(set *api.PetSet) {
				set.Spec.Template.Spec.Volumes = append(set.Spec.Template.Spec.Volumes,
					v1.Volume{Name: "extra", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}})
				set.Spec.Template.Spec.Containers[0].Resources.Requests = resList("200m", "512Mi")
			},
			wantOnly: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			set := inPlaceTestSet()
			// The live pod is at the current revision.
			curRev := newRevisionOrDie(set, 1)
			pod := newTestPetSetPod(set, 0)
			setPodRevision(pod, curRev.Name)

			updSet := set.DeepCopy()
			tc.mutate(updSet)
			updRev := newRevisionOrDie(updSet, 2)

			only, err := onlyResourcesDiffer(set, pod, curRev, updRev)
			if err != nil {
				t.Fatalf("onlyResourcesDiffer returned error: %v", err)
			}
			if only != tc.wantOnly {
				t.Errorf("onlyResourcesDiffer = %v, want %v", only, tc.wantOnly)
			}
		})
	}
}

func TestInPlaceResizeEligibleGateOff(t *testing.T) {
	set := inPlaceTestSet()
	curRev := newRevisionOrDie(set, 1)
	pod := newTestPetSetPod(set, 0)
	setPodRevision(pod, curRev.Name)

	updSet := set.DeepCopy()
	updSet.Spec.Template.Spec.Containers[0].Resources.Requests = resList("200m", "512Mi")
	updRev := newRevisionOrDie(updSet, 2)

	// Gate on => eligible (resource-only diff).
	featuregatetesting.SetFeatureGateDuringTest(t, features.DefaultFeatureGate, features.InPlaceVerticalScaling, true)
	if eligible, err := inPlaceResizeEligible(set, pod, curRev, updRev); err != nil || !eligible {
		t.Fatalf("gate on: inPlaceResizeEligible = (%v, %v), want (true, nil)", eligible, err)
	}

	// Gate off => never eligible.
	featuregatetesting.SetFeatureGateDuringTest(t, features.DefaultFeatureGate, features.InPlaceVerticalScaling, false)
	if eligible, err := inPlaceResizeEligible(set, pod, curRev, updRev); err != nil || eligible {
		t.Fatalf("gate off: inPlaceResizeEligible = (%v, %v), want (false, nil)", eligible, err)
	}
}

func TestResourcesMatch(t *testing.T) {
	set := inPlaceTestSet()
	pod := newTestPetSetPod(set, 0)

	target := pod.DeepCopy()
	if !resourcesMatch(target, pod) {
		t.Errorf("resourcesMatch on identical pods = false, want true")
	}

	target.Spec.Containers[0].Resources.Requests = resList("200m", "512Mi")
	if resourcesMatch(target, pod) {
		t.Errorf("resourcesMatch with differing requests = true, want false")
	}
}

func conditionTrue(t v1.PodConditionType, reason string) v1.PodCondition {
	return v1.PodCondition{Type: t, Status: v1.ConditionTrue, Reason: reason}
}

func TestResizeState(t *testing.T) {
	desired := v1.ResourceRequirements{Requests: resList("200m", "512Mi")}
	makePod := func(conds []v1.PodCondition, statusRes *v1.ResourceRequirements) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "p-0"},
			Spec: v1.PodSpec{
				Containers: []v1.Container{{Name: "nginx", Resources: desired}},
			},
			Status: v1.PodStatus{
				Conditions:        conds,
				ContainerStatuses: []v1.ContainerStatus{{Name: "nginx", Resources: statusRes}},
			},
		}
	}

	tests := []struct {
		name string
		pod  *v1.Pod
		want resizePhase
	}{
		{
			name: "pending infeasible",
			pod:  makePod([]v1.PodCondition{conditionTrue(v1.PodResizePending, v1.PodReasonInfeasible)}, &desired),
			want: resizeInfeasible,
		},
		{
			name: "pending deferred",
			pod:  makePod([]v1.PodCondition{conditionTrue(v1.PodResizePending, v1.PodReasonDeferred)}, nil),
			want: resizeDeferred,
		},
		{
			name: "in progress condition",
			pod:  makePod([]v1.PodCondition{conditionTrue(v1.PodResizeInProgress, "")}, nil),
			want: resizeInProgress,
		},
		{
			name: "status not yet actuated",
			pod:  makePod(nil, &v1.ResourceRequirements{Requests: resList("100m", "256Mi")}),
			want: resizeInProgress,
		},
		{
			name: "done",
			pod:  makePod(nil, &desired),
			want: resizeDone,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resizeState(tc.pod); got != tc.want {
				t.Errorf("resizeState = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsResizeUnsupported(t *testing.T) {
	gr := schema.GroupResource{Resource: "pods"}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"notfound", apierrors.NewNotFound(gr, "resize"), true},
		{"method not supported", apierrors.NewMethodNotSupported(gr, "resize"), true},
		{"gate off forbidden", apierrors.NewForbidden(gr, "p-0",
			&fakeErr{"pod updates may not change fields other than spec.containers[*].resources unless InPlacePodVerticalScaling is enabled"}), true},
		{"transient conflict", apierrors.NewConflict(gr, "p-0", &fakeErr{"conflict"}), false},
		{"server timeout", apierrors.NewServerTimeout(gr, "resize", 1), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isResizeUnsupported(tc.err); got != tc.want {
				t.Errorf("isResizeUnsupported(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

type fakeErr struct{ s string }

func (e *fakeErr) Error() string { return e.s }
