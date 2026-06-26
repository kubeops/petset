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

package controller

import (
	"testing"

	api "kubeops.dev/petset/apis/apps/v1"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func placementTestPetSet(replicas int32) *api.PetSet {
	return &api.PetSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: api.PetSetSpec{
			Replicas: &replicas,
		},
	}
}

func placementTestTemplate(labels map[string]string) *api.PodTemplateSpec {
	return &api.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{Name: "main", Image: "busybox"}},
		},
	}
}

// ---------------------------------------------------------------------------
// Upsert helpers
// ---------------------------------------------------------------------------

func TestUpsertTopologySpreadConstraint(t *testing.T) {
	a := v1.TopologySpreadConstraint{TopologyKey: v1.LabelTopologyZone, MaxSkew: 1}
	b := v1.TopologySpreadConstraint{TopologyKey: v1.LabelHostname, MaxSkew: 2}

	// append into empty list
	lst := UpsertTopologySpreadConstraint(nil, a)
	if len(lst) != 1 || lst[0].TopologyKey != v1.LabelTopologyZone {
		t.Fatalf("expected zone constraint appended, got %+v", lst)
	}

	// append a different topology key
	lst = UpsertTopologySpreadConstraint(lst, b)
	if len(lst) != 2 {
		t.Fatalf("expected 2 constraints, got %d: %+v", len(lst), lst)
	}

	// upsert the existing zone key replaces it in place (no growth)
	updated := v1.TopologySpreadConstraint{TopologyKey: v1.LabelTopologyZone, MaxSkew: 5}
	lst = UpsertTopologySpreadConstraint(lst, updated)
	if len(lst) != 2 {
		t.Fatalf("expected list to stay at 2 after replace, got %d", len(lst))
	}
	for _, c := range lst {
		if c.TopologyKey == v1.LabelTopologyZone && c.MaxSkew != 5 {
			t.Errorf("expected zone MaxSkew updated to 5, got %d", c.MaxSkew)
		}
	}
}

func TestUpsertWeightedPodAffinityTerm(t *testing.T) {
	zone := v1.WeightedPodAffinityTerm{Weight: 10, PodAffinityTerm: v1.PodAffinityTerm{TopologyKey: v1.LabelTopologyZone}}
	host := v1.WeightedPodAffinityTerm{Weight: 20, PodAffinityTerm: v1.PodAffinityTerm{TopologyKey: v1.LabelHostname}}

	lst := UpsertWeightedPodAffinityTerm(nil, zone)
	lst = UpsertWeightedPodAffinityTerm(lst, host)
	if len(lst) != 2 {
		t.Fatalf("expected 2 weighted terms, got %d", len(lst))
	}

	// replace zone by topology key
	lst = UpsertWeightedPodAffinityTerm(lst, v1.WeightedPodAffinityTerm{Weight: 99, PodAffinityTerm: v1.PodAffinityTerm{TopologyKey: v1.LabelTopologyZone}})
	if len(lst) != 2 {
		t.Fatalf("expected list to stay at 2 after replace, got %d", len(lst))
	}
	for _, term := range lst {
		if term.PodAffinityTerm.TopologyKey == v1.LabelTopologyZone && term.Weight != 99 {
			t.Errorf("expected zone weight updated to 99, got %d", term.Weight)
		}
	}
}

func TestUpsertPodAffinityTerm(t *testing.T) {
	zone := v1.PodAffinityTerm{TopologyKey: v1.LabelTopologyZone}
	host := v1.PodAffinityTerm{TopologyKey: v1.LabelHostname}

	lst := UpsertPodAffinityTerm(nil, zone)
	lst = UpsertPodAffinityTerm(lst, host)
	if len(lst) != 2 {
		t.Fatalf("expected 2 terms, got %d", len(lst))
	}
	// replacing the same topology key does not grow the slice
	lst = UpsertPodAffinityTerm(lst, zone)
	if len(lst) != 2 {
		t.Fatalf("expected list to stay at 2 after replace, got %d", len(lst))
	}
}

func TestUpsertNodeSelectorRequirements(t *testing.T) {
	a := v1.NodeSelectorRequirement{Key: "zone", Operator: v1.NodeSelectorOpIn, Values: []string{"a"}}
	b := v1.NodeSelectorRequirement{Key: "rack", Operator: v1.NodeSelectorOpIn, Values: []string{"1"}}

	lst := UpsertNodeSelectorRequirements(nil, a)
	lst = UpsertNodeSelectorRequirements(lst, b)
	if len(lst) != 2 {
		t.Fatalf("expected 2 requirements, got %d", len(lst))
	}
	// replace existing key "zone" with new values
	lst = UpsertNodeSelectorRequirements(lst, v1.NodeSelectorRequirement{Key: "zone", Operator: v1.NodeSelectorOpIn, Values: []string{"b", "c"}})
	if len(lst) != 2 {
		t.Fatalf("expected list to stay at 2 after replace, got %d", len(lst))
	}
	for _, req := range lst {
		if req.Key == "zone" && !cmp.Equal(req.Values, []string{"b", "c"}) {
			t.Errorf("expected zone values replaced, got %v", req.Values)
		}
	}
}

// ---------------------------------------------------------------------------
// CalculateForPodPlacement: nil policy
// ---------------------------------------------------------------------------

func TestCalculateForPodPlacement_NilPolicy(t *testing.T) {
	tmpl := placementTestTemplate(map[string]string{"app": "test"})
	pInfo := NewPodInfo(placementTestPetSet(3), tmpl, nil, 0, &v1.PodList{})

	got, err := CalculateForPodPlacement(&pInfo)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cmp.Equal(got, tmpl.Spec) {
		t.Errorf("expected pod spec unchanged for nil policy, diff:\n%s", cmp.Diff(tmpl.Spec, got))
	}
	if got.Affinity != nil {
		t.Errorf("expected no affinity injected for nil policy, got %+v", got.Affinity)
	}
}

// ---------------------------------------------------------------------------
// Spread constraints + pod anti-affinity
// ---------------------------------------------------------------------------

func TestCalculateForPodPlacement_ZoneSpreadDoNotSchedule(t *testing.T) {
	labels := map[string]string{"app": "test"}
	pp := &api.PlacementPolicy{
		Spec: api.PlacementPolicySpec{
			ZoneSpreadConstraint: &api.ZoneSpreadConstraint{
				MaxSkew:           1,
				WhenUnsatisfiable: v1.DoNotSchedule,
			},
		},
	}
	pInfo := NewPodInfo(placementTestPetSet(3), placementTestTemplate(labels), pp, 0, &v1.PodList{})

	got, err := CalculateForPodPlacement(&pInfo)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(got.TopologySpreadConstraints) != 1 {
		t.Fatalf("expected 1 topology spread constraint, got %d", len(got.TopologySpreadConstraints))
	}
	tsc := got.TopologySpreadConstraints[0]
	if tsc.TopologyKey != v1.LabelTopologyZone || tsc.MaxSkew != 1 || tsc.WhenUnsatisfiable != v1.DoNotSchedule {
		t.Errorf("unexpected topology spread constraint: %+v", tsc)
	}
	if !cmp.Equal(tsc.LabelSelector.MatchLabels, labels) {
		t.Errorf("expected label selector %v, got %v", labels, tsc.LabelSelector.MatchLabels)
	}

	if got.Affinity == nil || got.Affinity.PodAntiAffinity == nil {
		t.Fatalf("expected pod anti-affinity to be set")
	}
	req := got.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(req) != 1 || req[0].TopologyKey != v1.LabelTopologyZone {
		t.Errorf("expected required zone anti-affinity, got %+v", req)
	}
	if len(got.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 0 {
		t.Errorf("did not expect preferred anti-affinity for DoNotSchedule")
	}
}

func TestCalculateForPodPlacement_NodeSpreadScheduleAnyway(t *testing.T) {
	labels := map[string]string{"app": "test"}
	pp := &api.PlacementPolicy{
		Spec: api.PlacementPolicySpec{
			NodeSpreadConstraint: &api.NodeSpreadConstraint{
				MaxSkew:           2,
				WhenUnsatisfiable: v1.ScheduleAnyway,
			},
		},
	}
	pInfo := NewPodInfo(placementTestPetSet(3), placementTestTemplate(labels), pp, 0, &v1.PodList{})

	got, err := CalculateForPodPlacement(&pInfo)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(got.TopologySpreadConstraints) != 1 || got.TopologySpreadConstraints[0].TopologyKey != v1.LabelHostname {
		t.Fatalf("expected 1 hostname topology spread constraint, got %+v", got.TopologySpreadConstraints)
	}

	if got.Affinity == nil || got.Affinity.PodAntiAffinity == nil {
		t.Fatalf("expected pod anti-affinity to be set")
	}
	preferred := got.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(preferred) != 1 {
		t.Fatalf("expected 1 preferred anti-affinity term, got %d", len(preferred))
	}
	if preferred[0].Weight != 100 || preferred[0].PodAffinityTerm.TopologyKey != v1.LabelHostname {
		t.Errorf("unexpected preferred term: %+v", preferred[0])
	}
	if len(got.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
		t.Errorf("did not expect required anti-affinity for ScheduleAnyway")
	}
}

// ---------------------------------------------------------------------------
// Node affinity from placement
// ---------------------------------------------------------------------------

func TestCalculateForPodPlacement_NodeAffinityDoNotSchedule(t *testing.T) {
	labels := map[string]string{"app": "test"}
	pp := &api.PlacementPolicy{
		Spec: api.PlacementPolicySpec{
			Affinity: &api.Affinity{
				NodeAffinity: []api.NodeAffinityRule{
					{
						TopologyKey:       "topology.kubernetes.io/zone",
						WhenUnsatisfiable: v1.DoNotSchedule,
						Domains: []api.TopologyDomain{
							{Values: []string{"zone-a"}, Replicas: ""}, // unlimited
						},
					},
				},
			},
		},
	}
	pInfo := NewPodInfo(placementTestPetSet(3), placementTestTemplate(labels), pp, 0, &v1.PodList{})

	got, err := CalculateForPodPlacement(&pInfo)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Affinity == nil || got.Affinity.NodeAffinity == nil || got.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatalf("expected required node affinity to be set")
	}
	terms := got.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 {
		t.Fatalf("expected single required node selector term, got %+v", terms)
	}
	req := terms[0].MatchExpressions[0]
	if req.Key != "topology.kubernetes.io/zone" || req.Operator != v1.NodeSelectorOpIn || !cmp.Equal(req.Values, []string{"zone-a"}) {
		t.Errorf("unexpected node selector requirement: %+v", req)
	}
}

func TestCalculateForPodPlacement_NodeAffinityScheduleAnyway(t *testing.T) {
	labels := map[string]string{"app": "test"}
	pp := &api.PlacementPolicy{
		Spec: api.PlacementPolicySpec{
			Affinity: &api.Affinity{
				NodeAffinity: []api.NodeAffinityRule{
					{
						TopologyKey:       "topology.kubernetes.io/zone",
						WhenUnsatisfiable: v1.ScheduleAnyway,
						Weight:            50,
						Domains: []api.TopologyDomain{
							{Values: []string{"zone-a"}, Replicas: ""},
						},
					},
				},
			},
		},
	}
	pInfo := NewPodInfo(placementTestPetSet(3), placementTestTemplate(labels), pp, 0, &v1.PodList{})

	got, err := CalculateForPodPlacement(&pInfo)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Affinity == nil || got.Affinity.NodeAffinity == nil {
		t.Fatalf("expected node affinity to be set")
	}
	if got.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		t.Errorf("did not expect required node affinity for ScheduleAnyway")
	}
	preferred := got.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(preferred) != 1 || preferred[0].Weight != 50 {
		t.Fatalf("expected one preferred scheduling term with weight 50, got %+v", preferred)
	}
}

// ---------------------------------------------------------------------------
// getAppropriateDomainIndex
// ---------------------------------------------------------------------------

func podWithRequiredZone(zone string) v1.Pod {
	return v1.Pod{
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{Key: "topology.kubernetes.io/zone", Operator: v1.NodeSelectorOpIn, Values: []string{zone}},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestGetAppropriateDomainIndex(t *testing.T) {
	rule := api.NodeAffinityRule{
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: v1.DoNotSchedule,
		Domains: []api.TopologyDomain{
			{Values: []string{"zone-a"}, Replicas: "2"},
			{Values: []string{"zone-b"}, Replicas: "2"},
		},
	}

	tests := []struct {
		name      string
		pods      []v1.Pod
		wantIndex int
		wantErr   bool
	}{
		{
			name:      "empty pod list picks first domain",
			pods:      nil,
			wantIndex: 0,
		},
		{
			name:      "first domain partially filled still picks first",
			pods:      []v1.Pod{podWithRequiredZone("zone-a")},
			wantIndex: 0,
		},
		{
			name:      "first domain full rolls over to second",
			pods:      []v1.Pod{podWithRequiredZone("zone-a"), podWithRequiredZone("zone-a")},
			wantIndex: 1,
		},
		{
			name: "all domains full returns error",
			pods: []v1.Pod{
				podWithRequiredZone("zone-a"), podWithRequiredZone("zone-a"),
				podWithRequiredZone("zone-b"), podWithRequiredZone("zone-b"),
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pInfo := NewPodInfo(placementTestPetSet(4), placementTestTemplate(map[string]string{"app": "test"}),
				&api.PlacementPolicy{ObjectMeta: metav1.ObjectMeta{Name: "pp"}}, 0,
				&v1.PodList{Items: tc.pods})
			if err := preCalc(&pInfo); err != nil {
				t.Fatalf("preCalc failed: %v", err)
			}
			idx, err := getAppropriateDomainIndex(rule, pInfo)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got index %d", idx)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if idx != tc.wantIndex {
				t.Errorf("got domain index %d, want %d", idx, tc.wantIndex)
			}
		})
	}
}

func TestGetAppropriateDomainIndex_UnlimitedDomain(t *testing.T) {
	rule := api.NodeAffinityRule{
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: v1.DoNotSchedule,
		Domains: []api.TopologyDomain{
			{Values: []string{"zone-a"}, Replicas: "1"},
			{Values: []string{"zone-b"}, Replicas: ""}, // unlimited
		},
	}
	// zone-a is full, but zone-b is unlimited so it should always be selectable.
	pInfo := NewPodInfo(placementTestPetSet(10), placementTestTemplate(map[string]string{"app": "test"}),
		&api.PlacementPolicy{ObjectMeta: metav1.ObjectMeta{Name: "pp"}}, 0,
		&v1.PodList{Items: []v1.Pod{
			podWithRequiredZone("zone-a"),
			podWithRequiredZone("zone-b"), podWithRequiredZone("zone-b"),
		}})
	if err := preCalc(&pInfo); err != nil {
		t.Fatalf("preCalc failed: %v", err)
	}
	idx, err := getAppropriateDomainIndex(rule, pInfo)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx != 1 {
		t.Errorf("expected unlimited zone-b (index 1), got %d", idx)
	}
}

// ---------------------------------------------------------------------------
// evaluateCEL
// ---------------------------------------------------------------------------

func TestEvaluateCEL(t *testing.T) {
	pInfo := NewPodInfo(placementTestPetSet(7), placementTestTemplate(map[string]string{"app": "test"}), nil, 0, &v1.PodList{})
	if err := preCalc(&pInfo); err != nil {
		t.Fatalf("preCalc failed: %v", err)
	}

	tests := []struct {
		name    string
		rule    string
		want    int64
		wantErr bool
	}{
		{name: "empty string means unlimited", rule: "", want: -1},
		{name: "plain integer", rule: "5", want: 5},
		{name: "negative integer", rule: "-3", want: -3},
		{name: "cel reads replicas", rule: "obj.spec.replicas", want: 7},
		{name: "cel arithmetic", rule: "obj.spec.replicas / 2", want: 3},
		{name: "cel returning string errors", rule: "obj.metadata.name", wantErr: true},
		{name: "invalid cel compile error", rule: "this is ((( not valid", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := evaluateCEL(&pInfo, tc.rule)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for rule %q, got %d", tc.rule, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for rule %q: %v", tc.rule, err)
			}
			if got != tc.want {
				t.Errorf("evaluateCEL(%q) = %d, want %d", tc.rule, got, tc.want)
			}
		})
	}
}

func TestEvaluateCEL_ProgramCache(t *testing.T) {
	pInfo := NewPodInfo(placementTestPetSet(4), placementTestTemplate(map[string]string{"app": "test"}), nil, 0, &v1.PodList{})
	if err := preCalc(&pInfo); err != nil {
		t.Fatalf("preCalc failed: %v", err)
	}

	const rule = "obj.spec.replicas"
	for range 3 {
		if _, err := evaluateCEL(&pInfo, rule); err != nil {
			t.Fatalf("evaluateCEL failed: %v", err)
		}
	}
	// Plain integers and the empty string short-circuit before compilation,
	// so only the single CEL expression should be cached.
	if _, err := evaluateCEL(&pInfo, "5"); err != nil {
		t.Fatalf("evaluateCEL failed: %v", err)
	}
	if got := len(pInfo.programCache); got != 1 {
		t.Errorf("expected 1 cached CEL program, got %d", got)
	}
}
