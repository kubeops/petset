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
	"strings"
	"testing"

	core "k8s.io/api/core/v1"
)

func dcdr(rules ...DistributionRule) *ClusterSpreadConstraint {
	return &ClusterSpreadConstraint{
		DistributionRules: rules,
		FailoverPolicy:    &FailoverPolicy{Mode: FailoverModeTwoDC, FailoverGroupRef: &core.LocalObjectReference{Name: "g"}},
	}
}

var arbiter = DistributionRule{ClusterName: "hub", Role: DCRoleArbiter}

func TestValidateTopology(t *testing.T) {
	topo := func(dc string, master, data []int32) DistributionRule {
		return DistributionRule{ClusterName: dc, Role: DCRoleMember, Topology: map[string][]int32{"master": master, "data": data}}
	}
	cases := []struct {
		name string
		c    *ClusterSpreadConstraint
		err  string
	}{
		{"combined", dcdr(
			DistributionRule{ClusterName: "a", ReplicaIndices: []int32{0, 1, 2}},
			DistributionRule{ClusterName: "b", ReplicaIndices: []int32{3, 4, 5}}, arbiter), ""},
		{"topology", dcdr(topo("a", []int32{0, 1, 2}, []int32{0, 1}), topo("b", []int32{3, 4, 5}, []int32{2, 3}), arbiter), ""},
		{"uneven topology", dcdr(topo("a", []int32{0, 1, 2}, []int32{0, 1}), topo("b", []int32{3}, []int32{2}), arbiter), ""},
		{"both on one rule", dcdr(
			DistributionRule{ClusterName: "a", ReplicaIndices: []int32{0}, Topology: map[string][]int32{"master": {0}}},
			topo("b", []int32{1}, []int32{1}), arbiter), "sets both replicaIndices and topology"},
		{"mixed modes", dcdr(
			DistributionRule{ClusterName: "a", ReplicaIndices: []int32{0}},
			topo("b", []int32{1}, []int32{1}), arbiter), "leave replicaIndices empty in topology mode"},
		{"member with neither", dcdr(
			DistributionRule{ClusterName: "a", Topology: map[string][]int32{"master": {}}},
			topo("b", []int32{1}, []int32{1}), arbiter), "has role Member but no replicaIndices or topology"},
		{"arbiter with topology", dcdr(topo("a", []int32{0}, []int32{0}), topo("b", []int32{1}, []int32{1}),
			DistributionRule{ClusterName: "hub", Role: DCRoleArbiter, Topology: map[string][]int32{"master": {2}}}), "has role Arbiter but carries"},
		{"overlap", dcdr(topo("a", []int32{0, 1}, []int32{0}), topo("b", []int32{1, 2}, []int32{1}), arbiter), `index 1 of role "master" is placed on both`},
		{"negative", dcdr(topo("a", []int32{-1}, []int32{0}), topo("b", []int32{1}, []int32{1}), arbiter), "negative index"},
		{"empty role name", dcdr(
			DistributionRule{ClusterName: "a", Topology: map[string][]int32{"": {0}}},
			topo("b", []int32{1}, []int32{1}), arbiter), "empty role name"},
	}
	for _, tc := range cases {
		err := tc.c.Validate()
		switch {
		case tc.err == "" && err != nil:
			t.Errorf("%s: unexpected error %v", tc.name, err)
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%s: want error containing %q, got %v", tc.name, tc.err, err)
		}
	}
}

func TestTopologyHelpers(t *testing.T) {
	c := dcdr(
		DistributionRule{ClusterName: "a", Topology: map[string][]int32{"master": {0, 1, 2}, "ingest": {0, 1}}},
		DistributionRule{ClusterName: "b", Topology: map[string][]int32{"master": {3, 4, 5}, "ingest": {2}}}, arbiter)
	if !c.IsTopology() {
		t.Fatal("IsTopology: want true")
	}
	if n := c.RoleTotal("master"); n != 6 {
		t.Errorf("RoleTotal(master) = %d, want 6", n)
	}
	if n := c.RoleTotal("ingest"); n != 3 {
		t.Errorf("RoleTotal(ingest) = %d, want 3", n)
	}
	if n, ok := c.DistributionRules[1].RoleReplicas("ingest"); n != 1 || !ok {
		t.Errorf("RoleReplicas(ingest) on b = %d,%v, want 1,true", n, ok)
	}
	if _, ok := c.DistributionRules[1].RoleReplicas("data"); ok {
		t.Error("RoleReplicas(data) on b: want not named")
	}
	if !c.DistributionRules[0].IsDataBearing() || arbiter.IsDataBearing() {
		t.Error("IsDataBearing: want member true, arbiter false")
	}
	combined := dcdr(DistributionRule{ClusterName: "a", ReplicaIndices: []int32{0}}, DistributionRule{ClusterName: "b", ReplicaIndices: []int32{1}}, arbiter)
	if combined.IsTopology() {
		t.Error("IsTopology on combined: want false")
	}
}
