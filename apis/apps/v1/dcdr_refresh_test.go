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
	"encoding/json"
	"strings"
	"testing"
)

// A database placement policy always sets the slice and must serialize it as
// before; an application policy leaves it unset and must not emit it.
func TestClusterSpreadConstraintSliceOptional(t *testing.T) {
	withSlice := ClusterSpreadConstraint{
		DistributionRules: []DistributionRule{{ClusterName: "a", ReplicaIndices: []int32{0}}},
		Slice:             &KubeSliceConfig{ProjectNamespace: "kubeslice-demo", SliceName: "demo-slice"},
	}
	b, err := json.Marshal(withSlice)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"slice":{"projectNamespace":"kubeslice-demo","sliceName":"demo-slice"}`) {
		t.Fatalf("slice not serialized: %s", b)
	}

	without := ClusterSpreadConstraint{
		DistributionRules: []DistributionRule{{ClusterName: "a", ReplicaIndices: []int32{0}}},
	}
	b, err = json.Marshal(without)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "slice") {
		t.Fatalf("an unset slice must be omitted: %s", b)
	}
	var back ClusterSpreadConstraint
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Slice != nil {
		t.Fatalf("unset slice decoded as %+v", back.Slice)
	}

	// DeepCopy must copy, not alias, a set slice and keep an unset one nil.
	cp := withSlice.DeepCopy()
	cp.Slice.SliceName = "other"
	if withSlice.Slice.SliceName != "demo-slice" {
		t.Fatal("DeepCopy aliased the slice")
	}
	if without.DeepCopy().Slice != nil {
		t.Fatal("DeepCopy of an unset slice must stay nil")
	}
}

// HandoffTo is optional on a member and absent from the wire when unset, so
// existing reporters (the database operators) serialize exactly as before.
func TestFailoverGroupMemberHandoffTo(t *testing.T) {
	m := FailoverGroupMember{APIGroup: "kubedb.com", Kind: "Postgres", Namespace: "demo", Name: "pg", DC: "spoke-1", Ready: true}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "handoffTo") {
		t.Fatalf("an unset handoffTo must be omitted: %s", b)
	}
	m.HandoffTo = "spoke-2"
	b, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"handoffTo":"spoke-2"`) {
		t.Fatalf("handoffTo not serialized: %s", b)
	}
}
