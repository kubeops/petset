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

import "testing"

func member(name string) DistributionRule {
	return DistributionRule{ClusterName: name, ReplicaIndices: []int32{0, 1}, Role: DCRoleMember}
}

func arbiter(name string) DistributionRule {
	return DistributionRule{ClusterName: name, Role: DCRoleArbiter}
}

func twoDC(rules ...DistributionRule) *ClusterSpreadConstraint {
	return &ClusterSpreadConstraint{
		DistributionRules: rules,
		FailoverPolicy: &FailoverPolicy{
			Mode:    FailoverModeTwoDC,
			Trigger: FailoverTrigger{Scope: FailoverScopeGlobal},
		},
	}
}

func TestValidate(t *testing.T) {
	// A nil FailoverPolicy is not a DC/DR policy: always valid.
	if err := (&ClusterSpreadConstraint{}).Validate(); err != nil {
		t.Fatalf("non DC/DR policy must be valid, got %v", err)
	}

	// TwoDC: two Members plus one Arbiter is the only valid two-data-DC shape.
	if err := twoDC(member("dc-a"), member("dc-b"), arbiter("dc-c")).Validate(); err != nil {
		t.Fatalf("two Members plus an Arbiter must be valid, got %v", err)
	}

	// TwoDC without an Arbiter is rejected (the third site must be a vote-only Arbiter).
	if err := twoDC(member("dc-a"), member("dc-b")).Validate(); err == nil {
		t.Fatal("TwoDC without an Arbiter must be rejected")
	}

	// The Witness role is removed: it is now an unknown role and must be rejected.
	witness := DistributionRule{ClusterName: "dc-c", ReplicaIndices: []int32{0}, Role: DCRole("Witness")}
	if err := twoDC(member("dc-a"), member("dc-b"), witness).Validate(); err == nil {
		t.Fatal("the Witness role must be rejected as an unknown role")
	}

	// An Arbiter must not carry data ordinals.
	badArbiter := DistributionRule{ClusterName: "dc-c", ReplicaIndices: []int32{0}, Role: DCRoleArbiter}
	if err := twoDC(member("dc-a"), member("dc-b"), badArbiter).Validate(); err == nil {
		t.Fatal("an Arbiter carrying replicaIndices must be rejected")
	}

	// A Member must carry data ordinals.
	emptyMember := DistributionRule{ClusterName: "dc-a", Role: DCRoleMember}
	if err := twoDC(emptyMember, member("dc-b"), arbiter("dc-c")).Validate(); err == nil {
		t.Fatal("a Member without replicaIndices must be rejected")
	}
}
