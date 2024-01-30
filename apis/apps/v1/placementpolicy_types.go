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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +genclient:skipVerbs=updateStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope=Cluster

// PlacementPolicy represents a set of pods with consistent identities.
// Identities are defined as:
//   - Network: A single stable DNS and hostname.
//   - Storage: As many VolumeClaims as requested.
//
// The PlacementPolicy guarantees that a given network identity will always
// map to the same storage identity.
type PlacementPolicy struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired identities of pods in this set.
	// +optional
	Spec PlacementPolicySpec `json:"spec,omitempty"`
}

// A PlacementPolicySpec is the specification of a PlacementPolicy.
type PlacementPolicySpec struct {
	// +optional
	ZoneSpreadConstraint *ZoneSpreadConstraint `json:"zoneSpreadConstraint,omitempty"`

	// +optional
	NodeSpreadConstraint *NodeSpreadConstraint `json:"nodeSpreadConstraint,omitempty"`

	// +optional
	NodeAffinity []NodeAffinityRules `json:"nodeAffinity,omitempty"`
}

type ZoneSpreadConstraint struct {
	MaxSkew           int32                            `json:"maxSkew"`
	WhenUnsatisfiable v1.UnsatisfiableConstraintAction `json:"whenUnsatisfiable"`
}

type NodeSpreadConstraint struct {
	MaxSkew           int32                            `json:"maxSkew"`
	WhenUnsatisfiable v1.UnsatisfiableConstraintAction `json:"whenUnsatisfiable"`
}

type NodeAffinityRules struct {
	TopologyKey       string                           `json:"topologyKey"`
	Domains           []TopologyDomain                 `json:"domains"`
	WhenUnsatisfiable v1.UnsatisfiableConstraintAction `json:"whenUnsatisfiable"`
}

type TopologyDomain struct {
	Values   []string `json:"values"`
	Replicas string   `json:"replicas"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PlacementPolicyList is a collection of PlacementPolicys.
type PlacementPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the list of stateful sets.
	Items []PlacementPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PlacementPolicy{}, &PlacementPolicyList{})
}
