/*
Copyright 2014 The Kubernetes Authors.

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
	"fmt"
	"strconv"
	"strings"

	api "kubeops.dev/petset/apis/apps/v1"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

type PodInfo struct {
	PetSet          *api.PetSet
	Template        *api.PodTemplateSpec
	PlacementPolicy *api.PlacementPolicy
	PodIndex        int
	PodList         *v1.PodList
	Obj             map[string]interface{}
	Env             *cel.Env
}

func NewPodInfo(set *api.PetSet, template *api.PodTemplateSpec, place *api.PlacementPolicy, podIndex int, podList *v1.PodList) PodInfo {
	return PodInfo{
		PetSet:          set,
		Template:        template,
		PlacementPolicy: place,
		PodIndex:        podIndex,
		PodList:         podList,
	}
}

func CalculateForPodPlacement(pInfo *PodInfo) (v1.PodSpec, error) {
	podSpec := *pInfo.Template.Spec.DeepCopy()
	if pInfo.PlacementPolicy == nil {
		return podSpec, nil
	}
	err := preCalc(pInfo)
	if err != nil {
		return podSpec, err
	}
	podSpec = setSpreadConstraintsFromPlacement(podSpec, *pInfo)
	return setNodeAffinityFromPlacement(podSpec, *pInfo)
}

func setSpreadConstraintsFromPlacement(podSpec v1.PodSpec, pInfo PodInfo) v1.PodSpec {
	pl := pInfo.PlacementPolicy.Spec
	podLabels := pInfo.Template.Labels
	tsc := v1.TopologySpreadConstraint{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: podLabels,
		},
	}

	if pl.ZoneSpreadConstraint != nil {
		tsc.TopologyKey = v1.LabelTopologyZone
		tsc.MaxSkew = pl.ZoneSpreadConstraint.MaxSkew
		tsc.WhenUnsatisfiable = pl.ZoneSpreadConstraint.WhenUnsatisfiable
		podSpec.TopologySpreadConstraints = UpsertTopologySpreadConstraint(podSpec.TopologySpreadConstraints, tsc)
	}
	if pl.NodeSpreadConstraint != nil {
		tsc.TopologyKey = v1.LabelHostname
		tsc.MaxSkew = pl.NodeSpreadConstraint.MaxSkew
		tsc.WhenUnsatisfiable = pl.NodeSpreadConstraint.WhenUnsatisfiable
		podSpec.TopologySpreadConstraints = UpsertTopologySpreadConstraint(podSpec.TopologySpreadConstraints, tsc)
	}
	if podSpec.Affinity == nil {
		podSpec.Affinity = &v1.Affinity{}
	}
	setAntiAffinityRules(podSpec.Affinity, pl, podLabels)
	return podSpec
}

func UpsertTopologySpreadConstraint(lst []v1.TopologySpreadConstraint, tsc v1.TopologySpreadConstraint) []v1.TopologySpreadConstraint {
	for i, constraint := range lst {
		if constraint.TopologyKey == tsc.TopologyKey && constraint.WhenUnsatisfiable == tsc.WhenUnsatisfiable {
			lst[i] = tsc
			return lst
		}
	}
	return append(lst, tsc)
}

func setAntiAffinityRules(aff *v1.Affinity, pl api.PlacementPolicySpec, podLabels map[string]string) {
	if pl.ZoneSpreadConstraint == nil && pl.NodeSpreadConstraint == nil {
		return
	}
	if aff.PodAntiAffinity == nil {
		aff.PodAntiAffinity = &v1.PodAntiAffinity{}
	}
	if aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = make([]v1.PodAffinityTerm, 0)
	}

	term := v1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: podLabels,
		},
	}
	if pl.ZoneSpreadConstraint != nil && pl.ZoneSpreadConstraint.WhenUnsatisfiable == v1.DoNotSchedule {
		term.TopologyKey = v1.LabelTopologyZone
		aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = UpsertPodAffinityTerm(aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, term)
	}

	if pl.NodeSpreadConstraint != nil && pl.NodeSpreadConstraint.WhenUnsatisfiable == v1.DoNotSchedule {
		term.TopologyKey = v1.LabelHostname
		aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = UpsertPodAffinityTerm(aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, term)
	}
}

func UpsertPodAffinityTerm(lst []v1.PodAffinityTerm, term v1.PodAffinityTerm) []v1.PodAffinityTerm {
	for i, affinityTerm := range lst {
		if affinityTerm.TopologyKey == term.TopologyKey {
			lst[i] = term
			return lst
		}
	}
	return append(lst, term)
}

func setNodeAffinityFromPlacement(podSpec v1.PodSpec, pInfo PodInfo) (v1.PodSpec, error) {
	pl := pInfo.PlacementPolicy.Spec
	if pl.Affinity == nil || pl.Affinity.NodeAffinity == nil {
		return podSpec, nil
	}

	// --------- Just initializing stuffs to avoid nil pointer errors ------------
	if podSpec.Affinity == nil {
		podSpec.Affinity = &v1.Affinity{}
	}
	if podSpec.Affinity.NodeAffinity == nil {
		podSpec.Affinity.NodeAffinity = &v1.NodeAffinity{}
	}

	requiredAff := podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if requiredAff == nil {
		requiredAff = &v1.NodeSelector{}
	}
	if requiredAff.NodeSelectorTerms == nil {
		requiredAff.NodeSelectorTerms = make([]v1.NodeSelectorTerm, 0)
	}
	preferredAff := podSpec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if preferredAff == nil {
		preferredAff = make([]v1.PreferredSchedulingTerm, 0)
	}
	// ---------- initializing ends ------------

	// we are going to put all the rules, having un-satisfiability value `DoNotSchedule`, into a single term
	singleRequiredTerm := v1.NodeSelectorTerm{
		MatchExpressions: make([]v1.NodeSelectorRequirement, 0),
	}
	for _, rule := range pl.Affinity.NodeAffinity {
		domainIndex, err := getAppropriateDomainIndex(rule, pInfo)
		if err != nil {
			return podSpec, err
		}
		req := v1.NodeSelectorRequirement{
			Key:      rule.TopologyKey,
			Operator: v1.NodeSelectorOpIn,
			Values:   rule.Domains[domainIndex].Values,
		}
		if rule.WhenUnsatisfiable == v1.DoNotSchedule {
			singleRequiredTerm.MatchExpressions = UpsertNodeSelectorRequirements(singleRequiredTerm.MatchExpressions, req)
		}
		if rule.WhenUnsatisfiable == v1.ScheduleAnyway {
			preferredAff = append(preferredAff, v1.PreferredSchedulingTerm{
				Weight: rule.Weight,
				Preference: v1.NodeSelectorTerm{
					MatchExpressions: []v1.NodeSelectorRequirement{req},
				},
			})
		}
	}
	if len(singleRequiredTerm.MatchExpressions) > 0 { // there were rules with un-satisfiability value `DoNotSchedule`
		requiredAff.NodeSelectorTerms = append(requiredAff.NodeSelectorTerms, singleRequiredTerm)
		podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = requiredAff
	}
	podSpec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = preferredAff
	return podSpec, nil
}

type calculatedDomain struct {
	values          string
	replicas        int64
	alreadyAssigned int64
}

func getAppropriateDomainIndex(rule api.NodeAffinityRule, pInfo PodInfo) (int, error) {
	calculatedDomains := make([]calculatedDomain, 0)
	for _, domain := range rule.Domains {
		eval, err := evaluateCEL(pInfo.Obj, pInfo.Env, domain.Replicas)
		if err != nil {
			return 0, err
		}
		calculatedDomains = append(calculatedDomains, calculatedDomain{
			values:   strings.Join(domain.Values, ","),
			replicas: eval,
		})
	}

	updateAssignedCount := func(val string) {
		for i := range calculatedDomains {
			if calculatedDomains[i].values == val {
				calculatedDomains[i].alreadyAssigned++
				return
			}
		}
	}
	for _, pod := range pInfo.PodList.Items {
		aff := pod.Spec.Affinity.NodeAffinity
		if rule.WhenUnsatisfiable == v1.DoNotSchedule {
			for _, term := range aff.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				for _, req := range term.MatchExpressions {
					if req.Key == rule.TopologyKey {
						updateAssignedCount(strings.Join(req.Values, ","))
						break
					}
				}
			}
		} else {
			for _, term := range aff.PreferredDuringSchedulingIgnoredDuringExecution {
				for _, req := range term.Preference.MatchExpressions {
					if req.Key == rule.TopologyKey {
						updateAssignedCount(strings.Join(req.Values, ","))
						break
					}
				}
			}
		}
	}
	for i, domain := range calculatedDomains {
		if domain.replicas == -1 {
			return i, nil
		}
		if domain.alreadyAssigned < domain.replicas {
			return i, nil
		}
	}
	return 0, fmt.Errorf("invalid domains %v, mismatched with podIndex %v", rule.Domains, pInfo.PodIndex)
}

func UpsertNodeSelectorRequirements(reqList []v1.NodeSelectorRequirement, req v1.NodeSelectorRequirement) []v1.NodeSelectorRequirement {
	for i, requirement := range reqList {
		if requirement.Key == req.Key {
			reqList[i] = req
		}
	}
	return append(reqList, req)
}

const (
	defaultCELVar = "obj"
)

func preCalc(pInfo *PodInfo) error {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pInfo.PetSet)
	if err != nil {
		klog.Errorf("error while converting to unstructured: %s", err.Error())
		return err
	}
	pInfo.Obj = obj

	env, err := cel.NewEnv(
		cel.Declarations(
			decls.NewVar(defaultCELVar, decls.Dyn)))
	if err != nil {
		klog.Errorf("error while creating new CEL env: %s", err.Error())
		return err
	}
	pInfo.Env = env
	return nil
}

func evaluateCEL(obj map[string]interface{}, env *cel.Env, rule string) (int64, error) {
	if rule == "" {
		return -1, nil
	}

	parseInt, err := strconv.ParseInt(rule, 10, 64)
	if err == nil {
		return parseInt, nil
	}
	ast, iss := env.Compile(rule)
	if iss != nil && iss.Err() != nil {
		klog.Errorf("CEL compile error %s", iss.Err())
		return 0, iss.Err()
	}

	checked, iss := env.Check(ast)
	if iss != nil && iss.Err() != nil {
		klog.Errorf("CEL check error %s", iss.Err())
		return 0, iss.Err()
	}
	program, err := env.Program(checked)
	if err != nil {
		klog.Errorf("CEL program error %s", err.Error())
		return 0, err
	}

	res := make(map[string]interface{})
	res[defaultCELVar] = obj

	val, _, err := program.Eval(res)
	if err != nil {
		klog.Errorf("CEL eval error %s", err.Error())
		return 0, err
	}
	return val.Value().(int64), nil
}
