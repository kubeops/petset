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

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"

	v1 "kubeops.dev/petset/apis/apps/v1"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakePetSets implements PetSetInterface
type FakePetSets struct {
	Fake *FakeAppsV1
	ns   string
}

var petsetsResource = v1.SchemeGroupVersion.WithResource("petsets")

var petsetsKind = v1.SchemeGroupVersion.WithKind("PetSet")

// Get takes name of the petSet, and returns the corresponding petSet object, and an error if there is any.
func (c *FakePetSets) Get(ctx context.Context, name string, options metav1.GetOptions) (result *v1.PetSet, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(petsetsResource, c.ns, name), &v1.PetSet{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.PetSet), err
}

// List takes label and field selectors, and returns the list of PetSets that match those selectors.
func (c *FakePetSets) List(ctx context.Context, opts metav1.ListOptions) (result *v1.PetSetList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(petsetsResource, petsetsKind, c.ns, opts), &v1.PetSetList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1.PetSetList{ListMeta: obj.(*v1.PetSetList).ListMeta}
	for _, item := range obj.(*v1.PetSetList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested petSets.
func (c *FakePetSets) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(petsetsResource, c.ns, opts))

}

// Create takes the representation of a petSet and creates it.  Returns the server's representation of the petSet, and an error, if there is any.
func (c *FakePetSets) Create(ctx context.Context, petSet *v1.PetSet, opts metav1.CreateOptions) (result *v1.PetSet, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(petsetsResource, c.ns, petSet), &v1.PetSet{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.PetSet), err
}

// Update takes the representation of a petSet and updates it. Returns the server's representation of the petSet, and an error, if there is any.
func (c *FakePetSets) Update(ctx context.Context, petSet *v1.PetSet, opts metav1.UpdateOptions) (result *v1.PetSet, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(petsetsResource, c.ns, petSet), &v1.PetSet{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.PetSet), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakePetSets) UpdateStatus(ctx context.Context, petSet *v1.PetSet, opts metav1.UpdateOptions) (*v1.PetSet, error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceAction(petsetsResource, "status", c.ns, petSet), &v1.PetSet{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.PetSet), err
}

// Delete takes name of the petSet and deletes it. Returns an error if one occurs.
func (c *FakePetSets) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(petsetsResource, c.ns, name, opts), &v1.PetSet{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakePetSets) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(petsetsResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &v1.PetSetList{})
	return err
}

// Patch applies the patch and returns the patched petSet.
func (c *FakePetSets) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.PetSet, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(petsetsResource, c.ns, name, pt, data, subresources...), &v1.PetSet{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.PetSet), err
}

// GetScale takes name of the petSet, and returns the corresponding scale object, and an error if there is any.
func (c *FakePetSets) GetScale(ctx context.Context, petSetName string, options metav1.GetOptions) (result *autoscalingv1.Scale, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetSubresourceAction(petsetsResource, c.ns, "scale", petSetName), &autoscalingv1.Scale{})

	if obj == nil {
		return nil, err
	}
	return obj.(*autoscalingv1.Scale), err
}

// UpdateScale takes the representation of a scale and updates it. Returns the server's representation of the scale, and an error, if there is any.
func (c *FakePetSets) UpdateScale(ctx context.Context, petSetName string, scale *autoscalingv1.Scale, opts metav1.UpdateOptions) (result *autoscalingv1.Scale, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceAction(petsetsResource, "scale", c.ns, scale), &autoscalingv1.Scale{})

	if obj == nil {
		return nil, err
	}
	return obj.(*autoscalingv1.Scale), err
}
