/*
Copyright AppsCode Inc. and Contributors

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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
)

// +kubebuilder:validation:Enum=Aws;Azure;DigitalOcean;GoogleCloud;Linode;Packet;Scaleway;Vultr;BareMetal;KIND;Generic;Private
type HostingProvider string

const (
	HostingProviderAWS          HostingProvider = "Aws"
	HostingProviderAzure        HostingProvider = "Azure"
	HostingProviderDigitalOcean HostingProvider = "DigitalOcean"
	HostingProviderGoogleCloud  HostingProvider = "GoogleCloud"
	HostingProviderExoscale     HostingProvider = "Exoscale"
	HostingProviderLinode       HostingProvider = "Linode"
	HostingProviderPacket       HostingProvider = "Packet"
	HostingProviderScaleway     HostingProvider = "Scaleway"
	HostingProviderVultr        HostingProvider = "Vultr"
	HostingProviderBareMetal    HostingProvider = "BareMetal"
	HostingProviderKIND         HostingProvider = "KIND"
	HostingProviderGeneric      HostingProvider = "Generic"
	HostingProviderPrivate      HostingProvider = "Private"
)

const (
	AceInfoConfigMapName = "ace-info"

	ClusterNameKey         string = "cluster.appscode.com/name"
	ClusterDisplayNameKey  string = "cluster.appscode.com/display-name"
	ClusterProviderNameKey string = "cluster.appscode.com/provider"
)

type ClusterMetadata struct {
	UID         string          `json:"uid" protobuf:"bytes,1,opt,name=uid"`
	Name        string          `json:"name,omitempty" protobuf:"bytes,2,opt,name=name"`
	DisplayName string          `json:"displayName,omitempty" protobuf:"bytes,3,opt,name=displayName"`
	Provider    HostingProvider `json:"provider,omitempty" protobuf:"bytes,4,opt,name=provider,casttype=HostingProvider"`
	OwnerID     string          `json:"ownerID,omitempty"`
	OwnerType   string          `json:"ownerType,omitempty"`
	APIEndpoint string          `json:"apiEndpoint,omitempty"`
	CABundle    string          `json:"caBundle,omitempty"`
}

func (md ClusterMetadata) State() string {
	hasher := hmac.New(sha256.New, []byte(md.UID))
	state := fmt.Sprintf("%s,%s", md.APIEndpoint, md.OwnerID)
	hasher.Write([]byte(state))
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

/*
ENUM(

	ACE                         = 1
	OCMHub                      = 2
	OCMMulticlusterControlplane = 4
	OCMSpoke                    = 8
	OpenShift                   = 16
	Rancher                     = 32
	VirtualCluster              = 64

)
*/
type ClusterManager int

const (
	ClusterManagerACE ClusterManager = 1 << iota
	ClusterManagerOCMHub
	ClusterManagerOCMMulticlusterControlplane
	ClusterManagerOCMSpoke
	ClusterManagerOpenShift
	ClusterManagerRancher
	ClusterManagerVirtualCluster
)

func (cm ClusterManager) ManagedByACE() bool {
	return cm&ClusterManagerACE == ClusterManagerACE
}

func (cm ClusterManager) ManagedByOCMHub() bool {
	return cm&ClusterManagerOCMHub == ClusterManagerOCMHub
}

func (cm ClusterManager) ManagedByOCMSpoke() bool {
	return cm&ClusterManagerOCMSpoke == ClusterManagerOCMSpoke
}

func (cm ClusterManager) ManagedByOCMMulticlusterControlplane() bool {
	return cm&ClusterManagerOCMMulticlusterControlplane == ClusterManagerOCMMulticlusterControlplane
}

func (cm ClusterManager) ManagedByRancher() bool {
	return cm&ClusterManagerRancher == ClusterManagerRancher
}

func (cm ClusterManager) ManagedByOpenShift() bool {
	return cm&ClusterManagerOpenShift == ClusterManagerOpenShift
}

func (cm ClusterManager) ManagedByVirtualCluster() bool {
	return cm&ClusterManagerVirtualCluster == ClusterManagerVirtualCluster
}

func (cm ClusterManager) Strings() []string {
	out := make([]string, 0, 7)
	if cm.ManagedByACE() {
		out = append(out, "ACE")
	}
	if cm.ManagedByOCMHub() {
		out = append(out, "OCMHub")
	}
	if cm.ManagedByOCMSpoke() {
		out = append(out, "OCMSpoke")
	}
	if cm.ManagedByOCMMulticlusterControlplane() {
		out = append(out, "OCMMulticlusterControlplane")
	}
	if cm.ManagedByRancher() {
		out = append(out, "Rancher")
	}
	if cm.ManagedByOpenShift() {
		out = append(out, "OpenShift")
	}
	if cm.ManagedByVirtualCluster() {
		out = append(out, "vcluster")
	}
	return out
}

func (cm ClusterManager) String() string {
	return strings.Join(cm.Strings(), ",")
}

type CAPIClusterInfo struct {
	Provider    CAPIProvider `json:"provider"`
	Namespace   string       `json:"namespace"`
	ClusterName string       `json:"clusterName"`
}

// ClusterInfo used in ace-installer
type ClusterInfo struct {
	UID             string   `json:"uid"`
	Name            string   `json:"name"`
	ClusterManagers []string `json:"clusterManagers"`
	// +optional
	CAPI CAPIClusterInfo `json:"capi"`
}

// +kubebuilder:validation:Enum=capa;capg;capz
type CAPIProvider string

const (
	CAPIProviderUnknown CAPIProvider = ""
	CAPIProviderCAPA    CAPIProvider = "capa"
	CAPIProviderCAPG    CAPIProvider = "capg"
	CAPIProviderCAPZ    CAPIProvider = "capz"
	CAPIProviderCAPH    CAPIProvider = "caph"
)