/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SupersetMaintenancePageSpec is the fully-resolved, flat spec for a maintenance page.
type SupersetMaintenancePageSpec struct {
	FlatComponentSpec `json:",inline"`

	// Checksum stamped as pod template annotation for rolling restarts.
	// +optional
	ConfigChecksum string `json:"configChecksum,omitempty"`
}

// SupersetMaintenancePageStatus defines the observed state of SupersetMaintenancePage.
type SupersetMaintenancePageStatus struct {
	ChildComponentStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 46",message="metadata.name must be at most 46 characters (sub-resource suffix '-maintenance-page' requires 17 characters within the 63-character name limit)"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="metadata.name must be a valid DNS label (lowercase alphanumeric and hyphens only, no dots or underscores); the operator derives resource names from CR names"

// SupersetMaintenancePage is the Schema for the supersetmaintenancepages API.
// It manages a lightweight maintenance page Deployment served during lifecycle tasks.
type SupersetMaintenancePage struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SupersetMaintenancePageSpec   `json:"spec,omitempty"`
	Status SupersetMaintenancePageStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SupersetMaintenancePageList contains a list of SupersetMaintenancePage.
type SupersetMaintenancePageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SupersetMaintenancePage `json:"items"`
}

// GetFlatSpec returns the flat component spec.
func (s *SupersetMaintenancePage) GetFlatSpec() *FlatComponentSpec {
	return &s.Spec.FlatComponentSpec
}

// GetConfigChecksum returns the config checksum for rolling restarts.
func (s *SupersetMaintenancePage) GetConfigChecksum() string { return s.Spec.ConfigChecksum }

// GetService returns nil — maintenance page has no dedicated Service.
func (s *SupersetMaintenancePage) GetService() *ComponentServiceSpec { return nil }

// GetAutoscaling returns nil — maintenance page does not scale.
func (s *SupersetMaintenancePage) GetAutoscaling() *AutoscalingSpec { return nil }

// GetPDB returns nil — maintenance page has no PDB.
func (s *SupersetMaintenancePage) GetPDB() *PDBSpec { return nil }

// GetComponentStatus returns the child component status.
func (s *SupersetMaintenancePage) GetComponentStatus() *ChildComponentStatus {
	return &s.Status.ChildComponentStatus
}

func init() {
	SchemeBuilder.Register(&SupersetMaintenancePage{}, &SupersetMaintenancePageList{})
}
