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

// SupersetMcpServerSpec is the fully-resolved, flat spec for the MCP server.
type SupersetMcpServerSpec struct {
	FlatComponentSpec `json:",inline"`

	// The fully rendered superset_config.py content.
	// +optional
	Config string `json:"config,omitempty"`

	// Checksum for rolling restarts.
	// +optional
	ConfigChecksum string `json:"configChecksum,omitempty"`

	// Service configuration.
	// +optional
	Service *ComponentServiceSpec `json:"service,omitempty"`
}

// SupersetMcpServerStatus defines the observed state of SupersetMcpServer.
type SupersetMcpServerStatus struct {
	ChildComponentStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image.tag`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 52",message="metadata.name must be at most 52 characters (sub-resource suffix '-mcp-server' requires 11 characters within the 63-character Service name limit)"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="metadata.name must be a valid DNS label (lowercase alphanumeric and hyphens only, no dots or underscores); the operator derives Service names from CR names"

// SupersetMcpServer is the Schema for the supersetmcpservers API.
// It manages the FastMCP server Deployment and Service.
type SupersetMcpServer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SupersetMcpServerSpec   `json:"spec,omitempty"`
	Status SupersetMcpServerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SupersetMcpServerList contains a list of SupersetMcpServer.
type SupersetMcpServerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SupersetMcpServer `json:"items"`
}

// GetFlatSpec returns the flat component spec.
func (s *SupersetMcpServer) GetFlatSpec() *FlatComponentSpec { return &s.Spec.FlatComponentSpec }

// GetConfig returns the rendered superset_config.py content.
func (s *SupersetMcpServer) GetConfig() string { return s.Spec.Config }

// GetConfigChecksum returns the config checksum for rolling restarts.
func (s *SupersetMcpServer) GetConfigChecksum() string { return s.Spec.ConfigChecksum }

// GetService returns the service configuration.
func (s *SupersetMcpServer) GetService() *ComponentServiceSpec { return s.Spec.Service }

// GetAutoscaling returns the autoscaling configuration.
func (s *SupersetMcpServer) GetAutoscaling() *AutoscalingSpec { return s.Spec.Autoscaling }

// GetPDB returns the PodDisruptionBudget configuration.
func (s *SupersetMcpServer) GetPDB() *PDBSpec { return s.Spec.PodDisruptionBudget }

// GetComponentStatus returns the child component status.
func (s *SupersetMcpServer) GetComponentStatus() *ChildComponentStatus {
	return &s.Status.ChildComponentStatus
}

func init() {
	SchemeBuilder.Register(&SupersetMcpServer{}, &SupersetMcpServerList{})
}
