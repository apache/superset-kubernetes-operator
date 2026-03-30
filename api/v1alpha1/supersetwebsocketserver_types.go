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

// SupersetWebsocketServerSpec is the fully-resolved, flat spec for a websocket server.
// The websocket server is a Node.js application — it does NOT use superset_config.py.
type SupersetWebsocketServerSpec struct {
	FlatComponentSpec `json:",inline"`

	// Service configuration.
	// +optional
	Service *ComponentServiceSpec `json:"service,omitempty"`
}

// SupersetWebsocketServerStatus defines the observed state of SupersetWebsocketServer.
type SupersetWebsocketServerStatus struct {
	ChildComponentStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image.tag`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 46",message="metadata.name must be at most 46 characters (sub-resource suffix '-websocket-server' requires 17 characters within the 63-character Service name limit)"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="metadata.name must be a valid DNS label (lowercase alphanumeric and hyphens only, no dots or underscores); the operator derives Service names from CR names"

// SupersetWebsocketServer is the Schema for the supersetwebsocketservers API.
// It manages the Superset websocket server Deployment.
type SupersetWebsocketServer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SupersetWebsocketServerSpec   `json:"spec,omitempty"`
	Status SupersetWebsocketServerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SupersetWebsocketServerList contains a list of SupersetWebsocketServer.
type SupersetWebsocketServerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SupersetWebsocketServer `json:"items"`
}

// GetFlatSpec returns the flat component spec.
func (s *SupersetWebsocketServer) GetFlatSpec() *FlatComponentSpec { return &s.Spec.FlatComponentSpec }

// GetConfig returns empty string (websocket server is Node.js, no superset_config.py).
func (s *SupersetWebsocketServer) GetConfig() string { return "" }

// GetConfigChecksum returns empty string (websocket server has no config).
func (s *SupersetWebsocketServer) GetConfigChecksum() string { return "" }

// GetService returns the service configuration.
func (s *SupersetWebsocketServer) GetService() *ComponentServiceSpec { return s.Spec.Service }

// GetAutoscaling returns the autoscaling configuration.
func (s *SupersetWebsocketServer) GetAutoscaling() *AutoscalingSpec { return s.Spec.Autoscaling }

// GetPDB returns the PodDisruptionBudget configuration.
func (s *SupersetWebsocketServer) GetPDB() *PDBSpec { return s.Spec.PodDisruptionBudget }

// GetComponentStatus returns the child component status.
func (s *SupersetWebsocketServer) GetComponentStatus() *ChildComponentStatus {
	return &s.Status.ChildComponentStatus
}

func init() {
	SchemeBuilder.Register(&SupersetWebsocketServer{}, &SupersetWebsocketServerList{})
}
