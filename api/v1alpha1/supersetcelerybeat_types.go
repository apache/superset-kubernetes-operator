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

// SupersetCeleryBeatSpec is the fully-resolved, flat spec for celery beat.
// Beat is always a singleton (1 replica).
type SupersetCeleryBeatSpec struct {
	FlatComponentSpec `json:",inline"`

	// The fully rendered superset_config.py content.
	// +optional
	Config string `json:"config,omitempty"`

	// Checksum for rolling restarts.
	// +optional
	ConfigChecksum string `json:"configChecksum,omitempty"`
}

// SupersetCeleryBeatStatus defines the observed state of SupersetCeleryBeat.
type SupersetCeleryBeatStatus struct {
	ChildComponentStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image.tag`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 51",message="metadata.name must be at most 51 characters (sub-resource suffix '-celery-beat' requires 12 characters within the 63-character Service name limit)"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="metadata.name must be a valid DNS label (lowercase alphanumeric and hyphens only, no dots or underscores); the operator derives Service names from CR names"

// SupersetCeleryBeat is the Schema for the supersetcelerybeats API.
// It manages the Celery beat scheduler Deployment (singleton).
type SupersetCeleryBeat struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SupersetCeleryBeatSpec   `json:"spec,omitempty"`
	Status SupersetCeleryBeatStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SupersetCeleryBeatList contains a list of SupersetCeleryBeat.
type SupersetCeleryBeatList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SupersetCeleryBeat `json:"items"`
}

// GetFlatSpec returns the flat component spec.
func (s *SupersetCeleryBeat) GetFlatSpec() *FlatComponentSpec { return &s.Spec.FlatComponentSpec }

// GetConfig returns the rendered superset_config.py content.
func (s *SupersetCeleryBeat) GetConfig() string { return s.Spec.Config }

// GetConfigChecksum returns the config checksum for rolling restarts.
func (s *SupersetCeleryBeat) GetConfigChecksum() string { return s.Spec.ConfigChecksum }

// GetService returns nil (celery beat has no service).
func (s *SupersetCeleryBeat) GetService() *ComponentServiceSpec { return nil }

// GetAutoscaling returns nil (celery beat is a singleton, no autoscaling).
func (s *SupersetCeleryBeat) GetAutoscaling() *AutoscalingSpec { return nil }

// GetPDB returns nil (celery beat is a singleton, no PDB).
func (s *SupersetCeleryBeat) GetPDB() *PDBSpec { return nil }

// GetComponentStatus returns the child component status.
func (s *SupersetCeleryBeat) GetComponentStatus() *ChildComponentStatus {
	return &s.Status.ChildComponentStatus
}

func init() {
	SchemeBuilder.Register(&SupersetCeleryBeat{}, &SupersetCeleryBeatList{})
}
