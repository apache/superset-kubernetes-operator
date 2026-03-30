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

// SupersetCeleryWorkerSpec is the fully-resolved, flat spec for a celery worker.
type SupersetCeleryWorkerSpec struct {
	FlatComponentSpec `json:",inline"`

	// The fully rendered superset_config.py content.
	// +optional
	Config string `json:"config,omitempty"`

	// Checksum for rolling restarts.
	// +optional
	ConfigChecksum string `json:"configChecksum,omitempty"`
}

// SupersetCeleryWorkerStatus defines the observed state of SupersetCeleryWorker.
type SupersetCeleryWorkerStatus struct {
	ChildComponentStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image.tag`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 49",message="metadata.name must be at most 49 characters (sub-resource suffix '-celery-worker' requires 14 characters within the 63-character Service name limit)"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="metadata.name must be a valid DNS label (lowercase alphanumeric and hyphens only, no dots or underscores); the operator derives Service names from CR names"

// SupersetCeleryWorker is the Schema for the supersetceleryworkers API.
// It manages the Celery worker Deployment.
type SupersetCeleryWorker struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SupersetCeleryWorkerSpec   `json:"spec,omitempty"`
	Status SupersetCeleryWorkerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SupersetCeleryWorkerList contains a list of SupersetCeleryWorker.
type SupersetCeleryWorkerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SupersetCeleryWorker `json:"items"`
}

// GetFlatSpec returns the flat component spec.
func (s *SupersetCeleryWorker) GetFlatSpec() *FlatComponentSpec { return &s.Spec.FlatComponentSpec }

// GetConfig returns the rendered superset_config.py content.
func (s *SupersetCeleryWorker) GetConfig() string { return s.Spec.Config }

// GetConfigChecksum returns the config checksum for rolling restarts.
func (s *SupersetCeleryWorker) GetConfigChecksum() string { return s.Spec.ConfigChecksum }

// GetService returns nil (celery workers have no service).
func (s *SupersetCeleryWorker) GetService() *ComponentServiceSpec { return nil }

// GetAutoscaling returns the autoscaling configuration.
func (s *SupersetCeleryWorker) GetAutoscaling() *AutoscalingSpec { return s.Spec.Autoscaling }

// GetPDB returns the PodDisruptionBudget configuration.
func (s *SupersetCeleryWorker) GetPDB() *PDBSpec { return s.Spec.PodDisruptionBudget }

// GetComponentStatus returns the child component status.
func (s *SupersetCeleryWorker) GetComponentStatus() *ChildComponentStatus {
	return &s.Status.ChildComponentStatus
}

func init() {
	SchemeBuilder.Register(&SupersetCeleryWorker{}, &SupersetCeleryWorkerList{})
}
