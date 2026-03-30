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

// SupersetInitSpec defines the fully-resolved spec for initialization.
type SupersetInitSpec struct {
	FlatComponentSpec `json:",inline"`

	// Rendered superset_config.py content.
	// +optional
	Config string `json:"config,omitempty"`

	// Config checksum for detecting config changes.
	// +optional
	ConfigChecksum string `json:"configChecksum,omitempty"`

	// Maximum number of retries before permanent failure.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	MaxRetries *int32 `json:"maxRetries,omitempty"`

	// Maximum timeout per init pod attempt.
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// Pod retention policy for completed init pods.
	// +optional
	PodRetention *PodRetentionSpec `json:"podRetention,omitempty"`
}

// SupersetInitStatus reports the status of initialization.
type SupersetInitStatus struct {
	// +optional
	// +kubebuilder:validation:Enum=Pending;Running;Complete;Failed
	State string `json:"state,omitempty"`
	// +optional
	PodName string `json:"podName,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	Duration string `json:"duration,omitempty"`
	// +optional
	Attempts int32 `json:"attempts,omitempty"`
	// +optional
	Image string `json:"image,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// Config checksum that was active when init last completed.
	// Used to detect config changes and trigger re-initialization.
	// +optional
	ConfigChecksum string `json:"configChecksum,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Attempts",type=integer,JSONPath=`.status.attempts`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 58",message="metadata.name must be at most 58 characters (sub-resource suffix '-init' requires 5 characters within the 63-character Service name limit)"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="metadata.name must be a valid DNS label (lowercase alphanumeric and hyphens only, no dots or underscores); the operator derives Service names from CR names"

// SupersetInit is the Schema for the supersetinits API.
// It manages the initialization lifecycle (database migrations, init commands).
type SupersetInit struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SupersetInitSpec   `json:"spec,omitempty"`
	Status SupersetInitStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SupersetInitList contains a list of SupersetInit.
type SupersetInitList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SupersetInit `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SupersetInit{}, &SupersetInitList{})
}
