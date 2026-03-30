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

package controller

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

func TestSetCondition(t *testing.T) {
	var conditions []metav1.Condition

	// Add a new condition.
	setCondition(&conditions, supersetv1alpha1.ConditionTypeReady, metav1.ConditionTrue, "AllReady", "All good", 1)

	if len(conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conditions))
	}
	if conditions[0].Type != supersetv1alpha1.ConditionTypeReady {
		t.Errorf("expected Ready type")
	}
	if conditions[0].Status != metav1.ConditionTrue {
		t.Errorf("expected True status")
	}
	if conditions[0].Reason != "AllReady" {
		t.Errorf("expected AllReady reason, got %s", conditions[0].Reason)
	}
	if conditions[0].ObservedGeneration != 1 {
		t.Errorf("expected ObservedGeneration 1, got %d", conditions[0].ObservedGeneration)
	}

	// Update existing condition.
	setCondition(&conditions, supersetv1alpha1.ConditionTypeReady, metav1.ConditionFalse, "NotReady", "Degraded", 2)

	if len(conditions) != 1 {
		t.Fatalf("expected still 1 condition after update, got %d", len(conditions))
	}
	if conditions[0].Status != metav1.ConditionFalse {
		t.Errorf("expected updated status False")
	}

	// Add a second condition type.
	setCondition(&conditions, supersetv1alpha1.ConditionTypeProgressing, metav1.ConditionFalse, "Done", "", 2)

	if len(conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(conditions))
	}
}

func TestSetCondition_NoOpWhenUnchanged(t *testing.T) {
	ts := metav1.Now()
	conditions := []metav1.Condition{
		{Type: supersetv1alpha1.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: "AllReady", LastTransitionTime: ts},
	}

	setCondition(&conditions, supersetv1alpha1.ConditionTypeReady, metav1.ConditionTrue, "AllReady", "All good", 0)

	if !conditions[0].LastTransitionTime.Equal(&ts) {
		t.Errorf("expected LastTransitionTime to be unchanged")
	}
}

func TestSetCondition_ReasonChangePreservesTransitionTime(t *testing.T) {
	ts := metav1.Now()
	conditions := []metav1.Condition{
		{Type: supersetv1alpha1.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "NotReady", LastTransitionTime: ts, ObservedGeneration: 1},
	}

	setCondition(&conditions, supersetv1alpha1.ConditionTypeReady, metav1.ConditionFalse, "PartiallyReady", "Some ready", 1)

	if conditions[0].Reason != "PartiallyReady" {
		t.Errorf("expected reason to be updated, got %s", conditions[0].Reason)
	}
	if !conditions[0].LastTransitionTime.Equal(&ts) {
		t.Errorf("expected LastTransitionTime preserved when only reason changes")
	}
}

func TestUpdateComponentStatusFromDeployment(t *testing.T) {
	tests := []struct {
		name          string
		replicas      int32
		readyReplicas int32
		wantReady     string
		wantCondition metav1.ConditionStatus
	}{
		{"all ready", 3, 3, "3/3", metav1.ConditionTrue},
		{"partially ready", 3, 1, "1/3", metav1.ConditionFalse},
		{"not ready", 2, 0, "0/2", metav1.ConditionFalse},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicas := tt.replicas
			deploy := &appsv1.Deployment{
				Spec:   appsv1.DeploymentSpec{Replicas: &replicas},
				Status: appsv1.DeploymentStatus{Replicas: tt.replicas, ReadyReplicas: tt.readyReplicas},
			}

			status := &supersetv1alpha1.ChildComponentStatus{}
			updateComponentStatusFromDeployment(status, deploy, 0)

			if status.Ready != tt.wantReady {
				t.Errorf("expected Ready=%s, got %s", tt.wantReady, status.Ready)
			}
			var gotStatus metav1.ConditionStatus
			for _, c := range status.Conditions {
				if c.Type == supersetv1alpha1.ConditionTypeReady {
					gotStatus = c.Status
				}
			}
			if gotStatus != tt.wantCondition {
				t.Errorf("expected Ready condition %s, got %s", tt.wantCondition, gotStatus)
			}
		})
	}
}
