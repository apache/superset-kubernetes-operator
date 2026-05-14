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
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
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

func TestSetCondition_MessageChangeUpdatesMessageNotTransitionTime(t *testing.T) {
	ts := metav1.Now()
	conditions := []metav1.Condition{
		{Type: supersetv1alpha1.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "NotReady", Message: "old diagnostic", LastTransitionTime: ts, ObservedGeneration: 1},
	}

	setCondition(&conditions, supersetv1alpha1.ConditionTypeReady, metav1.ConditionFalse, "NotReady", "new diagnostic", 1)

	if conditions[0].Message != "new diagnostic" {
		t.Errorf("expected message updated to %q, got %q", "new diagnostic", conditions[0].Message)
	}
	if !conditions[0].LastTransitionTime.Equal(&ts) {
		t.Errorf("expected LastTransitionTime preserved when only message changes")
	}
}

func TestGetComponentStatusFromDeployment(t *testing.T) {
	tests := []struct {
		name          string
		replicas      int32
		readyReplicas int32
		wantReady     string
	}{
		{"all ready", 3, 3, "3/3"},
		{"partially ready", 3, 1, "1/3"},
		{"not ready", 2, 0, "0/2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := testScheme(t)
			replicas := tt.replicas
			deploy := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-web-server",
					Namespace: "default",
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: &replicas,
					Template: corePodTemplateWithChecksum("sha256:test"),
				},
				Status: appsv1.DeploymentStatus{Replicas: tt.replicas, ReadyReplicas: tt.readyReplicas},
			}
			superset := &supersetv1alpha1.Superset{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: supersetv1alpha1.SupersetSpec{
					Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
					WebServer: &supersetv1alpha1.WebServerComponentSpec{},
				},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, deploy).Build()
			r := &SupersetReconciler{Client: c, Scheme: scheme}

			status := r.getComponentStatus(context.Background(), superset, webServerDescriptor)

			if status.Ready != tt.wantReady {
				t.Errorf("expected Ready=%s, got %s", tt.wantReady, status.Ready)
			}
			if status.Ref != "Deployment/test-web-server" {
				t.Errorf("expected deployment ref, got %s", status.Ref)
			}
			if status.ConfigChecksum != "sha256:test" {
				t.Errorf("expected config checksum, got %s", status.ConfigChecksum)
			}
		})
	}
}

func corePodTemplateWithChecksum(checksum string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				common.AnnotationConfigChecksum: checksum,
			},
		},
	}
}

func TestGetComponentStatusMissingDeployment(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme}

	status := r.getComponentStatus(context.Background(), superset, webServerDescriptor)
	if status.Ready != "0/1" {
		t.Fatalf("expected missing deployment to report 0/1, got %s", status.Ready)
	}
	if status.Ref != "Deployment/test-web-server" {
		t.Fatalf("expected missing deployment ref, got %s", status.Ref)
	}

	deploy := &appsv1.Deployment{}
	err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, deploy)
	if err == nil {
		t.Fatal("expected deployment to remain absent")
	}
}
