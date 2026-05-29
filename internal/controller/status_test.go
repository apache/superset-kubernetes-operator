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

	"github.com/stretchr/testify/assert"
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
	}{
		{"all ready", 3, 3},
		{"partially ready", 3, 1},
		{"not ready", 2, 0},
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

			if status.Replicas != tt.replicas || status.ReadyReplicas != tt.readyReplicas {
				t.Errorf("expected replicas=%d ready=%d, got replicas=%d ready=%d",
					tt.replicas, tt.readyReplicas, status.Replicas, status.ReadyReplicas)
			}
			if !hasComponentResource(status.Resources, "Deployment", "test-web-server", "Present") {
				t.Errorf("expected Deployment/test-web-server resource Present, got %#v", status.Resources)
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
	if status.Replicas != 1 || status.ReadyReplicas != 0 {
		t.Fatalf("expected missing deployment to report replicas=1 ready=0, got replicas=%d ready=%d", status.Replicas, status.ReadyReplicas)
	}
	if !hasComponentResource(status.Resources, "Deployment", "test-web-server", "Missing") {
		t.Fatalf("expected Deployment/test-web-server resource Missing, got %#v", status.Resources)
	}

	deploy := &appsv1.Deployment{}
	err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, deploy)
	if err == nil {
		t.Fatal("expected deployment to remain absent")
	}
}

func TestDrainedComponentStatusUsesValidResourceStatus(t *testing.T) {
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
		},
	}

	status := drainedComponentStatus(superset, webServerDescriptor)
	if status.Phase != componentPhaseDrained {
		t.Fatalf("expected drained phase, got %q", status.Phase)
	}
	if status.Replicas != 1 {
		t.Fatalf("expected drained desired replicas 1, got %d", status.Replicas)
	}
	if len(status.Resources) != 1 {
		t.Fatalf("expected deployment resource status, got %d resources", len(status.Resources))
	}
	if status.Resources[0].Status != "Missing" {
		t.Fatalf("expected valid resource status Missing, got %q", status.Resources[0].Status)
	}
}

func TestUpdateLifecycleComponentStatusCountsOnlyWebServerUnavailableDuringMaintenance(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:        supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer:    &supersetv1alpha1.WebServerComponentSpec{},
			CeleryWorker: &supersetv1alpha1.CeleryWorkerComponentSpec{},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{MaintenanceActive: true},
		},
	}
	webDeploy := readyDeployment("test-web-server")
	workerDeploy := readyDeployment("test-celery-worker")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, webDeploy, workerDeploy).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme}

	r.updateLifecycleComponentStatus(ctx, superset, "cfg")

	if superset.Status.Ready != "1/2" {
		t.Fatalf("expected aggregate Ready=1/2 during maintenance, got %q", superset.Status.Ready)
	}
	if superset.Status.Components.WebServer.ReadyReplicas != 0 {
		t.Fatalf("expected web-server ready replicas to be suppressed during maintenance, got %d", superset.Status.Components.WebServer.ReadyReplicas)
	}
	if superset.Status.Components.CeleryWorker.ReadyReplicas != 1 || superset.Status.Components.CeleryWorker.Replicas != 1 {
		t.Fatalf("expected celery worker 1/1 ready replicas during maintenance, got %d/%d",
			superset.Status.Components.CeleryWorker.ReadyReplicas, superset.Status.Components.CeleryWorker.Replicas)
	}
	if !hasConditionReason(superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable, "MaintenanceActive") {
		t.Fatalf("expected Available condition reason MaintenanceActive, got %#v", superset.Status.Conditions)
	}
}

func TestUpdateStatusKeepsRestoringLifecycleUntilComponentsReady(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:        supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer:    &supersetv1alpha1.WebServerComponentSpec{},
			CeleryWorker: &supersetv1alpha1.CeleryWorkerComponentSpec{},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Phase:     phaseUpgrading,
			Lifecycle: &supersetv1alpha1.LifecycleStatus{Phase: lifecyclePhaseRestoring},
		},
	}
	webDeploy := readyDeployment("test-web-server")
	workerDeploy := progressingDeployment("test-celery-worker")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, webDeploy, workerDeploy).
		WithStatusSubresource(&supersetv1alpha1.Superset{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme}

	if err := r.updateStatus(ctx, superset, superset.DeepCopy()); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}

	if superset.Status.Phase != phaseUpgrading {
		t.Fatalf("expected parent phase to remain Upgrading, got %q", superset.Status.Phase)
	}
	if superset.Status.Lifecycle.Phase != lifecyclePhaseRestoring {
		t.Fatalf("expected lifecycle phase Restoring, got %q", superset.Status.Lifecycle.Phase)
	}
	if superset.Status.Ready != "1/2" {
		t.Fatalf("expected aggregate Ready=1/2 while restoring, got %q", superset.Status.Ready)
	}
	if !hasConditionReason(superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable, "ComponentsRestoring") {
		t.Fatalf("expected Available condition reason ComponentsRestoring, got %#v", superset.Status.Conditions)
	}
}

func TestUpdateStatusCompletesRestoringLifecycleWhenComponentsReady(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:        supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer:    &supersetv1alpha1.WebServerComponentSpec{},
			CeleryWorker: &supersetv1alpha1.CeleryWorkerComponentSpec{},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Phase:     phaseUpgrading,
			Lifecycle: &supersetv1alpha1.LifecycleStatus{Phase: lifecyclePhaseRestoring},
		},
	}
	webDeploy := readyDeployment("test-web-server")
	workerDeploy := readyDeployment("test-celery-worker")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, webDeploy, workerDeploy).
		WithStatusSubresource(&supersetv1alpha1.Superset{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme}

	if err := r.updateStatus(ctx, superset, superset.DeepCopy()); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}

	if superset.Status.Phase != phaseRunning {
		t.Fatalf("expected parent phase Running, got %q", superset.Status.Phase)
	}
	if superset.Status.Lifecycle.Phase != lifecyclePhaseComplete {
		t.Fatalf("expected lifecycle phase Complete, got %q", superset.Status.Lifecycle.Phase)
	}
	if superset.Status.Ready != "2/2" {
		t.Fatalf("expected aggregate Ready=2/2, got %q", superset.Status.Ready)
	}
	if !hasConditionReason(superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable, "AllComponentsReady") {
		t.Fatalf("expected Available condition reason AllComponentsReady, got %#v", superset.Status.Conditions)
	}
}

func readyDeployment(name string) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "default",
			Generation: 1,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corePodTemplateWithChecksum("sha256:test"),
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Replicas:           replicas,
			ReadyReplicas:      replicas,
			UpdatedReplicas:    replicas,
			AvailableReplicas:  replicas,
		},
	}
}

func progressingDeployment(name string) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "default",
			Generation: 1,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corePodTemplateWithChecksum("sha256:test"),
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Replicas:           replicas,
			UpdatedReplicas:    replicas,
		},
	}
}

func hasConditionReason(conditions []metav1.Condition, conditionType, reason string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Reason == reason {
			return true
		}
	}
	return false
}

func hasComponentResource(resources []supersetv1alpha1.ComponentResourceStatus, kind, name, status string) bool {
	for _, r := range resources {
		if r.Kind == kind && r.Name == name && r.Status == status {
			return true
		}
	}
	return false
}

func TestEffectiveAutoscalingForStatus(t *testing.T) {
	componentAS := &supersetv1alpha1.AutoscalingSpec{MinReplicas: int32Ptr(3)}
	topAS := &supersetv1alpha1.AutoscalingSpec{MinReplicas: int32Ptr(2)}

	t.Run("per-component value wins", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{Autoscaling: topAS}}
		got := effectiveAutoscalingForStatus(superset, &componentAccessor{autoscaling: componentAS})
		assert.Same(t, componentAS, got)
	})

	t.Run("falls back to top-level", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{Autoscaling: topAS}}
		got := effectiveAutoscalingForStatus(superset, &componentAccessor{})
		assert.Same(t, topAS, got)
	})

	t.Run("nil when neither set", func(t *testing.T) {
		assert.Nil(t, effectiveAutoscalingForStatus(&supersetv1alpha1.Superset{}, nil))
	})
}

func TestEffectivePDBForStatus(t *testing.T) {
	componentPDB := &supersetv1alpha1.PDBSpec{}
	topPDB := &supersetv1alpha1.PDBSpec{}

	t.Run("per-component value wins", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{PodDisruptionBudget: topPDB}}
		assert.Same(t, componentPDB, effectivePDBForStatus(superset, &componentAccessor{pdb: componentPDB}))
	})

	t.Run("falls back to top-level", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{PodDisruptionBudget: topPDB}}
		assert.Same(t, topPDB, effectivePDBForStatus(superset, &componentAccessor{}))
	})

	t.Run("nil when neither set", func(t *testing.T) {
		assert.Nil(t, effectivePDBForStatus(&supersetv1alpha1.Superset{}, nil))
	})
}

func TestDesiredReplicasForStatus(t *testing.T) {
	webDesc := &componentDescriptor{componentType: common.ComponentWebServer}
	beatDesc := &componentDescriptor{componentType: common.ComponentCeleryBeat}

	t.Run("celery beat is always a singleton", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{Replicas: int32Ptr(5)}}
		assert.Equal(t, celeryBeatSingletonReplica, desiredReplicasForStatus(superset, beatDesc, &componentAccessor{}))
	})

	t.Run("autoscaling minReplicas takes precedence", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{Replicas: int32Ptr(5)}}
		accessor := &componentAccessor{autoscaling: &supersetv1alpha1.AutoscalingSpec{MinReplicas: int32Ptr(3)}}
		assert.Equal(t, int32(3), desiredReplicasForStatus(superset, webDesc, accessor))
	})

	t.Run("autoscaling without minReplicas defaults to 1", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{Replicas: int32Ptr(5)}}
		accessor := &componentAccessor{autoscaling: &supersetv1alpha1.AutoscalingSpec{}}
		assert.Equal(t, int32(1), desiredReplicasForStatus(superset, webDesc, accessor))
	})

	t.Run("per-component replicas win over top-level", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{Replicas: int32Ptr(5)}}
		assert.Equal(t, int32(2), desiredReplicasForStatus(superset, webDesc, &componentAccessor{replicas: int32Ptr(2)}))
	})

	t.Run("falls back to top-level replicas", func(t *testing.T) {
		superset := &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{Replicas: int32Ptr(4)}}
		assert.Equal(t, int32(4), desiredReplicasForStatus(superset, webDesc, &componentAccessor{}))
	})

	t.Run("defaults to 1 when nothing is set", func(t *testing.T) {
		assert.Equal(t, int32(1), desiredReplicasForStatus(&supersetv1alpha1.Superset{}, webDesc, &componentAccessor{}))
	})
}

func TestComponentPhaseAndMessage(t *testing.T) {
	t.Run("scaled to zero is Ready", func(t *testing.T) {
		phase, msg := componentPhaseAndMessage(&appsv1.Deployment{}, 0)
		assert.Equal(t, componentPhaseReady, phase)
		assert.Equal(t, "Scaled to zero", msg)
	})

	t.Run("all replicas ready is Ready with no message", func(t *testing.T) {
		deploy := &appsv1.Deployment{Status: appsv1.DeploymentStatus{
			ReadyReplicas: 2, UpdatedReplicas: 2, AvailableReplicas: 2,
		}}
		phase, msg := componentPhaseAndMessage(deploy, 2)
		assert.Equal(t, componentPhaseReady, phase)
		assert.Empty(t, msg)
	})

	t.Run("unobserved generation is Progressing", func(t *testing.T) {
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Generation: 2},
			Status:     appsv1.DeploymentStatus{ObservedGeneration: 1},
		}
		phase, msg := componentPhaseAndMessage(deploy, 2)
		assert.Equal(t, componentPhaseProgressing, phase)
		assert.Equal(t, "Deployment has not observed the latest generation", msg)
	})

	t.Run("progress deadline exceeded is Unavailable", func(t *testing.T) {
		deploy := &appsv1.Deployment{Status: appsv1.DeploymentStatus{
			Conditions: []appsv1.DeploymentCondition{{
				Type:    appsv1.DeploymentProgressing,
				Status:  corev1.ConditionFalse,
				Reason:  "ProgressDeadlineExceeded",
				Message: "deadline exceeded",
			}},
		}}
		phase, msg := componentPhaseAndMessage(deploy, 2)
		assert.Equal(t, componentPhaseUnavailable, phase)
		assert.Equal(t, "deadline exceeded", msg)
	})

	t.Run("partially ready is Progressing", func(t *testing.T) {
		deploy := &appsv1.Deployment{Status: appsv1.DeploymentStatus{ReadyReplicas: 1}}
		phase, msg := componentPhaseAndMessage(deploy, 3)
		assert.Equal(t, componentPhaseProgressing, phase)
		assert.Equal(t, "1 of 3 replicas are ready", msg)
	})

	t.Run("no replicas ready is Unavailable", func(t *testing.T) {
		deploy := &appsv1.Deployment{Status: appsv1.DeploymentStatus{ReadyReplicas: 0}}
		phase, msg := componentPhaseAndMessage(deploy, 3)
		assert.Equal(t, componentPhaseUnavailable, phase)
		assert.Equal(t, "0 of 3 replicas are ready", msg)
	})
}
