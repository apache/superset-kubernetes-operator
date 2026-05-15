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
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestDrainIfNeededEmitsStartedWhenPodsRemain(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	recorder := events.NewFakeRecorder(10)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-web-server-abc",
			Namespace: "default",
			Labels: map[string]string{
				common.LabelKeyParent:    "test",
				common.LabelKeyComponent: string(common.ComponentWebServer),
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, pod).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	result, err := r.drainIfNeeded(ctx, superset, false, true, false, false, "cfg", phaseUpgrading)
	if err != nil {
		t.Fatalf("drainIfNeeded: %v", err)
	}
	if result.Complete || result.RequeueAfter == 0 {
		t.Fatalf("expected drain wait result, got %#v", result)
	}
	if superset.Status.Lifecycle.Phase != lifecyclePhaseDraining || superset.Status.Phase != phaseUpgrading {
		t.Fatalf("expected draining phases, got lifecycle=%q phase=%q", superset.Status.Lifecycle.Phase, superset.Status.Phase)
	}
	assertNextEventContains(t, recorder, "Normal DrainingStarted Draining component workloads before lifecycle tasks")
}

func TestDrainIfNeededEmitsCompletedAfterWaitingForPods(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	recorder := events.NewFakeRecorder(10)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{Phase: lifecyclePhaseDraining},
			Conditions: []metav1.Condition{
				{
					Type:   supersetv1alpha1.ConditionTypeLifecycleComplete,
					Status: metav1.ConditionFalse,
					Reason: "Draining",
				},
			},
		},
	}
	maintenancePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-maintenance-page-abc",
			Namespace: "default",
			Labels: map[string]string{
				common.LabelKeyParent:    "test",
				common.LabelKeyComponent: string(common.ComponentMaintenancePage),
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, maintenancePod).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	result, err := r.drainIfNeeded(ctx, superset, false, true, false, false, "cfg", phaseInitializing)
	if err != nil {
		t.Fatalf("drainIfNeeded: %v", err)
	}
	if !result.Complete {
		t.Fatalf("expected completed drain result, got %#v", result)
	}
	if !hasLifecycleConditionReason(superset, "ComponentsDrained") {
		t.Fatalf("expected lifecycle condition reason ComponentsDrained, got %#v", superset.Status.Conditions)
	}
	assertNextEventContains(t, recorder, "Normal DrainingCompleted All component workloads drained; lifecycle tasks can proceed")
}

func TestDrainIfNeededSkipsWhenNoComponentHasDesiredReplicas(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	recorder := events.NewFakeRecorder(10)
	zero := int32(0)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
					Replicas: &zero,
				},
			},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-web-server-abc",
			Namespace: "default",
			Labels: map[string]string{
				common.LabelKeyParent:    "test",
				common.LabelKeyComponent: string(common.ComponentWebServer),
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, pod).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	result, err := r.drainIfNeeded(ctx, superset, false, true, false, false, "cfg", phaseInitializing)
	if err != nil {
		t.Fatalf("drainIfNeeded: %v", err)
	}
	if !result.Complete {
		t.Fatalf("expected drain to be skipped, got %#v", result)
	}
	assertNoEvents(t, recorder)
}

func TestPrepareMaintenancePageSkipsWhenNoComponentHasDesiredReplicas(t *testing.T) {
	ctx := context.Background()
	zero := int32(0)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
					Replicas: &zero,
				},
			},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				MaintenancePage: &supersetv1alpha1.MaintenancePageSpec{},
			},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{},
		},
	}
	r := &SupersetReconciler{}

	result, err := r.prepareMaintenancePage(ctx, superset, false, true, false, false, "cfg", phaseInitializing)
	if err != nil {
		t.Fatalf("prepareMaintenancePage: %v", err)
	}
	if !result.Complete {
		t.Fatalf("expected maintenance page to be skipped, got %#v", result)
	}
}

func TestPrepareMaintenancePageSkipsWhenWebServerHasNoDesiredReplicas(t *testing.T) {
	ctx := context.Background()
	zero := int32(0)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
					Replicas: &zero,
				},
			},
			CeleryWorker: &supersetv1alpha1.CeleryWorkerComponentSpec{},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				MaintenancePage: &supersetv1alpha1.MaintenancePageSpec{},
			},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{},
		},
	}
	r := &SupersetReconciler{}

	result, err := r.prepareMaintenancePage(ctx, superset, false, true, false, false, "cfg", phaseInitializing)
	if err != nil {
		t.Fatalf("prepareMaintenancePage: %v", err)
	}
	if !result.Complete {
		t.Fatalf("expected maintenance page to be skipped, got %#v", result)
	}
}

func TestPrepareMaintenancePageSkipsInitialInstallWithoutWebServerWorkload(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	recorder := events.NewFakeRecorder(10)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "test-uid"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				MaintenancePage: &supersetv1alpha1.MaintenancePageSpec{},
			},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	result, err := r.prepareMaintenancePage(ctx, superset, false, true, false, false, "cfg", phaseInitializing)
	if err != nil {
		t.Fatalf("prepareMaintenancePage: %v", err)
	}
	if !result.Complete {
		t.Fatalf("expected maintenance page to be skipped on initial install, got %#v", result)
	}

	maintenanceDeployment := &appsv1.Deployment{}
	err = c.Get(ctx, client.ObjectKey{Namespace: "default", Name: maintenanceDeploymentName("test")}, maintenanceDeployment)
	if err == nil {
		t.Fatal("expected no maintenance Deployment on initial install")
	}
	assertNoEvents(t, recorder)
}

func TestPrepareMaintenancePageStartsWhenWebServerDeploymentExists(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	recorder := events.NewFakeRecorder(10)
	replicas := int32(1)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "test-uid"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				MaintenancePage: &supersetv1alpha1.MaintenancePageSpec{},
			},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{},
		},
	}
	webDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.ResourceBaseName("test", common.ComponentWebServer),
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, webDeployment).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	result, err := r.prepareMaintenancePage(ctx, superset, false, true, false, false, "cfg", phaseInitializing)
	if err != nil {
		t.Fatalf("prepareMaintenancePage: %v", err)
	}
	if result.Complete || result.RequeueAfter == 0 {
		t.Fatalf("expected maintenance page readiness wait, got %#v", result)
	}

	maintenanceDeployment := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: maintenanceDeploymentName("test")}, maintenanceDeployment); err != nil {
		t.Fatalf("expected maintenance Deployment to be created: %v", err)
	}
	assertNoEvents(t, recorder)
}

func TestDrainIfNeededSkipsWhenOnlyNonDrainInitWillRun(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	recorder := events.NewFakeRecorder(10)
	r := &SupersetReconciler{Recorder: recorder}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "test-uid"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "6.1.0-dev"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					Source: supersetv1alpha1.CloneSourceSpec{Host: "postgres", Database: "prod", Username: "superset"},
				},
			},
		},
	}
	superset.Status.Lifecycle = &supersetv1alpha1.LifecycleStatus{
		LastCompletedChecksums: completedLifecycleChecksums(r, superset, "old-config"),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-web-server-abc",
			Namespace: "default",
			Labels: map[string]string{
				common.LabelKeyParent:    "test",
				common.LabelKeyComponent: string(common.ComponentWebServer),
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, pod).
		Build()
	r.Client = c
	r.Scheme = scheme

	result, err := r.drainIfNeeded(ctx, superset, true, true, false, true, "new-config", phaseInitializing)
	if err != nil {
		t.Fatalf("drainIfNeeded: %v", err)
	}
	if !result.Complete {
		t.Fatalf("expected drain to be skipped when only init will run, got %#v", result)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: pod.Name}, &corev1.Pod{}); err != nil {
		t.Fatalf("expected web-server pod to remain: %v", err)
	}
	assertNoEvents(t, recorder)
}

func completedLifecycleChecksums(r *SupersetReconciler, superset *supersetv1alpha1.Superset, configChecksum string) map[string]string {
	checksums := make(map[string]string)
	incomingChecksum := string(superset.UID)

	cloneCmd := r.buildCloneCommand(superset)
	checksums[taskTypeClone] = r.computeStepChecksum(incomingChecksum, taskTypeClone, cloneCmd, r.cloneInputs(superset))
	incomingChecksum = checksums[taskTypeClone]

	migrateCmd := defaultMigrateCommand(superset)
	checksums[taskTypeMigrate] = r.computeStepChecksum(incomingChecksum, taskTypeMigrate, migrateCmd, r.migrateInputs(superset))
	incomingChecksum = checksums[taskTypeMigrate]

	initCmd := defaultInitCommand(superset)
	checksums[taskTypeInit] = r.computeStepChecksum(incomingChecksum, taskTypeInit, initCmd, r.initInputs(superset, configChecksum))

	return checksums
}

func assertNextEventContains(t *testing.T, recorder *events.FakeRecorder, want string) {
	t.Helper()

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, want) {
			t.Fatalf("expected event to contain %q, got %q", want, event)
		}
	default:
		t.Fatalf("expected event containing %q, got none", want)
	}
}

func assertNoEvents(t *testing.T, recorder *events.FakeRecorder) {
	t.Helper()

	select {
	case event := <-recorder.Events:
		t.Fatalf("expected no events, got %q", event)
	default:
	}
}
