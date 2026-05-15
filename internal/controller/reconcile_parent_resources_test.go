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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestReconcile_CreatesParentOwnedComponentResources(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{}
	spec.CeleryBeat = &supersetv1alpha1.CeleryBeatComponentSpec{}
	spec.CeleryFlower = &supersetv1alpha1.CeleryFlowerComponentSpec{}
	spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{}
	spec.McpServer = &supersetv1alpha1.McpServerComponentSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	for _, name := range []string{
		"test-web-server",
		"test-celery-worker",
		"test-celery-beat",
		"test-celery-flower",
		"test-websocket-server",
		"test-mcp-server",
	} {
		deploy := &appsv1.Deployment{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, deploy); err != nil {
			t.Fatalf("expected Deployment %s: %v", name, err)
		}
		if !isOwnedBy(deploy, superset) {
			t.Fatalf("expected Deployment %s to be owned by Superset", name)
		}
	}

	for _, name := range []string{"test-web-server-config", "test-celery-worker-config", "test-mcp-server-config"} {
		cm := &corev1.ConfigMap{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, cm); err != nil {
			t.Fatalf("expected ConfigMap %s: %v", name, err)
		}
	}

	for _, name := range []string{"test-web-server", "test-celery-flower", "test-websocket-server", "test-mcp-server"} {
		svc := &corev1.Service{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, svc); err != nil {
			t.Fatalf("expected Service %s: %v", name, err)
		}
	}
}

func TestReconcile_DisabledComponentDeletesParentOwnedResources(t *testing.T) {
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       minimalSupersetSpec(),
	}
	workerDeploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "test-celery-worker", Namespace: "default"}}
	workerConfig := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "test-celery-worker-config", Namespace: "default"}}

	c := reconcileOnce(t, scheme, superset).WithObjects(workerDeploy, workerConfig).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-celery-worker", Namespace: "default"}, &appsv1.Deployment{}); err == nil {
		t.Fatal("expected disabled celery worker Deployment to be deleted")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-celery-worker-config", Namespace: "default"}, &corev1.ConfigMap{}); err == nil {
		t.Fatal("expected disabled celery worker ConfigMap to be deleted")
	}
}

func TestReconcile_LifecycleCreatesParentOwnedTaskJobAndStatus(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{}
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	jobs := &batchv1.JobList{}
	if err := c.List(context.Background(), jobs,
		client.MatchingLabels{labelInitInstance: "test-migrate"},
	); err != nil {
		t.Fatalf("list task jobs: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected one migrate task job, got %d", len(jobs.Items))
	}
	if jobs.Items[0].Labels[common.LabelKeyParent] != "test" {
		t.Fatalf("expected task job parent label, got %q", jobs.Items[0].Labels[common.LabelKeyParent])
	}

	updated := &supersetv1alpha1.Superset{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get Superset: %v", err)
	}
	if updated.Status.Lifecycle == nil || updated.Status.Lifecycle.Migrate == nil {
		t.Fatal("expected migrate status on parent lifecycle status")
	}
	if updated.Status.Lifecycle.Migrate.State != taskStateRunning {
		t.Fatalf("expected migrate state Running, got %q", updated.Status.Lifecycle.Migrate.State)
	}
	if updated.Status.Lifecycle.Migrate.DesiredChecksum == "" {
		t.Fatal("expected migrate desired checksum")
	}
	if updated.Status.Lifecycle.Migrate.Ref != "Job/test-migrate" {
		t.Fatalf("expected migrate job ref, got %q", updated.Status.Lifecycle.Migrate.Ref)
	}
}
