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
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func specEnv(spec supersetv1alpha1.FlatComponentSpec) []corev1.EnvVar {
	if spec.PodTemplate != nil && spec.PodTemplate.Container != nil {
		return spec.PodTemplate.Container.Env
	}
	return nil
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := supersetv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(superset): %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(corev1): %v", err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(appsv1): %v", err)
	}
	if err := networkingv1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(networkingv1): %v", err)
	}
	if err := gatewayv1.Install(s); err != nil {
		t.Fatalf("Install(gatewayv1): %v", err)
	}
	return s
}

func minimalSupersetSpec() supersetv1alpha1.SupersetSpec {
	return supersetv1alpha1.SupersetSpec{
		Image: supersetv1alpha1.ImageSpec{
			Repository: "apache/superset",
			Tag:        "latest",
		},
		SecretKeyFrom: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "app-secret"},
			Key:                  "secret-key",
		},
		WebServer: &supersetv1alpha1.WebServerComponentSpec{},
		Lifecycle: &supersetv1alpha1.LifecycleSpec{Disabled: boolPtr(true)},
	}
}

func reconcileOnce(t *testing.T, scheme *runtime.Scheme, superset *supersetv1alpha1.Superset) *fake.ClientBuilder {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset).
		WithStatusSubresource(
			&supersetv1alpha1.Superset{},
			&supersetv1alpha1.SupersetWebServer{},
			&supersetv1alpha1.SupersetCeleryWorker{},
			&supersetv1alpha1.SupersetCeleryBeat{},
			&supersetv1alpha1.SupersetCeleryFlower{},
			&supersetv1alpha1.SupersetWebsocketServer{},
			&supersetv1alpha1.SupersetMcpServer{},
		)
}

func doReconcile(t *testing.T, r *SupersetReconciler, name string) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: name, Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
}

func TestReconcile_MinimalSuperset_CreatesWebServer(t *testing.T) {
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       minimalSupersetSpec(),
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ww := &supersetv1alpha1.SupersetWebServer{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, ww); err != nil {
		t.Fatalf("expected web server to be created: %v", err)
	}
	if ww.Spec.Image.Repository != "apache/superset" {
		t.Errorf("expected repository apache/superset, got %s", ww.Spec.Image.Repository)
	}
}

func TestReconcile_CeleryEnabled_CreatesAllCeleryChildren(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{}
	spec.CeleryBeat = &supersetv1alpha1.CeleryBeatComponentSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()

	cw := &supersetv1alpha1.SupersetCeleryWorker{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, cw); err != nil {
		t.Fatalf("expected celery worker: %v", err)
	}

	cb := &supersetv1alpha1.SupersetCeleryBeat{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, cb); err != nil {
		t.Fatalf("expected celery beat: %v", err)
	}
}

func TestReconcile_SuspendAndResume(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Suspend = boolPtr(true)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	ctx := context.Background()

	// Suspend: condition should be True, no child CRs created.
	doReconcile(t, r, "test")

	updated := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	foundSuspended := false
	for _, cond := range updated.Status.Conditions {
		if cond.Type == supersetv1alpha1.ConditionTypeSuspended && cond.Status == metav1.ConditionTrue {
			foundSuspended = true
		}
	}
	if !foundSuspended {
		t.Error("expected Suspended condition to be True")
	}
	if updated.Status.Phase != "Suspended" {
		t.Errorf("expected phase Suspended, got %q", updated.Status.Phase)
	}

	// Resume: clear suspend flag and reconcile again.
	updated.Spec.Suspend = nil
	if err := c.Update(ctx, updated); err != nil {
		t.Fatalf("update superset: %v", err)
	}
	doReconcile(t, r, "test")

	resumed := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, resumed); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	for _, cond := range resumed.Status.Conditions {
		if cond.Type == supersetv1alpha1.ConditionTypeSuspended && cond.Status != metav1.ConditionFalse {
			t.Errorf("expected Suspended condition to be False after resume, got %s", cond.Status)
		}
	}
}

func TestReconcile_ComponentDisabled_DeletesChildCR(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	ctx := context.Background()

	doReconcile(t, r, "test")

	cw := &supersetv1alpha1.SupersetCeleryWorker{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, cw); err != nil {
		t.Fatalf("expected celery worker to be created: %v", err)
	}

	// Disable celery worker.
	updated := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	updated.Spec.CeleryWorker = nil
	if err := c.Update(ctx, updated); err != nil {
		t.Fatalf("update superset: %v", err)
	}

	doReconcile(t, r, "test")

	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, &supersetv1alpha1.SupersetCeleryWorker{}); !errors.IsNotFound(err) {
		t.Errorf("expected celery worker to be deleted after disabling, got err=%v", err)
	}
}

func TestReconcile_ImageOverride(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{
		ComponentSpec: supersetv1alpha1.ComponentSpec{
			Image: &supersetv1alpha1.ImageOverrideSpec{Tag: strPtr("5.0.0-beta")},
		},
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ww := &supersetv1alpha1.SupersetWebServer{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, ww); err != nil {
		t.Fatalf("expected web server: %v", err)
	}
	if ww.Spec.Image.Tag != "5.0.0-beta" {
		t.Errorf("expected overridden tag 5.0.0-beta, got %s", ww.Spec.Image.Tag)
	}
	if ww.Spec.Image.Repository != "apache/superset" {
		t.Errorf("expected repository apache/superset (inherited), got %s", ww.Spec.Image.Repository)
	}
}

func TestReconcile_MetastoreModes(t *testing.T) {
	scheme := testScheme(t)

	t.Run("passthrough", func(t *testing.T) {
		spec := minimalSupersetSpec()
		spec.Metastore = &supersetv1alpha1.MetastoreSpec{
			URI: common.Ptr("postgresql://user:pass@host/db"),
		}

		superset := &supersetv1alpha1.Superset{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pt", Namespace: "default"},
			Spec:       spec,
		}

		c := reconcileOnce(t, scheme, superset).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
		doReconcile(t, r, "test-pt")

		ww := &supersetv1alpha1.SupersetWebServer{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: "test-pt", Namespace: "default"}, ww); err != nil {
			t.Fatalf("expected web server: %v", err)
		}

		hasDBUri := false
		for _, env := range specEnv(ww.Spec.FlatComponentSpec) {
			if env.Name == "SUPERSET_OPERATOR__DB_URI" {
				hasDBUri = true
			}
		}
		if !hasDBUri {
			t.Error("expected SUPERSET_OPERATOR__DB_URI env var")
		}
	})

	t.Run("structured", func(t *testing.T) {
		spec := minimalSupersetSpec()
		spec.Metastore = &supersetv1alpha1.MetastoreSpec{
			Host:     common.Ptr("db.example.com"),
			Database: common.Ptr("superset"),
			Username: common.Ptr("admin"),
			Password: common.Ptr("secret"),
		}

		superset := &supersetv1alpha1.Superset{
			ObjectMeta: metav1.ObjectMeta{Name: "test-st", Namespace: "default"},
			Spec:       spec,
		}

		c := reconcileOnce(t, scheme, superset).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
		doReconcile(t, r, "test-st")

		ww := &supersetv1alpha1.SupersetWebServer{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: "test-st", Namespace: "default"}, ww); err != nil {
			t.Fatalf("expected web server: %v", err)
		}

		envMap := make(map[string]string)
		for _, env := range specEnv(ww.Spec.FlatComponentSpec) {
			envMap[env.Name] = env.Value
		}
		if envMap["SUPERSET_OPERATOR__DB_HOST"] != "db.example.com" {
			t.Errorf("expected SUPERSET_OPERATOR__DB_HOST=db.example.com, got %q", envMap["SUPERSET_OPERATOR__DB_HOST"])
		}
		if envMap["SUPERSET_OPERATOR__DB_NAME"] != "superset" {
			t.Errorf("expected SUPERSET_OPERATOR__DB_NAME=superset, got %q", envMap["SUPERSET_OPERATOR__DB_NAME"])
		}
	})
}

func TestReconcile_AllComponents_FullFeatures(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Environment = common.Ptr("dev")
	spec.SecretKey = common.Ptr("test-secret-key")
	spec.SecretKeyFrom = nil
	spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		URI: common.Ptr("postgresql://user:pass@host/db"),
	}
	spec.ForceReload = "2026-03-18T00:00:00Z"
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{Disabled: boolPtr(true)}

	replicas2 := int32(2)
	replicas4 := int32(4)
	spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{
		ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{Replicas: &replicas2},
		Config:                strPtr("WEB_SETTING = True"),
	}
	spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{
		ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{Replicas: &replicas4},
		Config:                strPtr("WORKER_SETTING = True"),
	}
	spec.CeleryBeat = &supersetv1alpha1.CeleryBeatComponentSpec{}
	spec.CeleryFlower = &supersetv1alpha1.CeleryFlowerComponentSpec{}
	spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{}
	spec.McpServer = &supersetv1alpha1.McpServerComponentSpec{
		Config: strPtr("MCP_SETTING = True"),
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "full", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(20)}
	doReconcile(t, r, "full")

	ctx := context.Background()

	// All 6 child CRs should exist.
	childChecks := []struct {
		name string
		obj  client.Object
	}{
		{"full", &supersetv1alpha1.SupersetWebServer{}},
		{"full", &supersetv1alpha1.SupersetCeleryWorker{}},
		{"full", &supersetv1alpha1.SupersetCeleryBeat{}},
		{"full", &supersetv1alpha1.SupersetCeleryFlower{}},
		{"full", &supersetv1alpha1.SupersetWebsocketServer{}},
		{"full", &supersetv1alpha1.SupersetMcpServer{}},
	}
	for _, cc := range childChecks {
		if err := c.Get(ctx, types.NamespacedName{Name: cc.name, Namespace: "default"}, cc.obj); err != nil {
			t.Fatalf("expected %T %s to exist: %v", cc.obj, cc.name, err)
		}
	}

	// Web server child created.
	ww := &supersetv1alpha1.SupersetWebServer{}
	_ = c.Get(ctx, types.NamespacedName{Name: "full", Namespace: "default"}, ww)

	// Celery worker child created.
	cw := &supersetv1alpha1.SupersetCeleryWorker{}
	_ = c.Get(ctx, types.NamespacedName{Name: "full", Namespace: "default"}, cw)

	// WebsocketServer: no config volume (Node.js).
	wss := &supersetv1alpha1.SupersetWebsocketServer{}
	_ = c.Get(ctx, types.NamespacedName{Name: "full", Namespace: "default"}, wss)

	// MCP server child created.
	ms := &supersetv1alpha1.SupersetMcpServer{}
	_ = c.Get(ctx, types.NamespacedName{Name: "full", Namespace: "default"}, ms)

	// All Python components should have SECRET_KEY env vars.
	cb := &supersetv1alpha1.SupersetCeleryBeat{}
	_ = c.Get(ctx, types.NamespacedName{Name: "full", Namespace: "default"}, cb)
	cf := &supersetv1alpha1.SupersetCeleryFlower{}
	_ = c.Get(ctx, types.NamespacedName{Name: "full", Namespace: "default"}, cf)

	pythonComponents := []struct {
		name string
		envs []corev1.EnvVar
	}{
		{"web-server", specEnv(ww.Spec.FlatComponentSpec)},
		{"celery-worker", specEnv(cw.Spec.FlatComponentSpec)},
		{"celery-beat", specEnv(cb.Spec.FlatComponentSpec)},
		{"celery-flower", specEnv(cf.Spec.FlatComponentSpec)},
		{"mcp-server", specEnv(ms.Spec.FlatComponentSpec)},
	}

	for _, pc := range pythonComponents {
		envMap := make(map[string]string)
		for _, env := range pc.envs {
			envMap[env.Name] = env.Value
		}
		if envMap["SUPERSET_OPERATOR__SECRET_KEY"] != "test-secret-key" {
			t.Errorf("%s should have SUPERSET_OPERATOR__SECRET_KEY env var in dev mode", pc.name)
		}
		if envMap["SUPERSET_OPERATOR__FORCE_RELOAD"] != "2026-03-18T00:00:00Z" {
			t.Errorf("%s should have SUPERSET_OPERATOR__FORCE_RELOAD env var", pc.name)
		}
	}

	// ForceReload also on non-Python components.
	wsEnvMap := make(map[string]string)
	for _, env := range specEnv(wss.Spec.FlatComponentSpec) {
		wsEnvMap[env.Name] = env.Value
	}
	if wsEnvMap["SUPERSET_OPERATOR__FORCE_RELOAD"] != "2026-03-18T00:00:00Z" {
		t.Error("websocket-server should have SUPERSET_OPERATOR__FORCE_RELOAD env var")
	}

	// Status should be set.
	updated := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "full", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	if updated.Status.Phase != "Degraded" {
		t.Errorf("expected Phase=Degraded (no Deployments exist yet), got %q", updated.Status.Phase)
	}
	if updated.Status.Version != "latest" {
		t.Errorf("expected status version latest, got %s", updated.Status.Version)
	}

	// ServiceAccount should be created.
	sa := &corev1.ServiceAccount{}
	if err := c.Get(ctx, types.NamespacedName{Name: "full", Namespace: "default"}, sa); err != nil {
		t.Fatalf("expected ServiceAccount to be created: %v", err)
	}

	// Security: all child CRs should have non-empty checksums, and components
	// with different per-component config should have different checksums.
	checksums := map[string]string{
		"web-server":    ww.Spec.ConfigChecksum,
		"celery-worker": cw.Spec.ConfigChecksum,
		"celery-beat":   cb.Spec.ConfigChecksum,
		"celery-flower": cf.Spec.ConfigChecksum,
		"mcp-server":    ms.Spec.ConfigChecksum,
	}
	for name, cs := range checksums {
		if cs == "" {
			t.Errorf("%s should have a non-empty ConfigChecksum", name)
		}
	}
	if checksums["web-server"] == checksums["celery-worker"] {
		t.Error("web-server and celery-worker have different per-component config, checksums should differ")
	}
	if checksums["web-server"] == checksums["mcp-server"] {
		t.Error("web-server and mcp-server have different per-component config, checksums should differ")
	}
	if checksums["celery-beat"] == checksums["web-server"] {
		t.Error("celery-beat (no per-component config) and web-server (has config) checksums should differ")
	}

	// Security: all child CRs should have ServiceAccountName set.
	for _, check := range []struct {
		name string
		sa   string
	}{
		{"web-server", ww.Spec.ServiceAccountName},
		{"celery-worker", cw.Spec.ServiceAccountName},
		{"celery-beat", cb.Spec.ServiceAccountName},
		{"celery-flower", cf.Spec.ServiceAccountName},
		{"websocket-server", wss.Spec.ServiceAccountName},
		{"mcp-server", ms.Spec.ServiceAccountName},
	} {
		if check.sa != "full" {
			t.Errorf("%s should have ServiceAccountName 'full', got %q", check.name, check.sa)
		}
	}

	// Parent label should appear on pod templates for instance-scoped NetworkPolicy.
	for _, check := range []struct {
		name   string
		labels map[string]string
	}{
		{"web-server", ww.Spec.PodTemplate.Labels},
		{"celery-worker", cw.Spec.PodTemplate.Labels},
		{"celery-beat", cb.Spec.PodTemplate.Labels},
		{"celery-flower", cf.Spec.PodTemplate.Labels},
		{"websocket-server", wss.Spec.PodTemplate.Labels},
		{"mcp-server", ms.Spec.PodTemplate.Labels},
	} {
		if check.labels[common.LabelKeyParent] != "full" {
			t.Errorf("%s pod template should have parent label 'full', got %q", check.name, check.labels[common.LabelKeyParent])
		}
	}
}

func TestReconcile_TopLevelAutoscalingPDB_Inherited(t *testing.T) {
	scheme := testScheme(t)

	minAvail := intstr.FromInt32(1)
	spec := minimalSupersetSpec()
	spec.Environment = common.Ptr("dev")
	spec.SecretKey = common.Ptr("test-key")
	spec.SecretKeyFrom = nil
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{Disabled: boolPtr(true)}
	spec.Autoscaling = &supersetv1alpha1.AutoscalingSpec{MaxReplicas: 10}
	spec.PodDisruptionBudget = &supersetv1alpha1.PDBSpec{MinAvailable: &minAvail}

	workerMax := int32(20)
	spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{}
	spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{
		ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
			Autoscaling: &supersetv1alpha1.AutoscalingSpec{MaxReplicas: workerMax},
		},
	}
	spec.CeleryBeat = &supersetv1alpha1.CeleryBeatComponentSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "hpa", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "hpa")

	ctx := context.Background()

	// WebServer inherits top-level autoscaling and PDB.
	ww := &supersetv1alpha1.SupersetWebServer{}
	if err := c.Get(ctx, types.NamespacedName{Name: "hpa", Namespace: "default"}, ww); err != nil {
		t.Fatalf("get web server: %v", err)
	}
	if ww.Spec.Autoscaling == nil || ww.Spec.Autoscaling.MaxReplicas != 10 {
		t.Error("web server should inherit top-level autoscaling (maxReplicas=10)")
	}
	if ww.Spec.PodDisruptionBudget == nil || ww.Spec.PodDisruptionBudget.MinAvailable == nil {
		t.Error("web server should inherit top-level PDB")
	}

	// CeleryWorker overrides autoscaling but inherits PDB.
	cw := &supersetv1alpha1.SupersetCeleryWorker{}
	if err := c.Get(ctx, types.NamespacedName{Name: "hpa", Namespace: "default"}, cw); err != nil {
		t.Fatalf("get celery worker: %v", err)
	}
	if cw.Spec.Autoscaling == nil || cw.Spec.Autoscaling.MaxReplicas != workerMax {
		t.Errorf("celery worker should override autoscaling to maxReplicas=%d", workerMax)
	}
	if cw.Spec.PodDisruptionBudget == nil || cw.Spec.PodDisruptionBudget.MinAvailable == nil {
		t.Error("celery worker should inherit top-level PDB")
	}

	// CeleryBeat is a singleton — no autoscaling or PDB.
	cb := &supersetv1alpha1.SupersetCeleryBeat{}
	if err := c.Get(ctx, types.NamespacedName{Name: "hpa", Namespace: "default"}, cb); err != nil {
		t.Fatalf("get celery beat: %v", err)
	}
	if cb.Spec.Autoscaling != nil {
		t.Error("celery beat should not have autoscaling (singleton)")
	}
	if cb.Spec.PodDisruptionBudget != nil {
		t.Error("celery beat should not have PDB (singleton)")
	}
}

func TestReconcile_PerComponentConfigMerging(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Config = strPtr("BASE = True")
	spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{
		Config: strPtr("WEB = True"),
	}
	spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "cfg")

	ctx := context.Background()

	// Web server should have a config checksum.
	ww := &supersetv1alpha1.SupersetWebServer{}
	if err := c.Get(ctx, types.NamespacedName{Name: "cfg", Namespace: "default"}, ww); err != nil {
		t.Fatalf("expected web server: %v", err)
	}

	// Celery worker should have a config checksum.
	cw := &supersetv1alpha1.SupersetCeleryWorker{}
	if err := c.Get(ctx, types.NamespacedName{Name: "cfg", Namespace: "default"}, cw); err != nil {
		t.Fatalf("expected celery worker: %v", err)
	}

	// Components with different rendered config should have different checksums.
	if ww.Spec.ConfigChecksum == cw.Spec.ConfigChecksum {
		t.Error("web server (has per-component config) and celery worker (no per-component config) should have different checksums")
	}
}

func TestReconcile_InitGatesComponentDeployment(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()

	// Migrate task CR should exist (first task in the lifecycle).
	migrateCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, migrateCR); err != nil {
		t.Fatalf("expected migrate CR to exist: %v", err)
	}

	// Web server should NOT exist — lifecycle gates component deployment.
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, &supersetv1alpha1.SupersetWebServer{}); !errors.IsNotFound(err) {
		t.Errorf("expected web server to NOT exist while init is pending, got err=%v", err)
	}
}

func TestReconcile_InitGatesOnStaleChecksum(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{Strategy: strPtr("Never")},
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()

	// Simulate init completing with the current checksum.
	initCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-init", Namespace: "default"}, initCR); err != nil {
		t.Fatalf("get init CR: %v", err)
	}
	initCR.Status.State = "Complete"
	initCR.Status.ConfigChecksum = initCR.Spec.ConfigChecksum
	if err := c.Status().Update(ctx, initCR); err != nil {
		t.Fatalf("update init status: %v", err)
	}

	// Reconcile again — init is complete, components should be created.
	doReconcile(t, r, "test")
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, &supersetv1alpha1.SupersetWebServer{}); err != nil {
		t.Fatalf("expected web server to exist after init completes: %v", err)
	}

	// Now change the spec (image tag) to trigger a new checksum.
	updated := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	updated.Spec.Image.Tag = "new-tag"
	if err := c.Update(ctx, updated); err != nil {
		t.Fatalf("update superset: %v", err)
	}

	// Reconcile — parent sees init CR with mismatched checksum → deletes it.
	doReconcile(t, r, "test")

	// The old init CR should be deleted (parent uses delete+create pattern).
	err := c.Get(ctx, types.NamespacedName{Name: "test-init", Namespace: "default"}, initCR)
	if err == nil {
		t.Fatal("expected init CR to be deleted after image change (checksum mismatch)")
	}
	if !errors.IsNotFound(err) {
		t.Fatalf("unexpected error: %v", err)
	}

	// Parent status should reflect the gate (lifecycle incomplete).
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	if updated.Status.Phase == "Running" {
		t.Error("expected phase != Running while init is being re-run")
	}
}

func TestReconcile_InitCommand_PropagatedToChildCR(t *testing.T) {
	// Init gates component deployment (reconcile returns early), so this can't
	// be folded into the all-components happy-path test. Tests the parent →
	// child CR command propagation path that broke silently during the
	// DeploymentTemplate refactor.
	scheme := testScheme(t)

	customCmd := []string{"/bin/sh", "-c", "superset db upgrade --sql"}
	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{
			Command: customCmd,
		},
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()

	initCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, initCR); err != nil {
		t.Fatalf("expected migrate CR to exist: %v", err)
	}

	// Verify migrate.command reaches the child CR's podTemplate.container.command.
	if initCR.Spec.PodTemplate == nil || initCR.Spec.PodTemplate.Container == nil {
		t.Fatal("expected init CR to have podTemplate.container")
	}
	gotCmd := initCR.Spec.PodTemplate.Container.Command
	if len(gotCmd) != len(customCmd) {
		t.Fatalf("expected command %v, got %v", customCmd, gotCmd)
	}
	for i, c := range customCmd {
		if gotCmd[i] != c {
			t.Errorf("command[%d]: expected %q, got %q", i, c, gotCmd[i])
		}
	}
}

func TestReconcile_InitNilSpec_DefaultCommand(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{Strategy: strPtr("Never")},
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()

	initCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-init", Namespace: "default"}, initCR); err != nil {
		t.Fatalf("expected init CR to exist: %v", err)
	}

	if initCR.Spec.PodTemplate == nil || initCR.Spec.PodTemplate.Container == nil {
		t.Fatal("expected init CR to have podTemplate.container")
	}
	gotCmd := initCR.Spec.PodTemplate.Container.Command
	expectedCmd := []string{"/bin/sh", "-c", "superset init"}
	if len(gotCmd) != len(expectedCmd) {
		t.Fatalf("expected default command %v, got %v", expectedCmd, gotCmd)
	}
	for i, c := range expectedCmd {
		if gotCmd[i] != c {
			t.Errorf("command[%d]: expected %q, got %q", i, c, gotCmd[i])
		}
	}
}

func TestReconcile_InitChecksumChangesOnImageAndCommand(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{
			Command: []string{"/bin/sh", "-c", "superset db upgrade"},
		},
	}
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset).
		WithStatusSubresource(&supersetv1alpha1.Superset{}, &supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	ctx := context.Background()
	topLevel := convertTopLevelSpec(&superset.Spec)
	_, _, err := r.reconcileLifecycle(ctx, superset, computeChecksum("base"), topLevel, resolveServiceAccountName(superset))
	if err != nil {
		t.Fatalf("initial reconcileLifecycle: %v", err)
	}

	// Get the migrate CR and simulate completion.
	initCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, initCR); err != nil {
		t.Fatalf("get migrate CR: %v", err)
	}
	originalChecksum := initCR.Spec.ConfigChecksum
	initCR.Status.State = "Complete"
	initCR.Status.ConfigChecksum = originalChecksum
	if err := c.Status().Update(ctx, initCR); err != nil {
		t.Fatalf("update migrate status: %v", err)
	}

	// Change spec — image tag and command — to trigger a different checksum.
	updated := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	updated.Spec.Image.Tag = "next"
	updated.Spec.Lifecycle.Migrate.Command = []string{"/bin/sh", "-c", "superset db upgrade && superset init"}
	if err := c.Update(ctx, updated); err != nil {
		t.Fatalf("update superset: %v", err)
	}

	// Reconcile — parent sees checksum mismatch → deletes old CR.
	topLevel = convertTopLevelSpec(&updated.Spec)
	_, _, err = r.reconcileLifecycle(ctx, updated, computeChecksum("base"), topLevel, resolveServiceAccountName(updated))
	if err != nil {
		t.Fatalf("updated reconcileLifecycle: %v", err)
	}

	// Old CR should be deleted (parent deletes on checksum mismatch).
	err = c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, initCR)
	if err == nil {
		t.Fatal("expected migrate CR to be deleted when checksum changes")
	}
	if !errors.IsNotFound(err) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReconcile_InitChecksum_StableWhenAutoscalingChanges(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{
			Command: []string{"/bin/sh", "-c", "superset db upgrade"},
		},
	}
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset).
		WithStatusSubresource(&supersetv1alpha1.Superset{}, &supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	ctx := context.Background()
	topLevel := convertTopLevelSpec(&superset.Spec)
	_, _, err := r.reconcileLifecycle(ctx, superset, computeChecksum("base"), topLevel, resolveServiceAccountName(superset))
	if err != nil {
		t.Fatalf("initial reconcileLifecycle: %v", err)
	}

	initCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, initCR); err != nil {
		t.Fatalf("get migrate CR: %v", err)
	}
	originalChecksum := initCR.Spec.ConfigChecksum

	updated := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	updated.Spec.Autoscaling = &supersetv1alpha1.AutoscalingSpec{MaxReplicas: 10}
	minAvail := intstr.FromInt32(1)
	updated.Spec.PodDisruptionBudget = &supersetv1alpha1.PDBSpec{MinAvailable: &minAvail}
	if err := c.Update(ctx, updated); err != nil {
		t.Fatalf("update superset: %v", err)
	}

	topLevel = convertTopLevelSpec(&updated.Spec)
	_, _, err = r.reconcileLifecycle(ctx, updated, computeChecksum("base"), topLevel, resolveServiceAccountName(updated))
	if err != nil {
		t.Fatalf("updated reconcileLifecycle: %v", err)
	}

	if err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, initCR); err != nil {
		t.Fatalf("get updated migrate CR: %v", err)
	}
	if initCR.Spec.ConfigChecksum != originalChecksum {
		t.Fatal("migrate checksum should not change when only top-level autoscaling/PDB change")
	}
	if initCR.Spec.Autoscaling != nil {
		t.Error("init should not have autoscaling")
	}
	if initCR.Spec.PodDisruptionBudget != nil {
		t.Error("init should not have PDB")
	}
}

func TestReconcile_InitAdminUser_CommandAndEnvVars(t *testing.T) {
	scheme := testScheme(t)

	dev := "dev"
	spec := minimalSupersetSpec()
	spec.Environment = &dev
	spec.SecretKey = strPtr("test-secret")
	spec.SecretKeyFrom = nil
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{Strategy: strPtr("Never")},
		Init: &supersetv1alpha1.InitTaskSpec{
			AdminUser: &supersetv1alpha1.AdminUserSpec{},
		},
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	initCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-init", Namespace: "default"}, initCR); err != nil {
		t.Fatalf("expected init CR: %v", err)
	}

	if initCR.Spec.PodTemplate == nil || initCR.Spec.PodTemplate.Container == nil {
		t.Fatal("expected init CR to have podTemplate.container")
	}
	gotCmd := initCR.Spec.PodTemplate.Container.Command
	if len(gotCmd) != 3 || gotCmd[0] != "/bin/sh" || gotCmd[1] != "-c" {
		t.Fatalf("expected shell command, got %v", gotCmd)
	}
	script := gotCmd[2]
	if !strings.Contains(script, "superset init") {
		t.Errorf("expected script to contain 'superset init', got %q", script)
	}
	if !strings.Contains(script, "superset fab create-admin") {
		t.Error("expected script to contain 'superset fab create-admin'")
	}
	if !strings.Contains(script, "$SUPERSET_OPERATOR__ADMIN_USERNAME") {
		t.Error("expected create-admin to reference env vars, not inline values")
	}
	if !strings.Contains(script, "|| true)") {
		t.Error("expected create-admin to be idempotent (|| true)")
	}

	envs := initCR.Spec.PodTemplate.Container.Env
	envMap := make(map[string]string)
	for _, e := range envs {
		envMap[e.Name] = e.Value
	}
	if envMap["SUPERSET_OPERATOR__ADMIN_USERNAME"] != "admin" {
		t.Errorf("expected ADMIN_USERNAME=admin, got %q", envMap["SUPERSET_OPERATOR__ADMIN_USERNAME"])
	}
	if envMap["SUPERSET_OPERATOR__ADMIN_EMAIL"] != "admin@example.com" {
		t.Errorf("expected ADMIN_EMAIL=admin@example.com, got %q", envMap["SUPERSET_OPERATOR__ADMIN_EMAIL"])
	}
}

func TestReconcile_InitLoadExamples_CommandConstruction(t *testing.T) {
	scheme := testScheme(t)

	dev := "dev"
	spec := minimalSupersetSpec()
	spec.Environment = &dev
	spec.SecretKey = strPtr("test-secret")
	spec.SecretKeyFrom = nil
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{Strategy: strPtr("Never")},
		Init: &supersetv1alpha1.InitTaskSpec{
			LoadExamples: boolPtr(true),
		},
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	initCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-init", Namespace: "default"}, initCR); err != nil {
		t.Fatalf("expected init CR: %v", err)
	}

	script := initCR.Spec.PodTemplate.Container.Command[2]
	if !strings.Contains(script, "superset load-examples") {
		t.Error("expected script to contain 'superset load-examples'")
	}
	if strings.Contains(script, "create-admin") {
		t.Error("did not expect create-admin without adminUser")
	}
}

func TestReconcile_InitAdminAndExamples_Combined(t *testing.T) {
	scheme := testScheme(t)

	dev := "dev"
	spec := minimalSupersetSpec()
	spec.Environment = &dev
	spec.SecretKey = strPtr("test-secret")
	spec.SecretKeyFrom = nil
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{Strategy: strPtr("Never")},
		Init: &supersetv1alpha1.InitTaskSpec{
			AdminUser:    &supersetv1alpha1.AdminUserSpec{},
			LoadExamples: boolPtr(true),
		},
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	initCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-init", Namespace: "default"}, initCR); err != nil {
		t.Fatalf("expected init CR: %v", err)
	}

	script := initCR.Spec.PodTemplate.Container.Command[2]
	createIdx := strings.Index(script, "create-admin")
	examplesIdx := strings.Index(script, "load-examples")
	initIdx := strings.Index(script, "superset init")
	if createIdx < 0 || examplesIdx < 0 || initIdx < 0 {
		t.Fatalf("expected all three commands in script: %s", script)
	}
	if initIdx > createIdx {
		t.Error("expected 'superset init' before 'create-admin'")
	}
	if createIdx > examplesIdx {
		t.Error("expected 'create-admin' before 'load-examples'")
	}
}

func TestReconcile_ServiceAccount_CreateFalseWithName(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.ServiceAccount = &supersetv1alpha1.ServiceAccountSpec{
		Create: boolPtr(false),
		Name:   "existing-sa",
	}
	spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	cb := reconcileOnce(t, scheme, superset)
	c := cb.Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	// ServiceAccount should NOT be created.
	sa := &corev1.ServiceAccount{}
	err := c.Get(context.Background(), types.NamespacedName{Name: "existing-sa", Namespace: "default"}, sa)
	if !errors.IsNotFound(err) {
		t.Errorf("expected ServiceAccount to not be created, got err: %v", err)
	}

	// Child CRs should reference the user-managed SA name.
	ctx := context.Background()
	ww := &supersetv1alpha1.SupersetWebServer{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, ww); err != nil {
		t.Fatalf("get web-server: %v", err)
	}
	if ww.Spec.ServiceAccountName != "existing-sa" {
		t.Errorf("web-server should have ServiceAccountName 'existing-sa', got %q", ww.Spec.ServiceAccountName)
	}

	cw := &supersetv1alpha1.SupersetCeleryWorker{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, cw); err != nil {
		t.Fatalf("get celery-worker: %v", err)
	}
	if cw.Spec.ServiceAccountName != "existing-sa" {
		t.Errorf("celery-worker should have ServiceAccountName 'existing-sa', got %q", cw.Spec.ServiceAccountName)
	}
}

func TestReconcile_ServiceAccount_CleanupOnCreateDisabled(t *testing.T) {
	scheme := testScheme(t)

	// Start with create enabled (default).
	spec := minimalSupersetSpec()
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	cb := reconcileOnce(t, scheme, superset)
	c := cb.Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	// SA should exist.
	sa := &corev1.ServiceAccount{}
	ctx := context.Background()
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, sa); err != nil {
		t.Fatalf("expected SA to be created: %v", err)
	}

	// Now flip create to false.
	existing := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, existing); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	existing.Spec.ServiceAccount = &supersetv1alpha1.ServiceAccountSpec{
		Create: boolPtr(false),
		Name:   "external-sa",
	}
	if err := c.Update(ctx, existing); err != nil {
		t.Fatalf("update superset: %v", err)
	}
	doReconcile(t, r, "test")

	// Operator-managed SA should be cleaned up.
	err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, &corev1.ServiceAccount{})
	if !errors.IsNotFound(err) {
		t.Errorf("expected operator-managed SA to be deleted, got err: %v", err)
	}
}

func TestReconcile_ServiceAccount_PrunesOldSAOnNameChange(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	cb := reconcileOnce(t, scheme, superset)
	c := cb.Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()

	// Default SA (named after the CR) should exist.
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, &corev1.ServiceAccount{}); err != nil {
		t.Fatalf("expected default SA to be created: %v", err)
	}

	// Rename the ServiceAccount.
	existing := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, existing); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	existing.Spec.ServiceAccount = &supersetv1alpha1.ServiceAccountSpec{
		Name: "renamed-sa",
	}
	if err := c.Update(ctx, existing); err != nil {
		t.Fatalf("update superset: %v", err)
	}
	doReconcile(t, r, "test")

	// Old SA should be pruned.
	err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, &corev1.ServiceAccount{})
	if !errors.IsNotFound(err) {
		t.Errorf("expected old SA to be pruned, got err: %v", err)
	}

	// New SA should exist.
	if err := c.Get(ctx, types.NamespacedName{Name: "renamed-sa", Namespace: "default"}, &corev1.ServiceAccount{}); err != nil {
		t.Fatalf("expected renamed SA to be created: %v", err)
	}
}

func TestReconcile_ServiceAccount_RefusesAdoptionOfExistingUnownedSA(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.ServiceAccount = &supersetv1alpha1.ServiceAccountSpec{
		Name: "pre-existing-sa",
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	// Pre-create a SA that is NOT owned by the Superset CR.
	unownedSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "pre-existing-sa", Namespace: "default"},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, unownedSA).
		WithStatusSubresource(&supersetv1alpha1.Superset{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err == nil {
		t.Fatal("expected error when adopting unowned ServiceAccount, got nil")
	}
	if !strings.Contains(err.Error(), "already exists and is not owned by") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestReconcile_InitTerminalFailure_NoRequeue(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-test-1"},
		Spec:       spec,
	}

	// First reconcile to create the migrate CR and learn the expected checksum.
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset).
		WithStatusSubresource(&supersetv1alpha1.Superset{}, &supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()
	migrateCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, migrateCR); err != nil {
		t.Fatalf("get migrate CR: %v", err)
	}

	// Simulate terminal failure with matching checksum (same config that was requested).
	migrateCR.Status.State = "Failed"
	migrateCR.Status.Attempts = defaultMaxRetries
	migrateCR.Status.Message = "init command failed"
	migrateCR.Status.ConfigChecksum = migrateCR.Spec.ConfigChecksum
	if err := c.Status().Update(ctx, migrateCR); err != nil {
		t.Fatalf("update migrate status: %v", err)
	}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue for terminal init failure, got RequeueAfter=%v", result.RequeueAfter)
	}
}

func TestReconcile_DowngradeBlocked(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Image.Tag = "4.0.0"
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
		Status: supersetv1alpha1.SupersetStatus{
			LastLifecycleImage: "apache/superset:4.1.0",
		},
	}

	c := reconcileOnce(t, scheme, superset).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue for blocked downgrade, got RequeueAfter=%v", result.RequeueAfter)
	}

	updated := &supersetv1alpha1.Superset{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	if updated.Status.Phase != "Blocked" {
		t.Errorf("expected phase Blocked, got %q", updated.Status.Phase)
	}

	tasks := &supersetv1alpha1.SupersetLifecycleTaskList{}
	if err := c.List(context.Background(), tasks); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks.Items) != 0 {
		t.Errorf("expected no task CRs for blocked downgrade, got %d", len(tasks.Items))
	}
}

func TestReconcile_SupervisedMode_AwaitsApproval(t *testing.T) {
	scheme := testScheme(t)

	supervised := "Supervised"
	spec := minimalSupersetSpec()
	spec.Image.Tag = "4.1.0"
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		UpgradeMode: &supervised,
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
		Status: supersetv1alpha1.SupersetStatus{
			LastLifecycleImage: "apache/superset:4.0.0",
		},
	}

	c := reconcileOnce(t, scheme, superset).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()
	updated := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	if updated.Status.Phase != "AwaitingApproval" {
		t.Errorf("expected phase AwaitingApproval, got %q", updated.Status.Phase)
	}

	tasks := &supersetv1alpha1.SupersetLifecycleTaskList{}
	if err := c.List(ctx, tasks); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks.Items) != 0 {
		t.Errorf("expected no task CRs before approval, got %d", len(tasks.Items))
	}

	// Approve and reconcile again.
	updated.Annotations = map[string]string{"superset.apache.org/approve-upgrade": "true"}
	if err := c.Update(ctx, updated); err != nil {
		t.Fatalf("update superset with approval: %v", err)
	}
	doReconcile(t, r, "test")

	migrateCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, migrateCR); err != nil {
		t.Fatalf("expected migrate CR after approval: %v", err)
	}
}

func TestReconcile_ImageUnchanged_SkipsLifecycleTasks(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
		Status: supersetv1alpha1.SupersetStatus{
			LastLifecycleImage: "apache/superset:latest",
		},
	}

	c := reconcileOnce(t, scheme, superset).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	tasks := &supersetv1alpha1.SupersetLifecycleTaskList{}
	if err := c.List(context.Background(), tasks); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks.Items) != 0 {
		t.Errorf("expected no task CRs when image unchanged, got %d", len(tasks.Items))
	}

	ww := &supersetv1alpha1.SupersetWebServer{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, ww); err != nil {
		t.Fatalf("expected web server when lifecycle already complete: %v", err)
	}
}

func TestReconcile_StrategyNever_SkipsMigrateTask(t *testing.T) {
	scheme := testScheme(t)

	never := "Never"
	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{Strategy: &never},
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()

	migrateCR := &supersetv1alpha1.SupersetLifecycleTask{}
	err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, migrateCR)
	if !errors.IsNotFound(err) {
		t.Fatalf("expected migrate CR to not exist (strategy=Never), got err: %v", err)
	}

	initCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-init", Namespace: "default"}, initCR); err != nil {
		t.Fatalf("expected init CR to exist: %v", err)
	}
}

func TestReconcile_StrategyAlways_RunsOnConfigChange(t *testing.T) {
	scheme := testScheme(t)

	always := "Always"
	never := "Never"
	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{Strategy: &always},
		Init:    &supersetv1alpha1.InitTaskSpec{Strategy: &never},
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
		Status: supersetv1alpha1.SupersetStatus{
			LastLifecycleImage: "apache/superset:latest",
		},
	}

	c := reconcileOnce(t, scheme, superset).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()

	migrateCR := &supersetv1alpha1.SupersetLifecycleTask{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-migrate", Namespace: "default"}, migrateCR); err != nil {
		t.Fatalf("expected migrate CR with strategy=Always even when image unchanged: %v", err)
	}
}

func TestReconcile_DrainStrategy_DeletesChildCRs(t *testing.T) {
	scheme := testScheme(t)

	drain := "Drain"
	spec := minimalSupersetSpec()
	spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		UpgradeStrategy: &drain,
	}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       spec,
	}

	// Pre-create a WebServer child CR (simulating existing deployment).
	webServer := &supersetv1alpha1.SupersetWebServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
			Labels:    map[string]string{common.LabelKeyParent: "test"},
		},
	}

	c := reconcileOnce(t, scheme, superset).
		WithObjects(webServer).
		WithStatusSubresource(&supersetv1alpha1.SupersetLifecycleTask{}, &supersetv1alpha1.SupersetWebServer{}).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r, "test")

	ctx := context.Background()

	// WebServer child CR should be deleted (drain deletes children before migrate).
	ws := &supersetv1alpha1.SupersetWebServer{}
	err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, ws)
	if !errors.IsNotFound(err) {
		t.Fatalf("expected WebServer child CR to be deleted during drain, got err: %v", err)
	}

	// Status should show lifecycle is in progress (drain completed immediately
	// in fake client since no Deployments exist, so it moved to migrate phase).
	updated := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get superset: %v", err)
	}
	if updated.Status.Phase != "Upgrading" && updated.Status.Phase != "Draining" {
		t.Errorf("expected phase Upgrading or Draining, got %q", updated.Status.Phase)
	}
}
