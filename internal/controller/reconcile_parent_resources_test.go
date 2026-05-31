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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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
	doReconcile(t, r)

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
	doReconcile(t, r)

	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-celery-worker", Namespace: "default"}, &appsv1.Deployment{}); err == nil {
		t.Fatal("expected disabled celery worker Deployment to be deleted")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-celery-worker-config", Namespace: "default"}, &corev1.ConfigMap{}); err == nil {
		t.Fatal("expected disabled celery worker ConfigMap to be deleted")
	}
}

func TestReconcile_WebsocketInlineConfigCreatesConfigMapAndMount(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Environment = common.Ptr("Development")
	spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{
		ComponentSpec: supersetv1alpha1.ComponentSpec{
			Image: &supersetv1alpha1.ImageOverrideSpec{Repository: common.Ptr("example.com/superset-websocket")},
		},
		Config: &apiextensionsv1.JSON{Raw: []byte(`{"port":8080,"jwtSecret":"dev-secret"}`)},
	}
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r)

	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-websocket-server-config", Namespace: "default"}, cm); err != nil {
		t.Fatalf("expected websocket ConfigMap: %v", err)
	}
	if cm.Data[websocketConfigKey] == "" || !containsAll(cm.Data[websocketConfigKey], []string{`"port": 8080`, `"jwtSecret": "dev-secret"`}) {
		t.Fatalf("unexpected websocket config data: %q", cm.Data[websocketConfigKey])
	}

	deploy := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-websocket-server", Namespace: "default"}, deploy); err != nil {
		t.Fatalf("expected websocket Deployment: %v", err)
	}
	if deploy.Spec.Template.Annotations[common.AnnotationConfigChecksum] == "" {
		t.Fatal("expected websocket config checksum annotation")
	}
	vol := findVolume(deploy.Spec.Template.Spec.Volumes, websocketConfigVolume)
	if vol == nil || vol.ConfigMap == nil || vol.ConfigMap.Name != "test-websocket-server-config" {
		t.Fatalf("expected websocket ConfigMap volume, got %#v", vol)
	}
	mount := findVolumeMount(deploy.Spec.Template.Spec.Containers[0].VolumeMounts, websocketConfigVolume)
	if mount == nil || mount.MountPath != websocketConfigMountPath || mount.SubPath != websocketConfigKey {
		t.Fatalf("expected websocket config mount, got %#v", mount)
	}
}

func TestReconcile_WebsocketConfigFromMountsSecret(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{
		ComponentSpec: supersetv1alpha1.ComponentSpec{
			Image: &supersetv1alpha1.ImageOverrideSpec{Repository: common.Ptr("example.com/superset-websocket")},
		},
		ConfigFrom: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "ws-config"},
			Key:                  "config.json",
		},
	}
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r)

	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-websocket-server-config", Namespace: "default"}, &corev1.ConfigMap{}); err == nil {
		t.Fatal("did not expect an operator-owned websocket ConfigMap for configFrom")
	}

	deploy := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-websocket-server", Namespace: "default"}, deploy); err != nil {
		t.Fatalf("expected websocket Deployment: %v", err)
	}
	if deploy.Spec.Template.Annotations[common.AnnotationConfigChecksum] == "" {
		t.Fatal("expected websocket config reference checksum annotation")
	}
	vol := findVolume(deploy.Spec.Template.Spec.Volumes, websocketConfigVolume)
	if vol == nil || vol.Secret == nil || vol.Secret.SecretName != "ws-config" {
		t.Fatalf("expected websocket Secret volume, got %#v", vol)
	}
	if len(vol.Secret.Items) != 1 || vol.Secret.Items[0].Key != "config.json" || vol.Secret.Items[0].Path != websocketConfigKey {
		t.Fatalf("expected websocket Secret key mapping, got %#v", vol.Secret.Items)
	}
}

func TestReconcile_BootstrapScriptAppliesToPythonComponentsOnly(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.BootstrapScript = common.Ptr("echo top-level")
	spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{
		BootstrapScript: common.Ptr("echo worker"),
	}
	spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{
		ComponentSpec: supersetv1alpha1.ComponentSpec{
			Image: &supersetv1alpha1.ImageOverrideSpec{Repository: common.Ptr("example.com/superset-websocket")},
		},
	}
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r)

	webCM := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server-config", Namespace: "default"}, webCM); err != nil {
		t.Fatalf("expected web ConfigMap: %v", err)
	}
	if webCM.Data[bootstrapScriptKey] != "echo top-level" {
		t.Fatalf("expected top-level bootstrap on web, got %q", webCM.Data[bootstrapScriptKey])
	}

	workerCM := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-celery-worker-config", Namespace: "default"}, workerCM); err != nil {
		t.Fatalf("expected worker ConfigMap: %v", err)
	}
	if workerCM.Data[bootstrapScriptKey] != "echo worker" {
		t.Fatalf("expected worker bootstrap override, got %q", workerCM.Data[bootstrapScriptKey])
	}
	workerDeploy := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-celery-worker", Namespace: "default"}, workerDeploy); err != nil {
		t.Fatalf("expected worker Deployment: %v", err)
	}
	workerCommand := workerDeploy.Spec.Template.Spec.Containers[0].Command
	if len(workerCommand) < 3 || !strings.Contains(workerCommand[2], "superset_bootstrap.sh") {
		t.Fatalf("expected worker command to source bootstrap, got %#v", workerCommand)
	}

	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-websocket-server-config", Namespace: "default"}, &corev1.ConfigMap{}); err == nil {
		t.Fatal("did not expect top-level bootstrap ConfigMap for websocket")
	}
}

// TestReconcile_ComponentResourcesCarryLabels asserts that every parent-owned
// component resource (Deployment, ConfigMap, Service, HPA, PDB) carries the
// operator-managed labels on its ObjectMeta. The internals doc promises label
// discoverability via `kubectl … -l app.kubernetes.io/instance=<parent>`, so
// missing labels on any of these would silently break that contract.
func TestReconcile_ComponentResourcesCarryLabels(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r)

	expected := map[string]string{
		"app.kubernetes.io/name":      "superset",
		"app.kubernetes.io/component": "web-server",
		"app.kubernetes.io/instance":  "test",
	}
	assertLabels := func(t *testing.T, kind string, labels map[string]string) {
		t.Helper()
		for k, want := range expected {
			if got := labels[k]; got != want {
				t.Errorf("%s missing label %s=%s (got %q)", kind, k, want, got)
			}
		}
	}

	deploy := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, deploy); err != nil {
		t.Fatalf("get Deployment: %v", err)
	}
	assertLabels(t, "Deployment", deploy.Labels)

	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server-config", Namespace: "default"}, cm); err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	assertLabels(t, "ConfigMap", cm.Labels)

	svc := &corev1.Service{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, svc); err != nil {
		t.Fatalf("get Service: %v", err)
	}
	assertLabels(t, "Service", svc.Labels)
}

// TestReconcile_DeploymentTemplateLabelsAndAnnotations asserts that
// user-supplied deploymentTemplate labels/annotations land on the Deployment's
// ObjectMeta (merged top-level + per-component), while operator-managed labels
// still win on conflict and cannot be removed.
func TestReconcile_DeploymentTemplateLabelsAndAnnotations(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.DeploymentTemplate = &supersetv1alpha1.DeploymentTemplate{
		Labels:      map[string]string{"team": "data", "tier": "top"},
		Annotations: map[string]string{"owner": "platform"},
	}
	spec.WebServer.DeploymentTemplate = &supersetv1alpha1.DeploymentTemplate{
		// Component overrides "tier" and tries (in vain) to override the
		// operator-managed component label.
		Labels:      map[string]string{"tier": "web", "app.kubernetes.io/component": "hijack"},
		Annotations: map[string]string{"scrape": "true"},
	}
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r)

	deploy := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, deploy); err != nil {
		t.Fatalf("get Deployment: %v", err)
	}

	// User labels present; component value wins over top-level on "tier".
	if got := deploy.Labels["team"]; got != "data" {
		t.Errorf("Deployment label team = %q, want data", got)
	}
	if got := deploy.Labels["tier"]; got != "web" {
		t.Errorf("Deployment label tier = %q, want web (component overrides top-level)", got)
	}
	// Operator-managed label wins over the user's hijack attempt.
	if got := deploy.Labels["app.kubernetes.io/component"]; got != "web-server" {
		t.Errorf("Deployment label app.kubernetes.io/component = %q, want web-server (operator protected)", got)
	}
	// Annotations merge from both levels.
	if got := deploy.Annotations["owner"]; got != "platform" {
		t.Errorf("Deployment annotation owner = %q, want platform", got)
	}
	if got := deploy.Annotations["scrape"]; got != "true" {
		t.Errorf("Deployment annotation scrape = %q, want true", got)
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
	doReconcile(t, r)

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
	if jobs.Items[0].Name != "test-migrate" {
		t.Fatalf("expected deterministic migrate Job name, got %q", jobs.Items[0].Name)
	}
}

func containsAll(s string, needles []string) bool {
	for _, needle := range needles {
		if !strings.Contains(s, needle) {
			return false
		}
	}
	return true
}

func findVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

func findVolumeMount(mounts []corev1.VolumeMount, name string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == name {
			return &mounts[i]
		}
	}
	return nil
}
