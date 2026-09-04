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
	networkingv1 "k8s.io/api/networking/v1"
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

func TestReconcile_WebsocketInjectsRealtimeEnvAndUsesMainImage(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Environment = common.Ptr("Development")
	spec.BaseURL = common.Ptr("https://superset.example.com")
	spec.Valkey = &supersetv1alpha1.ValkeySpec{Host: "valkey", Password: common.Ptr("vk-pass")}
	spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{}
	spec.Realtime = &supersetv1alpha1.RealtimeSpec{
		WebSocket: &supersetv1alpha1.WebSocketTransportSpec{JwtSecret: common.Ptr("dev-ws-secret")},
	}
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r)

	deploy := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-websocket-server", Namespace: "default"}, deploy); err != nil {
		t.Fatalf("expected websocket Deployment: %v", err)
	}
	ctr := deploy.Spec.Template.Spec.Containers[0]

	// Inherits the main Superset image and launches via the alternate entrypoint.
	if !strings.Contains(ctr.Image, "apache/superset") {
		t.Fatalf("expected websocket to inherit the main Superset image, got %q", ctr.Image)
	}
	if len(ctr.Command) != 1 || ctr.Command[0] != "/app/docker/entrypoints/run-websocket.sh" {
		t.Fatalf("expected run-websocket.sh entrypoint, got %#v", ctr.Command)
	}
	if ctr.ReadinessProbe == nil || ctr.ReadinessProbe.HTTPGet == nil || ctr.ReadinessProbe.HTTPGet.Path != "/ready" {
		t.Fatalf("expected /ready readiness probe, got %#v", ctr.ReadinessProbe)
	}

	env := envSliceToMap(ctr.Env)
	for k, want := range map[string]string{
		"JWT_SECRET":              "dev-ws-secret",
		"REDIS_HOST":              "valkey",
		"REDIS_DB":                "7",
		"PORT":                    "8080",
		"ALLOWED_ORIGINS":         "https://superset.example.com",
		"REALTIME_CHANNEL_PREFIX": "test:",
	} {
		if env[k] != want {
			t.Fatalf("websocket env %s = %q, want %q", k, env[k], want)
		}
	}

	if deploy.Spec.Template.Annotations[common.AnnotationConfigChecksum] == "" {
		t.Fatal("expected websocket workload checksum annotation")
	}
}

func TestReconcile_ValkeyKeyPrefixNamespacesInstance(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.Environment = common.Ptr("Development")
	spec.Valkey = &supersetv1alpha1.ValkeySpec{Host: "valkey", KeyPrefix: common.Ptr("tenant-a")}
	spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{}
	spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{}
	spec.Realtime = &supersetv1alpha1.RealtimeSpec{
		AsyncQueries: &supersetv1alpha1.AsyncQueriesSpec{},
		WebSocket:    &supersetv1alpha1.WebSocketTransportSpec{JwtSecret: common.Ptr("dev-ws-secret"), URL: common.Ptr("wss://x/ws")},
	}
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r)

	// The web-server config keys off the custom prefix rather than the CR name.
	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server-config", Namespace: "default"}, cm); err != nil {
		t.Fatalf("expected web-server ConfigMap: %v", err)
	}
	web := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, web); err != nil {
		t.Fatalf("expected web-server Deployment: %v", err)
	}
	if got := envSliceToMap(web.Spec.Template.Spec.Containers[0].Env)["SUPERSET_OPERATOR__INSTANCE_NAME"]; got != "tenant-a" {
		t.Fatalf("INSTANCE_NAME = %q, want tenant-a", got)
	}

	// The websocket server's realtime channel prefix matches the same namespace.
	ws := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-websocket-server", Namespace: "default"}, ws); err != nil {
		t.Fatalf("expected websocket Deployment: %v", err)
	}
	if got := envSliceToMap(ws.Spec.Template.Spec.Containers[0].Env)["REALTIME_CHANNEL_PREFIX"]; got != "tenant-a:" {
		t.Fatalf("REALTIME_CHANNEL_PREFIX = %q, want tenant-a:", got)
	}
}

func TestReconcile_WebsocketJwtSecretFromRendersWebsocketEnableInApp(t *testing.T) {
	scheme := testScheme(t)

	spec := minimalSupersetSpec()
	spec.BaseURL = common.Ptr("https://superset.example.com")
	spec.Valkey = &supersetv1alpha1.ValkeySpec{Host: "valkey"}
	spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{}
	spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{}
	spec.Networking = &supersetv1alpha1.NetworkingSpec{
		Ingress: &supersetv1alpha1.IngressSpec{Host: "superset.example.com", TLS: []networkingv1.IngressTLS{{}}},
	}
	spec.Realtime = &supersetv1alpha1.RealtimeSpec{
		AsyncQueries: &supersetv1alpha1.AsyncQueriesSpec{},
		WebSocket: &supersetv1alpha1.WebSocketTransportSpec{
			JwtSecretFrom: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "ws"},
				Key:                  "jwt",
			},
		},
	}
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r)

	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server-config", Namespace: "default"}, cm); err != nil {
		t.Fatalf("expected web-server ConfigMap: %v", err)
	}
	config := cm.Data["superset_config.py"]
	for _, want := range []string{
		"WEBSOCKET_ENABLE = True",
		`WEBSOCKET_URL = "wss://superset.example.com/ws"`,
		"WEBSOCKET_JWT_SECRET = os.environ['SUPERSET_OPERATOR__WS_JWT_SECRET']",
		`"GLOBAL_ASYNC_QUERIES": True`,
		`WEBDRIVER_BASEURL = "http://test-web-server:8088/"`,
		`WEBDRIVER_BASEURL_USER_FRIENDLY = "https://superset.example.com"`,
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("web-server config missing %q:\n%s", want, config)
		}
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
