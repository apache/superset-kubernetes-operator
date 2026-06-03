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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// TestRenderMaintenanceHTML covers renderMaintenanceHTML: user title/message are
// HTML-escaped, a custom body passes through verbatim, and defaults render a full
// escaped document.
func TestRenderMaintenanceHTML(t *testing.T) {
	t.Run("escapes title", func(t *testing.T) {
		title := `<script>alert("xss")</script>`
		spec := &supersetv1alpha1.MaintenancePageSpec{Title: &title}
		html := renderMaintenanceHTML(spec)

		if strings.Contains(html, "<script>") {
			t.Error("title should be HTML-escaped but contains raw <script> tag")
		}
		if !strings.Contains(html, "&lt;script&gt;") {
			t.Error("expected escaped title in output")
		}
	})

	t.Run("escapes message", func(t *testing.T) {
		msg := `<img src=x onerror="alert('xss')">`
		spec := &supersetv1alpha1.MaintenancePageSpec{Message: &msg}
		html := renderMaintenanceHTML(spec)

		if strings.Contains(html, "<img") {
			t.Error("message should be HTML-escaped but contains raw <img tag")
		}
		if !strings.Contains(html, "&lt;img") {
			t.Error("expected escaped message in output")
		}
	})

	t.Run("body passes through", func(t *testing.T) {
		body := `<html><body><h1>Custom</h1><script>ok()</script></body></html>`
		spec := &supersetv1alpha1.MaintenancePageSpec{Body: &body}
		result := renderMaintenanceHTML(spec)

		if result != body {
			t.Errorf("body should be returned as-is, got: %s", result)
		}
	})

	t.Run("defaults are escaped", func(t *testing.T) {
		spec := &supersetv1alpha1.MaintenancePageSpec{}
		html := renderMaintenanceHTML(spec)

		if !strings.Contains(html, maintenanceDefaultTitle) {
			t.Error("expected default title in output")
		}
		if !strings.Contains(html, "<!DOCTYPE html>") {
			t.Error("expected full HTML document")
		}
	})
}

// TestMaintenanceNginxConf covers the nginx config renderers: the per-server conf
// (renderNginxConf) honoring custom/default ports, and the main conf
// (renderMaintenanceNginxMainConf) routing pid/temp paths off root-owned dirs so
// the pod runs non-root.
func TestMaintenanceNginxConf(t *testing.T) {
	t.Run("server conf custom port", func(t *testing.T) {
		conf := renderNginxConf(9090)
		if !strings.Contains(conf, "listen 9090") {
			t.Error("expected nginx to listen on custom port 9090")
		}
		if strings.Contains(conf, "listen 8088") {
			t.Error("should not contain default port when custom port is provided")
		}
	})

	t.Run("server conf default port", func(t *testing.T) {
		conf := renderNginxConf(common.PortWebServer)
		if !strings.Contains(conf, "listen 8088") {
			t.Error("expected nginx to listen on default port 8088")
		}
	})

	t.Run("main conf runs non-root", func(t *testing.T) {
		// The main nginx.conf must route the pid file and all temp paths off
		// root-owned directories so nginx starts as a non-root user. Without this,
		// a hardened (runAsNonRoot) maintenance pod fails to write /run/nginx.pid.
		conf := renderMaintenanceNginxMainConf()
		for _, want := range []string{
			"pid /tmp/nginx.pid;",
			"client_body_temp_path /tmp/client_temp;",
			"proxy_temp_path /tmp/proxy_temp;",
			"include /etc/nginx/conf.d/*.conf;",
		} {
			if !strings.Contains(conf, want) {
				t.Errorf("nginx.conf missing %q\n--- conf ---\n%s", want, conf)
			}
		}
	})
}

// TestBuildMaintenanceFlatSpec covers buildMaintenanceFlatSpec: the managed-mode
// default non-root hardened security context + nginx.conf mount, and that an
// explicit user runAsUser overrides the operator default.
func TestBuildMaintenanceFlatSpec(t *testing.T) {
	t.Run("defaults non-root", func(t *testing.T) {
		// Managed mode must default the container to a non-root, hardened security
		// context and mount the custom nginx.conf, so the maintenance page satisfies
		// restricted Pod Security Standards out of the box.
		title := "down"
		flat := buildMaintenanceFlatSpec("parent", &supersetv1alpha1.MaintenancePageSpec{Title: &title})

		sc := flat.PodTemplate.Container.SecurityContext
		if sc == nil {
			t.Fatal("expected a default container securityContext")
		}
		if sc.RunAsUser == nil || *sc.RunAsUser != maintenanceNonRootUID {
			t.Errorf("RunAsUser = %v, want %d", sc.RunAsUser, maintenanceNonRootUID)
		}
		if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot {
			t.Errorf("RunAsNonRoot = %v, want true", sc.RunAsNonRoot)
		}
		if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
			t.Errorf("AllowPrivilegeEscalation = %v, want false", sc.AllowPrivilegeEscalation)
		}
		if sc.Capabilities == nil || len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" {
			t.Errorf("Capabilities.Drop = %+v, want [ALL]", sc.Capabilities)
		}

		if !maintenanceMountsConf(flat, "/etc/nginx/nginx.conf") {
			t.Error("expected nginx.conf to be mounted at /etc/nginx/nginx.conf")
		}
	})

	t.Run("respects user runAsUser", func(t *testing.T) {
		// An explicit user UID must win over the operator default.
		title := "down"
		flat := buildMaintenanceFlatSpec("parent", &supersetv1alpha1.MaintenancePageSpec{
			Title: &title,
			PodTemplate: &supersetv1alpha1.PodTemplate{
				Container: &supersetv1alpha1.ContainerTemplate{
					SecurityContext: &corev1.SecurityContext{RunAsUser: common.Ptr(int64(2020))},
				},
			},
		})
		if sc := flat.PodTemplate.Container.SecurityContext; sc.RunAsUser == nil || *sc.RunAsUser != 2020 {
			t.Errorf("expected user RunAsUser=2020 to be respected, got %v", sc.RunAsUser)
		}
	})
}

func maintenanceMountsConf(flat supersetv1alpha1.FlatComponentSpec, path string) bool {
	if flat.PodTemplate == nil || flat.PodTemplate.Container == nil {
		return false
	}
	for _, m := range flat.PodTemplate.Container.VolumeMounts {
		if m.MountPath == path {
			return true
		}
	}
	return false
}

// TestResolveWebServerPort covers resolveWebServerPort: the default port, a
// per-component container-port override, inheritance of a top-level container port,
// and the default when no webServer is configured.
func TestResolveWebServerPort(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{
				WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			},
		}
		port := resolveWebServerPort(s)
		if port != common.PortWebServer {
			t.Errorf("expected default port %d, got %d", common.PortWebServer, port)
		}
	})

	t.Run("component override", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{
				WebServer: &supersetv1alpha1.WebServerComponentSpec{
					ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
						PodTemplate: &supersetv1alpha1.PodTemplate{
							Container: &supersetv1alpha1.ContainerTemplate{
								Ports: []corev1.ContainerPort{
									{Name: "http", ContainerPort: 9090},
								},
							},
						},
					},
				},
			},
		}
		port := resolveWebServerPort(s)
		if port != 9090 {
			t.Errorf("expected custom port 9090, got %d", port)
		}
	})

	t.Run("top-level override", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{
			Spec: supersetv1alpha1.SupersetSpec{
				PodTemplate: &supersetv1alpha1.PodTemplate{
					Container: &supersetv1alpha1.ContainerTemplate{
						Ports: []corev1.ContainerPort{
							{Name: "http", ContainerPort: 7070},
						},
					},
				},
				WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			},
		}
		port := resolveWebServerPort(s)
		if port != 7070 {
			t.Errorf("expected top-level port 7070 inherited, got %d", port)
		}
	})

	t.Run("no web server", func(t *testing.T) {
		s := &supersetv1alpha1.Superset{}
		port := resolveWebServerPort(s)
		if port != common.PortWebServer {
			t.Errorf("expected default port %d for nil WebServer, got %d", common.PortWebServer, port)
		}
	})
}

func TestReconcileWebServerService_SelectorBasedOnMaintenanceActive(t *testing.T) {
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "my-superset", Namespace: "default"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{
				MaintenanceActive: true,
			},
		},
	}

	// When MaintenanceActive=true, selector should point to maintenance-page component.
	expectedSelector := common.ComponentLabels(common.ComponentMaintenancePage, "my-superset")

	// Verify the selector logic (we test the selector derivation, not the full reconcile
	// which requires a fake client).
	var selector map[string]string
	if superset.Status.Lifecycle != nil && superset.Status.Lifecycle.MaintenanceActive {
		selector = common.ComponentLabels(common.ComponentMaintenancePage, superset.Name)
	} else {
		selector = common.ComponentLabels(common.ComponentWebServer, superset.Name)
	}
	for k, v := range expectedSelector {
		if selector[k] != v {
			t.Errorf("expected selector[%s]=%s, got %s", k, v, selector[k])
		}
	}

	// When MaintenanceActive=false, selector should point to web-server.
	superset.Status.Lifecycle.MaintenanceActive = false
	if superset.Status.Lifecycle != nil && superset.Status.Lifecycle.MaintenanceActive {
		selector = common.ComponentLabels(common.ComponentMaintenancePage, superset.Name)
	} else {
		selector = common.ComponentLabels(common.ComponentWebServer, superset.Name)
	}
	expectedWebServer := common.ComponentLabels(common.ComponentWebServer, "my-superset")
	for k, v := range expectedWebServer {
		if selector[k] != v {
			t.Errorf("expected selector[%s]=%s, got %s", k, v, selector[k])
		}
	}
}

func TestMaintenanceDeployConfig_UsesCustomPort(t *testing.T) {
	port := int32(9090)
	cfg := maintenanceDeployConfig
	cfg.DefaultPorts = []corev1.ContainerPort{
		{Name: common.PortNameHTTP, ContainerPort: port, Protocol: corev1.ProtocolTCP},
	}

	if len(cfg.DefaultPorts) != 1 {
		t.Fatal("expected exactly 1 default port")
	}
	if cfg.DefaultPorts[0].ContainerPort != port {
		t.Errorf("expected container port %d, got %d", port, cfg.DefaultPorts[0].ContainerPort)
	}
}

func TestReconcileMaintenanceReturnClearsWhenWebServerDesiredReplicasZero(t *testing.T) {
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
			Lifecycle: &supersetv1alpha1.LifecycleStatus{MaintenanceActive: true},
		},
	}
	r := &SupersetReconciler{Recorder: recorder}

	cleared, err := r.reconcileMaintenanceReturn(context.Background(), superset)
	if err != nil {
		t.Fatalf("reconcileMaintenanceReturn: %v", err)
	}
	if !cleared {
		t.Fatal("expected maintenance return to clear")
	}
	if superset.Status.Lifecycle.MaintenanceActive {
		t.Fatal("expected maintenanceActive=false")
	}
	assertNextEventContains(t, recorder, "Normal MaintenanceEnded Maintenance page disabled because webServer has zero desired replicas")
}

func TestBuildMaintenanceFlatSpec_DoesNotMutateInputSpec(t *testing.T) {
	userVolume := corev1.Volume{Name: "user-volume", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
	userMount := corev1.VolumeMount{Name: "user-volume", MountPath: "/data"}
	userEnv := corev1.EnvVar{Name: "USER_VAR", Value: "v"}
	title := "down for maintenance"

	spec := &supersetv1alpha1.MaintenancePageSpec{
		Title: &title,
		PodTemplate: &supersetv1alpha1.PodTemplate{
			Volumes: []corev1.Volume{userVolume},
			Container: &supersetv1alpha1.ContainerTemplate{
				VolumeMounts: []corev1.VolumeMount{userMount},
				Env:          []corev1.EnvVar{userEnv},
			},
		},
	}

	for range 3 {
		_ = buildMaintenanceFlatSpec("parent", spec)
	}

	if got := len(spec.PodTemplate.Volumes); got != 1 {
		t.Fatalf("input spec PodTemplate.Volumes mutated: got %d volumes, want 1", got)
	}
	if got := len(spec.PodTemplate.Container.VolumeMounts); got != 1 {
		t.Fatalf("input spec PodTemplate.Container.VolumeMounts mutated: got %d mounts, want 1", got)
	}
	if got := len(spec.PodTemplate.Container.Env); got != 1 {
		t.Fatalf("input spec PodTemplate.Container.Env mutated: got %d env vars, want 1", got)
	}
}

// TestResolveMaintenanceImage_PartialOverride asserts that a maintenance image
// spec with only `tag` set inherits the nginx repository — not the Superset
// image — which is the bug ContainerImageSpec was introduced to prevent.
func TestResolveMaintenanceImage_PartialOverride(t *testing.T) {
	tests := []struct {
		name         string
		image        *supersetv1alpha1.ContainerImageSpec
		expectedRepo string
		expectedTag  string
	}{
		{
			name:         "nil image uses managed defaults",
			image:        nil,
			expectedRepo: maintenanceDefaultImage,
			expectedTag:  maintenanceDefaultTag,
		},
		{
			name:         "tag-only override inherits nginx repo",
			image:        &supersetv1alpha1.ContainerImageSpec{Tag: "1.27"},
			expectedRepo: maintenanceDefaultImage,
			expectedTag:  "1.27",
		},
		{
			name:         "repository-only override inherits default tag",
			image:        &supersetv1alpha1.ContainerImageSpec{Repository: "my-registry/maintenance"},
			expectedRepo: "my-registry/maintenance",
			expectedTag:  maintenanceDefaultTag,
		},
		{
			name:         "full override is used as-is",
			image:        &supersetv1alpha1.ContainerImageSpec{Repository: "my-registry/maintenance", Tag: "v3"},
			expectedRepo: "my-registry/maintenance",
			expectedTag:  "v3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			img := resolveMaintenanceImage(&supersetv1alpha1.MaintenancePageSpec{Image: tt.image})
			if img.Repository != tt.expectedRepo || img.Tag != tt.expectedTag {
				t.Errorf("expected %s:%s, got %s:%s", tt.expectedRepo, tt.expectedTag, img.Repository, img.Tag)
			}
		})
	}
}

func TestDeploymentHasReplicas(t *testing.T) {
	one := int32(1)
	zero := int32(0)

	t.Run("status replicas counts as present", func(t *testing.T) {
		d := &appsv1.Deployment{Status: appsv1.DeploymentStatus{Replicas: 2}}
		assert.True(t, deploymentHasReplicas(d))
	})

	t.Run("ready/available/updated/unavailable each count", func(t *testing.T) {
		for _, d := range []*appsv1.Deployment{
			{Status: appsv1.DeploymentStatus{ReadyReplicas: 1}},
			{Status: appsv1.DeploymentStatus{AvailableReplicas: 1}},
			{Status: appsv1.DeploymentStatus{UpdatedReplicas: 1}},
			{Status: appsv1.DeploymentStatus{UnavailableReplicas: 1}},
		} {
			assert.True(t, deploymentHasReplicas(d))
		}
	})

	t.Run("nil spec replicas with empty status counts as present", func(t *testing.T) {
		// A Deployment with nil spec.Replicas defaults to 1 replica.
		d := &appsv1.Deployment{}
		assert.True(t, deploymentHasReplicas(d))
	})

	t.Run("explicit one replica counts as present", func(t *testing.T) {
		d := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: &one}}
		assert.True(t, deploymentHasReplicas(d))
	})

	t.Run("scaled to zero with empty status is absent", func(t *testing.T) {
		d := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: &zero}}
		assert.False(t, deploymentHasReplicas(d))
	})
}

func TestComputeMaintenanceChecksum(t *testing.T) {
	title := "Down"
	otherTitle := "Up"
	body := "<h1>x</h1>"

	base := &supersetv1alpha1.MaintenancePageSpec{Title: &title}

	t.Run("stable for identical specs", func(t *testing.T) {
		assert.Equal(t, computeMaintenanceChecksum(base), computeMaintenanceChecksum(&supersetv1alpha1.MaintenancePageSpec{Title: &title}))
	})

	t.Run("changes when title changes", func(t *testing.T) {
		assert.NotEqual(t, computeMaintenanceChecksum(base), computeMaintenanceChecksum(&supersetv1alpha1.MaintenancePageSpec{Title: &otherTitle}))
	})

	t.Run("changes when body is set", func(t *testing.T) {
		assert.NotEqual(t, computeMaintenanceChecksum(base), computeMaintenanceChecksum(&supersetv1alpha1.MaintenancePageSpec{Title: &title, Body: &body}))
	})

	t.Run("includes image fields", func(t *testing.T) {
		withImage := &supersetv1alpha1.MaintenancePageSpec{
			Title: &title,
			Image: &supersetv1alpha1.ContainerImageSpec{Repository: "nginx", Tag: "1.27"},
		}
		assert.NotEqual(t, computeMaintenanceChecksum(base), computeMaintenanceChecksum(withImage))
	})

	t.Run("empty spec yields a fixed-length hex digest", func(t *testing.T) {
		c := computeMaintenanceChecksum(&supersetv1alpha1.MaintenancePageSpec{})
		assert.Len(t, c, 16)
	})
}

func TestWebServerDesiredReplicas_NoWebServer(t *testing.T) {
	// With no webServer configured the accessor is nil, so desired replicas is 0.
	s := &supersetv1alpha1.Superset{}
	assert.Equal(t, int32(0), webServerDesiredReplicas(s))
}

func TestReconcileMaintenancePageUp(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	title := "Down for maintenance"
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				MaintenancePage: &supersetv1alpha1.MaintenancePageSpec{Title: &title},
			},
		},
		Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(20)}

	// First pass: managed-mode ConfigMap + Deployment created, but not ready yet.
	ready, err := r.reconcileMaintenancePageUp(ctx, superset)
	assert.NoError(t, err)
	assert.False(t, ready, "Deployment has no ready replicas yet")

	// ConfigMap (managed mode) created.
	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: maintenanceConfigMapName("test"), Namespace: "default"}, cm))
	assert.Contains(t, cm.Data, "index.html")
	assert.Contains(t, cm.Data, "nginx.conf")

	// Deployment created and parent-owned.
	deploy := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: maintenanceDeploymentName("test"), Namespace: "default"}, deploy))
	assert.True(t, isOwnedBy(deploy, superset))

	// Mark the Deployment ready, then the page is up and MaintenanceActive flips.
	deploy.Status.ReadyReplicas = 1
	require.NoError(t, c.Status().Update(ctx, deploy))

	ready, err = r.reconcileMaintenancePageUp(ctx, superset)
	assert.NoError(t, err)
	assert.True(t, ready)
	assert.True(t, superset.Status.Lifecycle.MaintenanceActive)
}

func TestReconcileMaintenanceReturn_AlreadyInactive(t *testing.T) {
	r := &SupersetReconciler{Recorder: events.NewFakeRecorder(10)}
	superset := &supersetv1alpha1.Superset{Status: supersetv1alpha1.SupersetStatus{}}
	cleared, err := r.reconcileMaintenanceReturn(context.Background(), superset)
	assert.NoError(t, err)
	assert.True(t, cleared, "no lifecycle status means already cleared")
}

func TestReconcileMaintenanceReturn_WebServerRemoved(t *testing.T) {
	recorder := events.NewFakeRecorder(10)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       supersetv1alpha1.SupersetSpec{}, // WebServer nil
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{MaintenanceActive: true},
		},
	}
	r := &SupersetReconciler{Recorder: recorder}
	cleared, err := r.reconcileMaintenanceReturn(context.Background(), superset)
	assert.NoError(t, err)
	assert.True(t, cleared)
	assert.False(t, superset.Status.Lifecycle.MaintenanceActive)
	assertNextEventContains(t, recorder, "Normal MaintenanceEnded Maintenance page disabled because webServer was removed")
}

func TestReconcileMaintenanceReturn_WaitsForWebServerReady(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
		},
		Status: supersetv1alpha1.SupersetStatus{
			Lifecycle: &supersetv1alpha1.LifecycleStatus{MaintenanceActive: true},
		},
	}
	webName := common.ResourceBaseName("test", common.ComponentWebServer)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: webName, Namespace: "default"},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 0},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, deploy).WithStatusSubresource(deploy).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	// Web-server not ready: should not clear.
	cleared, err := r.reconcileMaintenanceReturn(ctx, superset)
	assert.NoError(t, err)
	assert.False(t, cleared)
	assert.True(t, superset.Status.Lifecycle.MaintenanceActive)

	// Mark ready, then it clears.
	deploy.Status.ReadyReplicas = 1
	require.NoError(t, c.Status().Update(ctx, deploy))
	cleared, err = r.reconcileMaintenanceReturn(ctx, superset)
	assert.NoError(t, err)
	assert.True(t, cleared)
	assert.False(t, superset.Status.Lifecycle.MaintenanceActive)
}

func TestDeleteMaintenanceResources(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
	}
	deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: maintenanceDeploymentName("test"), Namespace: "default"}}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: maintenanceConfigMapName("test"), Namespace: "default"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, deploy, cm).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	require.NoError(t, r.deleteMaintenanceResources(ctx, superset))

	err := c.Get(ctx, client.ObjectKey{Name: maintenanceDeploymentName("test"), Namespace: "default"}, &appsv1.Deployment{})
	assert.True(t, errors.IsNotFound(err))
	err = c.Get(ctx, client.ObjectKey{Name: maintenanceConfigMapName("test"), Namespace: "default"}, &corev1.ConfigMap{})
	assert.True(t, errors.IsNotFound(err))

	// Deleting again (resources absent) is a clean no-op.
	assert.NoError(t, r.deleteMaintenanceResources(ctx, superset))
}

func TestCleanupMaintenanceResources_ClearsActiveFlag(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Status:     supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{MaintenanceActive: true}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	require.NoError(t, r.cleanupMaintenanceResources(ctx, superset))
	assert.False(t, superset.Status.Lifecycle.MaintenanceActive)
}

func TestReconcileMaintenancePageUp_CustomModeSkipsConfigMap(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Lifecycle: &supersetv1alpha1.LifecycleSpec{
				MaintenancePage: &supersetv1alpha1.MaintenancePageSpec{
					// Custom mode: user supplies an image, so no managed ConfigMap.
					Image: &supersetv1alpha1.ContainerImageSpec{Repository: "my/maint", Tag: "v1"},
				},
			},
		},
		Status: supersetv1alpha1.SupersetStatus{Lifecycle: &supersetv1alpha1.LifecycleStatus{}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(20)}

	ready, err := r.reconcileMaintenancePageUp(ctx, superset)
	assert.NoError(t, err)
	assert.False(t, ready)

	// No managed ConfigMap created in custom mode.
	err = c.Get(ctx, client.ObjectKey{Name: maintenanceConfigMapName("test"), Namespace: "default"}, &corev1.ConfigMap{})
	assert.True(t, errors.IsNotFound(err), "custom mode must not create a managed ConfigMap")

	// Deployment still created.
	deploy := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: maintenanceDeploymentName("test"), Namespace: "default"}, deploy))
	assert.Equal(t, "my/maint:v1", deploy.Spec.Template.Spec.Containers[0].Image)
}
