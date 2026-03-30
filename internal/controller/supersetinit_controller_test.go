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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestBuildInitPod(t *testing.T) {
	uid := int64(1000)
	runAsNonRoot := true

	spec := &supersetv1alpha1.FlatComponentSpec{
		Image: supersetv1alpha1.ImageSpec{
			Repository:  "apache/superset",
			Tag:         "latest",
			PullPolicy:  corev1.PullIfNotPresent,
			PullSecrets: []corev1.LocalObjectReference{{Name: "reg-secret"}},
		},
		ServiceAccountName: "superset-sa",
		PodTemplate: &supersetv1alpha1.PodTemplate{
			PriorityClassName: common.Ptr("high-priority"),
			NodeSelector:      map[string]string{"node-type": "compute"},
			Tolerations:       []corev1.Toleration{{Key: "special", Effect: corev1.TaintEffectNoSchedule}},
			Affinity:          &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}},
			HostAliases:       []corev1.HostAlias{{IP: "10.0.0.1", Hostnames: []string{"db"}}},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
				{MaxSkew: 1, TopologyKey: "zone"},
			},
			PodSecurityContext:            &corev1.PodSecurityContext{RunAsNonRoot: &runAsNonRoot},
			Volumes:                       []corev1.Volume{{Name: "config"}},
			Sidecars:                      []corev1.Container{{Name: "sidecar", Image: "sidecar:1.0"}},
			InitContainers:                []corev1.Container{{Name: "pre-init", Image: "pre:1.0"}},
			TerminationGracePeriodSeconds: common.Ptr(int64(120)),
			RuntimeClassName:              common.Ptr("gvisor"),
			ShareProcessNamespace:         common.Ptr(true),
			EnableServiceLinks:            common.Ptr(false),
			DNSPolicy:                     common.Ptr(corev1.DNSClusterFirstWithHostNet),
			DNSConfig:                     &corev1.PodDNSConfig{Nameservers: []string{"8.8.8.8"}},
			Container: &supersetv1alpha1.ContainerTemplate{
				Command: []string{"/bin/sh", "-c", "superset db upgrade && superset init"},
				Resources: &corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				},
				Env:             []corev1.EnvVar{{Name: "SECRET_KEY", Value: "test"}},
				VolumeMounts:    []corev1.VolumeMount{{Name: "config", MountPath: "/app/superset/config"}},
				SecurityContext: &corev1.SecurityContext{RunAsUser: &uid},
			},
		},
	}

	podSpec := buildInitPod(spec)

	// Core pod properties.
	if podSpec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("expected RestartPolicyNever, got %s", podSpec.RestartPolicy)
	}
	if podSpec.ServiceAccountName != "superset-sa" {
		t.Errorf("expected SA name, got %s", podSpec.ServiceAccountName)
	}

	// Main container.
	if len(podSpec.Containers) != 2 {
		t.Fatalf("expected 2 containers (main + sidecar), got %d", len(podSpec.Containers))
	}
	container := podSpec.Containers[0]
	if container.Name != common.Container {
		t.Errorf("expected container name '%s', got %s", common.Container, container.Name)
	}
	if container.Image != "apache/superset:latest" {
		t.Errorf("expected image apache/superset:latest, got %s", container.Image)
	}
	if container.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("expected PullIfNotPresent, got %s", container.ImagePullPolicy)
	}
	if len(container.Command) != 3 || container.Command[0] != "/bin/sh" {
		t.Errorf("expected /bin/sh command prefix, got %v", container.Command)
	}

	// Resources.
	if container.Resources.Limits.Cpu().String() != "500m" {
		t.Errorf("expected CPU limit 500m, got %s", container.Resources.Limits.Cpu().String())
	}
	if container.Resources.Limits.Memory().String() != "512Mi" {
		t.Errorf("expected memory limit 512Mi, got %s", container.Resources.Limits.Memory().String())
	}

	// Env, volumes, mounts.
	if len(container.Env) != 1 || container.Env[0].Name != "SECRET_KEY" {
		t.Errorf("expected env vars, got %v", container.Env)
	}
	if len(container.VolumeMounts) != 1 || container.VolumeMounts[0].Name != "config" {
		t.Errorf("expected config volume mount, got %v", container.VolumeMounts)
	}
	if len(podSpec.Volumes) != 1 || podSpec.Volumes[0].Name != "config" {
		t.Errorf("expected config volume, got %v", podSpec.Volumes)
	}

	// Security contexts.
	if podSpec.SecurityContext == nil || !*podSpec.SecurityContext.RunAsNonRoot {
		t.Error("expected pod security context to be set")
	}
	if container.SecurityContext == nil || *container.SecurityContext.RunAsUser != 1000 {
		t.Error("expected container security context to be set")
	}

	// Sidecars and init containers.
	if podSpec.Containers[1].Name != "sidecar" {
		t.Errorf("expected sidecar container, got %s", podSpec.Containers[1].Name)
	}
	if len(podSpec.InitContainers) != 1 || podSpec.InitContainers[0].Name != "pre-init" {
		t.Errorf("expected 1 init container, got %v", podSpec.InitContainers)
	}

	// Scheduling fields.
	if podSpec.PriorityClassName != "high-priority" {
		t.Errorf("expected priority class, got %s", podSpec.PriorityClassName)
	}
	if podSpec.NodeSelector["node-type"] != "compute" {
		t.Errorf("expected node selector, got %v", podSpec.NodeSelector)
	}
	if len(podSpec.Tolerations) != 1 {
		t.Errorf("expected 1 toleration, got %d", len(podSpec.Tolerations))
	}
	if podSpec.Affinity == nil {
		t.Error("expected affinity to be set")
	}
	if len(podSpec.HostAliases) != 1 {
		t.Errorf("expected 1 host alias, got %d", len(podSpec.HostAliases))
	}
	if len(podSpec.TopologySpreadConstraints) != 1 {
		t.Errorf("expected 1 topology spread constraint, got %d", len(podSpec.TopologySpreadConstraints))
	}
	if len(podSpec.ImagePullSecrets) != 1 || podSpec.ImagePullSecrets[0].Name != "reg-secret" {
		t.Errorf("expected image pull secrets, got %v", podSpec.ImagePullSecrets)
	}

	// Pod-level fields that must match buildDeploymentSpec.
	if podSpec.TerminationGracePeriodSeconds == nil || *podSpec.TerminationGracePeriodSeconds != 120 {
		t.Error("expected terminationGracePeriodSeconds=120")
	}
	if podSpec.RuntimeClassName == nil || *podSpec.RuntimeClassName != "gvisor" {
		t.Error("expected runtimeClassName=gvisor")
	}
	if podSpec.ShareProcessNamespace == nil || !*podSpec.ShareProcessNamespace {
		t.Error("expected shareProcessNamespace=true")
	}
	if podSpec.EnableServiceLinks == nil || *podSpec.EnableServiceLinks {
		t.Error("expected enableServiceLinks=false")
	}
	if podSpec.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
		t.Errorf("expected dnsPolicy ClusterFirstWithHostNet, got %s", podSpec.DNSPolicy)
	}
	if podSpec.DNSConfig == nil || len(podSpec.DNSConfig.Nameservers) != 1 || podSpec.DNSConfig.Nameservers[0] != "8.8.8.8" {
		t.Error("expected dnsConfig with nameserver 8.8.8.8")
	}
}

func TestGetInitMaxRetries(t *testing.T) {
	// Default.
	initCR := &supersetv1alpha1.SupersetInit{}
	if got := getInitMaxRetries(initCR); got != 3 {
		t.Errorf("expected default 3, got %d", got)
	}

	// Custom.
	maxRetries := int32(5)
	initCR.Spec.MaxRetries = &maxRetries
	if got := getInitMaxRetries(initCR); got != 5 {
		t.Errorf("expected 5, got %d", got)
	}
}

func TestGetInitTimeout(t *testing.T) {
	// Default.
	initCR := &supersetv1alpha1.SupersetInit{}
	if got := getInitTimeout(initCR); got != 300*time.Second {
		t.Errorf("expected 300s, got %s", got)
	}

	// Custom.
	timeout := metav1.Duration{Duration: 600 * time.Second}
	initCR.Spec.Timeout = &timeout
	if got := getInitTimeout(initCR); got != 600*time.Second {
		t.Errorf("expected 600s, got %s", got)
	}
}

func TestGetInitRetentionPolicy(t *testing.T) {
	tests := []struct {
		name   string
		initCR *supersetv1alpha1.SupersetInit
		want   string
	}{
		{
			"default",
			&supersetv1alpha1.SupersetInit{},
			"Delete",
		},
		{
			"retain",
			&supersetv1alpha1.SupersetInit{
				Spec: supersetv1alpha1.SupersetInitSpec{
					PodRetention: &supersetv1alpha1.PodRetentionSpec{
						Policy: strPtr("Retain"),
					},
				},
			},
			"Retain",
		},
		{
			"retain on failure",
			&supersetv1alpha1.SupersetInit{
				Spec: supersetv1alpha1.SupersetInitSpec{
					PodRetention: &supersetv1alpha1.PodRetentionSpec{
						Policy: strPtr("RetainOnFailure"),
					},
				},
			},
			"RetainOnFailure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getInitRetentionPolicy(tt.initCR)
			if got != tt.want {
				t.Errorf("getInitRetentionPolicy() = %s, want %s", got, tt.want)
			}
		})
	}
}

// --- Init controller lifecycle tests ---

func minimalInitCR() *supersetv1alpha1.SupersetInit {
	return &supersetv1alpha1.SupersetInit{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-init-1"},
		Spec: supersetv1alpha1.SupersetInitSpec{
			FlatComponentSpec: supersetv1alpha1.FlatComponentSpec{
				Image: supersetv1alpha1.ImageSpec{
					Repository: "apache/superset",
					Tag:        "latest",
				},
				PodTemplate: &supersetv1alpha1.PodTemplate{
					Container: &supersetv1alpha1.ContainerTemplate{
						Command: []string{"/bin/sh", "-c", "superset db upgrade && superset init"},
					},
				},
			},
			Config:         "# test config",
			ConfigChecksum: "abc123",
		},
	}
}

func TestInitReconcile_CreatesConfigMapAndPod(t *testing.T) {
	scheme := testScheme(t)
	initCR := minimalInitCR()

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initCR).
		WithStatusSubresource(initCR).
		Build()

	r := &SupersetInitReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Should requeue to poll init pod status.
	if result.RequeueAfter == 0 {
		t.Error("expected RequeueAfter > 0")
	}

	// ConfigMap should exist.
	cm := &corev1.ConfigMap{}
	cmName := common.ConfigMapName("test-init")
	if err := c.Get(context.Background(), types.NamespacedName{Name: cmName, Namespace: "default"}, cm); err != nil {
		t.Fatalf("expected ConfigMap: %v", err)
	}
	if cm.Data["superset_config.py"] != "# test config" {
		t.Errorf("expected config data, got %q", cm.Data["superset_config.py"])
	}

	// Pod should have been created.
	podList := &corev1.PodList{}
	if err := c.List(context.Background(), podList); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(podList.Items) != 1 {
		t.Fatalf("expected 1 pod, got %d", len(podList.Items))
	}

	pod := &podList.Items[0]
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("expected RestartPolicyNever, got %s", pod.Spec.RestartPolicy)
	}
	if pod.Labels[labelInitInstance] != "test-init" {
		t.Errorf("expected init instance label, got %v", pod.Labels)
	}

	// Status should be updated.
	updatedCR := &supersetv1alpha1.SupersetInit{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, updatedCR); err != nil {
		t.Fatalf("get updated CR: %v", err)
	}
	if updatedCR.Status.State != initStateRunning {
		t.Errorf("expected state Running, got %s", updatedCR.Status.State)
	}
	if updatedCR.Status.Image != "apache/superset:latest" {
		t.Errorf("expected image apache/superset:latest, got %s", updatedCR.Status.Image)
	}
}

func TestInitReconcile_PodLabelsAndAnnotations(t *testing.T) {
	scheme := testScheme(t)
	initCR := minimalInitCR()
	initCR.Spec.PodTemplate = &supersetv1alpha1.PodTemplate{
		Labels: map[string]string{
			"custom-label":    "value",
			labelInitInstance: "attacker-value",
		},
		Annotations: map[string]string{
			"prometheus.io/scrape": "true",
		},
		Container: &supersetv1alpha1.ContainerTemplate{
			Command: []string{"/bin/sh", "-c", "superset db upgrade && superset init"},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initCR).
		WithStatusSubresource(initCR).
		Build()

	r := &SupersetInitReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	podList := &corev1.PodList{}
	if err := c.List(context.Background(), podList); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(podList.Items) != 1 {
		t.Fatalf("expected 1 pod, got %d", len(podList.Items))
	}

	pod := &podList.Items[0]
	if pod.Labels["custom-label"] != "value" {
		t.Errorf("expected user label on pod, got labels %v", pod.Labels)
	}
	if pod.Labels[labelInitInstance] != "test-init" {
		t.Errorf("operator label should not be overridden, got %q", pod.Labels[labelInitInstance])
	}
	if pod.Annotations["prometheus.io/scrape"] != "true" {
		t.Errorf("expected user annotation on pod, got annotations %v", pod.Annotations)
	}
}

func TestInitReconcile_PodSucceeded(t *testing.T) {
	scheme := testScheme(t)
	initCR := minimalInitCR()
	initCR.Status.State = initStateRunning
	now := metav1.Now()
	initCR.Status.StartedAt = &now

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-init-abc",
			Namespace: "default",
			Labels: map[string]string{
				labelInitInstance: "test-init",
				labelInitTask:     initTaskName,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initCR, pod).
		WithStatusSubresource(initCR).
		Build()

	r := &SupersetInitReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue on completion, got %v", result.RequeueAfter)
	}

	updatedCR := &supersetv1alpha1.SupersetInit{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, updatedCR); err != nil {
		t.Fatalf("get updated CR: %v", err)
	}
	if updatedCR.Status.State != initStateComplete {
		t.Errorf("expected state Complete, got %s", updatedCR.Status.State)
	}
	if updatedCR.Status.CompletedAt == nil {
		t.Error("expected CompletedAt to be set")
	}
	if updatedCR.Status.ConfigChecksum != "abc123" {
		t.Errorf("expected config checksum abc123, got %s", updatedCR.Status.ConfigChecksum)
	}
}

func TestInitReconcile_PodFailed_Retries(t *testing.T) {
	scheme := testScheme(t)
	initCR := minimalInitCR()
	initCR.Status.State = initStateRunning
	initCR.Status.Attempts = 0

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-init-abc",
			Namespace: "default",
			Labels: map[string]string{
				labelInitInstance: "test-init",
				labelInitTask:     initTaskName,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}}},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initCR, pod).
		WithStatusSubresource(initCR).
		Build()

	r := &SupersetInitReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected RequeueAfter > 0 for retry")
	}

	updatedCR := &supersetv1alpha1.SupersetInit{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, updatedCR); err != nil {
		t.Fatalf("get updated CR: %v", err)
	}
	if updatedCR.Status.Attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", updatedCR.Status.Attempts)
	}
	if updatedCR.Status.State != initStatePending {
		t.Errorf("expected state Pending (for retry), got %s", updatedCR.Status.State)
	}
}

func TestInitReconcile_PodFailed_ExhaustsRetries(t *testing.T) {
	scheme := testScheme(t)
	initCR := minimalInitCR()
	initCR.Status.State = initStateRunning
	initCR.Status.Attempts = 2

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-init-abc",
			Namespace: "default",
			Labels: map[string]string{
				labelInitInstance: "test-init",
				labelInitTask:     initTaskName,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "OOMKilled"}}},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initCR, pod).
		WithStatusSubresource(initCR).
		Build()

	r := &SupersetInitReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	updatedCR := &supersetv1alpha1.SupersetInit{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, updatedCR); err != nil {
		t.Fatalf("get updated CR: %v", err)
	}
	if updatedCR.Status.State != initStateFailed {
		t.Errorf("expected state Failed, got %s", updatedCR.Status.State)
	}
	if updatedCR.Status.Attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", updatedCR.Status.Attempts)
	}
	if updatedCR.Status.Message != "Exit code 1: OOMKilled" {
		t.Errorf("expected message 'Exit code 1: OOMKilled', got %s", updatedCR.Status.Message)
	}
}

func TestInitReconcile_AlreadyComplete_Noop(t *testing.T) {
	scheme := testScheme(t)
	initCR := minimalInitCR()
	initCR.Status.State = initStateComplete
	initCR.Status.ConfigChecksum = "abc123"

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initCR).
		WithStatusSubresource(initCR).
		Build()

	r := &SupersetInitReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue for completed init, got %v", result.RequeueAfter)
	}
}

func TestInitReconcile_ConfigChanged_ReRunsInit(t *testing.T) {
	scheme := testScheme(t)
	initCR := minimalInitCR()
	initCR.Spec.ConfigChecksum = "new-checksum"
	initCR.Status.State = initStateComplete
	initCR.Status.ConfigChecksum = "old-checksum"

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initCR).
		WithStatusSubresource(initCR).
		Build()

	r := &SupersetInitReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected requeue for config change")
	}

	updatedCR := &supersetv1alpha1.SupersetInit{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, updatedCR); err != nil {
		t.Fatalf("get updated CR: %v", err)
	}
	if updatedCR.Status.State != initStateRunning {
		t.Errorf("expected state Running after config change, got %s", updatedCR.Status.State)
	}
	if updatedCR.Status.Attempts != 0 {
		t.Errorf("expected attempts reset to 0, got %d", updatedCR.Status.Attempts)
	}
}

func TestInitReconcile_FailedExhausted_ConfigChanged_ReRunsInit(t *testing.T) {
	scheme := testScheme(t)
	initCR := minimalInitCR()
	initCR.Spec.ConfigChecksum = "new-checksum"
	initCR.Spec.PodRetention = &supersetv1alpha1.PodRetentionSpec{Policy: strPtr("RetainOnFailure")}
	initCR.Status.State = initStateFailed
	initCR.Status.Attempts = 3
	initCR.Status.ConfigChecksum = "old-checksum"
	initCR.Status.Message = "OOMKilled"

	retainedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-init-old",
			Namespace: "default",
			Labels: map[string]string{
				labelInitInstance: "test-init",
				labelInitTask:     initTaskName,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodFailed},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initCR, retainedPod).
		WithStatusSubresource(initCR).
		Build()

	r := &SupersetInitReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected requeue for config change after failure")
	}

	updatedCR := &supersetv1alpha1.SupersetInit{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, updatedCR); err != nil {
		t.Fatalf("get updated CR: %v", err)
	}
	if updatedCR.Status.State != initStateRunning {
		t.Errorf("expected state Running after config change, got %s", updatedCR.Status.State)
	}
	if updatedCR.Status.Attempts != 0 {
		t.Errorf("expected attempts reset to 0, got %d", updatedCR.Status.Attempts)
	}

	// Retained pod from the old run should be deleted, replaced by a fresh pod.
	podList := &corev1.PodList{}
	if err := c.List(context.Background(), podList); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(podList.Items) != 1 {
		t.Fatalf("expected 1 pod (fresh), got %d", len(podList.Items))
	}
	if podList.Items[0].Name == "test-init-old" {
		t.Error("expected the retained pod to be deleted and replaced by a fresh pod")
	}
}

func TestInitReconcile_NotFound(t *testing.T) {
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &SupersetInitReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("expected no error for not found: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue for not found")
	}
}

func TestFindInitPod_ReturnsMostRecent(t *testing.T) {
	scheme := testScheme(t)
	initCR := minimalInitCR()

	now := metav1.Now()
	older := metav1.NewTime(now.Add(-1 * time.Hour))

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-init-old", Namespace: "default",
			CreationTimestamp: older,
			Labels:            map[string]string{labelInitInstance: "test-init", labelInitTask: initTaskName},
		},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-init-new", Namespace: "default",
			CreationTimestamp: now,
			Labels:            map[string]string{labelInitInstance: "test-init", labelInitTask: initTaskName},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(initCR, pod1, pod2).Build()
	r := &SupersetInitReconciler{Client: c, Scheme: scheme}

	pod, err := r.findInitPod(context.Background(), initCR, "test-init")
	if err != nil {
		t.Fatalf("findInitPod: %v", err)
	}
	if pod == nil {
		t.Fatal("expected a pod")
	}
	if pod.Name != "test-init-new" {
		t.Errorf("expected most recent pod test-init-new, got %s", pod.Name)
	}
}

func TestFindInitPod_NoPods(t *testing.T) {
	scheme := testScheme(t)
	initCR := minimalInitCR()

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(initCR).Build()
	r := &SupersetInitReconciler{Client: c, Scheme: scheme}

	pod, err := r.findInitPod(context.Background(), initCR, "test-init")
	if err != nil {
		t.Fatalf("findInitPod: %v", err)
	}
	if pod != nil {
		t.Errorf("expected nil pod, got %s", pod.Name)
	}
}

// --- Pure function tests (moved from initpod_test.go) ---

func TestIsInitDisabled(t *testing.T) {
	tests := []struct {
		name     string
		superset *supersetv1alpha1.Superset
		want     bool
	}{
		{"nil init spec", &supersetv1alpha1.Superset{}, false},
		{"nil disabled", &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{Init: &supersetv1alpha1.InitSpec{}}}, false},
		{"disabled false", &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{Init: &supersetv1alpha1.InitSpec{Disabled: boolPtr(false)}}}, false},
		{"disabled true", &supersetv1alpha1.Superset{Spec: supersetv1alpha1.SupersetSpec{Init: &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)}}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isInitDisabled(tt.superset); got != tt.want {
				t.Errorf("isInitDisabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff(t *testing.T) {
	tests := []struct {
		attempt  int32
		expected time.Duration
	}{
		{1, 10 * time.Second},
		{2, 20 * time.Second},
		{3, 40 * time.Second},
		{4, 80 * time.Second},
		{5, 160 * time.Second},
	}

	for _, tt := range tests {
		if got := calculateBackoff(tt.attempt); got != tt.expected {
			t.Errorf("calculateBackoff(%d) = %s, want %s", tt.attempt, got, tt.expected)
		}
	}

	if got := calculateBackoff(10); got > 300*time.Second {
		t.Errorf("backoff should be capped at 300s, got %s", got)
	}
}

func TestPodFailureMessage(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}}},
			},
		},
	}
	if msg := podFailureMessage(pod); msg != "Exit code 1: Error" {
		t.Errorf("expected 'Exit code 1: Error', got %s", msg)
	}

	pod.Status.ContainerStatuses[0].State.Terminated.Message = "OOMKilled"
	if msg := podFailureMessage(pod); msg != "Exit code 1: Error: OOMKilled" {
		t.Errorf("expected 'Exit code 1: Error: OOMKilled', got %s", msg)
	}

	if msg := podFailureMessage(&corev1.Pod{}); msg != "Pod failed" {
		t.Errorf("expected 'Pod failed', got %s", msg)
	}

	// Long termination messages should be truncated.
	longMsg := strings.Repeat("x", 300)
	pod.Status.ContainerStatuses[0].State.Terminated.Message = longMsg
	msg := podFailureMessage(pod)
	if len(msg) > maxTerminationMessageLen+50 {
		t.Errorf("expected truncated message, got length %d", len(msg))
	}
	if msg[len(msg)-3:] != "..." {
		t.Error("expected truncated message to end with '...'")
	}
}

// --- Retention policy tests ---

func TestApplyRetentionPolicy(t *testing.T) {
	tests := []struct {
		name       string
		policy     *string
		phase      corev1.PodPhase
		wantDelete bool
	}{
		{"Delete/Succeeded", nil, corev1.PodSucceeded, true},
		{"Delete/Failed", nil, corev1.PodFailed, true},
		{"Retain/Succeeded", strPtr("Retain"), corev1.PodSucceeded, false},
		{"Retain/Failed", strPtr("Retain"), corev1.PodFailed, false},
		{"RetainOnFailure/Succeeded", strPtr("RetainOnFailure"), corev1.PodSucceeded, true},
		{"RetainOnFailure/Failed", strPtr("RetainOnFailure"), corev1.PodFailed, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := testScheme(t)
			initCR := minimalInitCR()
			if tt.policy != nil {
				initCR.Spec.PodRetention = &supersetv1alpha1.PodRetentionSpec{Policy: tt.policy}
			}

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
				Status:     corev1.PodStatus{Phase: tt.phase},
			}

			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(initCR, pod).Build()
			r := &SupersetInitReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

			r.applyRetentionPolicy(context.Background(), initCR, pod)

			err := c.Get(context.Background(), types.NamespacedName{Name: "test-pod", Namespace: "default"}, &corev1.Pod{})
			deleted := err != nil
			if deleted != tt.wantDelete {
				t.Errorf("expected deleted=%v, got deleted=%v (err=%v)", tt.wantDelete, deleted, err)
			}
		})
	}
}
