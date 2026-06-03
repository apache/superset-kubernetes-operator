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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestCalculateBackoff(t *testing.T) {
	tests := []struct {
		name    string
		attempt int32
		want    time.Duration
	}{
		{name: "attempt 1", attempt: 1, want: 10 * time.Second},
		{name: "attempt 2", attempt: 2, want: 20 * time.Second},
		{name: "attempt 3", attempt: 3, want: 40 * time.Second},
		{name: "attempt 4", attempt: 4, want: 80 * time.Second},
		{name: "attempt 5", attempt: 5, want: 160 * time.Second},
		{name: "attempt 6 caps at 300s", attempt: 6, want: 300 * time.Second},
		{name: "attempt 10 stays capped", attempt: 10, want: 300 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, calculateBackoff(tt.attempt))
		})
	}
}

func TestBuildInitPod(t *testing.T) {
	t.Run("minimal spec builds a never-restart pod with the superset container", func(t *testing.T) {
		spec := &supersetv1alpha1.FlatComponentSpec{
			Image: supersetv1alpha1.ImageSpec{
				Repository: "apache/superset",
				Tag:        "4.0.0",
				PullPolicy: corev1.PullIfNotPresent,
			},
		}
		pod := buildInitPod(spec)
		assert.Equal(t, corev1.RestartPolicyNever, pod.RestartPolicy)
		require.Len(t, pod.Containers, 1)
		c := pod.Containers[0]
		assert.Equal(t, common.Container, c.Name)
		assert.Equal(t, "apache/superset:4.0.0", c.Image)
		assert.Equal(t, corev1.PullIfNotPresent, c.ImagePullPolicy)
		assert.Empty(t, pod.ServiceAccountName)
	})

	t.Run("propagates container and pod template fields", func(t *testing.T) {
		runAsUser := int64(1000)
		gracePeriod := int64(45)
		spec := &supersetv1alpha1.FlatComponentSpec{
			Image:              supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			ServiceAccountName: "sa-init",
			PodTemplate: &supersetv1alpha1.PodTemplate{
				NodeSelector:                  map[string]string{"disktype": "ssd"},
				TerminationGracePeriodSeconds: &gracePeriod,
				PodSecurityContext:            &corev1.PodSecurityContext{RunAsUser: &runAsUser},
				Volumes: []corev1.Volume{
					{Name: "config", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				},
				Sidecars:       []corev1.Container{{Name: "sidecar"}},
				InitContainers: []corev1.Container{{Name: "pre"}},
				Container: &supersetv1alpha1.ContainerTemplate{
					Command:      []string{"/bin/sh", "-c", "superset init"},
					Args:         []string{"--flag"},
					Env:          []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
					VolumeMounts: []corev1.VolumeMount{{Name: "config", MountPath: "/app/pythonpath"}},
				},
			},
		}
		pod := buildInitPod(spec)

		assert.Equal(t, "sa-init", pod.ServiceAccountName)
		assert.Equal(t, map[string]string{"disktype": "ssd"}, pod.NodeSelector)
		require.NotNil(t, pod.TerminationGracePeriodSeconds)
		assert.Equal(t, int64(45), *pod.TerminationGracePeriodSeconds)
		require.NotNil(t, pod.SecurityContext)
		assert.Equal(t, int64(1000), *pod.SecurityContext.RunAsUser)
		assert.Len(t, pod.Volumes, 1)

		// Main container is first; sidecar appended after.
		require.Len(t, pod.Containers, 2)
		assert.Equal(t, common.Container, pod.Containers[0].Name)
		assert.Equal(t, []string{"/bin/sh", "-c", "superset init"}, pod.Containers[0].Command)
		assert.Equal(t, []string{"--flag"}, pod.Containers[0].Args)
		assert.Equal(t, "sidecar", pod.Containers[1].Name)

		require.Len(t, pod.InitContainers, 1)
		assert.Equal(t, "pre", pod.InitContainers[0].Name)
	})

	t.Run("dns policy and priority class are applied when set", func(t *testing.T) {
		dnsPolicy := corev1.DNSClusterFirstWithHostNet
		prio := "high"
		spec := &supersetv1alpha1.FlatComponentSpec{
			Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			PodTemplate: &supersetv1alpha1.PodTemplate{
				DNSPolicy:         &dnsPolicy,
				PriorityClassName: &prio,
			},
		}
		pod := buildInitPod(spec)
		assert.Equal(t, dnsPolicy, pod.DNSPolicy)
		assert.Equal(t, "high", pod.PriorityClassName)
	})
}

func TestShouldDeletePod(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		phase  corev1.PodPhase
		want   bool
	}{
		{name: "Delete always deletes succeeded", policy: retentionDelete, phase: corev1.PodSucceeded, want: true},
		{name: "Delete always deletes failed", policy: retentionDelete, phase: corev1.PodFailed, want: true},
		{name: "Retain never deletes succeeded", policy: retentionRetain, phase: corev1.PodSucceeded, want: false},
		{name: "Retain never deletes failed", policy: retentionRetain, phase: corev1.PodFailed, want: false},
		{name: "RetainOnFailure deletes succeeded", policy: retentionRetainOnFail, phase: corev1.PodSucceeded, want: true},
		{name: "RetainOnFailure keeps failed", policy: retentionRetainOnFail, phase: corev1.PodFailed, want: false},
		{name: "RetainOnFailure deletes running", policy: retentionRetainOnFail, phase: corev1.PodRunning, want: true},
		{name: "unknown policy deletes (safe default)", policy: "Bogus", phase: corev1.PodFailed, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ShouldDeletePod(tt.policy, tt.phase))
		})
	}
}
