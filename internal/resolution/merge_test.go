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

package resolution

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestMergeMaps(t *testing.T) {
	tests := []struct {
		name   string
		maps   []map[string]string
		expect map[string]string
	}{
		{
			name:   "nil inputs",
			maps:   []map[string]string{nil, nil},
			expect: nil,
		},
		{
			name:   "empty inputs",
			maps:   []map[string]string{{}, {}},
			expect: nil,
		},
		{
			name:   "single map",
			maps:   []map[string]string{{"a": "1"}},
			expect: map[string]string{"a": "1"},
		},
		{
			name:   "no conflict",
			maps:   []map[string]string{{"a": "1"}, {"b": "2"}},
			expect: map[string]string{"a": "1", "b": "2"},
		},
		{
			name:   "key conflict later wins",
			maps:   []map[string]string{{"a": "1"}, {"a": "2"}},
			expect: map[string]string{"a": "2"},
		},
		{
			name:   "three maps with conflicts",
			maps:   []map[string]string{{"a": "1", "b": "1"}, {"b": "2", "c": "2"}, {"c": "3"}},
			expect: map[string]string{"a": "1", "b": "2", "c": "3"},
		},
		{
			name:   "nil among non-nil",
			maps:   []map[string]string{{"a": "1"}, nil, {"b": "2"}},
			expect: map[string]string{"a": "1", "b": "2"},
		},
		{
			name:   "no arguments",
			maps:   nil,
			expect: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeMaps(tt.maps...)
			assertMapsEqual(t, tt.expect, result)
		})
	}
}

func TestMergeEnvVars(t *testing.T) {
	tests := []struct {
		name   string
		slices [][]corev1.EnvVar
		expect []corev1.EnvVar
	}{
		{
			name:   "nil inputs",
			slices: [][]corev1.EnvVar{nil, nil},
			expect: nil,
		},
		{
			name: "no conflict",
			slices: [][]corev1.EnvVar{
				{{Name: "A", Value: "1"}},
				{{Name: "B", Value: "2"}},
			},
			expect: []corev1.EnvVar{{Name: "A", Value: "1"}, {Name: "B", Value: "2"}},
		},
		{
			name: "name conflict replaces in place",
			slices: [][]corev1.EnvVar{
				{{Name: "A", Value: "1"}, {Name: "B", Value: "1"}},
				{{Name: "A", Value: "2"}},
			},
			expect: []corev1.EnvVar{{Name: "A", Value: "2"}, {Name: "B", Value: "1"}},
		},
		{
			name: "three slices",
			slices: [][]corev1.EnvVar{
				{{Name: "A", Value: "1"}},
				{{Name: "B", Value: "2"}},
				{{Name: "A", Value: "3"}, {Name: "C", Value: "3"}},
			},
			expect: []corev1.EnvVar{{Name: "A", Value: "3"}, {Name: "B", Value: "2"}, {Name: "C", Value: "3"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeEnvVars(tt.slices...)
			if len(result) != len(tt.expect) {
				t.Fatalf("expected %d env vars, got %d", len(tt.expect), len(result))
			}
			for i, e := range tt.expect {
				if result[i].Name != e.Name || result[i].Value != e.Value {
					t.Errorf("index %d: expected {%s=%s}, got {%s=%s}", i, e.Name, e.Value, result[i].Name, result[i].Value)
				}
			}
		})
	}
}

func TestMergeVolumes(t *testing.T) {
	configVol := corev1.Volume{
		Name: "config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cm1"},
			},
		},
	}
	configVolOverride := corev1.Volume{
		Name: "config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cm2"},
			},
		},
	}
	secretVol := corev1.Volume{
		Name: "secret",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "s1"},
		},
	}

	tests := []struct {
		name   string
		slices [][]corev1.Volume
		expect []corev1.Volume
	}{
		{
			name:   "nil inputs",
			slices: [][]corev1.Volume{nil, nil},
			expect: nil,
		},
		{
			name:   "no conflict",
			slices: [][]corev1.Volume{{configVol}, {secretVol}},
			expect: []corev1.Volume{configVol, secretVol},
		},
		{
			name:   "name conflict replaces in place",
			slices: [][]corev1.Volume{{configVol, secretVol}, {configVolOverride}},
			expect: []corev1.Volume{configVolOverride, secretVol},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeVolumes(tt.slices...)
			if len(result) != len(tt.expect) {
				t.Fatalf("expected %d volumes, got %d", len(tt.expect), len(result))
			}
			for i, e := range tt.expect {
				if result[i].Name != e.Name {
					t.Errorf("index %d: expected name %s, got %s", i, e.Name, result[i].Name)
				}
			}
		})
	}
}

func TestMergeVolumeMounts(t *testing.T) {
	tests := []struct {
		name   string
		slices [][]corev1.VolumeMount
		expect []corev1.VolumeMount
	}{
		{
			name:   "nil inputs",
			slices: [][]corev1.VolumeMount{nil, nil},
			expect: nil,
		},
		{
			name: "name conflict replaces in place",
			slices: [][]corev1.VolumeMount{
				{{Name: "data", MountPath: "/old"}},
				{{Name: "data", MountPath: "/new"}},
			},
			expect: []corev1.VolumeMount{{Name: "data", MountPath: "/new"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeVolumeMounts(tt.slices...)
			if len(result) != len(tt.expect) {
				t.Fatalf("expected %d mounts, got %d", len(tt.expect), len(result))
			}
			for i, e := range tt.expect {
				if result[i].Name != e.Name || result[i].MountPath != e.MountPath {
					t.Errorf("index %d: expected {%s@%s}, got {%s@%s}", i, e.Name, e.MountPath, result[i].Name, result[i].MountPath)
				}
			}
		})
	}
}

func TestMergeHostAliases(t *testing.T) {
	tests := []struct {
		name   string
		slices [][]corev1.HostAlias
		expect []corev1.HostAlias
	}{
		{
			name:   "nil inputs",
			slices: [][]corev1.HostAlias{nil, nil},
			expect: nil,
		},
		{
			name: "IP conflict replaces in place",
			slices: [][]corev1.HostAlias{
				{{IP: "10.0.0.1", Hostnames: []string{"a"}}},
				{{IP: "10.0.0.1", Hostnames: []string{"b"}}},
			},
			expect: []corev1.HostAlias{{IP: "10.0.0.1", Hostnames: []string{"b"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeHostAliases(tt.slices...)
			if len(result) != len(tt.expect) {
				t.Fatalf("expected %d aliases, got %d", len(tt.expect), len(result))
			}
		})
	}
}

func TestMergeContainers(t *testing.T) {
	tests := []struct {
		name   string
		slices [][]corev1.Container
		expect []corev1.Container
	}{
		{
			name:   "nil inputs",
			slices: [][]corev1.Container{nil, nil},
			expect: nil,
		},
		{
			name: "name conflict replaces in place",
			slices: [][]corev1.Container{
				{{Name: "sidecar", Image: "old:1"}},
				{{Name: "sidecar", Image: "new:2"}},
			},
			expect: []corev1.Container{{Name: "sidecar", Image: "new:2"}},
		},
		{
			name: "no conflict preserves order",
			slices: [][]corev1.Container{
				{{Name: "a", Image: "a:1"}},
				{{Name: "b", Image: "b:1"}},
			},
			expect: []corev1.Container{{Name: "a", Image: "a:1"}, {Name: "b", Image: "b:1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeContainers(tt.slices...)
			if len(result) != len(tt.expect) {
				t.Fatalf("expected %d containers, got %d", len(tt.expect), len(result))
			}
			for i, e := range tt.expect {
				if result[i].Name != e.Name || result[i].Image != e.Image {
					t.Errorf("index %d: expected {%s %s}, got {%s %s}", i, e.Name, e.Image, result[i].Name, result[i].Image)
				}
			}
		})
	}
}

func TestMergeEnvFromSources(t *testing.T) {
	s1 := corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "s1"}}}
	s2 := corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "s2"}}}

	result := MergeEnvFromSources([]corev1.EnvFromSource{s1}, []corev1.EnvFromSource{s2})
	if len(result) != 2 {
		t.Fatalf("expected 2 envFrom sources, got %d", len(result))
	}
}

func TestMergeContainerPorts(t *testing.T) {
	tests := []struct {
		name   string
		slices [][]corev1.ContainerPort
		expect []corev1.ContainerPort
	}{
		{
			"nil inputs",
			nil,
			nil,
		},
		{
			"no conflict",
			[][]corev1.ContainerPort{
				{{Name: "http", ContainerPort: 8088}},
				{{Name: "debug", ContainerPort: 5005}},
			},
			[]corev1.ContainerPort{
				{Name: "http", ContainerPort: 8088},
				{Name: "debug", ContainerPort: 5005},
			},
		},
		{
			"name conflict replaces in place",
			[][]corev1.ContainerPort{
				{{Name: "http", ContainerPort: 8088}},
				{{Name: "http", ContainerPort: 9090}},
			},
			[]corev1.ContainerPort{
				{Name: "http", ContainerPort: 9090},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeContainerPorts(tt.slices...)
			if len(result) != len(tt.expect) {
				t.Fatalf("expected %d ports, got %d", len(tt.expect), len(result))
			}
			for i, e := range tt.expect {
				if result[i].Name != e.Name || result[i].ContainerPort != e.ContainerPort {
					t.Errorf("index %d: expected {%s %d}, got {%s %d}", i, e.Name, e.ContainerPort, result[i].Name, result[i].ContainerPort)
				}
			}
		})
	}
}

func TestResolveScalar(t *testing.T) {
	v1 := int32(5)
	v2 := int32(10)

	tests := []struct {
		name   string
		values []*int32
		expect int32
	}{
		{"first non-nil wins", []*int32{&v1, &v2}, 5},
		{"skip nil", []*int32{nil, &v2}, 10},
		{"all nil returns zero", []*int32{nil, nil}, 0},
		{"no values returns zero", nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ResolveScalar(tt.values...)
			if result != tt.expect {
				t.Errorf("expected %d, got %d", tt.expect, result)
			}
		})
	}
}

func TestResolveResource(t *testing.T) {
	r1 := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
	}
	r2 := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
	}

	t.Run("first non-nil wins", func(t *testing.T) {
		result := ResolveResource(r1, r2)
		if !result.Requests.Cpu().Equal(resource.MustParse("500m")) {
			t.Errorf("expected 500m, got %s", result.Requests.Cpu())
		}
	})

	t.Run("all nil returns empty", func(t *testing.T) {
		result := ResolveResource(nil, nil)
		if result.Requests != nil {
			t.Errorf("expected nil requests, got %v", result.Requests)
		}
	})
}

func TestResolveOverridableMap(t *testing.T) {
	t.Run("override present replaces entirely", func(t *testing.T) {
		result := ResolveOverridableMap(map[string]string{"a": "1"}, map[string]string{"b": "2"})
		if result["a"] != "1" || result["b"] != "" {
			t.Errorf("expected override only, got %v", result)
		}
	})

	t.Run("override empty map replaces with empty", func(t *testing.T) {
		emptyMap := map[string]string{}
		result := ResolveOverridableMap(emptyMap, map[string]string{"b": "2"})
		if len(result) != 0 {
			t.Errorf("expected empty map, got %v", result)
		}
	})

	t.Run("override nil uses default", func(t *testing.T) {
		result := ResolveOverridableMap(nil, map[string]string{"b": "2"})
		if result["b"] != "2" {
			t.Errorf("expected default, got %v", result)
		}
	})
}

func TestResolveOverridableSlice(t *testing.T) {
	t.Run("override present replaces entirely", func(t *testing.T) {
		override := []corev1.Toleration{{Key: "override"}}
		dflt := []corev1.Toleration{{Key: "default"}}
		result := ResolveOverridableSlice(override, dflt)
		if len(result) != 1 || result[0].Key != "override" {
			t.Errorf("expected override, got %v", result)
		}
	})

	t.Run("override empty slice replaces with empty", func(t *testing.T) {
		override := []corev1.Toleration{}
		dflt := []corev1.Toleration{{Key: "default"}}
		result := ResolveOverridableSlice(override, dflt)
		if len(result) != 0 {
			t.Errorf("expected empty, got %v", result)
		}
	})

	t.Run("override nil uses default", func(t *testing.T) {
		var override []corev1.Toleration
		dflt := []corev1.Toleration{{Key: "default"}}
		result := ResolveOverridableSlice(override, dflt)
		if len(result) != 1 || result[0].Key != "default" {
			t.Errorf("expected default, got %v", result)
		}
	})
}

func TestResolveOverridableValue(t *testing.T) {
	v1 := corev1.PodSecurityContext{RunAsUser: Ptr(int64(1000))}
	v2 := corev1.PodSecurityContext{RunAsUser: Ptr(int64(2000))}

	t.Run("override non-nil wins", func(t *testing.T) {
		result := ResolveOverridableValue(&v1, &v2)
		if *result.RunAsUser != 1000 {
			t.Errorf("expected 1000, got %d", *result.RunAsUser)
		}
	})

	t.Run("override nil uses default", func(t *testing.T) {
		result := ResolveOverridableValue((*corev1.PodSecurityContext)(nil), &v2)
		if *result.RunAsUser != 2000 {
			t.Errorf("expected 2000, got %d", *result.RunAsUser)
		}
	})
}

// assertMapsEqual compares two string maps for equality.
func assertMapsEqual(t *testing.T, expected, actual map[string]string) {
	t.Helper()
	if len(expected) == 0 && len(actual) == 0 {
		return
	}
	if len(expected) != len(actual) {
		t.Errorf("map length: expected %d, got %d\n  expected: %v\n  actual:   %v", len(expected), len(actual), expected, actual)
		return
	}
	for k, v := range expected {
		if actual[k] != v {
			t.Errorf("key %q: expected %q, got %q", k, v, actual[k])
		}
	}
}
