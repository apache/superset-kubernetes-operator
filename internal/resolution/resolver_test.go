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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// pt is a helper to build a PodTemplate with an optional container template.
func pt(p *supersetv1alpha1.PodTemplate, ct *supersetv1alpha1.ContainerTemplate) *supersetv1alpha1.PodTemplate {
	if p == nil {
		p = &supersetv1alpha1.PodTemplate{}
	}
	if ct != nil {
		p.Container = ct
	}
	return p
}

func TestResolveChildSpec_NilTopLevelAndNilComponent(t *testing.T) {
	result := ResolveChildSpec(ComponentWebServer, nil, nil, nil, nil)

	if result.Replicas != 1 {
		t.Errorf("expected default replicas 1, got %d", result.Replicas)
	}
}

func TestResolveChildSpec_TopLevelInheritedWhenComponentNil(t *testing.T) {
	topLevel := &SharedInput{
		Replicas: Ptr(int32(3)),
		PodTemplate: pt(
			&supersetv1alpha1.PodTemplate{
				Annotations:        map[string]string{"prometheus.io/scrape": "true"},
				Labels:             map[string]string{"team": "platform"},
				NodeSelector:       map[string]string{"workload": "data"},
				Tolerations:        []corev1.Toleration{{Key: "data-workload"}},
				PriorityClassName:  Ptr("high"),
				Affinity:           &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}},
				PodSecurityContext: &corev1.PodSecurityContext{RunAsUser: Ptr(int64(1000))},
				Sidecars:           []corev1.Container{{Name: "sidecar", Image: "sidecar:1"}},
				InitContainers:     []corev1.Container{{Name: "init", Image: "init:1"}},
				HostAliases:        []corev1.HostAlias{{IP: "10.0.0.1", Hostnames: []string{"db"}}},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
					{MaxSkew: 1, TopologyKey: "topology.kubernetes.io/zone"},
				},
			},
			&supersetv1alpha1.ContainerTemplate{
				Resources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
				},
				Env:             []corev1.EnvVar{{Name: "ENV", Value: "prod"}},
				SecurityContext: &corev1.SecurityContext{RunAsNonRoot: Ptr(true)},
			},
		),
	}

	result := ResolveChildSpec(ComponentWebServer, topLevel, nil, nil, nil)
	pt := result.PodTemplate
	ct := pt.Container

	if result.Replicas != 3 {
		t.Errorf("expected 3 replicas, got %d", result.Replicas)
	}
	if !ct.Resources.Requests.Cpu().Equal(resource.MustParse("1")) {
		t.Error("expected resources from topLevel")
	}
	if pt.Annotations["prometheus.io/scrape"] != "true" {
		t.Error("expected annotations from topLevel")
	}
	if pt.Labels["team"] != "platform" {
		t.Error("expected labels from topLevel")
	}
	if pt.NodeSelector["workload"] != "data" {
		t.Error("expected nodeSelector from topLevel")
	}
	if *pt.PriorityClassName != "high" {
		t.Error("expected priorityClassName from topLevel")
	}
	if pt.Affinity == nil {
		t.Error("expected affinity from topLevel")
	}
	if pt.PodSecurityContext == nil || *pt.PodSecurityContext.RunAsUser != 1000 {
		t.Error("expected podSecurityContext from topLevel")
	}
	if ct.SecurityContext == nil || *ct.SecurityContext.RunAsNonRoot != true {
		t.Error("expected containerSecurityContext from topLevel")
	}
	if len(ct.Env) != 1 || ct.Env[0].Name != "ENV" {
		t.Error("expected env from topLevel")
	}
	if len(pt.Tolerations) != 1 || pt.Tolerations[0].Key != "data-workload" {
		t.Error("expected tolerations from topLevel")
	}
	if len(pt.Sidecars) != 1 || pt.Sidecars[0].Name != "sidecar" {
		t.Error("expected sidecars from topLevel")
	}
	if len(pt.InitContainers) != 1 || pt.InitContainers[0].Name != "init" {
		t.Error("expected initContainers from topLevel")
	}
	if len(pt.HostAliases) != 1 || pt.HostAliases[0].IP != "10.0.0.1" {
		t.Error("expected hostAliases from topLevel")
	}
	if len(pt.TopologySpreadConstraints) != 1 {
		t.Error("expected topologySpreadConstraints from topLevel")
	}
}

func TestResolveChildSpec_ComponentMergesWithTopLevel(t *testing.T) {
	topLevel := &SharedInput{
		Replicas: Ptr(int32(2)),
		PodTemplate: pt(
			&supersetv1alpha1.PodTemplate{
				Annotations:        map[string]string{"prometheus.io/scrape": "true", "shared": "top"},
				Labels:             map[string]string{"team": "platform", "env": "prod"},
				NodeSelector:       map[string]string{"workload": "data", "region": "us"},
				Tolerations:        []corev1.Toleration{{Key: "top-toleration"}},
				PriorityClassName:  Ptr("low"),
				Affinity:           &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}},
				PodSecurityContext: &corev1.PodSecurityContext{RunAsUser: Ptr(int64(1000))},
				Sidecars:           []corev1.Container{{Name: "vault-agent", Image: "vault:1.15"}},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
					{MaxSkew: 1, TopologyKey: "topology.kubernetes.io/zone"},
				},
			},
			&supersetv1alpha1.ContainerTemplate{
				Resources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
				},
				Env:             []corev1.EnvVar{{Name: "ENV", Value: "prod"}, {Name: "TOP_ONLY", Value: "yes"}},
				SecurityContext: &corev1.SecurityContext{RunAsNonRoot: Ptr(true)},
			},
		),
	}

	compAffinity := &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}}
	component := &ComponentInput{
		SharedInput: SharedInput{
			Replicas: Ptr(int32(8)),
			PodTemplate: pt(
				&supersetv1alpha1.PodTemplate{
					Annotations:        map[string]string{"istio/inject": "true", "shared": "comp"},
					Labels:             map[string]string{"team": "overridden"},
					NodeSelector:       map[string]string{"workload": "data-heavy", "node-type": "compute"},
					Tolerations:        []corev1.Toleration{{Key: "comp-toleration"}},
					PriorityClassName:  Ptr("critical"),
					Affinity:           compAffinity,
					PodSecurityContext: &corev1.PodSecurityContext{RunAsUser: Ptr(int64(2000))},
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
						{MaxSkew: 2, TopologyKey: "kubernetes.io/hostname"},
					},
				},
				&supersetv1alpha1.ContainerTemplate{
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
					},
					Env:             []corev1.EnvVar{{Name: "COMP_ONLY", Value: "yes"}, {Name: "ENV", Value: "staging"}},
					SecurityContext: &corev1.SecurityContext{RunAsNonRoot: Ptr(false)},
					Command:         []string{"celery", "worker"},
				},
			),
		},
	}

	operatorLabels := map[string]string{
		"app.kubernetes.io/name":      "superset",
		"app.kubernetes.io/component": "celery-worker",
	}
	operator := &OperatorInjected{
		Env: []corev1.EnvVar{{Name: "PYTHONPATH", Value: "/app/superset/config"}},
	}

	result := ResolveChildSpec(ComponentCeleryWorker, topLevel, component, operatorLabels, operator)
	pt := result.PodTemplate
	ct := pt.Container

	// Replicas: component wins
	if result.Replicas != 8 {
		t.Errorf("replicas: expected 8, got %d", result.Replicas)
	}

	// Resources: component wins (REPLACE at struct level)
	if !ct.Resources.Requests.Cpu().Equal(resource.MustParse("4")) {
		t.Error("resources: expected 4 CPU from component")
	}

	// Annotations: merged (component wins on conflict)
	if pt.Annotations["prometheus.io/scrape"] != "true" {
		t.Error("expected top-level annotation preserved")
	}
	if pt.Annotations["istio/inject"] != "true" {
		t.Error("expected component annotation present")
	}
	if pt.Annotations["shared"] != "comp" {
		t.Errorf("expected component annotation to win on conflict, got %q", pt.Annotations["shared"])
	}

	// Labels: operator wins last
	if pt.Labels["team"] != "overridden" {
		t.Error("expected component label to win")
	}
	if pt.Labels["env"] != "prod" {
		t.Error("expected top-level label preserved")
	}
	if pt.Labels["app.kubernetes.io/component"] != "celery-worker" {
		t.Error("expected operator label present and protected")
	}

	// NodeSelector: merged (component wins on conflict)
	if pt.NodeSelector["workload"] != "data-heavy" {
		t.Error("expected component nodeSelector to win on conflict")
	}
	if pt.NodeSelector["region"] != "us" {
		t.Error("expected top-level nodeSelector key preserved")
	}

	// Tolerations: appended
	if len(pt.Tolerations) != 2 {
		t.Fatalf("expected 2 tolerations, got %d", len(pt.Tolerations))
	}

	// TopologySpreadConstraints: appended
	if len(pt.TopologySpreadConstraints) != 2 {
		t.Fatalf("expected 2 TSCs, got %d", len(pt.TopologySpreadConstraints))
	}

	// PriorityClassName: component wins
	if *pt.PriorityClassName != "critical" {
		t.Error("expected priorityClassName from component")
	}

	// Affinity: component wins
	if pt.Affinity != compAffinity {
		t.Error("expected affinity from component")
	}

	// PodSecurityContext: component wins
	if *pt.PodSecurityContext.RunAsUser != 2000 {
		t.Error("expected podSecurityContext from component")
	}

	// ContainerSecurityContext: component wins
	if *ct.SecurityContext.RunAsNonRoot != false {
		t.Error("expected containerSecurityContext from component")
	}

	// Env: merged (component + top-level + operator)
	envMap := make(map[string]string)
	for _, e := range ct.Env {
		envMap[e.Name] = e.Value
	}
	if envMap["ENV"] != "staging" {
		t.Error("component ENV should override top-level")
	}
	if envMap["TOP_ONLY"] != "yes" {
		t.Error("top-level TOP_ONLY should be preserved")
	}
	if envMap["COMP_ONLY"] != "yes" {
		t.Error("component COMP_ONLY should be present")
	}
	if envMap["PYTHONPATH"] != "/app/superset/config" {
		t.Error("operator PYTHONPATH should be present")
	}

	// Command: from component (no inheritance)
	if len(ct.Command) != 2 || ct.Command[0] != "celery" {
		t.Error("expected command from component")
	}

	// Sidecars: from top-level (component didn't set any)
	if len(pt.Sidecars) != 1 || pt.Sidecars[0].Name != "vault-agent" {
		t.Error("expected sidecars from top-level")
	}
}

func TestResolveChildSpec_BeatSingleton(t *testing.T) {
	topLevel := &SharedInput{Replicas: Ptr(int32(4))}
	component := &ComponentInput{
		SharedInput: SharedInput{Replicas: Ptr(int32(3))},
	}

	result := ResolveChildSpec(ComponentCeleryBeat, topLevel, component, nil, nil)

	if result.Replicas != 1 {
		t.Errorf("beat must always be 1 replica, got %d", result.Replicas)
	}
}

func TestResolveChildSpec_BeatSingletonNilInputs(t *testing.T) {
	result := ResolveChildSpec(ComponentCeleryBeat, nil, nil, nil, nil)

	if result.Replicas != 1 {
		t.Errorf("beat must always be 1 replica even with nil inputs, got %d", result.Replicas)
	}
}

func TestResolveChildSpec_OperatorLabelsWithMinimalInput(t *testing.T) {
	operatorLabels := map[string]string{"app.kubernetes.io/name": "superset"}

	result := ResolveChildSpec(ComponentWebServer, nil, nil, operatorLabels, nil)

	if result.PodTemplate.Labels["app.kubernetes.io/name"] != "superset" {
		t.Errorf("expected operator labels, got %v", result.PodTemplate.Labels)
	}
}

func TestResolveChildSpec_OperatorInjectedMerged(t *testing.T) {
	topLevel := &SharedInput{
		PodTemplate: pt(
			&supersetv1alpha1.PodTemplate{
				Volumes: []corev1.Volume{
					{Name: "user-vol", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				},
			},
			&supersetv1alpha1.ContainerTemplate{
				Env: []corev1.EnvVar{{Name: "USER_VAR", Value: "user"}},
			},
		),
	}
	operator := &OperatorInjected{
		Env: []corev1.EnvVar{{Name: "OPERATOR_VAR", Value: "operator"}},
		Volumes: []corev1.Volume{
			{Name: "config", VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "cm1"},
				},
			}},
		},
		VolumeMounts:   []corev1.VolumeMount{{Name: "config", MountPath: "/app/superset/config"}},
		InitContainers: []corev1.Container{{Name: "op-init", Image: "init:op"}},
	}

	result := ResolveChildSpec(ComponentWebServer, topLevel, nil, nil, operator)
	pt := result.PodTemplate
	ct := pt.Container

	// Env: topLevel + operator
	foundEnv := make(map[string]string)
	for _, e := range ct.Env {
		foundEnv[e.Name] = e.Value
	}
	if foundEnv["USER_VAR"] != "user" || foundEnv["OPERATOR_VAR"] != "operator" {
		t.Errorf("expected merged env, got %v", foundEnv)
	}

	// Volumes: topLevel first, then operator (operator wins on conflict)
	if len(pt.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(pt.Volumes))
	}
	if pt.Volumes[0].Name != "user-vol" {
		t.Errorf("expected user volume first, got %s", pt.Volumes[0].Name)
	}
	if pt.Volumes[1].Name != "config" {
		t.Errorf("expected operator volume second, got %s", pt.Volumes[1].Name)
	}

	// VolumeMounts: operator injected
	if len(ct.VolumeMounts) != 1 || ct.VolumeMounts[0].MountPath != "/app/superset/config" {
		t.Error("expected operator volume mount")
	}

	// InitContainers: operator injected
	if len(pt.InitContainers) != 1 || pt.InitContainers[0].Name != "op-init" {
		t.Error("expected operator init container")
	}
}

func TestResolveChildSpec_OperatorVolumesWinOnConflict(t *testing.T) {
	topLevel := &SharedInput{
		PodTemplate: pt(
			&supersetv1alpha1.PodTemplate{
				Volumes: []corev1.Volume{
					{Name: "superset-config", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				},
			},
			&supersetv1alpha1.ContainerTemplate{
				VolumeMounts: []corev1.VolumeMount{{Name: "superset-config", MountPath: "/user/path"}},
			},
		),
	}
	operator := &OperatorInjected{
		Volumes: []corev1.Volume{
			{Name: "superset-config", VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "real-config"},
				},
			}},
		},
		VolumeMounts: []corev1.VolumeMount{{Name: "superset-config", MountPath: "/app/superset/config"}},
	}

	result := ResolveChildSpec(ComponentWebServer, topLevel, nil, nil, operator)
	pt := result.PodTemplate

	if len(pt.Volumes) != 1 {
		t.Fatalf("expected 1 volume (merged by name), got %d", len(pt.Volumes))
	}
	if pt.Volumes[0].ConfigMap == nil || pt.Volumes[0].ConfigMap.Name != "real-config" {
		t.Error("expected operator volume to win on name conflict")
	}

	ct := pt.Container
	if len(ct.VolumeMounts) != 1 {
		t.Fatalf("expected 1 volume mount (merged by name), got %d", len(ct.VolumeMounts))
	}
	if ct.VolumeMounts[0].MountPath != "/app/superset/config" {
		t.Errorf("expected operator mount path to win, got %s", ct.VolumeMounts[0].MountPath)
	}
}

func TestResolveChildSpec_DeploymentLevelFields(t *testing.T) {
	strategy := &appsv1.DeploymentStrategy{
		Type: appsv1.RollingUpdateDeploymentStrategyType,
	}
	topLevel := &SharedInput{
		DeploymentTemplate: &supersetv1alpha1.DeploymentTemplate{
			RevisionHistoryLimit: Ptr(int32(5)),
			Strategy:             strategy,
		},
	}

	result := ResolveChildSpec(ComponentWebServer, topLevel, nil, nil, nil)

	if result.DeploymentTemplate.RevisionHistoryLimit == nil || *result.DeploymentTemplate.RevisionHistoryLimit != 5 {
		t.Error("expected revisionHistoryLimit from topLevel")
	}
	if result.DeploymentTemplate.Strategy != strategy {
		t.Error("expected strategy from topLevel")
	}
}

func TestResolveChildSpec_ContainerCommandNotInherited(t *testing.T) {
	topLevel := &SharedInput{
		PodTemplate: pt(nil, &supersetv1alpha1.ContainerTemplate{
			Command: []string{"top-level-cmd"},
		}),
	}

	result := ResolveChildSpec(ComponentWebServer, topLevel, nil, nil, nil)
	ct := result.PodTemplate.Container

	// Command should NOT be inherited from top-level (it's component-only behavior)
	if len(ct.Command) != 0 {
		t.Errorf("expected no command inheritance from top-level, got %v", ct.Command)
	}
}

func TestResolveChildSpec_ContainerProbesFromComponent(t *testing.T) {
	probe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.FromInt(8088)},
		},
	}
	component := &ComponentInput{
		SharedInput: SharedInput{
			PodTemplate: pt(nil, &supersetv1alpha1.ContainerTemplate{
				LivenessProbe:  probe,
				ReadinessProbe: probe,
				StartupProbe:   probe,
				Command:        []string{"gunicorn"},
				Args:           []string{"--bind", "0.0.0.0:8088"},
			}),
		},
	}

	result := ResolveChildSpec(ComponentWebServer, nil, component, nil, nil)
	ct := result.PodTemplate.Container

	if ct.LivenessProbe != probe {
		t.Error("expected livenessProbe from component")
	}
	if ct.ReadinessProbe != probe {
		t.Error("expected readinessProbe from component")
	}
	if ct.StartupProbe != probe {
		t.Error("expected startupProbe from component")
	}
	if len(ct.Command) != 1 || ct.Command[0] != "gunicorn" {
		t.Error("expected command from component")
	}
	if len(ct.Args) != 2 || ct.Args[0] != "--bind" {
		t.Error("expected args from component")
	}
}

func TestResolveChildSpec_AllComponentTypes(t *testing.T) {
	types := []ComponentType{
		ComponentWebServer, ComponentCeleryWorker, ComponentCeleryBeat,
		ComponentCeleryFlower, ComponentWebsocketServer, ComponentMcpServer,
	}

	for _, ct := range types {
		t.Run(string(ct), func(t *testing.T) {
			result := ResolveChildSpec(ct, nil, nil, nil, nil)
			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if ct == ComponentCeleryBeat && result.Replicas != 1 {
				t.Errorf("beat should be 1 replica, got %d", result.Replicas)
			}
		})
	}
}

func TestResolveChildSpec_NewPodFields(t *testing.T) {
	gracePeriod := int64(120)
	dnsPolicy := corev1.DNSClusterFirstWithHostNet
	topLevel := &SharedInput{
		DeploymentTemplate: &supersetv1alpha1.DeploymentTemplate{
			MinReadySeconds:         Ptr(int32(10)),
			ProgressDeadlineSeconds: Ptr(int32(300)),
		},
		PodTemplate: pt(
			&supersetv1alpha1.PodTemplate{
				TerminationGracePeriodSeconds: &gracePeriod,
				DNSPolicy:                     &dnsPolicy,
				RuntimeClassName:              Ptr("gvisor"),
				ShareProcessNamespace:         Ptr(true),
				EnableServiceLinks:            Ptr(false),
			},
			&supersetv1alpha1.ContainerTemplate{
				Lifecycle: &corev1.Lifecycle{
					PreStop: &corev1.LifecycleHandler{
						Exec: &corev1.ExecAction{Command: []string{"sleep", "15"}},
					},
				},
			},
		),
	}

	result := ResolveChildSpec(ComponentWebServer, topLevel, nil, nil, nil)
	d := result.DeploymentTemplate
	podTpl := result.PodTemplate
	ct := podTpl.Container

	if d.MinReadySeconds == nil || *d.MinReadySeconds != 10 {
		t.Error("expected minReadySeconds")
	}
	if d.ProgressDeadlineSeconds == nil || *d.ProgressDeadlineSeconds != 300 {
		t.Error("expected progressDeadlineSeconds")
	}
	if *podTpl.TerminationGracePeriodSeconds != 120 {
		t.Error("expected terminationGracePeriodSeconds")
	}
	if *podTpl.DNSPolicy != dnsPolicy {
		t.Error("expected dnsPolicy")
	}
	if *podTpl.RuntimeClassName != "gvisor" {
		t.Error("expected runtimeClassName")
	}
	if *podTpl.ShareProcessNamespace != true {
		t.Error("expected shareProcessNamespace")
	}
	if *podTpl.EnableServiceLinks != false {
		t.Error("expected enableServiceLinks")
	}
	if ct.Lifecycle == nil || ct.Lifecycle.PreStop == nil {
		t.Error("expected lifecycle")
	}
}

func TestResolveChildSpec_AutoscalingPDB_Inheritance(t *testing.T) {
	topLevel := &SharedInput{
		Autoscaling:         &supersetv1alpha1.AutoscalingSpec{MaxReplicas: 10},
		PodDisruptionBudget: &supersetv1alpha1.PDBSpec{MinAvailable: &intstr.IntOrString{IntVal: 1}},
	}

	t.Run("inherited when component is nil", func(t *testing.T) {
		result := ResolveChildSpec(ComponentWebServer, topLevel, nil, nil, nil)
		if result.Autoscaling == nil || result.Autoscaling.MaxReplicas != 10 {
			t.Error("expected top-level autoscaling inherited")
		}
		if result.PodDisruptionBudget == nil || result.PodDisruptionBudget.MinAvailable == nil {
			t.Error("expected top-level PDB inherited")
		}
	})

	t.Run("component overrides top-level", func(t *testing.T) {
		component := &ComponentInput{
			SharedInput: SharedInput{
				Autoscaling:         &supersetv1alpha1.AutoscalingSpec{MaxReplicas: 20},
				PodDisruptionBudget: &supersetv1alpha1.PDBSpec{MaxUnavailable: &intstr.IntOrString{IntVal: 2}},
			},
		}
		result := ResolveChildSpec(ComponentCeleryWorker, topLevel, component, nil, nil)
		if result.Autoscaling.MaxReplicas != 20 {
			t.Errorf("expected component autoscaling, got maxReplicas=%d", result.Autoscaling.MaxReplicas)
		}
		if result.PodDisruptionBudget.MaxUnavailable == nil {
			t.Error("expected component PDB to override top-level")
		}
		if result.PodDisruptionBudget.MinAvailable != nil {
			t.Error("expected top-level PDB MinAvailable to be replaced, not merged")
		}
	})

	t.Run("nil when neither set", func(t *testing.T) {
		result := ResolveChildSpec(ComponentWebServer, &SharedInput{}, nil, nil, nil)
		if result.Autoscaling != nil {
			t.Error("expected nil autoscaling when neither set")
		}
		if result.PodDisruptionBudget != nil {
			t.Error("expected nil PDB when neither set")
		}
	})
}
