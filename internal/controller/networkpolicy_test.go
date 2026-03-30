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

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestReconcileNetworkPolicies_Disabled(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       minimalSupersetSpec(),
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileNetworkPolicies(context.Background(), superset); err != nil {
		t.Fatalf("reconcileNetworkPolicies: %v", err)
	}
}

func TestReconcileNetworkPolicies_CreatesForEnabledComponents(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:         supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer:     &supersetv1alpha1.WebServerComponentSpec{},
			CeleryBeat:    &supersetv1alpha1.CeleryBeatComponentSpec{},
			Init:          &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			NetworkPolicy: &supersetv1alpha1.NetworkPolicySpec{},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileNetworkPolicies(context.Background(), superset); err != nil {
		t.Fatalf("reconcileNetworkPolicies: %v", err)
	}

	// WebServer NP should exist.
	wsNP := &networkingv1.NetworkPolicy{}
	wsNPName := common.ChildName("test", common.SuffixWebServer) + common.SuffixNetworkPolicy
	if err := c.Get(context.Background(), types.NamespacedName{Name: wsNPName, Namespace: "default"}, wsNP); err != nil {
		t.Fatalf("expected web server NetworkPolicy: %v", err)
	}

	// CeleryBeat NP should exist.
	beatNPName := common.ChildName("test", common.SuffixCeleryBeat) + common.SuffixNetworkPolicy
	if err := c.Get(context.Background(), types.NamespacedName{Name: beatNPName, Namespace: "default"}, &networkingv1.NetworkPolicy{}); err != nil {
		t.Fatalf("expected celery beat NetworkPolicy: %v", err)
	}

	// CeleryWorker NP should NOT exist (component not enabled).
	workerNPName := common.ChildName("test", common.SuffixCeleryWorker) + common.SuffixNetworkPolicy
	err := c.Get(context.Background(), types.NamespacedName{Name: workerNPName, Namespace: "default"}, &networkingv1.NetworkPolicy{})
	if !errors.IsNotFound(err) {
		t.Fatalf("expected celery worker NetworkPolicy to not exist, got: %v", err)
	}
}

func TestReconcileComponentNetworkPolicy_WebServer(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:         supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer:     &supersetv1alpha1.WebServerComponentSpec{},
			Init:          &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			NetworkPolicy: &supersetv1alpha1.NetworkPolicySpec{},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	npName := "test-web-server" + common.SuffixNetworkPolicy
	err := r.reconcileComponentNetworkPolicy(
		context.Background(), superset, npName,
		string(common.ComponentWebServer), "test", common.PortWebServer,
	)
	if err != nil {
		t.Fatalf("reconcileComponentNetworkPolicy: %v", err)
	}

	np := &networkingv1.NetworkPolicy{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: npName, Namespace: "default"}, np); err != nil {
		t.Fatalf("expected NetworkPolicy: %v", err)
	}

	// Verify pod selector uses the child CR name (not resourceBaseName) so it
	// matches the labels on Deployment pod templates.
	if np.Spec.PodSelector.MatchLabels[common.LabelKeyComponent] != string(common.ComponentWebServer) {
		t.Errorf("expected component label %s, got %s", common.ComponentWebServer, np.Spec.PodSelector.MatchLabels[common.LabelKeyComponent])
	}
	if np.Spec.PodSelector.MatchLabels[common.LabelKeyInstance] != "test" {
		t.Errorf("expected instance label %q (child CR name), got %q", "test", np.Spec.PodSelector.MatchLabels[common.LabelKeyInstance])
	}

	// Verify policy types.
	hasIngress, hasEgress := false, false
	for _, pt := range np.Spec.PolicyTypes {
		if pt == networkingv1.PolicyTypeIngress {
			hasIngress = true
		}
		if pt == networkingv1.PolicyTypeEgress {
			hasEgress = true
		}
	}
	if !hasIngress || !hasEgress {
		t.Errorf("expected both Ingress and Egress policy types")
	}

	// Verify egress allows all.
	if len(np.Spec.Egress) != 1 {
		t.Fatalf("expected 1 egress rule (allow all), got %d", len(np.Spec.Egress))
	}

	// For web server (external port), should have 2 ingress rules:
	// 1. Inter-component communication
	// 2. External access on port
	if len(np.Spec.Ingress) != 2 {
		t.Fatalf("expected 2 ingress rules for external component, got %d", len(np.Spec.Ingress))
	}

	// First rule: from same-instance superset pods.
	interComponentLabels := np.Spec.Ingress[0].From[0].PodSelector.MatchLabels
	if interComponentLabels[common.LabelKeyName] != common.LabelValueApp {
		t.Errorf("expected inter-component ingress rule with app name label")
	}
	if interComponentLabels[common.LabelKeyParent] != "test" {
		t.Errorf("expected inter-component ingress rule scoped to parent, got %v", interComponentLabels)
	}

	// Second rule: external port.
	if len(np.Spec.Ingress[1].Ports) != 1 {
		t.Fatalf("expected 1 port in external ingress rule, got %d", len(np.Spec.Ingress[1].Ports))
	}
	if np.Spec.Ingress[1].Ports[0].Port.IntValue() != int(common.PortWebServer) {
		t.Errorf("expected port %d, got %d", common.PortWebServer, np.Spec.Ingress[1].Ports[0].Port.IntValue())
	}
	if *np.Spec.Ingress[1].Ports[0].Protocol != corev1.ProtocolTCP {
		t.Errorf("expected TCP protocol")
	}
}

func TestReconcileNetworkPolicies_CustomContainerPort(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image: supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
					PodTemplate: &supersetv1alpha1.PodTemplate{
						Container: &supersetv1alpha1.ContainerTemplate{
							Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 9090}},
						},
					},
				},
			},
			Init:          &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			NetworkPolicy: &supersetv1alpha1.NetworkPolicySpec{},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileNetworkPolicies(context.Background(), superset); err != nil {
		t.Fatalf("reconcileNetworkPolicies: %v", err)
	}

	np := &networkingv1.NetworkPolicy{}
	npName := common.ChildName("test", common.SuffixWebServer) + common.SuffixNetworkPolicy
	if err := c.Get(context.Background(), types.NamespacedName{Name: npName, Namespace: "default"}, np); err != nil {
		t.Fatalf("expected NetworkPolicy: %v", err)
	}

	// External ingress rule should use the custom container port, not the default.
	if len(np.Spec.Ingress) < 2 {
		t.Fatalf("expected at least 2 ingress rules, got %d", len(np.Spec.Ingress))
	}
	if np.Spec.Ingress[1].Ports[0].Port.IntValue() != 9090 {
		t.Errorf("expected custom port 9090, got %d", np.Spec.Ingress[1].Ports[0].Port.IntValue())
	}
}

func TestNpContainerPort(t *testing.T) {
	t.Run("default when no overrides", func(t *testing.T) {
		got := npContainerPort(8088, nil, nil)
		if got != 8088 {
			t.Errorf("expected 8088, got %d", got)
		}
	})
	t.Run("component override wins", func(t *testing.T) {
		compPT := &supersetv1alpha1.PodTemplate{
			Container: &supersetv1alpha1.ContainerTemplate{
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 9090}},
			},
		}
		got := npContainerPort(8088, nil, compPT)
		if got != 9090 {
			t.Errorf("expected 9090, got %d", got)
		}
	})
	t.Run("top-level fallback", func(t *testing.T) {
		topPT := &supersetv1alpha1.PodTemplate{
			Container: &supersetv1alpha1.ContainerTemplate{
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 7070}},
			},
		}
		got := npContainerPort(8088, topPT, nil)
		if got != 7070 {
			t.Errorf("expected 7070, got %d", got)
		}
	})
	t.Run("component replaces top-level by name", func(t *testing.T) {
		topPT := &supersetv1alpha1.PodTemplate{
			Container: &supersetv1alpha1.ContainerTemplate{
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8088}},
			},
		}
		compPT := &supersetv1alpha1.PodTemplate{
			Container: &supersetv1alpha1.ContainerTemplate{
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 9090}},
			},
		}
		got := npContainerPort(8088, topPT, compPT)
		if got != 9090 {
			t.Errorf("expected 9090 (component overrides top-level by name), got %d", got)
		}
	})
}

func TestReconcileComponentNetworkPolicy_InternalOnly(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:         supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			CeleryWorker:  &supersetv1alpha1.CeleryWorkerComponentSpec{},
			Init:          &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			NetworkPolicy: &supersetv1alpha1.NetworkPolicySpec{},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	npName := "test-celery-worker" + common.SuffixNetworkPolicy
	// Port 0 means internal only — no external ingress rule.
	err := r.reconcileComponentNetworkPolicy(
		context.Background(), superset, npName,
		string(common.ComponentCeleryWorker), "test", 0,
	)
	if err != nil {
		t.Fatalf("reconcileComponentNetworkPolicy: %v", err)
	}

	np := &networkingv1.NetworkPolicy{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: npName, Namespace: "default"}, np); err != nil {
		t.Fatalf("expected NetworkPolicy: %v", err)
	}

	// Internal component should have only 1 ingress rule (inter-component).
	if len(np.Spec.Ingress) != 1 {
		t.Fatalf("expected 1 ingress rule for internal component, got %d", len(np.Spec.Ingress))
	}
}

func TestReconcileNetworkPolicies_ExtraRules(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Init:      &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			NetworkPolicy: &supersetv1alpha1.NetworkPolicySpec{
				ExtraIngress: []networkingv1.NetworkPolicyIngressRule{
					{
						From: []networkingv1.NetworkPolicyPeer{
							{
								NamespaceSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"team": "monitoring"},
								},
							},
						},
					},
				},
				ExtraEgress: []networkingv1.NetworkPolicyEgressRule{
					{
						To: []networkingv1.NetworkPolicyPeer{
							{
								NamespaceSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"team": "database"},
								},
							},
						},
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	npName := "test-web-server" + common.SuffixNetworkPolicy
	err := r.reconcileComponentNetworkPolicy(
		context.Background(), superset, npName,
		string(common.ComponentWebServer), "test", common.PortWebServer,
	)
	if err != nil {
		t.Fatalf("reconcileComponentNetworkPolicy: %v", err)
	}

	np := &networkingv1.NetworkPolicy{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: npName, Namespace: "default"}, np); err != nil {
		t.Fatalf("expected NetworkPolicy: %v", err)
	}

	// Should have 3 ingress rules: inter-component + external port + extra.
	if len(np.Spec.Ingress) != 3 {
		t.Fatalf("expected 3 ingress rules, got %d", len(np.Spec.Ingress))
	}
	// Should have 2 egress rules: allow-all + extra.
	if len(np.Spec.Egress) != 2 {
		t.Fatalf("expected 2 egress rules, got %d", len(np.Spec.Egress))
	}
}

func TestReconcileNetworkPolicies_DeletesWhenDisabled(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       minimalSupersetSpec(),
	}

	// Pre-create a NetworkPolicy with operator-managed labels.
	existingNP := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-web-server" + common.SuffixNetworkPolicy,
			Namespace: "default",
			Labels: map[string]string{
				common.LabelKeyName:      common.LabelValueApp,
				common.LabelKeyComponent: string(common.ComponentWebServer),
				common.LabelKeyParent:    "test",
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, existingNP).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileNetworkPolicies(context.Background(), superset); err != nil {
		t.Fatalf("reconcileNetworkPolicies: %v", err)
	}

	// NP should be cleaned up.
	np := &networkingv1.NetworkPolicy{}
	npName := "test-web-server" + common.SuffixNetworkPolicy
	err := c.Get(context.Background(), types.NamespacedName{Name: npName, Namespace: "default"}, np)
	if !errors.IsNotFound(err) {
		t.Fatalf("expected NetworkPolicy to be deleted, got: %v", err)
	}
}

func TestNetworkPolicySelectorMatchesDeploymentLabels(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:         supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer:     &supersetv1alpha1.WebServerComponentSpec{},
			Init:          &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			NetworkPolicy: &supersetv1alpha1.NetworkPolicySpec{},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).
		WithStatusSubresource(&supersetv1alpha1.Superset{}, &supersetv1alpha1.SupersetWebServer{}).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileNetworkPolicies(context.Background(), superset); err != nil {
		t.Fatalf("reconcileNetworkPolicies: %v", err)
	}

	npName := "test-web-server" + common.SuffixNetworkPolicy
	np := &networkingv1.NetworkPolicy{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: npName, Namespace: "default"}, np); err != nil {
		t.Fatalf("expected NetworkPolicy: %v", err)
	}

	// The pod selector must use the child CR name (defaults to parent name),
	// not the resourceBaseName, to match the labels on Deployment pod templates.
	deployLabels := componentLabels(string(common.ComponentWebServer), superset.Name)
	for k, v := range np.Spec.PodSelector.MatchLabels {
		if deployLabels[k] != v {
			t.Errorf("NetworkPolicy selector label %s=%s does not match Deployment pod label %s=%s", k, v, k, deployLabels[k])
		}
	}
}
