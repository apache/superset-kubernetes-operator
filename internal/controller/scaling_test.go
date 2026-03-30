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

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

var testLabels = map[string]string{
	common.LabelKeyName:      common.LabelValueApp,
	common.LabelKeyComponent: string(common.ComponentWebServer),
	common.LabelKeyInstance:  "test-web-server",
}

func TestReconcileHPA_CreatesHPA(t *testing.T) {
	scheme := testScheme(t)
	_ = autoscalingv2.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	owner := &supersetv1alpha1.SupersetWebServer{
		ObjectMeta: metav1.ObjectMeta{Name: "test-web-server", Namespace: "default", UID: "uid-1"},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()

	minReplicas := int32(2)
	autoscaling := &supersetv1alpha1.AutoscalingSpec{
		MinReplicas: &minReplicas,
		MaxReplicas: 10,
		Metrics: []autoscalingv2.MetricSpec{
			{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: corev1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: int32Ptr(75),
					},
				},
			},
		},
	}

	err := reconcileHPA(context.Background(), c, scheme, owner, autoscaling, testLabels, "test-web-server", "default")
	if err != nil {
		t.Fatalf("reconcileHPA: %v", err)
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, hpa); err != nil {
		t.Fatalf("expected HPA to exist: %v", err)
	}

	if hpa.Spec.MaxReplicas != 10 {
		t.Errorf("expected maxReplicas 10, got %d", hpa.Spec.MaxReplicas)
	}
	if *hpa.Spec.MinReplicas != 2 {
		t.Errorf("expected minReplicas 2, got %d", *hpa.Spec.MinReplicas)
	}
	if hpa.Spec.ScaleTargetRef.Kind != "Deployment" {
		t.Errorf("expected scaleTargetRef.Kind Deployment, got %s", hpa.Spec.ScaleTargetRef.Kind)
	}
	if len(hpa.Spec.Metrics) != 1 || hpa.Spec.Metrics[0].Type != autoscalingv2.ResourceMetricSourceType {
		t.Error("expected 1 Resource metric")
	}
	if hpa.Labels[common.LabelKeyComponent] != string(common.ComponentWebServer) {
		t.Errorf("expected component label on HPA, got %v", hpa.Labels)
	}
}

func TestReconcileHPA_NilAutoscaling(t *testing.T) {
	scheme := testScheme(t)
	_ = autoscalingv2.AddToScheme(scheme)

	owner := &supersetv1alpha1.SupersetWebServer{
		ObjectMeta: metav1.ObjectMeta{Name: "test-web-server", Namespace: "default", UID: "uid-1"},
	}

	t.Run("deletes labeled", func(t *testing.T) {
		existingHPA := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "test-web-server", Namespace: "default",
				Labels: testLabels,
			},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 5},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner, existingHPA).Build()

		if err := reconcileHPA(context.Background(), c, scheme, owner, nil, testLabels, "test-web-server", "default"); err != nil {
			t.Fatalf("reconcileHPA: %v", err)
		}

		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, hpa); !errors.IsNotFound(err) {
			t.Fatalf("expected HPA to be deleted, got: %v", err)
		}
	})

	t.Run("noop when not exists", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()
		if err := reconcileHPA(context.Background(), c, scheme, owner, nil, testLabels, "test-web-server", "default"); err != nil {
			t.Fatalf("expected no error: %v", err)
		}
	})
}

func TestReconcileHPA_CustomMetrics(t *testing.T) {
	scheme := testScheme(t)
	_ = autoscalingv2.AddToScheme(scheme)

	owner := &supersetv1alpha1.SupersetWebServer{
		ObjectMeta: metav1.ObjectMeta{Name: "test-web-server", Namespace: "default", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()

	autoscaling := &supersetv1alpha1.AutoscalingSpec{
		MaxReplicas: 20,
		Metrics: []autoscalingv2.MetricSpec{
			{
				Type: autoscalingv2.PodsMetricSourceType,
				Pods: &autoscalingv2.PodsMetricSource{
					Metric: autoscalingv2.MetricIdentifier{Name: "requests_per_second"},
					Target: autoscalingv2.MetricTarget{
						Type:         autoscalingv2.AverageValueMetricType,
						AverageValue: resource.NewQuantity(100, resource.DecimalSI),
					},
				},
			},
		},
	}

	if err := reconcileHPA(context.Background(), c, scheme, owner, autoscaling, testLabels, "test-web-server", "default"); err != nil {
		t.Fatalf("reconcileHPA: %v", err)
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, hpa); err != nil {
		t.Fatalf("expected HPA: %v", err)
	}

	if hpa.Spec.MaxReplicas != 20 {
		t.Errorf("expected maxReplicas 20, got %d", hpa.Spec.MaxReplicas)
	}
	if hpa.Spec.Metrics[0].Type != autoscalingv2.PodsMetricSourceType {
		t.Errorf("expected Pods metric, got %s", hpa.Spec.Metrics[0].Type)
	}
}

func TestReconcilePDB_CreatesPDB(t *testing.T) {
	scheme := testScheme(t)
	_ = policyv1.AddToScheme(scheme)

	owner := &supersetv1alpha1.SupersetWebServer{
		ObjectMeta: metav1.ObjectMeta{Name: "test-web-server", Namespace: "default", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()

	minAvailable := intstr.FromInt32(1)
	pdbSpec := &supersetv1alpha1.PDBSpec{MinAvailable: &minAvailable}
	labels := map[string]string{"app": "test"}

	if err := reconcilePDB(context.Background(), c, scheme, owner, pdbSpec, labels, "test-web-server", "default"); err != nil {
		t.Fatalf("reconcilePDB: %v", err)
	}

	pdb := &policyv1.PodDisruptionBudget{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, pdb); err != nil {
		t.Fatalf("expected PDB to exist: %v", err)
	}
	if pdb.Spec.MinAvailable.IntValue() != 1 {
		t.Errorf("expected minAvailable 1, got %v", pdb.Spec.MinAvailable)
	}
	if pdb.Spec.Selector == nil || pdb.Spec.Selector.MatchLabels["app"] != "test" {
		t.Errorf("expected selector with app=test label")
	}
}

func TestReconcilePDB_MaxUnavailable(t *testing.T) {
	scheme := testScheme(t)
	_ = policyv1.AddToScheme(scheme)

	owner := &supersetv1alpha1.SupersetWebServer{
		ObjectMeta: metav1.ObjectMeta{Name: "test-web-server", Namespace: "default", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()

	maxUnavailable := intstr.FromString("25%")
	pdbSpec := &supersetv1alpha1.PDBSpec{MaxUnavailable: &maxUnavailable}
	labels := map[string]string{"app": "test"}

	if err := reconcilePDB(context.Background(), c, scheme, owner, pdbSpec, labels, "test-web-server", "default"); err != nil {
		t.Fatalf("reconcilePDB: %v", err)
	}

	pdb := &policyv1.PodDisruptionBudget{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, pdb); err != nil {
		t.Fatalf("expected PDB: %v", err)
	}
	if pdb.Spec.MaxUnavailable.String() != "25%" {
		t.Errorf("expected maxUnavailable 25%%, got %v", pdb.Spec.MaxUnavailable)
	}
}

func TestReconcilePDB_NilSpec(t *testing.T) {
	scheme := testScheme(t)
	_ = policyv1.AddToScheme(scheme)

	owner := &supersetv1alpha1.SupersetWebServer{
		ObjectMeta: metav1.ObjectMeta{Name: "test-web-server", Namespace: "default", UID: "uid-1"},
	}
	labels := map[string]string{"app": "test"}

	t.Run("deletes labeled", func(t *testing.T) {
		existingPDB := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: "test-web-server", Namespace: "default",
				Labels: labels,
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner, existingPDB).Build()

		if err := reconcilePDB(context.Background(), c, scheme, owner, nil, labels, "test-web-server", "default"); err != nil {
			t.Fatalf("reconcilePDB: %v", err)
		}

		pdb := &policyv1.PodDisruptionBudget{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: "test-web-server", Namespace: "default"}, pdb); !errors.IsNotFound(err) {
			t.Fatalf("expected PDB to be deleted, got: %v", err)
		}
	})

	t.Run("noop when not exists", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()
		if err := reconcilePDB(context.Background(), c, scheme, owner, nil, labels, "test-web-server", "default"); err != nil {
			t.Fatalf("expected no error: %v", err)
		}
	})
}

func int32Ptr(i int32) *int32 { return &i }
