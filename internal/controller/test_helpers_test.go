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

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// Test helper functions shared across all test files.

func boolPtr(b bool) *bool { return &b }

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
	if err := autoscalingv2.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(autoscalingv2): %v", err)
	}
	if err := policyv1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(policyv1): %v", err)
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
		WithStatusSubresource(&supersetv1alpha1.Superset{})
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
