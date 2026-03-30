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

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestReconcileMonitoring_GracefulSkipWhenCRDAbsent(t *testing.T) {
	scheme := testScheme(t)

	interval := "60s"
	scrapeTimeout := "10s"
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Init:      &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			Monitoring: &supersetv1alpha1.MonitoringSpec{
				ServiceMonitor: &supersetv1alpha1.ServiceMonitorSpec{
					Interval:      &interval,
					ScrapeTimeout: &scrapeTimeout,
					Labels:        map[string]string{"release": "prometheus"},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	// ServiceMonitor CRD is not registered in fake scheme — reconcileMonitoring
	// should gracefully skip (return nil for NoMatchError).
	err := r.reconcileMonitoring(context.Background(), superset)
	if err != nil {
		t.Fatalf("expected graceful skip when CRD not installed, got: %v", err)
	}
}

func TestReconcileMonitoring_ServiceMonitorShape(t *testing.T) {
	scheme := testScheme(t)

	interval := "60s"
	scrapeTimeout := "10s"
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:     supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
			WebServer: &supersetv1alpha1.WebServerComponentSpec{},
			Init:      &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			Monitoring: &supersetv1alpha1.MonitoringSpec{
				ServiceMonitor: &supersetv1alpha1.ServiceMonitorSpec{
					Interval:      &interval,
					ScrapeTimeout: &scrapeTimeout,
					Labels:        map[string]string{"release": "prometheus"},
				},
			},
		},
	}

	// Pre-seed an unstructured ServiceMonitor so the fake client registers the GVK.
	seed := &unstructured.Unstructured{}
	seed.SetGroupVersionKind(serviceMonitorGVK)
	seed.SetName("test")
	seed.SetNamespace("default")

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, seed).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.reconcileMonitoring(context.Background(), superset); err != nil {
		t.Fatalf("reconcileMonitoring: %v", err)
	}

	sm := &unstructured.Unstructured{}
	sm.SetGroupVersionKind(serviceMonitorGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, sm); err != nil {
		t.Fatalf("get ServiceMonitor: %v", err)
	}

	spec, ok := sm.Object["spec"].(map[string]interface{})
	if !ok {
		t.Fatal("ServiceMonitor missing spec")
	}

	// Verify selector targets web-server component.
	selector, _ := spec["selector"].(map[string]interface{})
	matchLabels, _ := selector["matchLabels"].(map[string]interface{})
	if matchLabels["app.kubernetes.io/component"] != string(common.ComponentWebServer) {
		t.Errorf("selector component should be %q, got %v", common.ComponentWebServer, matchLabels["app.kubernetes.io/component"])
	}
	if matchLabels["app.kubernetes.io/instance"] != "test" {
		t.Errorf("selector instance should be test, got %v", matchLabels["app.kubernetes.io/instance"])
	}
	if matchLabels["app.kubernetes.io/name"] != "superset" {
		t.Errorf("selector name should be superset, got %v", matchLabels["app.kubernetes.io/name"])
	}

	// Verify endpoints shape.
	endpoints, _ := spec["endpoints"].([]interface{})
	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(endpoints))
	}
	ep, _ := endpoints[0].(map[string]interface{})
	if ep["port"] != common.PortNameHTTP {
		t.Errorf("endpoint port should be %q, got %v", common.PortNameHTTP, ep["port"])
	}
	if ep["interval"] != "60s" {
		t.Errorf("endpoint interval should be 60s, got %v", ep["interval"])
	}
	if ep["scrapeTimeout"] != "10s" {
		t.Errorf("endpoint scrapeTimeout should be 10s, got %v", ep["scrapeTimeout"])
	}

	// Verify namespace selector.
	nsSelector, _ := spec["namespaceSelector"].(map[string]interface{})
	matchNames, _ := nsSelector["matchNames"].([]interface{})
	if len(matchNames) != 1 || matchNames[0] != "default" {
		t.Errorf("namespaceSelector should match [default], got %v", matchNames)
	}

	// Verify labels include user labels and operator labels.
	labels := sm.GetLabels()
	if labels["release"] != "prometheus" {
		t.Error("expected user label release=prometheus")
	}
	if labels[common.LabelKeyName] != common.LabelValueApp {
		t.Error("expected operator label app.kubernetes.io/name=superset")
	}
	if labels[common.LabelKeyParent] != "test" {
		t.Error("expected operator label superset.apache.org/parent=test")
	}
}

func TestDeleteServiceMonitors(t *testing.T) {
	scheme := testScheme(t)

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       minimalSupersetSpec(),
	}

	t.Run("not found", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.deleteServiceMonitors(context.Background(), superset); err != nil {
			t.Fatalf("expected no error: %v", err)
		}
	})

	t.Run("deletes labeled", func(t *testing.T) {
		sm := &unstructured.Unstructured{}
		sm.SetGroupVersionKind(serviceMonitorGVK)
		sm.SetName("test")
		sm.SetNamespace("default")
		sm.SetLabels(parentLabels("test"))

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, sm).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.deleteServiceMonitors(context.Background(), superset); err != nil {
			t.Fatalf("deleteServiceMonitors: %v", err)
		}

		check := &unstructured.Unstructured{}
		check.SetGroupVersionKind(serviceMonitorGVK)
		err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, check)
		if !errors.IsNotFound(err) {
			t.Fatalf("expected ServiceMonitor to be deleted, got: %v", err)
		}
	})

	t.Run("skips unlabeled", func(t *testing.T) {
		sm := &unstructured.Unstructured{}
		sm.SetGroupVersionKind(serviceMonitorGVK)
		sm.SetName("test")
		sm.SetNamespace("default")

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(superset, sm).Build()
		r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.deleteServiceMonitors(context.Background(), superset); err != nil {
			t.Fatalf("deleteServiceMonitors: %v", err)
		}

		check := &unstructured.Unstructured{}
		check.SetGroupVersionKind(serviceMonitorGVK)
		err := c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, check)
		if err != nil {
			t.Fatalf("expected unlabeled ServiceMonitor to be preserved, got: %v", err)
		}
	})
}
