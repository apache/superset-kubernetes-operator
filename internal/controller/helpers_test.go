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
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestComponentLabels(t *testing.T) {
	labels := componentLabels(string(common.ComponentWebServer), "my-superset-web-server")

	if labels[common.LabelKeyName] != common.LabelValueApp {
		t.Errorf("expected name=%s, got %s", common.LabelValueApp, labels[common.LabelKeyName])
	}
	if labels[common.LabelKeyComponent] != string(common.ComponentWebServer) {
		t.Errorf("expected component=%s, got %s", string(common.ComponentWebServer), labels[common.LabelKeyComponent])
	}
	if labels[common.LabelKeyInstance] != "my-superset-web-server" {
		t.Errorf("expected instance=my-superset-web-server, got %s", labels[common.LabelKeyInstance])
	}
}

func TestMergeLabels(t *testing.T) {
	base := map[string]string{"a": "1", "b": "2"}
	extra := map[string]string{"b": "overridden", "c": "3"}

	merged := mergeLabels(base, extra)
	if len(merged) != 3 {
		t.Fatalf("expected 3 labels, got %d", len(merged))
	}
	if merged["a"] != "1" {
		t.Errorf("expected a=1, got %s", merged["a"])
	}
	if merged["b"] != "overridden" {
		t.Errorf("expected b=overridden, got %s", merged["b"])
	}

	// Nil extra returns base labels.
	merged2 := mergeLabels(base, nil)
	if merged2["a"] != "1" {
		t.Errorf("expected base labels returned for nil extra, got %v", merged2)
	}

	// Both nil/empty must return a non-nil empty map (required for label selectors).
	merged3 := mergeLabels(nil, nil)
	if merged3 == nil {
		t.Fatal("expected non-nil empty map for both-nil input")
	}
	if len(merged3) != 0 {
		t.Errorf("expected empty map, got %v", merged3)
	}
}

func TestMergeAnnotations(t *testing.T) {
	base := map[string]string{"a": "1"}
	extra := map[string]string{"b": "2"}

	merged := mergeAnnotations(base, extra)
	if len(merged) != 2 {
		t.Fatalf("expected 2 annotations, got %d", len(merged))
	}

	// Both nil returns nil.
	if mergeAnnotations(nil, nil) != nil {
		t.Error("expected nil for both-nil input")
	}
}

func TestDeleteIfNotForeignOwned(t *testing.T) {
	// Regression test: name-derived cleanup must never delete a resource that
	// is controller-owned by a foreign owner, even when it collides with a
	// managed name. Unowned and CR-owned resources are still cleaned up.
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	name := common.ResourceBaseName("test", common.ComponentWebServer)

	newDeploy := func(refs []metav1.OwnerReference) *appsv1.Deployment {
		return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", OwnerReferences: refs,
		}}
	}
	get := func(c client.Client) error {
		return c.Get(ctx, client.ObjectKey{Name: name, Namespace: "default"}, &appsv1.Deployment{})
	}

	t.Run("deletes unowned resource at managed name", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(newDeploy(nil)).Build()
		if err := deleteIfNotForeignOwned(ctx, c, superset, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		}); err != nil {
			t.Fatalf("deleteIfNotForeignOwned: %v", err)
		}
		if err := get(c); !errors.IsNotFound(err) {
			t.Errorf("expected unowned Deployment deleted, got %v", err)
		}
	})

	t.Run("deletes resource owned by the CR", func(t *testing.T) {
		owned := newDeploy([]metav1.OwnerReference{{
			APIVersion: supersetv1alpha1.GroupVersion.String(), Kind: "Superset",
			Name: "test", UID: "uid-1", Controller: boolPtr(true),
		}})
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owned).Build()
		if err := deleteIfNotForeignOwned(ctx, c, superset, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		}); err != nil {
			t.Fatalf("deleteIfNotForeignOwned: %v", err)
		}
		if err := get(c); !errors.IsNotFound(err) {
			t.Errorf("expected CR-owned Deployment deleted, got %v", err)
		}
	})

	t.Run("skips resource controller-owned by a foreign owner", func(t *testing.T) {
		foreign := newDeploy([]metav1.OwnerReference{{
			APIVersion: "apps.example.com/v1", Kind: "ForeignApp",
			Name: "billing", UID: "foreign-uid", Controller: boolPtr(true),
		}})
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()
		if err := deleteIfNotForeignOwned(ctx, c, superset, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		}); err != nil {
			t.Fatalf("deleteIfNotForeignOwned: %v", err)
		}
		if err := get(c); err != nil {
			t.Errorf("expected foreign controller-owned Deployment preserved, got %v", err)
		}
	})

	t.Run("missing object is not an error", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		if err := deleteIfNotForeignOwned(ctx, c, superset, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		}); err != nil {
			t.Fatalf("deleteIfNotForeignOwned: %v", err)
		}
	})
}
