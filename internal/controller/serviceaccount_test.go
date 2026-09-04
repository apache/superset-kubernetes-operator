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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

func TestResolveServiceAccountName(t *testing.T) {
	tests := []struct {
		name string
		sa   *supersetv1alpha1.ServiceAccountSpec
		want string
	}{
		{
			name: "nil spec defaults to parent name",
			sa:   nil,
			want: "test",
		},
		{
			name: "create=true with explicit name uses the name",
			sa:   &supersetv1alpha1.ServiceAccountSpec{Create: boolPtr(true), Name: "custom-sa"},
			want: "custom-sa",
		},
		{
			name: "create=true without name defaults to parent name",
			sa:   &supersetv1alpha1.ServiceAccountSpec{Create: boolPtr(true)},
			want: "test",
		},
		{
			name: "create unset with name uses the name",
			sa:   &supersetv1alpha1.ServiceAccountSpec{Name: "custom-sa"},
			want: "custom-sa",
		},
		{
			name: "create=false with name references the existing SA",
			sa:   &supersetv1alpha1.ServiceAccountSpec{Create: boolPtr(false), Name: "external-sa"},
			want: "external-sa",
		},
		{
			// CEL rejects create=false without a name at the apiserver
			// (api/v1alpha1 ServiceAccountSpec XValidation), so this input never
			// reaches the controller in practice. resolveServiceAccountName stays
			// defensive about it and returns empty; we assert that fallback rather
			// than implying create=false-without-name is a supported config.
			name: "create=false without name (CEL-rejected; defensive empty fallback)",
			sa:   &supersetv1alpha1.ServiceAccountSpec{Create: boolPtr(false)},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			superset := &supersetv1alpha1.Superset{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec:       supersetv1alpha1.SupersetSpec{ServiceAccount: tt.sa},
			}
			assert.Equal(t, tt.want, resolveServiceAccountName(superset))
		})
	}
}

func supersetForSA(annotations map[string]string) *supersetv1alpha1.Superset {
	return &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "superset-uid"},
		Spec: supersetv1alpha1.SupersetSpec{
			ServiceAccount: &supersetv1alpha1.ServiceAccountSpec{Create: boolPtr(true), Annotations: annotations},
		},
	}
}

// TestReconcileServiceAccount_RefusesForeignOwnedAdoption verifies the operator
// never adopts, mutates, or garbage-collects a ServiceAccount at the derived
// name that is controller-owned by someone else. The check runs inside the
// mutate closure, so a foreign SA's annotations (e.g. cloud IAM bindings) are
// left intact.
func TestReconcileServiceAccount_RefusesForeignOwnedAdoption(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test",
			Namespace:   "default",
			UID:         "existing-sa-uid",
			Annotations: map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::123:role/keep"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1", Kind: "ServiceAccount", Name: "other",
				UID: "foreign-uid", Controller: boolPtr(true),
			}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	err := r.reconcileServiceAccount(ctx, supersetForSA(map[string]string{"foo": "bar"}))
	require.Error(t, err)

	got := &corev1.ServiceAccount{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: "test", Namespace: "default"}, got))
	assert.Equal(t, "arn:aws:iam::123:role/keep", got.Annotations["eks.amazonaws.com/role-arn"],
		"foreign SA annotations must not be wiped")
	assert.NotContains(t, got.Annotations, "foo", "operator must not mutate a foreign SA")
	require.Len(t, got.OwnerReferences, 1)
	assert.Equal(t, "foreign-uid", string(got.OwnerReferences[0].UID), "foreign owner reference must be untouched")
}

// TestReconcileServiceAccount_RefusesUnownedAdoption verifies the operator
// refuses to adopt a pre-existing, unowned ServiceAccount at the derived name.
func TestReconcileServiceAccount_RefusesUnownedAdoption(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test",
			Namespace:   "default",
			UID:         "existing-sa-uid",
			Annotations: map[string]string{"keep": "me"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	err := r.reconcileServiceAccount(ctx, supersetForSA(nil))
	require.Error(t, err)

	got := &corev1.ServiceAccount{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: "test", Namespace: "default"}, got))
	assert.Equal(t, "me", got.Annotations["keep"], "unowned SA must not be adopted or mutated")
	assert.Empty(t, got.OwnerReferences, "operator must not set an owner reference on an unowned SA")
}

// TestReconcileServiceAccount_UpdatesOwned verifies the operator still manages a
// ServiceAccount it controller-owns.
func TestReconcileServiceAccount_UpdatesOwned(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
			UID:       "existing-sa-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "superset.apache.org/v1alpha1", Kind: "Superset", Name: "test",
				UID: "superset-uid", Controller: boolPtr(true),
			}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	err := r.reconcileServiceAccount(ctx, supersetForSA(map[string]string{"foo": "bar"}))
	require.NoError(t, err)

	got := &corev1.ServiceAccount{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: "test", Namespace: "default"}, got))
	assert.Equal(t, "bar", got.Annotations["foo"], "owned SA should receive spec annotations")
}
