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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// configMapsGR is the GroupResource used to construct realistic Conflict errors
// in these tests (ConfigMaps live in the core/"" group).
var configMapsGR = schema.GroupResource{Group: "", Resource: "configmaps"}

func newTestConfigMap(data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm", Namespace: "default"},
		Data:       data,
	}
}

// setData is a MutateFn that overwrites the ConfigMap's Data with the given map.
func setData(cm *corev1.ConfigMap, data map[string]string) controllerutil.MutateFn {
	return func() error {
		cm.Data = data
		return nil
	}
}

func TestCreateOrUpdateWithRetry_Create(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()

	cm := newTestConfigMap(nil)
	op, err := createOrUpdateWithRetry(ctx, c, cm, setData(cm, map[string]string{"k": "v"}))

	require.NoError(t, err)
	assert.Equal(t, controllerutil.OperationResultCreated, op)

	got := &corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cm), got))
	assert.Equal(t, "v", got.Data["k"])
}

func TestCreateOrUpdateWithRetry_Update(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(newTestConfigMap(map[string]string{"k": "old"})).Build()

	cm := newTestConfigMap(nil)
	op, err := createOrUpdateWithRetry(ctx, c, cm, setData(cm, map[string]string{"k": "new"}))

	require.NoError(t, err)
	assert.Equal(t, controllerutil.OperationResultUpdated, op)

	got := &corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cm), got))
	assert.Equal(t, "new", got.Data["k"])
}

func TestCreateOrUpdateWithRetry_NoOp(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(newTestConfigMap(map[string]string{"k": "v"})).Build()

	cm := newTestConfigMap(nil)
	op, err := createOrUpdateWithRetry(ctx, c, cm, setData(cm, map[string]string{"k": "v"}))

	require.NoError(t, err)
	assert.Equal(t, controllerutil.OperationResultNone, op)
}

func TestCreateOrUpdateWithRetry_RetriesConflictThenSucceeds(t *testing.T) {
	ctx := context.Background()
	updateCalls := 0
	base := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(newTestConfigMap(map[string]string{"k": "old"})).Build()
	c := interceptor.NewClient(base, interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			updateCalls++
			if updateCalls == 1 {
				// First write loses the optimistic-lock race.
				return apierrors.NewConflict(configMapsGR, obj.GetName(), errors.New("resourceVersion changed"))
			}
			return w.Update(ctx, obj, opts...)
		},
	})

	cm := newTestConfigMap(nil)
	op, err := createOrUpdateWithRetry(ctx, c, cm, setData(cm, map[string]string{"k": "new"}))

	require.NoError(t, err)
	assert.Equal(t, controllerutil.OperationResultUpdated, op)
	assert.Equal(t, 2, updateCalls, "should retry exactly once after the conflict")

	got := &corev1.ConfigMap{}
	require.NoError(t, base.Get(ctx, client.ObjectKeyFromObject(cm), got))
	assert.Equal(t, "new", got.Data["k"], "the mutation must be reapplied on the successful retry")
}

func TestCreateOrUpdateWithRetry_DoesNotRetryNonConflictError(t *testing.T) {
	ctx := context.Background()
	updateCalls := 0
	sentinel := apierrors.NewInternalError(errors.New("boom"))
	base := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(newTestConfigMap(map[string]string{"k": "old"})).Build()
	c := interceptor.NewClient(base, interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			updateCalls++
			return sentinel
		},
	})

	cm := newTestConfigMap(nil)
	op, err := createOrUpdateWithRetry(ctx, c, cm, setData(cm, map[string]string{"k": "new"}))

	require.Error(t, err)
	assert.False(t, apierrors.IsConflict(err), "non-conflict error should be returned as-is")
	assert.Equal(t, controllerutil.OperationResultNone, op)
	assert.Equal(t, 1, updateCalls, "non-conflict errors must not be retried")
}

func TestCreateOrUpdateWithRetry_ReturnsConflictAfterExhaustingRetries(t *testing.T) {
	ctx := context.Background()
	updateCalls := 0
	base := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(newTestConfigMap(map[string]string{"k": "old"})).Build()
	c := interceptor.NewClient(base, interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			updateCalls++
			return apierrors.NewConflict(configMapsGR, obj.GetName(), errors.New("persistent conflict"))
		},
	})

	cm := newTestConfigMap(nil)
	op, err := createOrUpdateWithRetry(ctx, c, cm, setData(cm, map[string]string{"k": "new"}))

	require.Error(t, err)
	assert.True(t, apierrors.IsConflict(err), "a persistent conflict must surface as a conflict error")
	assert.Equal(t, controllerutil.OperationResultNone, op)
	assert.Equal(t, retry.DefaultRetry.Steps, updateCalls, "should attempt exactly the configured number of retry steps")
}
