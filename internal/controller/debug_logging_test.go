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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

func TestDebugLoggingTargets(t *testing.T) {
	newSuperset := func() *supersetv1alpha1.Superset {
		return &supersetv1alpha1.Superset{
			Name: "test",
			Spec: supersetv1alpha1.SupersetSpec{
				WebServer:    &supersetv1alpha1.WebServerComponentSpec{},
				CeleryWorker: &supersetv1alpha1.CeleryWorkerComponentSpec{},
				WebsocketServer: &supersetv1alpha1.WebsocketServerComponentSpec{
					Image: &supersetv1alpha1.ImageOverrideSpec{Repository: new("example.com/superset-websocket")},
				},
			},
		}
	}

	tests := []struct {
		name   string
		mutate func(*supersetv1alpha1.Superset)
		want   []string
	}{
		{
			name:   "no level set",
			mutate: func(*supersetv1alpha1.Superset) {},
			want:   nil,
		},
		{
			// The websocket server has no typed logLevel and ignores the top-level value.
			name: "top-level debug is inherited by Python components and lifecycle",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.LogLevel = new(supersetv1alpha1.LogLevelDebug)
			},
			want: []string{"web-server", "celery-worker", "lifecycle"},
		},
		{
			name: "component override wins over top-level debug",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.LogLevel = new(supersetv1alpha1.LogLevelDebug)
				s.Spec.CeleryWorker.LogLevel = new(supersetv1alpha1.LogLevelWarning)
				s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{LogLevel: new(supersetv1alpha1.LogLevelInfo)}
			},
			want: []string{"web-server"},
		},
		{
			name: "component-level debug only",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.CeleryFlower = &supersetv1alpha1.CeleryFlowerComponentSpec{LogLevel: new(supersetv1alpha1.LogLevelDebug)}
			},
			want: []string{"celery-flower"},
		},
		{
			name: "lifecycle-level debug only",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{LogLevel: new(supersetv1alpha1.LogLevelDebug)}
			},
			want: []string{"lifecycle"},
		},
		{
			name: "disabled lifecycle is not reported",
			mutate: func(s *supersetv1alpha1.Superset) {
				s.Spec.LogLevel = new(supersetv1alpha1.LogLevelDebug)
				s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{Disabled: new(true)}
			},
			want: []string{"web-server", "celery-worker"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSuperset()
			tt.mutate(s)
			assert.Equal(t, tt.want, debugLoggingTargets(s))
		})
	}
}

func TestReconcileDebugLoggingCondition_Environments(t *testing.T) {
	tests := []struct {
		name        string
		environment supersetv1alpha1.Environment
		wantSet     bool
	}{
		{"production", supersetv1alpha1.EnvironmentProduction, true},
		{"staging", supersetv1alpha1.EnvironmentStaging, true},
		{"development", supersetv1alpha1.EnvironmentDevelopment, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			superset := &supersetv1alpha1.Superset{
				Name: "test",
				Spec: supersetv1alpha1.SupersetSpec{
					Environment: &tt.environment,
					LogLevel:    new(supersetv1alpha1.LogLevelDebug),
					WebServer:   &supersetv1alpha1.WebServerComponentSpec{},
					Lifecycle:   &supersetv1alpha1.LifecycleSpec{Disabled: new(true)},
				},
			}
			recorder := events.NewFakeRecorder(10)
			r := &SupersetReconciler{Recorder: recorder}

			r.reconcileDebugLoggingCondition(superset, superset.DeepCopy())

			cond := apimeta.FindStatusCondition(superset.Status.Conditions, "DebugLogging")
			if !tt.wantSet {
				assert.Nil(t, cond)
				assertNoEvents(t, recorder)
				return
			}
			require.NotNil(t, cond)
			assert.Equal(t, metav1.ConditionTrue, cond.Status)
			assert.Equal(t, "DebugLogLevel", cond.Reason)
			assert.Equal(t, "Log level is debug outside Development for: web-server", cond.Message)
			assertNextEventContains(t, recorder, "Warning DebugLogging")
		})
	}
}

// The condition tracks spec state through Reconcile: it appears with one
// Warning Event, isn't reannounced on later reconciles, and is removed when
// debug is turned off. Turning debug back on announces it again.
func TestReconcile_DebugLoggingConditionLifecycle(t *testing.T) {
	scheme := testScheme(t)
	spec := minimalSupersetSpec()
	spec.LogLevel = new(supersetv1alpha1.LogLevelDebug)
	superset := &supersetv1alpha1.Superset{Name: "test", Namespace: "default", UID: "uid-1", Spec: spec}

	c := reconcileOnce(t, scheme, superset).Build()
	recorder := events.NewFakeRecorder(10)
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	getSuperset := func() *supersetv1alpha1.Superset {
		got := &supersetv1alpha1.Superset{}
		require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "test", Namespace: "default"}, got))
		return got
	}
	setLogLevel := func(level supersetv1alpha1.LogLevel) {
		got := getSuperset()
		got.Spec.LogLevel = &level
		require.NoError(t, c.Update(context.Background(), got))
	}

	doReconcile(t, r)
	cond := apimeta.FindStatusCondition(getSuperset().Status.Conditions, "DebugLogging")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Log level is debug outside Development for: web-server", cond.Message)
	assertNextEventContains(t, recorder, "Warning DebugLogging")

	doReconcile(t, r)
	assertNoEvents(t, recorder)

	setLogLevel(supersetv1alpha1.LogLevelInfo)
	doReconcile(t, r)
	assert.Nil(t, apimeta.FindStatusCondition(getSuperset().Status.Conditions, "DebugLogging"))
	assertNoEvents(t, recorder)

	setLogLevel(supersetv1alpha1.LogLevelDebug)
	doReconcile(t, r)
	require.NotNil(t, apimeta.FindStatusCondition(getSuperset().Status.Conditions, "DebugLogging"))
	assertNextEventContains(t, recorder, "Warning DebugLogging")
}
