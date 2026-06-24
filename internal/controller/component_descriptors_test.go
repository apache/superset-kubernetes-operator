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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

func TestConvertComponent(t *testing.T) {
	t.Run("nil accessor returns nil", func(t *testing.T) {
		assert.Nil(t, convertComponent(nil))
	})

	t.Run("maps fields onto the shared input", func(t *testing.T) {
		replicas := int32(3)
		dt := &supersetv1alpha1.DeploymentTemplate{}
		pt := &supersetv1alpha1.PodTemplate{}
		as := &supersetv1alpha1.AutoscalingSpec{}
		pdb := &supersetv1alpha1.PDBSpec{}
		a := &componentAccessor{
			deploymentTemplate: dt,
			podTemplate:        pt,
			replicas:           &replicas,
			autoscaling:        as,
			pdb:                pdb,
		}
		comp := convertComponent(a)
		require.NotNil(t, comp)
		assert.Same(t, dt, comp.DeploymentTemplate)
		assert.Same(t, pt, comp.PodTemplate)
		assert.Equal(t, &replicas, comp.Replicas)
		assert.Same(t, as, comp.Autoscaling)
		assert.Same(t, pdb, comp.PodDisruptionBudget)
	})
}

func TestConvertCloneComponent(t *testing.T) {
	cmd := []string{"/bin/sh", "-c", "echo clone"}

	t.Run("nil pod template builds a container-only template", func(t *testing.T) {
		clone := &supersetv1alpha1.CloneTaskSpec{}
		comp := convertCloneComponent(clone, cmd)
		require.NotNil(t, comp)
		require.NotNil(t, comp.PodTemplate)
		require.NotNil(t, comp.PodTemplate.Container)
		assert.Equal(t, cmd, comp.PodTemplate.Container.Command)
	})

	t.Run("existing pod template is copied, command injected", func(t *testing.T) {
		origCmd := []string{"old"}
		clone := &supersetv1alpha1.CloneTaskSpec{
			PodTemplate: &supersetv1alpha1.PodTemplate{
				Container: &supersetv1alpha1.ContainerTemplate{Command: origCmd},
			},
		}
		comp := convertCloneComponent(clone, cmd)
		assert.Equal(t, cmd, comp.PodTemplate.Container.Command)
		// Source spec must not be mutated (copy semantics).
		assert.Equal(t, origCmd, clone.PodTemplate.Container.Command)
		assert.NotSame(t, clone.PodTemplate, comp.PodTemplate)
	})

	t.Run("pod template without container gets a fresh container", func(t *testing.T) {
		clone := &supersetv1alpha1.CloneTaskSpec{
			PodTemplate: &supersetv1alpha1.PodTemplate{},
		}
		comp := convertCloneComponent(clone, cmd)
		require.NotNil(t, comp.PodTemplate.Container)
		assert.Equal(t, cmd, comp.PodTemplate.Container.Command)
	})
}

func TestComponentResourceConfig(t *testing.T) {
	t.Run("known components return their config", func(t *testing.T) {
		cases := []struct {
			ct          common.ComponentType
			defaultPort int32
			hasScaling  bool
		}{
			{common.ComponentWebServer, 0, true},
			{common.ComponentCeleryWorker, 0, true},
			{common.ComponentCeleryBeat, 0, false},
			{common.ComponentCeleryFlower, common.PortCeleryFlower, true},
			{common.ComponentWebsocketServer, common.PortWebsocket, true},
			{common.ComponentMcpServer, common.PortMcpServer, true},
		}
		for _, c := range cases {
			cfg, ok := componentResourceConfig(c.ct)
			require.True(t, ok, "expected config for %s", c.ct)
			assert.Equal(t, string(c.ct), cfg.componentName)
			assert.Equal(t, c.defaultPort, cfg.defaultPort)
			assert.Equal(t, c.hasScaling, cfg.hasScaling)
		}
	})

	t.Run("unknown component returns false", func(t *testing.T) {
		_, ok := componentResourceConfig(common.ComponentType("nope"))
		assert.False(t, ok)
	})
}

func TestWarnEnvVarOverrides(t *testing.T) {
	op := &resolution.OperatorInjected{
		Env: []corev1.EnvVar{{Name: "SUPERSET_OPERATOR__SECRET_KEY", Value: "x"}},
	}
	envPT := func(name string) *supersetv1alpha1.PodTemplate {
		return &supersetv1alpha1.PodTemplate{
			Container: &supersetv1alpha1.ContainerTemplate{
				Env: []corev1.EnvVar{{Name: name, Value: "user"}},
			},
		}
	}

	// Exercise the collision branch (top-level + component) and the no-collision
	// branch. The function only logs; we assert it runs without panicking across
	// the relevant inputs.
	t.Run("collision on both top-level and component", func(t *testing.T) {
		tl := &resolution.SharedInput{PodTemplate: envPT("SUPERSET_OPERATOR__SECRET_KEY")}
		comp := &resolution.ComponentInput{SharedInput: resolution.SharedInput{PodTemplate: envPT("SUPERSET_OPERATOR__SECRET_KEY")}}
		warnEnvVarOverrides(context.Background(), tl, comp, op)
	})

	t.Run("no collision", func(t *testing.T) {
		tl := &resolution.SharedInput{PodTemplate: envPT("MY_OWN_VAR")}
		comp := &resolution.ComponentInput{SharedInput: resolution.SharedInput{PodTemplate: envPT("OTHER_VAR")}}
		warnEnvVarOverrides(context.Background(), tl, comp, op)
	})

	t.Run("nil component", func(t *testing.T) {
		tl := &resolution.SharedInput{PodTemplate: envPT("MY_OWN_VAR")}
		warnEnvVarOverrides(context.Background(), tl, nil, op)
	})
}

func TestInjectCeleryCommand(t *testing.T) {
	cmd := []string{"celery", "worker"}

	t.Run("nil component is a no-op", func(t *testing.T) {
		assert.NotPanics(t, func() {
			injectCeleryCommand(nil, cmd)
		})
	})

	t.Run("creates missing pod and container templates", func(t *testing.T) {
		comp := &resolution.ComponentInput{}

		injectCeleryCommand(comp, cmd)

		require.NotNil(t, comp.PodTemplate)
		require.NotNil(t, comp.PodTemplate.Container)
		assert.Equal(t, cmd, comp.PodTemplate.Container.Command)
	})

	t.Run("creates missing container template", func(t *testing.T) {
		comp := &resolution.ComponentInput{
			SharedInput: resolution.SharedInput{
				PodTemplate: &supersetv1alpha1.PodTemplate{},
			},
		}

		injectCeleryCommand(comp, cmd)

		require.NotNil(t, comp.PodTemplate.Container)
		assert.Equal(t, cmd, comp.PodTemplate.Container.Command)
	})

	t.Run("preserves explicit command", func(t *testing.T) {
		existing := []string{"custom", "worker"}
		comp := &resolution.ComponentInput{
			SharedInput: resolution.SharedInput{
				PodTemplate: &supersetv1alpha1.PodTemplate{
					Container: &supersetv1alpha1.ContainerTemplate{
						Command: existing,
					},
				},
			},
		}

		injectCeleryCommand(comp, cmd)

		assert.Equal(t, existing, comp.PodTemplate.Container.Command)
	})
}

func TestDeleteComponentResources(t *testing.T) {
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
	}

	// A web-server component (hasPythonConfig=true, defaultPort=0 so no Service).
	wsName := common.ResourceBaseName("test", common.ComponentWebServer)
	deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: wsName, Namespace: "default"}}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: common.ConfigMapName(wsName), Namespace: "default"}}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(superset, deploy, cm).
		Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	err := r.deleteComponentResources(context.Background(), superset, webServerDescriptor)
	require.NoError(t, err)

	// Deployment should be gone.
	gotDeploy := &appsv1.Deployment{}
	err = c.Get(context.Background(), client.ObjectKey{Name: wsName, Namespace: "default"}, gotDeploy)
	assert.True(t, errors.IsNotFound(err), "expected Deployment deleted, got %v", err)

	// ConfigMap should be gone (hasPythonConfig).
	gotCM := &corev1.ConfigMap{}
	err = c.Get(context.Background(), client.ObjectKey{Name: common.ConfigMapName(wsName), Namespace: "default"}, gotCM)
	assert.True(t, errors.IsNotFound(err), "expected ConfigMap deleted, got %v", err)
}
