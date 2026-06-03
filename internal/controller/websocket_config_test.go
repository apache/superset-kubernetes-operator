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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

func TestRenderWebsocketConfig(t *testing.T) {
	t.Run("nil config renders empty object", func(t *testing.T) {
		out, err := renderWebsocketConfig(nil)
		assert.NoError(t, err)
		assert.Equal(t, "{}", out)
	})

	t.Run("empty raw renders empty object", func(t *testing.T) {
		out, err := renderWebsocketConfig(&apiextensionsv1.JSON{Raw: []byte{}})
		assert.NoError(t, err)
		assert.Equal(t, "{}", out)
	})

	t.Run("valid JSON round-trips with indentation", func(t *testing.T) {
		out, err := renderWebsocketConfig(&apiextensionsv1.JSON{Raw: []byte(`{"port":8080,"pingTimeout":300}`)})
		assert.NoError(t, err)
		// Pretty-printed with two-space indent.
		assert.Contains(t, out, "\"port\": 8080")
		assert.Contains(t, out, "\"pingTimeout\": 300")
		assert.Contains(t, out, "\n  ")
	})

	t.Run("invalid JSON returns an error", func(t *testing.T) {
		out, err := renderWebsocketConfig(&apiextensionsv1.JSON{Raw: []byte(`{not valid`)})
		assert.Error(t, err)
		assert.Empty(t, out)
		assert.Contains(t, err.Error(), "parsing websocket config JSON")
	})
}

func TestWebsocketConfigRefChecksumInput(t *testing.T) {
	t.Run("nil ref yields empty string", func(t *testing.T) {
		assert.Equal(t, "", websocketConfigRefChecksumInput(nil))
	})

	t.Run("optional nil formats as false", func(t *testing.T) {
		ref := &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "ws-secret"},
			Key:                  "config.json",
		}
		assert.Equal(t, "secret:ws-secret:config.json:false", websocketConfigRefChecksumInput(ref))
	})

	t.Run("optional true formats as true", func(t *testing.T) {
		ref := &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "ws-secret"},
			Key:                  "config.json",
			Optional:             boolPtr(true),
		}
		assert.Equal(t, "secret:ws-secret:config.json:true", websocketConfigRefChecksumInput(ref))
	})

	t.Run("optional false formats as false", func(t *testing.T) {
		ref := &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "ws-secret"},
			Key:                  "config.json",
			Optional:             boolPtr(false),
		}
		assert.Equal(t, "secret:ws-secret:config.json:false", websocketConfigRefChecksumInput(ref))
	})
}

func TestInjectWebsocketConfigMap(t *testing.T) {
	op := &resolution.OperatorInjected{}
	injectWebsocketConfigMap(op, "my-superset-websocket-server")

	if assert.Len(t, op.Volumes, 1) {
		v := op.Volumes[0]
		assert.Equal(t, websocketConfigVolume, v.Name)
		if assert.NotNil(t, v.ConfigMap) {
			assert.True(t, strings.Contains(v.ConfigMap.Name, "websocket-server"))
			if assert.Len(t, v.ConfigMap.Items, 1) {
				assert.Equal(t, websocketConfigKey, v.ConfigMap.Items[0].Key)
				assert.Equal(t, websocketConfigKey, v.ConfigMap.Items[0].Path)
			}
		}
	}
	if assert.Len(t, op.VolumeMounts, 1) {
		m := op.VolumeMounts[0]
		assert.Equal(t, websocketConfigVolume, m.Name)
		assert.Equal(t, websocketConfigMountPath, m.MountPath)
		assert.Equal(t, websocketConfigKey, m.SubPath)
		assert.True(t, m.ReadOnly)
	}
}

func TestInjectWebsocketConfigSecret(t *testing.T) {
	op := &resolution.OperatorInjected{}
	configFrom := &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "ws-secret"},
		Key:                  "config",
		Optional:             boolPtr(true),
	}
	injectWebsocketConfigSecret(op, configFrom)

	if assert.Len(t, op.Volumes, 1) {
		v := op.Volumes[0]
		assert.Equal(t, websocketConfigVolume, v.Name)
		if assert.NotNil(t, v.Secret) {
			assert.Equal(t, "ws-secret", v.Secret.SecretName)
			assert.Equal(t, boolPtr(true), v.Secret.Optional)
			if assert.Len(t, v.Secret.Items, 1) {
				assert.Equal(t, "config", v.Secret.Items[0].Key)
				assert.Equal(t, websocketConfigKey, v.Secret.Items[0].Path)
			}
		}
	}
	if assert.Len(t, op.VolumeMounts, 1) {
		assert.Equal(t, websocketConfigMountPath, op.VolumeMounts[0].MountPath)
	}
}
