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
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

const (
	websocketConfigKey       = "config.json"
	websocketConfigVolume    = "websocket-config"
	websocketConfigMountPath = "/home/superset-websocket/config.json"
)

func renderWebsocketConfig(config *apiextensionsv1.JSON) (string, error) {
	if config == nil || len(config.Raw) == 0 {
		return "{}", nil
	}

	var decoded any
	if err := json.Unmarshal(config.Raw, &decoded); err != nil {
		return "", fmt.Errorf("parsing websocket config JSON: %w", err)
	}
	out, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		return "", fmt.Errorf("rendering websocket config JSON: %w", err)
	}
	return string(out), nil
}

func reconcileParentOwnedWebsocketConfigMap(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	parent *supersetv1alpha1.Superset,
	config string,
	resourceBaseName string,
	labels map[string]string,
) error {
	cmName := naming.ConfigMapName(resourceBaseName)

	if config == "" {
		cm := &corev1.ConfigMap{}
		cm.Name = cmName
		cm.Namespace = parent.Namespace
		return client.IgnoreNotFound(c.Delete(ctx, cm))
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: parent.Namespace,
		},
	}

	_, err := createOrUpdateWithRetry(ctx, c, cm, func() error {
		if err := controllerutil.SetControllerReference(parent, cm, scheme); err != nil {
			return err
		}
		cm.Labels = mergeLabels(cm.Labels, labels)
		cm.Data = map[string]string{websocketConfigKey: config}
		return nil
	})
	return err
}

func injectWebsocketConfigMap(op *resolution.OperatorInjected, resourceBaseName string) {
	op.Volumes = append(op.Volumes, corev1.Volume{
		Name: websocketConfigVolume,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: naming.ConfigMapName(resourceBaseName),
				},
				Items: []corev1.KeyToPath{{Key: websocketConfigKey, Path: websocketConfigKey}},
			},
		},
	})
	injectWebsocketConfigMount(op)
}

func injectWebsocketConfigSecret(op *resolution.OperatorInjected, configFrom *corev1.SecretKeySelector) {
	op.Volumes = append(op.Volumes, corev1.Volume{
		Name: websocketConfigVolume,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: configFrom.Name,
				Items:      []corev1.KeyToPath{{Key: configFrom.Key, Path: websocketConfigKey}},
				Optional:   configFrom.Optional,
			},
		},
	})
	injectWebsocketConfigMount(op)
}

func injectWebsocketConfigMount(op *resolution.OperatorInjected) {
	op.VolumeMounts = append(op.VolumeMounts, corev1.VolumeMount{
		Name:      websocketConfigVolume,
		MountPath: websocketConfigMountPath,
		SubPath:   websocketConfigKey,
		ReadOnly:  true,
	})
}

func websocketConfigRefChecksumInput(configFrom *corev1.SecretKeySelector) string {
	if configFrom == nil {
		return ""
	}
	optional := false
	if configFrom.Optional != nil {
		optional = *configFrom.Optional
	}
	return fmt.Sprintf("secret:%s:%s:%t", configFrom.Name, configFrom.Key, optional)
}
