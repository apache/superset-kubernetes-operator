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

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	supersetconfig "github.com/apache/superset-kubernetes-operator/internal/config"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

func (r *SupersetReconciler) buildTaskFlatSpec(
	superset *supersetv1alpha1.Superset,
	taskType string,
	command []string,
	_ string,
	topLevel *resolution.SharedInput,
	saName string,
) (supersetv1alpha1.FlatComponentSpec, string) {
	if taskType == taskTypeClone {
		return r.buildCloneTaskFlatSpec(superset, saName, topLevel), ""
	}
	return r.buildStandardTaskFlatSpec(superset, taskType, command, topLevel, saName)
}

// buildCloneTaskFlatSpec builds the flat spec for clone tasks (database-tool image, no Python config).
func (r *SupersetReconciler) buildCloneTaskFlatSpec(
	superset *supersetv1alpha1.Superset,
	saName string,
	topLevel *resolution.SharedInput,
) supersetv1alpha1.FlatComponentSpec {
	clone := superset.Spec.Lifecycle.Clone
	instanceName := superset.Name + suffixClone

	cloneEnvVars := collectCloneEnvVars(superset)
	cloneCmd := r.buildCloneCommand(superset)
	comp := convertCloneComponent(clone, cloneCmd)
	operatorInjected := &resolution.OperatorInjected{Env: cloneEnvVars}

	flat := resolution.ResolveComponentSpec(
		resolution.ComponentInit, topLevel, comp,
		podOperatorLabels(string(naming.ComponentInit), instanceName, superset.Name), operatorInjected,
	)

	cloneImage := resolveCloneImage(clone)
	one := int32(1)
	flatSpec := supersetv1alpha1.FlatComponentSpec{
		Image:              cloneImage,
		Replicas:           &one,
		PodTemplate:        flatPodTemplate(flat),
		ServiceAccountName: saName,
	}
	flatSpec.Autoscaling = nil
	flatSpec.PodDisruptionBudget = nil
	return flatSpec
}

// buildStandardTaskFlatSpec builds the flat spec for migrate/init tasks (Superset image + Python config).
func (r *SupersetReconciler) buildStandardTaskFlatSpec(
	superset *supersetv1alpha1.Superset,
	taskType string,
	command []string,
	topLevel *resolution.SharedInput,
	saName string,
) (supersetv1alpha1.FlatComponentSpec, string) {
	instanceName := superset.Name + suffixForTaskType(taskType)
	resourceBaseName := instanceName

	compConfigInput := buildConfigInput(&superset.Spec)
	if superset.Spec.Lifecycle != nil && superset.Spec.Lifecycle.Config != nil {
		compConfigInput.ComponentConfig = *superset.Spec.Lifecycle.Config
	}

	var lifecycleSQLASpec *supersetv1alpha1.SQLAlchemyEngineOptionsSpec
	if superset.Spec.Lifecycle != nil {
		lifecycleSQLASpec = superset.Spec.Lifecycle.SQLAlchemyEngineOptions
	}
	compConfigInput.EngineOptions = supersetconfig.ComputeEngineOptions(
		naming.ComponentInit, superset.Spec.SQLAlchemyEngineOptions, lifecycleSQLASpec, 0, 0,
	)

	comp := convertTaskComponent(superset.Spec.Lifecycle, command)
	renderedConfig := supersetconfig.RenderConfig(supersetconfig.ComponentInit, compConfigInput)

	secretEnvVars := collectSecretEnvVars(&superset.Spec, superset.Name)
	var initEnvVars []corev1.EnvVar
	if taskType == taskTypeInit {
		initEnvVars = collectLifecycleInitEnvVars(superset.Spec.Lifecycle)
	}
	operatorInjected := buildOperatorInjected(renderedConfig, resourceBaseName, superset.Spec.ForceReload, append(secretEnvVars, initEnvVars...))

	flat := resolution.ResolveComponentSpec(
		resolution.ComponentInit, topLevel, comp,
		podOperatorLabels(string(naming.ComponentInit), instanceName, superset.Name), operatorInjected,
	)

	var imageOverride *supersetv1alpha1.ImageOverrideSpec
	if superset.Spec.Lifecycle != nil {
		imageOverride = superset.Spec.Lifecycle.Image
	}
	flatSpec := flatSpecFromResolution(flat, &superset.Spec.Image, imageOverride, saName)
	flatSpec.Autoscaling = nil
	flatSpec.PodDisruptionBudget = nil
	return flatSpec, renderedConfig
}

func suffixForTaskType(taskType string) string {
	if desc := lifecycleTaskDescriptorByType(taskType); desc != nil {
		return desc.Suffix
	}
	return "-" + strings.ToLower(taskType)
}

func lifecycleImageOverride(superset *supersetv1alpha1.Superset) *supersetv1alpha1.ImageOverrideSpec {
	if superset.Spec.Lifecycle != nil {
		return superset.Spec.Lifecycle.Image
	}
	return nil
}

// flatPodTemplate extracts the PodTemplate from a resolved FlatSpec.
func flatPodTemplate(flat *resolution.FlatSpec) *supersetv1alpha1.PodTemplate {
	return flat.PodTemplate
}
