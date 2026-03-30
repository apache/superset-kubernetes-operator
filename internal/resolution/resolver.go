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

package resolution

import (
	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// ComponentType is an alias for common.ComponentType.
type ComponentType = common.ComponentType

// Component type constants re-exported for convenience.
const (
	ComponentWebServer       = common.ComponentWebServer
	ComponentCeleryWorker    = common.ComponentCeleryWorker
	ComponentCeleryBeat      = common.ComponentCeleryBeat
	ComponentCeleryFlower    = common.ComponentCeleryFlower
	ComponentWebsocketServer = common.ComponentWebsocketServer
	ComponentMcpServer       = common.ComponentMcpServer
	ComponentInit            = common.ComponentInit
)

// SharedInput holds top-level or per-component fields for resolution.
type SharedInput struct {
	Replicas            *int32
	DeploymentTemplate  *supersetv1alpha1.DeploymentTemplate
	PodTemplate         *supersetv1alpha1.PodTemplate
	Autoscaling         *supersetv1alpha1.AutoscalingSpec
	PodDisruptionBudget *supersetv1alpha1.PDBSpec
}

// ComponentInput captures per-component fields. Embeds SharedInput.
type ComponentInput struct {
	SharedInput
}

// OperatorInjected holds volumes, mounts, env vars, and init containers
// that the operator injects (e.g., config mount, TLS mounts, config env vars).
type OperatorInjected struct {
	Env            []corev1.EnvVar
	Volumes        []corev1.Volume
	VolumeMounts   []corev1.VolumeMount
	InitContainers []corev1.Container
}

// FlatSpec is the fully resolved output from the resolution engine.
type FlatSpec struct {
	Replicas            int32
	DeploymentTemplate  *supersetv1alpha1.DeploymentTemplate
	PodTemplate         *supersetv1alpha1.PodTemplate
	Autoscaling         *supersetv1alpha1.AutoscalingSpec
	PodDisruptionBudget *supersetv1alpha1.PDBSpec
}

// defaultReplicas is the fallback when neither component nor defaults specify replicas.
var defaultReplicas = int32(1)

// ResolveChildSpec produces a fully-flattened spec from a 2-level model:
// top-level SharedInput + per-component ComponentInput. The DeploymentTemplate
// is field-level merged (component wins on conflict for scalars/structs,
// collections merge by name or append). Operator-injected values are folded
// in at the appropriate template level.
func ResolveChildSpec(
	componentType ComponentType,
	topLevel *SharedInput,
	component *ComponentInput,
	operatorLabels map[string]string,
	operator *OperatorInjected,
) *FlatSpec {
	tl := safeSharedInput(topLevel)
	comp := safeComponentInput(component)
	op := safeOperatorInjected(operator)

	result := &FlatSpec{}

	result.Replicas = ResolveScalar(comp.Replicas, tl.Replicas, &defaultReplicas)
	if componentType == ComponentCeleryBeat {
		result.Replicas = 1
	}

	result.DeploymentTemplate = MergeDeploymentTemplate(comp.DeploymentTemplate, tl.DeploymentTemplate)
	result.PodTemplate = MergePodTemplate(comp.PodTemplate, tl.PodTemplate, operatorLabels, op)

	result.Autoscaling = ResolveOverridableValue(comp.Autoscaling, tl.Autoscaling)
	result.PodDisruptionBudget = ResolveOverridableValue(comp.PodDisruptionBudget, tl.PodDisruptionBudget)

	return result
}

// safeSharedInput returns a non-nil SharedInput.
func safeSharedInput(s *SharedInput) *SharedInput {
	if s == nil {
		return &SharedInput{}
	}
	return s
}

// safeComponentInput returns a non-nil ComponentInput.
func safeComponentInput(c *ComponentInput) *ComponentInput {
	if c == nil {
		return &ComponentInput{}
	}
	return c
}

// safeOperatorInjected returns a non-nil OperatorInjected.
func safeOperatorInjected(o *OperatorInjected) *OperatorInjected {
	if o == nil {
		return &OperatorInjected{}
	}
	return o
}
