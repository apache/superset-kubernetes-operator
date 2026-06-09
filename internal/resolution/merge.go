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
	"maps"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// MergeMaps merges multiple string maps. Later maps take precedence on key conflict.
// Returns nil if the result is empty.
func MergeMaps(inputs ...map[string]string) map[string]string {
	size := 0
	for _, m := range inputs {
		size += len(m)
	}
	if size == 0 {
		return nil
	}

	result := make(map[string]string, size)
	for _, m := range inputs {
		maps.Copy(result, m)
	}
	return result
}

// mergeByKey merges multiple slices, deduplicating by the key returned by key.
// Later entries with the same key replace earlier entries in place to preserve
// ordering.
func mergeByKey[T any](key func(T) string, slices ...[]T) []T {
	seen := make(map[string]int) // key -> index in result
	var result []T
	for _, slice := range slices {
		for _, item := range slice {
			if idx, exists := seen[key(item)]; exists {
				result[idx] = item
			} else {
				seen[key(item)] = len(result)
				result = append(result, item)
			}
		}
	}
	return result
}

// MergeEnvVars merges multiple env var slices. Later entries with the same Name
// replace earlier entries in place to preserve ordering.
func MergeEnvVars(slices ...[]corev1.EnvVar) []corev1.EnvVar {
	return mergeByKey(func(e corev1.EnvVar) string { return e.Name }, slices...)
}

// MergeVolumes merges multiple volume slices. Later entries with the same Name
// replace earlier entries in place to preserve ordering.
func MergeVolumes(slices ...[]corev1.Volume) []corev1.Volume {
	return mergeByKey(func(v corev1.Volume) string { return v.Name }, slices...)
}

// MergeVolumeMounts merges multiple volume mount slices. Later entries with the
// same Name replace earlier entries in place to preserve ordering.
func MergeVolumeMounts(slices ...[]corev1.VolumeMount) []corev1.VolumeMount {
	return mergeByKey(func(m corev1.VolumeMount) string { return m.Name }, slices...)
}

// MergeHostAliases merges multiple host alias slices. Later entries with the
// same IP replace earlier entries in place to preserve ordering.
func MergeHostAliases(slices ...[]corev1.HostAlias) []corev1.HostAlias {
	return mergeByKey(func(a corev1.HostAlias) string { return a.IP }, slices...)
}

// MergeContainerPorts merges multiple container port slices. Later entries with
// the same Name replace earlier entries in place to preserve ordering.
func MergeContainerPorts(slices ...[]corev1.ContainerPort) []corev1.ContainerPort {
	return mergeByKey(func(p corev1.ContainerPort) string { return p.Name }, slices...)
}

// MergeEnvFromSources concatenates multiple EnvFromSource slices.
// Unlike named resources (EnvVar, Volume, Container), EnvFromSource entries
// are not deduplicated because there is no single natural key: an EnvFromSource
// can reference either a ConfigMap or a Secret, each with an optional Prefix,
// and the same source may be intentionally included multiple times with
// different prefixes.
func MergeEnvFromSources(slices ...[]corev1.EnvFromSource) []corev1.EnvFromSource {
	var result []corev1.EnvFromSource
	for _, slice := range slices {
		result = append(result, slice...)
	}
	return result
}

// MergeContainers concatenates multiple container slices (for sidecars/initContainers).
// Later entries with the same Name replace earlier entries.
func MergeContainers(slices ...[]corev1.Container) []corev1.Container {
	return mergeByKey(func(c corev1.Container) string { return c.Name }, slices...)
}

// MergeDeploymentTemplate field-level merges two DeploymentTemplates.
// Only contains Deployment-level fields (no nested PodTemplate).
func MergeDeploymentTemplate(comp, tl *supersetv1alpha1.DeploymentTemplate) *supersetv1alpha1.DeploymentTemplate {
	c := orEmpty(comp)
	t := orEmpty(tl)
	result := &supersetv1alpha1.DeploymentTemplate{
		RevisionHistoryLimit:    ResolveOverridableValue(c.RevisionHistoryLimit, t.RevisionHistoryLimit),
		MinReadySeconds:         ResolveOverridableValue(c.MinReadySeconds, t.MinReadySeconds),
		ProgressDeadlineSeconds: ResolveOverridableValue(c.ProgressDeadlineSeconds, t.ProgressDeadlineSeconds),
		Strategy:                ResolveOverridableValue(c.Strategy, t.Strategy),
		Labels:                  MergeMaps(t.Labels, c.Labels),
		Annotations:             MergeMaps(t.Annotations, c.Annotations),
	}
	if result.RevisionHistoryLimit == nil && result.MinReadySeconds == nil &&
		result.ProgressDeadlineSeconds == nil && result.Strategy == nil &&
		result.Labels == nil && result.Annotations == nil {
		return nil
	}
	return result
}

// MergePodTemplate field-level merges two PodTemplates and folds in
// operator-injected values (volumes, init containers, labels).
func MergePodTemplate(comp, tl *supersetv1alpha1.PodTemplate, operatorLabels map[string]string, op *OperatorInjected) *supersetv1alpha1.PodTemplate {
	c := orEmpty(comp)
	t := orEmpty(tl)

	return &supersetv1alpha1.PodTemplate{
		Annotations:                   MergeMaps(t.Annotations, c.Annotations),
		Labels:                        MergeMaps(t.Labels, c.Labels, operatorLabels),
		NodeSelector:                  MergeMaps(t.NodeSelector, c.NodeSelector),
		Affinity:                      ResolveOverridableValue(c.Affinity, t.Affinity),
		PodSecurityContext:            ResolveOverridableValue(c.PodSecurityContext, t.PodSecurityContext),
		PriorityClassName:             ResolveOverridableValue(c.PriorityClassName, t.PriorityClassName),
		TerminationGracePeriodSeconds: ResolveOverridableValue(c.TerminationGracePeriodSeconds, t.TerminationGracePeriodSeconds),
		DNSPolicy:                     ResolveOverridableValue(c.DNSPolicy, t.DNSPolicy),
		DNSConfig:                     ResolveOverridableValue(c.DNSConfig, t.DNSConfig),
		RuntimeClassName:              ResolveOverridableValue(c.RuntimeClassName, t.RuntimeClassName),
		ShareProcessNamespace:         ResolveOverridableValue(c.ShareProcessNamespace, t.ShareProcessNamespace),
		EnableServiceLinks:            ResolveOverridableValue(c.EnableServiceLinks, t.EnableServiceLinks),
		Resources:                     ResolveOverridableValue(c.Resources, t.Resources),
		Volumes:                       MergeVolumes(t.Volumes, c.Volumes, op.Volumes),
		Sidecars:                      MergeContainers(t.Sidecars, c.Sidecars),
		InitContainers:                MergeContainers(t.InitContainers, c.InitContainers, op.InitContainers),
		HostAliases:                   MergeHostAliases(t.HostAliases, c.HostAliases),
		Tolerations:                   append(t.Tolerations, c.Tolerations...),
		TopologySpreadConstraints:     append(t.TopologySpreadConstraints, c.TopologySpreadConstraints...),
		Container:                     MergeContainerTemplate(c.Container, t.Container, op),
	}
}

// MergeContainerTemplate field-level merges two ContainerTemplates and folds
// in operator-injected env vars and volume mounts.
func MergeContainerTemplate(comp, tl *supersetv1alpha1.ContainerTemplate, op *OperatorInjected) *supersetv1alpha1.ContainerTemplate {
	c := orEmpty(comp)
	t := orEmpty(tl)

	result := &supersetv1alpha1.ContainerTemplate{
		Resources:       ResolveOverridableValue(c.Resources, t.Resources),
		SecurityContext: ResolveOverridableValue(c.SecurityContext, t.SecurityContext),
		LivenessProbe:   ResolveOverridableValue(c.LivenessProbe, t.LivenessProbe),
		ReadinessProbe:  ResolveOverridableValue(c.ReadinessProbe, t.ReadinessProbe),
		StartupProbe:    ResolveOverridableValue(c.StartupProbe, t.StartupProbe),
		Lifecycle:       ResolveOverridableValue(c.Lifecycle, t.Lifecycle),
		Env:             MergeEnvVars(t.Env, c.Env, op.Env),
		EnvFrom:         MergeEnvFromSources(t.EnvFrom, c.EnvFrom),
		VolumeMounts:    MergeVolumeMounts(t.VolumeMounts, c.VolumeMounts, op.VolumeMounts),
		Ports:           MergeContainerPorts(t.Ports, c.Ports),
	}

	// Command/Args: component wins if set, no inheritance.
	if len(c.Command) > 0 {
		result.Command = c.Command
	}
	if len(c.Args) > 0 {
		result.Args = c.Args
	}

	return result
}

// orEmpty returns p when non-nil, otherwise a pointer to a zero value of T.
// It lets merge logic dereference optional template pointers without per-type
// nil guards.
func orEmpty[T any](p *T) *T {
	if p == nil {
		return new(T)
	}
	return p
}
