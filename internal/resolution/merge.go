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
)

// MergeMaps merges multiple string maps. Later maps take precedence on key conflict.
// Returns nil if the result is empty.
func MergeMaps(maps ...map[string]string) map[string]string {
	size := 0
	for _, m := range maps {
		size += len(m)
	}
	if size == 0 {
		return nil
	}

	result := make(map[string]string, size)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// MergeEnvVars merges multiple env var slices. Later entries with the same Name
// replace earlier entries in place to preserve ordering.
func MergeEnvVars(slices ...[]corev1.EnvVar) []corev1.EnvVar {
	seen := make(map[string]int) // name -> index in result
	var result []corev1.EnvVar
	for _, slice := range slices {
		for _, env := range slice {
			if idx, exists := seen[env.Name]; exists {
				result[idx] = env
			} else {
				seen[env.Name] = len(result)
				result = append(result, env)
			}
		}
	}
	return result
}

// MergeVolumes merges multiple volume slices. Later entries with the same Name
// replace earlier entries in place to preserve ordering.
func MergeVolumes(slices ...[]corev1.Volume) []corev1.Volume {
	seen := make(map[string]int)
	var result []corev1.Volume
	for _, slice := range slices {
		for _, vol := range slice {
			if idx, exists := seen[vol.Name]; exists {
				result[idx] = vol
			} else {
				seen[vol.Name] = len(result)
				result = append(result, vol)
			}
		}
	}
	return result
}

// MergeVolumeMounts merges multiple volume mount slices. Later entries with the
// same Name replace earlier entries in place to preserve ordering.
func MergeVolumeMounts(slices ...[]corev1.VolumeMount) []corev1.VolumeMount {
	seen := make(map[string]int)
	var result []corev1.VolumeMount
	for _, slice := range slices {
		for _, mount := range slice {
			if idx, exists := seen[mount.Name]; exists {
				result[idx] = mount
			} else {
				seen[mount.Name] = len(result)
				result = append(result, mount)
			}
		}
	}
	return result
}

// MergeHostAliases merges multiple host alias slices. Later entries with the
// same IP replace earlier entries in place to preserve ordering.
func MergeHostAliases(slices ...[]corev1.HostAlias) []corev1.HostAlias {
	seen := make(map[string]int)
	var result []corev1.HostAlias
	for _, slice := range slices {
		for _, alias := range slice {
			if idx, exists := seen[alias.IP]; exists {
				result[idx] = alias
			} else {
				seen[alias.IP] = len(result)
				result = append(result, alias)
			}
		}
	}
	return result
}

// MergeContainerPorts merges multiple container port slices. Later entries with
// the same Name replace earlier entries in place to preserve ordering.
func MergeContainerPorts(slices ...[]corev1.ContainerPort) []corev1.ContainerPort {
	seen := make(map[string]int)
	var result []corev1.ContainerPort
	for _, slice := range slices {
		for _, port := range slice {
			if idx, exists := seen[port.Name]; exists {
				result[idx] = port
			} else {
				seen[port.Name] = len(result)
				result = append(result, port)
			}
		}
	}
	return result
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
	seen := make(map[string]int)
	var result []corev1.Container
	for _, slice := range slices {
		for _, c := range slice {
			if idx, exists := seen[c.Name]; exists {
				result[idx] = c
			} else {
				seen[c.Name] = len(result)
				result = append(result, c)
			}
		}
	}
	return result
}

// MergeDeploymentTemplate field-level merges two DeploymentTemplates.
// Only contains Deployment-level fields (no nested PodTemplate).
func MergeDeploymentTemplate(comp, tl *supersetv1alpha1.DeploymentTemplate) *supersetv1alpha1.DeploymentTemplate {
	c := safeDeploymentTemplate(comp)
	t := safeDeploymentTemplate(tl)
	result := &supersetv1alpha1.DeploymentTemplate{
		RevisionHistoryLimit:    ResolveOverridableValue(c.RevisionHistoryLimit, t.RevisionHistoryLimit),
		MinReadySeconds:         ResolveOverridableValue(c.MinReadySeconds, t.MinReadySeconds),
		ProgressDeadlineSeconds: ResolveOverridableValue(c.ProgressDeadlineSeconds, t.ProgressDeadlineSeconds),
		Strategy:                ResolveOverridableValue(c.Strategy, t.Strategy),
	}
	if *result == (supersetv1alpha1.DeploymentTemplate{}) {
		return nil
	}
	return result
}

// MergePodTemplate field-level merges two PodTemplates and folds in
// operator-injected values (volumes, init containers, labels).
func MergePodTemplate(comp, tl *supersetv1alpha1.PodTemplate, operatorLabels map[string]string, op *OperatorInjected) *supersetv1alpha1.PodTemplate {
	c := safePodTemplate(comp)
	t := safePodTemplate(tl)

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
	c := safeContainerTemplate(comp)
	t := safeContainerTemplate(tl)

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

func safeDeploymentTemplate(d *supersetv1alpha1.DeploymentTemplate) *supersetv1alpha1.DeploymentTemplate {
	if d == nil {
		return &supersetv1alpha1.DeploymentTemplate{}
	}
	return d
}

func safePodTemplate(p *supersetv1alpha1.PodTemplate) *supersetv1alpha1.PodTemplate {
	if p == nil {
		return &supersetv1alpha1.PodTemplate{}
	}
	return p
}

func safeContainerTemplate(ct *supersetv1alpha1.ContainerTemplate) *supersetv1alpha1.ContainerTemplate {
	if ct == nil {
		return &supersetv1alpha1.ContainerTemplate{}
	}
	return ct
}
