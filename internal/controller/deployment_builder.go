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
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// configVolumeName and configMountPath are package-level aliases for use by
// other files in the controller package (e.g. superset_controller.go).
var (
	configVolumeName = common.ConfigVolumeName
	configMountPath  = common.ConfigMountPath
)

// capabilityAll is the wildcard Linux capability used to drop the full default
// capability set from operator-created containers.
const capabilityAll corev1.Capability = "ALL"

// DeploymentConfig holds the component-specific defaults needed to build
// a Deployment for any Superset component.
type DeploymentConfig struct {
	// ContainerName is the name of the main container (e.g. "superset-web-server").
	ContainerName string

	// DefaultCommand is used when the spec Command is empty.
	DefaultCommand []string

	// DefaultArgs is used when the spec Args is empty.
	DefaultArgs []string

	// DefaultPorts are the container ports.
	DefaultPorts []corev1.ContainerPort

	// ForceReplicas, when non-nil, overrides the spec replicas (used for singletons like beat).
	ForceReplicas *int32

	// DefaultLivenessProbe is used when the spec LivenessProbe is nil.
	DefaultLivenessProbe *corev1.Probe

	// DefaultReadinessProbe is used when the spec ReadinessProbe is nil.
	DefaultReadinessProbe *corev1.Probe

	// DefaultStartupProbe is used when the spec StartupProbe is nil.
	DefaultStartupProbe *corev1.Probe
}

// buildDeploymentSpec constructs a complete DeploymentSpec from the flat component spec,
// component-specific defaults, pod annotations, and selector labels.
// All deployment/pod/container fields are read from the DeploymentTemplate hierarchy.
func buildDeploymentSpec(
	spec *supersetv1alpha1.FlatComponentSpec,
	cfg DeploymentConfig,
	podAnnotations map[string]string,
	selectorLabels map[string]string,
) appsv1.DeploymentSpec {
	dt := safeDeploymentTemplatePtr(spec.DeploymentTemplate)
	pt := safePodTemplatePtr(spec.PodTemplate)
	ct := safeContainerTemplatePtr(pt.Container)

	// Resolve replicas.
	var replicasPtr *int32
	hpaEnabled := spec.Autoscaling != nil
	if cfg.ForceReplicas != nil {
		replicasPtr = cfg.ForceReplicas
	} else if !hpaEnabled {
		replicasPtr = spec.Replicas
	}

	// Resolve command and args from container template, falling back to defaults.
	command := cfg.DefaultCommand
	if len(ct.Command) > 0 {
		command = ct.Command
	}
	args := cfg.DefaultArgs
	if len(ct.Args) > 0 {
		args = ct.Args
	}

	image := fmt.Sprintf("%s:%s", spec.Image.Repository, spec.Image.Tag)

	// Resolve probes: user-provided takes precedence over component defaults.
	livenessProbe := resolveProbe(ct.LivenessProbe, cfg.DefaultLivenessProbe)
	readinessProbe := resolveProbe(ct.ReadinessProbe, cfg.DefaultReadinessProbe)
	startupProbe := resolveProbe(ct.StartupProbe, cfg.DefaultStartupProbe)

	// Build the main container from ContainerTemplate.
	mainContainer := corev1.Container{
		Name:            cfg.ContainerName,
		Image:           image,
		ImagePullPolicy: spec.Image.PullPolicy,
		Command:         command,
		Env:             ct.Env,
		EnvFrom:         ct.EnvFrom,
		VolumeMounts:    ct.VolumeMounts,
		Ports:           cfg.DefaultPorts,
		LivenessProbe:   livenessProbe,
		ReadinessProbe:  readinessProbe,
		StartupProbe:    startupProbe,
		SecurityContext: applyContainerSecurityDefaults(ct.SecurityContext, pt.PodSecurityContext),
		Lifecycle:       ct.Lifecycle,
	}
	if ct.Resources != nil {
		mainContainer.Resources = *ct.Resources
	}
	if len(args) > 0 {
		mainContainer.Args = args
	}

	// Build containers list: main + sidecars.
	containers := make([]corev1.Container, 0, 1+len(pt.Sidecars))
	containers = append(containers, mainContainer)
	containers = append(containers, pt.Sidecars...)

	// Build pod labels and annotations. Operator-managed annotations (the
	// config-checksum that drives rollouts on config/SECRET_KEY changes) are
	// merged last so they win over user pod-template annotations and cannot be
	// pinned to freeze rollouts — matching the label rule and the lifecycle
	// task-Job path (lifecycle_job.go).
	podLabels := mergeLabels(pt.Labels, selectorLabels)
	allPodAnnotations := mergeAnnotations(pt.Annotations, podAnnotations)

	// Build PodSpec from PodTemplate.
	podSpec := corev1.PodSpec{
		InitContainers:                pt.InitContainers,
		Containers:                    containers,
		ImagePullSecrets:              spec.Image.PullSecrets,
		Volumes:                       pt.Volumes,
		SecurityContext:               pt.PodSecurityContext,
		Affinity:                      pt.Affinity,
		Tolerations:                   pt.Tolerations,
		NodeSelector:                  pt.NodeSelector,
		TopologySpreadConstraints:     pt.TopologySpreadConstraints,
		HostAliases:                   pt.HostAliases,
		TerminationGracePeriodSeconds: pt.TerminationGracePeriodSeconds,
		RuntimeClassName:              pt.RuntimeClassName,
		ShareProcessNamespace:         pt.ShareProcessNamespace,
		EnableServiceLinks:            pt.EnableServiceLinks,
		DNSConfig:                     pt.DNSConfig,
		Resources:                     pt.Resources,
	}
	if pt.PriorityClassName != nil {
		podSpec.PriorityClassName = *pt.PriorityClassName
	}
	if spec.ServiceAccountName != "" {
		podSpec.ServiceAccountName = spec.ServiceAccountName
	}
	if pt.DNSPolicy != nil {
		podSpec.DNSPolicy = *pt.DNSPolicy
	}

	// Build DeploymentSpec from DeploymentTemplate.
	deploySpec := appsv1.DeploymentSpec{
		Replicas:                replicasPtr,
		RevisionHistoryLimit:    dt.RevisionHistoryLimit,
		ProgressDeadlineSeconds: dt.ProgressDeadlineSeconds,
		Selector:                &metav1.LabelSelector{MatchLabels: selectorLabels},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels,
				Annotations: allPodAnnotations,
			},
			Spec: podSpec,
		},
	}
	if dt.MinReadySeconds != nil {
		deploySpec.MinReadySeconds = *dt.MinReadySeconds
	}
	if dt.Strategy != nil {
		deploySpec.Strategy = *dt.Strategy
	}

	return deploySpec
}

func safeDeploymentTemplatePtr(d *supersetv1alpha1.DeploymentTemplate) *supersetv1alpha1.DeploymentTemplate {
	if d != nil {
		return d
	}
	return &supersetv1alpha1.DeploymentTemplate{}
}

func safePodTemplatePtr(p *supersetv1alpha1.PodTemplate) *supersetv1alpha1.PodTemplate {
	if p != nil {
		return p
	}
	return &supersetv1alpha1.PodTemplate{}
}

func safeContainerTemplatePtr(c *supersetv1alpha1.ContainerTemplate) *supersetv1alpha1.ContainerTemplate {
	if c != nil {
		return c
	}
	return &supersetv1alpha1.ContainerTemplate{}
}

// resolveProbe returns the user probe, or the component default when the user
// set none. An empty user probe ({}) disables the probe, since a CR cannot use
// null to remove a default.
func resolveProbe(user, def *corev1.Probe) *corev1.Probe {
	if user != nil {
		if *user == (corev1.Probe{}) {
			return nil
		}
		return user
	}
	return def
}

// buildServiceSpec constructs a ServiceSpec from the component's ComponentServiceSpec,
// selector labels, and the component container port, which is also the default service port.
func buildServiceSpec(
	svcSpec *supersetv1alpha1.ComponentServiceSpec,
	labels map[string]string,
	containerPort int32,
) corev1.ServiceSpec {
	port := containerPort
	svcType := corev1.ServiceTypeClusterIP
	var nodePort int32

	if svcSpec != nil {
		if svcSpec.Port != nil && *svcSpec.Port != 0 {
			port = *svcSpec.Port
		}
		if svcSpec.Type != "" {
			svcType = svcSpec.Type
		}
		if svcSpec.NodePort != nil {
			nodePort = *svcSpec.NodePort
		}
	}

	svcPort := corev1.ServicePort{
		Name:       "http",
		Port:       port,
		TargetPort: intstr.FromInt32(containerPort),
		Protocol:   corev1.ProtocolTCP,
	}
	if nodePort != 0 {
		svcPort.NodePort = nodePort
	}

	return corev1.ServiceSpec{
		Type:     svcType,
		Selector: labels,
		Ports:    []corev1.ServicePort{svcPort},
	}
}

// applyContainerSecurityDefaults fills the operator's UID-independent
// container hardening defaults into a user-provided securityContext where the
// user left them unset: privilege escalation disabled, all Linux capabilities
// dropped, and the RuntimeDefault seccomp profile. Defaults are applied
// field-by-field, so any user-set field always wins.
//
// These harden workloads on permissive clusters and satisfy several of the
// restricted Pod Security Standard's requirements (privilege escalation,
// capabilities, seccomp) without pinning a UID; full restricted compatibility
// still requires user-provided non-root settings. runAsNonRoot/runAsUser are
// deliberately NOT defaulted: the Superset image declares a named user
// (`USER superset`), so runAsNonRoot alone is rejected by the kubelet
// ("non-numeric user"), and pinning a numeric UID collides with OpenShift's
// per-namespace SCC UID range and the Flower
// bootstrapScript root case. Achieving the restricted profile therefore stays a
// user opt-in via podTemplate/containerTemplate securityContext.
//
// podSC is the resolved pod-level securityContext. Container-level seccomp
// overrides pod-level, so a container-level seccomp default is only applied when
// neither level sets one — otherwise a user's pod-level profile would be
// silently overridden.
func applyContainerSecurityDefaults(sc *corev1.SecurityContext, podSC *corev1.PodSecurityContext) *corev1.SecurityContext {
	out := sc.DeepCopy()
	if out == nil {
		out = &corev1.SecurityContext{}
	}
	if out.AllowPrivilegeEscalation == nil {
		out.AllowPrivilegeEscalation = new(false)
	}
	// Default drop: [ALL] when no drop set is specified, preserving any
	// user-provided capability add/drop lists.
	if out.Capabilities == nil {
		out.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{capabilityAll}}
	} else if out.Capabilities.Drop == nil {
		out.Capabilities.Drop = []corev1.Capability{capabilityAll}
	}
	if out.SeccompProfile == nil && (podSC == nil || podSC.SeccompProfile == nil) {
		out.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	}
	return out
}
