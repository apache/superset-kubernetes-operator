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

// DeploymentConfig holds the component-specific defaults needed to build
// a Deployment for any Superset child component.
type DeploymentConfig struct {
	// ContainerName is the name of the main container (e.g. "superset-web-server").
	ContainerName string

	// DefaultCommand is used when the spec Command is empty.
	DefaultCommand []string

	// DefaultArgs is used when the spec Args is empty.
	DefaultArgs []string

	// DefaultPorts are the container ports when none are specified on the spec.
	DefaultPorts []corev1.ContainerPort

	// ForceReplicas, when non-nil, overrides the spec replicas (used for singletons like beat).
	ForceReplicas *int32
}

// buildDeploymentSpec constructs a complete DeploymentSpec from the flat child spec,
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

	// Use spec ports if provided, otherwise fall back to component defaults.
	ports := cfg.DefaultPorts
	if len(ct.Ports) > 0 {
		ports = ct.Ports
	}

	// Build the main container from ContainerTemplate.
	mainContainer := corev1.Container{
		Name:            cfg.ContainerName,
		Image:           image,
		ImagePullPolicy: spec.Image.PullPolicy,
		Command:         command,
		Env:             ct.Env,
		EnvFrom:         ct.EnvFrom,
		VolumeMounts:    ct.VolumeMounts,
		Ports:           ports,
		LivenessProbe:   ct.LivenessProbe,
		ReadinessProbe:  ct.ReadinessProbe,
		StartupProbe:    ct.StartupProbe,
		SecurityContext: ct.SecurityContext,
		Lifecycle:       ct.Lifecycle,
	}
	if ct.Resources != nil {
		mainContainer.Resources = *ct.Resources
	}
	if len(args) > 0 {
		mainContainer.Args = args
	}

	// Build containers list: main + sidecars.
	containers := []corev1.Container{mainContainer}
	containers = append(containers, pt.Sidecars...)

	// Build pod labels and annotations.
	podLabels := mergeLabels(pt.Labels, selectorLabels)
	allPodAnnotations := mergeAnnotations(podAnnotations, pt.Annotations)

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

// resolveContainerPort returns the first container port from the spec (if the
// user overrode container ports), otherwise the component default.
func resolveContainerPort(spec *supersetv1alpha1.FlatComponentSpec, defaultPort int32) int32 {
	if spec != nil && spec.PodTemplate != nil && spec.PodTemplate.Container != nil && len(spec.PodTemplate.Container.Ports) > 0 {
		return spec.PodTemplate.Container.Ports[0].ContainerPort
	}
	return defaultPort
}

// buildServiceSpec constructs a ServiceSpec from the component's ComponentServiceSpec,
// selector labels, container port, and a default service port.
func buildServiceSpec(
	svcSpec *supersetv1alpha1.ComponentServiceSpec,
	labels map[string]string,
	containerPort int32,
	defaultPort int32,
) corev1.ServiceSpec {
	port := defaultPort
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
