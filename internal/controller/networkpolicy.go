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

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

// reconcileNetworkPolicies reconciles NetworkPolicy resources for all components.
func (r *SupersetReconciler) reconcileNetworkPolicies(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	enabled := superset.Spec.NetworkPolicy != nil
	topPT := superset.Spec.PodTemplate

	// Define all components that need NetworkPolicies (no Init).
	components := []struct {
		resourceBaseName string
		childName        string
		component        string
		enabled          bool
		port             int32 // non-zero for externally-facing components
	}{
		{webServerDescriptor.resourceBaseName(&superset.Spec, superset.Name),
			webServerDescriptor.childName(&superset.Spec, superset.Name),
			string(common.ComponentWebServer), superset.Spec.WebServer != nil,
			npContainerPort(common.PortWebServer, topPT, scalablePT(superset.Spec.WebServer))},
		{celeryWorkerDescriptor.resourceBaseName(&superset.Spec, superset.Name),
			celeryWorkerDescriptor.childName(&superset.Spec, superset.Name),
			string(common.ComponentCeleryWorker), superset.Spec.CeleryWorker != nil, 0},
		{celeryBeatDescriptor.resourceBaseName(&superset.Spec, superset.Name),
			celeryBeatDescriptor.childName(&superset.Spec, superset.Name),
			string(common.ComponentCeleryBeat), superset.Spec.CeleryBeat != nil, 0},
		{celeryFlowerDescriptor.resourceBaseName(&superset.Spec, superset.Name),
			celeryFlowerDescriptor.childName(&superset.Spec, superset.Name),
			string(common.ComponentCeleryFlower), superset.Spec.CeleryFlower != nil,
			npContainerPort(common.PortCeleryFlower, topPT, scalablePT(superset.Spec.CeleryFlower))},
		{websocketServerDescriptor.resourceBaseName(&superset.Spec, superset.Name),
			websocketServerDescriptor.childName(&superset.Spec, superset.Name),
			string(common.ComponentWebsocketServer), superset.Spec.WebsocketServer != nil,
			npContainerPort(common.PortWebsocket, topPT, scalablePT(superset.Spec.WebsocketServer))},
		{mcpServerDescriptor.resourceBaseName(&superset.Spec, superset.Name),
			mcpServerDescriptor.childName(&superset.Spec, superset.Name),
			string(common.ComponentMcpServer), superset.Spec.McpServer != nil,
			npContainerPort(common.PortMcpServer, topPT, scalablePT(superset.Spec.McpServer))},
	}

	for _, comp := range components {
		desiredNPName := ""
		if enabled && comp.enabled {
			desiredNPName = comp.resourceBaseName + common.SuffixNetworkPolicy
		}

		if err := r.pruneOrphans(ctx, superset.Namespace, superset.Name,
			common.ComponentType(comp.component),
			func() client.ObjectList { return &networkingv1.NetworkPolicyList{} },
			desiredNPName,
		); err != nil {
			return err
		}

		if desiredNPName == "" {
			continue
		}

		if err := r.reconcileComponentNetworkPolicy(ctx, superset, desiredNPName, comp.component, comp.childName, comp.port); err != nil {
			return err
		}
	}

	return nil
}

func (r *SupersetReconciler) reconcileComponentNetworkPolicy(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	npName, component, childName string,
	externalPort int32,
) error {
	np := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      npName,
			Namespace: superset.Namespace,
		},
	}

	labels := componentLabels(component, childName)
	npSpec := superset.Spec.NetworkPolicy

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
		if err := controllerutil.SetControllerReference(superset, np, r.Scheme); err != nil {
			return err
		}

		np.Labels = mergeLabels(np.Labels, map[string]string{
			common.LabelKeyName:      common.LabelValueApp,
			common.LabelKeyComponent: component,
			common.LabelKeyParent:    superset.Name,
		})

		np.Spec = networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: labels},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
			// Allow all egress by default (components need access to databases, caches, etc.).
			Egress: []networkingv1.NetworkPolicyEgressRule{{}},
			// Allow ingress from pods belonging to the same Superset instance.
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: []networkingv1.NetworkPolicyPeer{
						{
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									common.LabelKeyName:   common.LabelValueApp,
									common.LabelKeyParent: superset.Name,
								},
							},
						},
					},
				},
			},
		}

		// For externally-facing components, allow ingress on the service port
		// from any source (Ingress controllers, Gateway controllers, load balancers).
		if externalPort > 0 {
			proto := corev1.ProtocolTCP
			port := intstr.FromInt32(externalPort)
			np.Spec.Ingress = append(np.Spec.Ingress, networkingv1.NetworkPolicyIngressRule{
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &proto, Port: &port},
				},
			})
		}

		// Append user-defined extra rules.
		if len(npSpec.ExtraIngress) > 0 {
			np.Spec.Ingress = append(np.Spec.Ingress, npSpec.ExtraIngress...)
		}
		if len(npSpec.ExtraEgress) > 0 {
			np.Spec.Egress = append(np.Spec.Egress, npSpec.ExtraEgress...)
		}

		return nil
	})
	return err
}

// npContainerPort resolves the container port for NetworkPolicy rules using the
// same merge semantics as the resolution engine: top-level ports first,
// component ports override by name, then take the first result.
func npContainerPort(defaultPort int32, topPT, compPT *supersetv1alpha1.PodTemplate) int32 {
	var topPorts, compPorts []corev1.ContainerPort
	if topPT != nil && topPT.Container != nil {
		topPorts = topPT.Container.Ports
	}
	if compPT != nil && compPT.Container != nil {
		compPorts = compPT.Container.Ports
	}
	merged := resolution.MergeContainerPorts(topPorts, compPorts)
	if len(merged) > 0 {
		return merged[0].ContainerPort
	}
	return defaultPort
}

// scalablePT extracts the PodTemplate from a scalable component spec, returning
// nil if the component is nil or has no PodTemplate.
func scalablePT(comp any) *supersetv1alpha1.PodTemplate {
	switch s := comp.(type) {
	case *supersetv1alpha1.WebServerComponentSpec:
		if s != nil {
			return s.PodTemplate
		}
	case *supersetv1alpha1.CeleryFlowerComponentSpec:
		if s != nil {
			return s.PodTemplate
		}
	case *supersetv1alpha1.WebsocketServerComponentSpec:
		if s != nil {
			return s.PodTemplate
		}
	case *supersetv1alpha1.McpServerComponentSpec:
		if s != nil {
			return s.PodTemplate
		}
	}
	return nil
}
