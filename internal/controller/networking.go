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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

// reconcileNetworking reconciles the networking resources (HTTPRoute or Ingress)
// based on the Superset spec.
func (r *SupersetReconciler) reconcileNetworking(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	// Determine which networking mode is active.
	gatewayEnabled := superset.Spec.Networking != nil && superset.Spec.Networking.Gateway != nil
	ingressEnabled := superset.Spec.Networking != nil && superset.Spec.Networking.Ingress != nil

	parentLbls := parentLabels(superset.Name)

	// Clean up resources not in use.
	if !gatewayEnabled {
		if err := r.deleteByLabels(ctx, superset.Namespace, parentLbls,
			func() client.ObjectList { return &gatewayv1.HTTPRouteList{} }, ""); err != nil {
			return err
		}
	}
	if !ingressEnabled {
		if err := r.deleteByLabels(ctx, superset.Namespace, parentLbls,
			func() client.ObjectList { return &networkingv1.IngressList{} }, ""); err != nil {
			return err
		}
	}

	// Create/update the active resource.
	if gatewayEnabled {
		return r.reconcileHTTPRoute(ctx, superset)
	}
	if ingressEnabled {
		return r.reconcileIngress(ctx, superset)
	}

	return nil
}

// webServerServiceRef returns the web server service name and port.
func webServerServiceRef(superset *supersetv1alpha1.Superset) (string, int32) {
	name := webServerDescriptor.resourceBaseName(&superset.Spec, superset.Name)
	port := common.PortWebServer
	if superset.Spec.WebServer != nil && superset.Spec.WebServer.Service != nil && superset.Spec.WebServer.Service.Port != nil {
		port = *superset.Spec.WebServer.Service.Port
	}
	return name, port
}

// reconcileWebServerService creates or updates the web-server Service, owned by
// the parent Superset CR. The selector is based on MaintenanceActive status:
// during maintenance it routes to maintenance page pods; otherwise to web-server pods.
func (r *SupersetReconciler) reconcileWebServerService(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	svcName, _ := webServerServiceRef(superset)

	if superset.Spec.WebServer == nil {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: superset.Namespace},
		}
		if err := r.Delete(ctx, svc); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("deleting web-server Service: %w", err)
		}
		return nil
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: superset.Namespace,
		},
	}

	// Determine selector based on lifecycle state.
	var selector map[string]string
	if superset.Status.Lifecycle != nil && superset.Status.Lifecycle.MaintenanceActive {
		selector = common.ComponentLabels(common.ComponentMaintenancePage, superset.Name)
	} else {
		selector = common.ComponentLabels(common.ComponentWebServer, superset.Name)
	}

	svcSpec := superset.Spec.WebServer.Service
	containerPort := resolveWebServerPort(superset)
	webServerLabels := componentLabels(string(common.ComponentWebServer), superset.Name)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		// Clear existing owner references to handle upgrades from earlier
		// versions where the Service may have been owned by another controller.
		svc.OwnerReferences = nil
		if err := controllerutil.SetControllerReference(superset, svc, r.Scheme); err != nil {
			return err
		}
		desiredSpec := buildServiceSpec(svcSpec, selector, containerPort, common.PortWebServer)
		preserveServiceAllocatedFields(&desiredSpec, svc.Spec)
		svc.Spec = desiredSpec
		var userLabels map[string]string
		var userAnnotations map[string]string
		if svcSpec != nil {
			userLabels = svcSpec.Labels
			userAnnotations = svcSpec.Annotations
		}
		svc.Labels = mergeLabels(userLabels, webServerLabels)
		svc.Annotations = mergeAnnotations(nil, userAnnotations)
		return nil
	})
	return err
}

// resolveWebServerPort returns the resolved web-server container port, taking
// into account top-level and per-component pod template port overrides. Mirrors
// the port resolution used by the generic component Service reconciler so the
// parent-owned web-server Service, the rendered SUPERSET_WEBSERVER_PORT, and
// the rendered Deployment container port all agree.
func resolveWebServerPort(superset *supersetv1alpha1.Superset) int32 {
	if superset == nil || superset.Spec.WebServer == nil {
		return common.PortWebServer
	}
	topLevel := convertTopLevelSpec(&superset.Spec)
	accessor := webServerDescriptor.extract(&superset.Spec)
	flat := resolution.ResolveComponentSpec(
		common.ComponentWebServer, topLevel, convertComponent(accessor),
		nil, &resolution.OperatorInjected{},
	)
	if flat != nil && flat.PodTemplate != nil && flat.PodTemplate.Container != nil && len(flat.PodTemplate.Container.Ports) > 0 {
		return flat.PodTemplate.Container.Ports[0].ContainerPort
	}
	return common.PortWebServer
}

func (r *SupersetReconciler) reconcileHTTPRoute(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	gw := superset.Spec.Networking.Gateway

	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      superset.Name,
			Namespace: superset.Namespace,
		},
	}

	_, webServerPort := webServerServiceRef(superset)
	webServerSvcName := gatewayv1.ObjectName(webServerDescriptor.resourceBaseName(&superset.Spec, superset.Name))
	gwPort := webServerPort

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, route, func() error {
		if err := controllerutil.SetControllerReference(superset, route, r.Scheme); err != nil {
			return err
		}

		route.Labels = mergeLabels(gw.Labels, parentLabels(superset.Name))
		route.Annotations = mergeAnnotations(nil, gw.Annotations)

		var rules []gatewayv1.HTTPRouteRule

		// Add websocket route FIRST (more specific) if websocket server is enabled.
		if superset.Spec.WebsocketServer != nil {
			svcName := gatewayv1.ObjectName(websocketServerDescriptor.resourceBaseName(&superset.Spec, superset.Name))
			port := resolveServicePort(superset.Spec.WebsocketServer.Service, common.PortWebsocket)
			path := resolveGatewayPath(superset.Spec.WebsocketServer.Service, "/ws")
			rules = append(rules, buildHTTPRouteRule(svcName, port, path))
		}

		// Add MCP route if MCP server is enabled.
		if superset.Spec.McpServer != nil {
			svcName := gatewayv1.ObjectName(mcpServerDescriptor.resourceBaseName(&superset.Spec, superset.Name))
			port := resolveServicePort(superset.Spec.McpServer.Service, common.PortMcpServer)
			path := resolveGatewayPath(superset.Spec.McpServer.Service, "/mcp")
			rules = append(rules, buildHTTPRouteRule(svcName, port, path))
		}

		// Add Celery Flower route if enabled.
		if superset.Spec.CeleryFlower != nil {
			svcName := gatewayv1.ObjectName(celeryFlowerDescriptor.resourceBaseName(&superset.Spec, superset.Name))
			port := resolveServicePort(superset.Spec.CeleryFlower.Service, common.PortCeleryFlower)
			path := resolveGatewayPath(superset.Spec.CeleryFlower.Service, "/flower")
			rules = append(rules, buildHTTPRouteRule(svcName, port, path))
		}

		// Default route for web server (less specific, listed LAST).
		if superset.Spec.WebServer != nil {
			rules = append(rules, buildHTTPRouteRule(webServerSvcName, gwPort, "/"))
		}

		route.Spec = gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{gw.GatewayRef},
			},
			Hostnames: gw.Hostnames,
			Rules:     rules,
		}

		return nil
	})
	if meta.IsNoMatchError(err) {
		logf.FromContext(ctx).Info("Gateway API CRDs not installed, skipping HTTPRoute reconciliation")
		return nil
	}
	return err
}

// resolveServicePort returns the custom port from the service spec, or the default.
func resolveServicePort(svc *supersetv1alpha1.ComponentServiceSpec, defaultPort int32) gatewayv1.PortNumber {
	if svc != nil && svc.Port != nil {
		return *svc.Port
	}
	return defaultPort
}

// resolveGatewayPath returns the custom gateway path from the service spec,
// or the default if unset.
func resolveGatewayPath(svc *supersetv1alpha1.ComponentServiceSpec, defaultPath string) string {
	if svc != nil && svc.GatewayPath != nil && *svc.GatewayPath != "" {
		return *svc.GatewayPath
	}
	return defaultPath
}

// buildHTTPRouteRule constructs a single HTTPRouteRule for a component backend.
func buildHTTPRouteRule(svcName gatewayv1.ObjectName, port gatewayv1.PortNumber, path string) gatewayv1.HTTPRouteRule {
	pathPrefix := gatewayv1.PathMatchPathPrefix
	return gatewayv1.HTTPRouteRule{
		Matches: []gatewayv1.HTTPRouteMatch{
			{
				Path: &gatewayv1.HTTPPathMatch{
					Type:  &pathPrefix,
					Value: &path,
				},
			},
		},
		BackendRefs: []gatewayv1.HTTPBackendRef{
			{
				BackendRef: gatewayv1.BackendRef{
					BackendObjectReference: gatewayv1.BackendObjectReference{
						Name: svcName,
						Port: &port,
					},
				},
			},
		},
	}
}

func (r *SupersetReconciler) reconcileIngress(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	ing := superset.Spec.Networking.Ingress

	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      superset.Name,
			Namespace: superset.Namespace,
		},
	}

	webServerSvcName, webServerPort := webServerServiceRef(superset)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ingress, func() error {
		if err := controllerutil.SetControllerReference(superset, ingress, r.Scheme); err != nil {
			return err
		}

		ingress.Labels = mergeLabels(ing.Labels, parentLabels(superset.Name))
		ingress.Annotations = mergeAnnotations(nil, ing.Annotations)

		ingress.Spec = networkingv1.IngressSpec{
			IngressClassName: ing.ClassName,
			TLS:              ing.TLS,
		}

		// When Hosts is empty but Host is set, create a default IngressHost from it.
		hosts := ing.Hosts
		if len(hosts) == 0 && ing.Host != "" {
			hosts = []supersetv1alpha1.IngressHost{
				{Host: ing.Host},
			}
		}

		// Build rules from hosts.
		for _, h := range hosts {
			rule := networkingv1.IngressRule{
				Host: h.Host,
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{},
				},
			}

			if len(h.Paths) > 0 {
				for _, p := range h.Paths {
					pathType := networkingv1.PathTypePrefix
					if p.PathType != nil {
						pathType = *p.PathType
					}
					rule.HTTP.Paths = append(rule.HTTP.Paths,
						networkingv1.HTTPIngressPath{
							Path:     p.Path,
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: webServerSvcName,
									Port: networkingv1.ServiceBackendPort{
										Number: webServerPort,
									},
								},
							},
						},
					)
				}
			} else {
				// Default path.
				pathType := networkingv1.PathTypePrefix
				rule.HTTP.Paths = []networkingv1.HTTPIngressPath{
					{
						Path:     "/",
						PathType: &pathType,
						Backend: networkingv1.IngressBackend{
							Service: &networkingv1.IngressServiceBackend{
								Name: webServerSvcName,
								Port: networkingv1.ServiceBackendPort{
									Number: webServerPort,
								},
							},
						},
					},
				}
			}

			ingress.Spec.Rules = append(ingress.Spec.Rules, rule)
		}

		return nil
	})
	return err
}
