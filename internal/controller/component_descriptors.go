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

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	supersetconfig "github.com/apache/superset-kubernetes-operator/internal/config"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// componentAccessor holds the common fields extracted from any parent component spec.
// Nil accessor means the component is absent (disabled).
type componentAccessor struct {
	deploymentTemplate *supersetv1alpha1.DeploymentTemplate
	podTemplate        *supersetv1alpha1.PodTemplate
	replicas           *int32
	autoscaling        *supersetv1alpha1.AutoscalingSpec
	pdb                *supersetv1alpha1.PDBSpec
	config             *string
	image              *supersetv1alpha1.ImageOverrideSpec
	service            *supersetv1alpha1.ComponentServiceSpec
	gunicorn           *supersetv1alpha1.GunicornSpec
	celery             *supersetv1alpha1.CeleryWorkerProcessSpec
	sqlaEngineOptions  *supersetv1alpha1.SQLAlchemyEngineOptionsSpec
}

// componentDescriptor captures all per-component variation needed to reconcile
// parent-owned resources from the parent Superset controller.
type componentDescriptor struct {
	componentType   naming.ComponentType
	hasPythonConfig bool

	extract        func(*supersetv1alpha1.SupersetSpec) *componentAccessor
	adjustSpec     func(*supersetv1alpha1.FlatComponentSpec)
	statusAccessor func(*supersetv1alpha1.ComponentStatusMap) **supersetv1alpha1.ComponentRefStatus
}

// instanceName returns the logical component instance name, currently the parent
// name for all components.
func (d *componentDescriptor) instanceName(_ *supersetv1alpha1.SupersetSpec, parentName string) string {
	return parentName
}

// resourceBaseName resolves the component resource base name:
// {componentInstanceName}-{componentType}.
func (d *componentDescriptor) resourceBaseName(spec *supersetv1alpha1.SupersetSpec, parentName string) string {
	instanceName := d.instanceName(spec, parentName)
	return naming.ResourceBaseName(instanceName, d.componentType)
}

// componentDescriptors lists all components reconciled by the parent controller.
var componentDescriptors = []*componentDescriptor{
	webServerDescriptor,
	celeryWorkerDescriptor,
	celeryBeatDescriptor,
	celeryFlowerDescriptor,
	mcpServerDescriptor,
	websocketServerDescriptor,
}

// convertComponent converts a componentAccessor to the resolution engine's ComponentInput.
func convertComponent(a *componentAccessor) *resolution.ComponentInput {
	if a == nil {
		return nil
	}
	return &resolution.ComponentInput{
		SharedInput: resolution.SharedInput{
			Replicas:            a.replicas,
			DeploymentTemplate:  a.deploymentTemplate,
			PodTemplate:         a.podTemplate,
			Autoscaling:         a.autoscaling,
			PodDisruptionBudget: a.pdb,
		},
	}
}

// warnEnvVarOverrides logs a warning when operator-injected env vars override
// user-specified values from the top-level or component spec.
func warnEnvVarOverrides(ctx context.Context, tl *resolution.SharedInput, comp *resolution.ComponentInput, op *resolution.OperatorInjected) {
	log := logf.FromContext(ctx)
	opNames := make(map[string]bool, len(op.Env))
	for _, e := range op.Env {
		opNames[e.Name] = true
	}
	tlEnv := envFromPodTemplate(tl.PodTemplate)
	for _, e := range tlEnv {
		if opNames[e.Name] {
			log.Info("operator env var overrides user-specified value", "var", e.Name, "source", "spec.podTemplate.container.env")
		}
	}
	if comp != nil {
		compEnv := envFromPodTemplate(comp.PodTemplate)
		for _, e := range compEnv {
			if opNames[e.Name] {
				log.Info("operator env var overrides user-specified value", "var", e.Name, "source", "component.podTemplate.container.env")
			}
		}
	}
}

func envFromPodTemplate(pt *supersetv1alpha1.PodTemplate) []corev1.EnvVar {
	if pt == nil || pt.Container == nil {
		return nil
	}
	return pt.Container.Env
}

// reconcileComponent is the generic reconciler for all component resources
// (both Python and non-Python). The parent Superset CR owns the rendered
// ConfigMap, Deployment, Service, HPA, and PDB resources directly.
func (r *SupersetReconciler) reconcileComponent(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	desc *componentDescriptor,
	topLevel *resolution.SharedInput,
	configChecksum, saName string,
) error {
	accessor := desc.extract(&superset.Spec)

	var desiredName string
	var desiredResourceBaseName string
	if accessor != nil {
		desiredName = desc.instanceName(&superset.Spec, superset.Name)
		desiredResourceBaseName = naming.ResourceBaseName(desiredName, desc.componentType)
	}

	if accessor == nil {
		return r.deleteComponentResources(ctx, superset, desc)
	}

	instanceName := desiredName
	resourceBaseName := desiredResourceBaseName

	comp := convertComponent(accessor)

	var renderedConfig string
	var secretEnvVars []corev1.EnvVar
	var operatorInjected *resolution.OperatorInjected

	if desc.hasPythonConfig {
		compConfigInput := buildConfigInput(&superset.Spec)
		if accessor.config != nil {
			compConfigInput.ComponentConfig = *accessor.config
		}
		if desc.componentType == naming.ComponentWebServer {
			compConfigInput.WebServerPort = resolveWebServerPort(superset)
		}

		// Compute SQLALCHEMY_ENGINE_OPTIONS per component.
		effectiveSQLASpec := superset.Spec.SQLAlchemyEngineOptions
		if accessor.sqlaEngineOptions != nil {
			effectiveSQLASpec = accessor.sqlaEngineOptions
		}
		var workers, threads int32
		switch desc.componentType {
		case naming.ComponentWebServer:
			g := supersetconfig.ResolveGunicorn(accessor.gunicorn)
			if !g.Disabled {
				workers, threads = g.Workers, g.Threads
			}
		case naming.ComponentCeleryWorker:
			c := supersetconfig.ResolveCelery(accessor.celery)
			if !c.Disabled {
				workers = c.Concurrency
			}
		}
		compConfigInput.EngineOptions = supersetconfig.ComputeEngineOptions(
			desc.componentType, effectiveSQLASpec, accessor.sqlaEngineOptions, workers, threads,
		)

		renderedConfig = supersetconfig.RenderConfig(desc.componentType, compConfigInput)
		secretEnvVars = collectSecretEnvVars(&superset.Spec, superset.Name)
		operatorInjected = buildOperatorInjected(renderedConfig, resourceBaseName, superset.Spec.ForceReload, secretEnvVars)

		// Create/update the component ConfigMap.
		if err := reconcileParentOwnedConfigMap(ctx, r.Client, r.Scheme, superset, renderedConfig, resourceBaseName); err != nil {
			return fmt.Errorf("reconciling ConfigMap for %s: %w", desc.componentType, err)
		}

		// Inject Gunicorn env vars for web server.
		if desc.componentType == naming.ComponentWebServer {
			g := supersetconfig.ResolveGunicorn(accessor.gunicorn)
			if !g.Disabled {
				operatorInjected.Env = append(operatorInjected.Env, g.EnvVars()...)
			}
		}

		// Inject celery worker command.
		if desc.componentType == naming.ComponentCeleryWorker {
			c := supersetconfig.ResolveCelery(accessor.celery)
			if !c.Disabled {
				injectCeleryCommand(comp, c.Command())
			}
		}
	} else {
		operatorInjected = &resolution.OperatorInjected{}
		if superset.Spec.ForceReload != "" {
			operatorInjected.Env = append(operatorInjected.Env, corev1.EnvVar{
				Name:  naming.EnvForceReload,
				Value: superset.Spec.ForceReload,
			})
		}
	}

	if desc.componentType == naming.ComponentCeleryFlower {
		operatorInjected.Env = append(operatorInjected.Env, corev1.EnvVar{
			Name:  naming.EnvFlowerURLPrefix,
			Value: resolveGatewayPath(accessor.service, "/flower"),
		})
	}

	warnEnvVarOverrides(ctx, topLevel, comp, operatorInjected)

	flat := resolution.ResolveComponentSpec(
		desc.componentType, topLevel, comp,
		podOperatorLabels(string(desc.componentType), instanceName, superset.Name), operatorInjected,
	)

	componentChecksum := computeChecksum(configChecksum + renderedConfig)

	flatSpec := flatSpecFromResolution(flat, &superset.Spec.Image, accessor.image, saName)
	if desc.adjustSpec != nil {
		desc.adjustSpec(&flatSpec)
	}

	cfg, ok := componentResourceConfig(desc.componentType)
	if !ok {
		return fmt.Errorf("missing resource config for %s", desc.componentType)
	}

	return reconcileComponentResources(ctx, r.Client, r.Scheme, r.Recorder, superset,
		&flatSpec, cfg, componentChecksum, accessor.service, flatSpec.Autoscaling, flatSpec.PodDisruptionBudget)
}

func (r *SupersetReconciler) deleteComponentResources(ctx context.Context, superset *supersetv1alpha1.Superset, desc *componentDescriptor) error {
	resourceBaseName := naming.ResourceBaseName(superset.Name, desc.componentType)

	deleteNamed := func(obj client.Object) error {
		obj.SetName(resourceBaseName)
		obj.SetNamespace(superset.Namespace)
		return client.IgnoreNotFound(r.Delete(ctx, obj))
	}

	if err := deleteNamed(&appsv1.Deployment{}); err != nil {
		return fmt.Errorf("deleting Deployment for disabled %s: %w", desc.componentType, err)
	}
	if err := deleteNamed(&autoscalingv2.HorizontalPodAutoscaler{}); err != nil {
		return fmt.Errorf("deleting HPA for disabled %s: %w", desc.componentType, err)
	}
	if err := deleteNamed(&policyv1.PodDisruptionBudget{}); err != nil {
		return fmt.Errorf("deleting PDB for disabled %s: %w", desc.componentType, err)
	}

	cfg, ok := componentResourceConfig(desc.componentType)
	if ok && cfg.defaultPort > 0 {
		if err := deleteNamed(&corev1.Service{}); err != nil {
			return fmt.Errorf("deleting Service for disabled %s: %w", desc.componentType, err)
		}
	}

	if desc.hasPythonConfig {
		if err := reconcileParentOwnedConfigMap(ctx, r.Client, r.Scheme, superset, "", resourceBaseName); err != nil {
			return fmt.Errorf("deleting ConfigMap for disabled %s: %w", desc.componentType, err)
		}
	}
	return nil
}

// injectCeleryCommand sets the celery worker command on the ComponentInput's
// pod template, allowing the resolution engine to use it instead of the
// component DeploymentConfig default command.
func injectCeleryCommand(comp *resolution.ComponentInput, cmd []string) {
	if comp == nil {
		return
	}
	if comp.PodTemplate == nil {
		comp.PodTemplate = &supersetv1alpha1.PodTemplate{}
	}
	if comp.PodTemplate.Container == nil {
		comp.PodTemplate.Container = &supersetv1alpha1.ContainerTemplate{}
	}
	if len(comp.PodTemplate.Container.Command) == 0 {
		comp.PodTemplate.Container.Command = cmd
	}
}

// --- Descriptor definitions ---

func extractScalable(s *supersetv1alpha1.ScalableComponentSpec, config *string, image *supersetv1alpha1.ImageOverrideSpec, service *supersetv1alpha1.ComponentServiceSpec) *componentAccessor {
	return &componentAccessor{
		deploymentTemplate: s.DeploymentTemplate,
		podTemplate:        s.PodTemplate,
		replicas:           s.Replicas,
		autoscaling:        s.Autoscaling,
		pdb:                s.PodDisruptionBudget,
		config:             config,
		image:              image,
		service:            service,
	}
}

var webServerDescriptor = &componentDescriptor{
	componentType:   naming.ComponentWebServer,
	hasPythonConfig: true,
	extract: func(spec *supersetv1alpha1.SupersetSpec) *componentAccessor {
		c := spec.WebServer
		if c == nil {
			return nil
		}
		a := extractScalable(&c.ScalableComponentSpec, c.Config, c.Image, c.Service)
		a.gunicorn = c.Gunicorn
		a.sqlaEngineOptions = c.SQLAlchemyEngineOptions
		return a
	},
	statusAccessor: func(m *supersetv1alpha1.ComponentStatusMap) **supersetv1alpha1.ComponentRefStatus {
		return &m.WebServer
	},
}

var celeryWorkerDescriptor = &componentDescriptor{
	componentType:   naming.ComponentCeleryWorker,
	hasPythonConfig: true,
	extract: func(spec *supersetv1alpha1.SupersetSpec) *componentAccessor {
		c := spec.CeleryWorker
		if c == nil {
			return nil
		}
		a := extractScalable(&c.ScalableComponentSpec, c.Config, c.Image, nil)
		a.celery = c.Celery
		a.sqlaEngineOptions = c.SQLAlchemyEngineOptions
		return a
	},
	statusAccessor: func(m *supersetv1alpha1.ComponentStatusMap) **supersetv1alpha1.ComponentRefStatus {
		return &m.CeleryWorker
	},
}

var celeryBeatDescriptor = &componentDescriptor{
	componentType:   naming.ComponentCeleryBeat,
	hasPythonConfig: true,
	extract: func(spec *supersetv1alpha1.SupersetSpec) *componentAccessor {
		c := spec.CeleryBeat
		if c == nil {
			return nil
		}
		return &componentAccessor{
			deploymentTemplate: c.DeploymentTemplate,
			podTemplate:        c.PodTemplate,
			config:             c.Config,
			image:              c.Image,
			sqlaEngineOptions:  c.SQLAlchemyEngineOptions,
		}
	},
	adjustSpec: func(flat *supersetv1alpha1.FlatComponentSpec) {
		flat.Autoscaling = nil
		flat.PodDisruptionBudget = nil
	},
	statusAccessor: func(m *supersetv1alpha1.ComponentStatusMap) **supersetv1alpha1.ComponentRefStatus {
		return &m.CeleryBeat
	},
}

var celeryFlowerDescriptor = &componentDescriptor{
	componentType:   naming.ComponentCeleryFlower,
	hasPythonConfig: true,
	extract: func(spec *supersetv1alpha1.SupersetSpec) *componentAccessor {
		c := spec.CeleryFlower
		if c == nil {
			return nil
		}
		return extractScalable(&c.ScalableComponentSpec, c.Config, c.Image, c.Service)
	},
	statusAccessor: func(m *supersetv1alpha1.ComponentStatusMap) **supersetv1alpha1.ComponentRefStatus {
		return &m.CeleryFlower
	},
}

var mcpServerDescriptor = &componentDescriptor{
	componentType:   naming.ComponentMcpServer,
	hasPythonConfig: true,
	extract: func(spec *supersetv1alpha1.SupersetSpec) *componentAccessor {
		c := spec.McpServer
		if c == nil {
			return nil
		}
		a := extractScalable(&c.ScalableComponentSpec, c.Config, c.Image, c.Service)
		a.sqlaEngineOptions = c.SQLAlchemyEngineOptions
		return a
	},
	statusAccessor: func(m *supersetv1alpha1.ComponentStatusMap) **supersetv1alpha1.ComponentRefStatus {
		return &m.McpServer
	},
}

var websocketServerDescriptor = &componentDescriptor{
	componentType:   naming.ComponentWebsocketServer,
	hasPythonConfig: false,
	extract: func(spec *supersetv1alpha1.SupersetSpec) *componentAccessor {
		c := spec.WebsocketServer
		if c == nil {
			return nil
		}
		return extractScalable(&c.ScalableComponentSpec, nil, c.Image, c.Service)
	},
	statusAccessor: func(m *supersetv1alpha1.ComponentStatusMap) **supersetv1alpha1.ComponentRefStatus {
		return &m.WebsocketServer
	},
}
