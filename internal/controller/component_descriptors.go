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

// componentDescriptor captures all per-component variation needed to
// reconcile a child CR from the parent Superset controller.
type componentDescriptor struct {
	suffix          string
	componentType   naming.ComponentType
	hasPythonConfig bool
	kind            string

	extract   func(*supersetv1alpha1.SupersetSpec) *componentAccessor
	newChild  func() client.Object
	newList   func() client.ObjectList
	applySpec func(obj client.Object, flat supersetv1alpha1.FlatComponentSpec, config, checksum string, a *componentAccessor)
	setStatus func(*supersetv1alpha1.ComponentStatusMap, *supersetv1alpha1.ComponentRefStatus)
}

// childName returns the child CR name, which is always the parent name.
func (d *componentDescriptor) childName(spec *supersetv1alpha1.SupersetSpec, parentName string) string {
	return parentName
}

// resourceBaseName resolves the sub-resource base name: {childCRName}-{componentType}.
func (d *componentDescriptor) resourceBaseName(spec *supersetv1alpha1.SupersetSpec, parentName string) string {
	childName := d.childName(spec, parentName)
	return naming.ResourceBaseName(childName, d.componentType)
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

// reconcileComponent is the generic reconciler for all child CRs (both Python and non-Python).
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
		desiredName = desc.childName(&superset.Spec, superset.Name)
		desiredResourceBaseName = naming.ResourceBaseName(desiredName, desc.componentType)
	}

	if err := r.pruneOrphans(ctx, superset.Namespace, superset.Name,
		desc.componentType, desc.newList, desiredName); err != nil {
		return err
	}

	if accessor == nil {
		return nil
	}

	childName := desiredName
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
		secretEnvVars = collectSecretEnvVars(&superset.Spec)
		operatorInjected = buildOperatorInjected(renderedConfig, resourceBaseName, superset.Spec.ForceReload, secretEnvVars)

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

	flat := resolution.ResolveChildSpec(
		desc.componentType, topLevel, comp,
		podOperatorLabels(string(desc.componentType), childName, superset.Name), operatorInjected,
	)

	componentChecksum := computeChecksum(configChecksum + renderedConfig)

	return r.applyChildCR(ctx, superset, childName, desc.componentType, flat, renderedConfig, componentChecksum, saName, accessor.image,
		desc.newChild,
		func(obj client.Object, flatSpec supersetv1alpha1.FlatComponentSpec, config, checksum string) {
			desc.applySpec(obj, flatSpec, config, checksum, accessor)
		},
	)
}

// injectCeleryCommand sets the celery worker command on the ComponentInput's
// pod template, allowing the resolution engine to use it instead of the
// child controller's DefaultCommand.
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
	suffix:          naming.SuffixWebServer,
	componentType:   naming.ComponentWebServer,
	hasPythonConfig: true,
	kind:            "SupersetWebServer",
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
	newChild: func() client.Object { return &supersetv1alpha1.SupersetWebServer{} },
	newList:  func() client.ObjectList { return &supersetv1alpha1.SupersetWebServerList{} },
	applySpec: func(obj client.Object, flat supersetv1alpha1.FlatComponentSpec, config, checksum string, a *componentAccessor) {
		ww := obj.(*supersetv1alpha1.SupersetWebServer)
		ww.Spec.FlatComponentSpec = flat
		ww.Spec.Config = config
		ww.Spec.ConfigChecksum = checksum
		ww.Spec.Service = a.service
	},
	setStatus: func(m *supersetv1alpha1.ComponentStatusMap, s *supersetv1alpha1.ComponentRefStatus) {
		m.WebServer = s
	},
}

var celeryWorkerDescriptor = &componentDescriptor{
	suffix:          naming.SuffixCeleryWorker,
	componentType:   naming.ComponentCeleryWorker,
	hasPythonConfig: true,
	kind:            "SupersetCeleryWorker",
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
	newChild: func() client.Object { return &supersetv1alpha1.SupersetCeleryWorker{} },
	newList:  func() client.ObjectList { return &supersetv1alpha1.SupersetCeleryWorkerList{} },
	applySpec: func(obj client.Object, flat supersetv1alpha1.FlatComponentSpec, config, checksum string, a *componentAccessor) {
		cw := obj.(*supersetv1alpha1.SupersetCeleryWorker)
		cw.Spec.FlatComponentSpec = flat
		cw.Spec.Config = config
		cw.Spec.ConfigChecksum = checksum
	},
	setStatus: func(m *supersetv1alpha1.ComponentStatusMap, s *supersetv1alpha1.ComponentRefStatus) {
		m.CeleryWorker = s
	},
}

var celeryBeatDescriptor = &componentDescriptor{
	suffix:          naming.SuffixCeleryBeat,
	componentType:   naming.ComponentCeleryBeat,
	hasPythonConfig: true,
	kind:            "SupersetCeleryBeat",
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
	newChild: func() client.Object { return &supersetv1alpha1.SupersetCeleryBeat{} },
	newList:  func() client.ObjectList { return &supersetv1alpha1.SupersetCeleryBeatList{} },
	applySpec: func(obj client.Object, flat supersetv1alpha1.FlatComponentSpec, config, checksum string, _ *componentAccessor) {
		cb := obj.(*supersetv1alpha1.SupersetCeleryBeat)
		flat.Autoscaling = nil
		flat.PodDisruptionBudget = nil
		cb.Spec.FlatComponentSpec = flat
		cb.Spec.Config = config
		cb.Spec.ConfigChecksum = checksum
	},
	setStatus: func(m *supersetv1alpha1.ComponentStatusMap, s *supersetv1alpha1.ComponentRefStatus) {
		m.CeleryBeat = s
	},
}

var celeryFlowerDescriptor = &componentDescriptor{
	suffix:          naming.SuffixCeleryFlower,
	componentType:   naming.ComponentCeleryFlower,
	hasPythonConfig: true,
	kind:            "SupersetCeleryFlower",
	extract: func(spec *supersetv1alpha1.SupersetSpec) *componentAccessor {
		c := spec.CeleryFlower
		if c == nil {
			return nil
		}
		return extractScalable(&c.ScalableComponentSpec, c.Config, c.Image, c.Service)
	},
	newChild: func() client.Object { return &supersetv1alpha1.SupersetCeleryFlower{} },
	newList:  func() client.ObjectList { return &supersetv1alpha1.SupersetCeleryFlowerList{} },
	applySpec: func(obj client.Object, flat supersetv1alpha1.FlatComponentSpec, config, checksum string, a *componentAccessor) {
		cf := obj.(*supersetv1alpha1.SupersetCeleryFlower)
		cf.Spec.FlatComponentSpec = flat
		cf.Spec.Config = config
		cf.Spec.ConfigChecksum = checksum
		cf.Spec.Service = a.service
	},
	setStatus: func(m *supersetv1alpha1.ComponentStatusMap, s *supersetv1alpha1.ComponentRefStatus) {
		m.CeleryFlower = s
	},
}

var mcpServerDescriptor = &componentDescriptor{
	suffix:          naming.SuffixMcpServer,
	componentType:   naming.ComponentMcpServer,
	hasPythonConfig: true,
	kind:            "SupersetMcpServer",
	extract: func(spec *supersetv1alpha1.SupersetSpec) *componentAccessor {
		c := spec.McpServer
		if c == nil {
			return nil
		}
		a := extractScalable(&c.ScalableComponentSpec, c.Config, c.Image, c.Service)
		a.sqlaEngineOptions = c.SQLAlchemyEngineOptions
		return a
	},
	newChild: func() client.Object { return &supersetv1alpha1.SupersetMcpServer{} },
	newList:  func() client.ObjectList { return &supersetv1alpha1.SupersetMcpServerList{} },
	applySpec: func(obj client.Object, flat supersetv1alpha1.FlatComponentSpec, config, checksum string, a *componentAccessor) {
		ms := obj.(*supersetv1alpha1.SupersetMcpServer)
		ms.Spec.FlatComponentSpec = flat
		ms.Spec.Config = config
		ms.Spec.ConfigChecksum = checksum
		ms.Spec.Service = a.service
	},
	setStatus: func(m *supersetv1alpha1.ComponentStatusMap, s *supersetv1alpha1.ComponentRefStatus) {
		m.McpServer = s
	},
}

var websocketServerDescriptor = &componentDescriptor{
	suffix:          naming.SuffixWebsocketServer,
	componentType:   naming.ComponentWebsocketServer,
	hasPythonConfig: false,
	kind:            "SupersetWebsocketServer",
	extract: func(spec *supersetv1alpha1.SupersetSpec) *componentAccessor {
		c := spec.WebsocketServer
		if c == nil {
			return nil
		}
		return extractScalable(&c.ScalableComponentSpec, nil, c.Image, c.Service)
	},
	newChild: func() client.Object { return &supersetv1alpha1.SupersetWebsocketServer{} },
	newList:  func() client.ObjectList { return &supersetv1alpha1.SupersetWebsocketServerList{} },
	applySpec: func(obj client.Object, flat supersetv1alpha1.FlatComponentSpec, _, _ string, a *componentAccessor) {
		wss := obj.(*supersetv1alpha1.SupersetWebsocketServer)
		wss.Spec.FlatComponentSpec = flat
		wss.Spec.Service = a.service
	},
	setStatus: func(m *supersetv1alpha1.ComponentStatusMap, s *supersetv1alpha1.ComponentRefStatus) {
		m.WebsocketServer = s
	},
}
