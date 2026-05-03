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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
	supersetconfig "github.com/apache/superset-kubernetes-operator/internal/config"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// SupersetReconciler reconciles a Superset object.
type SupersetReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=superset.apache.org,resources=supersets,verbs=get;list;watch
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebservers/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryworkers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryworkers/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetcelerybeats,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetcelerybeats/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryflowers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetceleryflowers/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebsocketservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetwebsocketservers/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetmcpservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetmcpservers/status,verbs=get
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetinits,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=superset.apache.org,resources=supersetinits/status,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete

func (r *SupersetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	superset := &supersetv1alpha1.Superset{}
	if err := r.Get(ctx, req.NamespacedName, superset); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle suspend.
	if superset.Spec.Suspend != nil && *superset.Spec.Suspend {
		log.Info("Reconciliation suspended", "name", superset.Name)
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeSuspended,
			metav1.ConditionTrue, "Suspended", "Reconciliation is suspended", superset.Generation)
		superset.Status.Phase = phaseSuspended
		superset.Status.ObservedGeneration = superset.Generation
		return ctrl.Result{}, r.Status().Update(ctx, superset)
	}

	// Clear Suspended condition when not suspended.
	setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeSuspended,
		metav1.ConditionFalse, "NotSuspended", "Reconciliation is not suspended", superset.Generation)

	log.Info("Reconciling Superset", "name", superset.Name)

	// Phase 1: Compute shared config checksum (per-component checksums are
	// derived from this combined with each component's rendered config).
	configChecksum := computeChecksum(struct {
		SecretKey           *string
		SecretKeyFrom       *corev1.SecretKeySelector
		Metastore           *supersetv1alpha1.MetastoreSpec
		Valkey              *supersetv1alpha1.ValkeySpec
		Config              *string
		SQLAEngineOptions   *supersetv1alpha1.SQLAlchemyEngineOptionsSpec
		WebServerGunicorn   *supersetv1alpha1.GunicornSpec
		CeleryWorkerProcess *supersetv1alpha1.CeleryWorkerProcessSpec
	}{
		superset.Spec.SecretKey, superset.Spec.SecretKeyFrom, superset.Spec.Metastore, superset.Spec.Valkey, superset.Spec.Config,
		superset.Spec.SQLAlchemyEngineOptions,
		gunicornSpecFrom(superset.Spec.WebServer),
		celerySpecFrom(superset.Spec.CeleryWorker),
	})

	// Phase 2: Reconcile shared resources.
	if err := r.reconcileServiceAccount(ctx, superset); err != nil {
		r.Recorder.Eventf(superset, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile ServiceAccount: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling ServiceAccount: %w", err)
	}

	// Phase 2.5: Init lifecycle via SupersetInit child CR.
	// Gates component deployment on init completion.
	topLevel := convertTopLevelSpec(&superset.Spec)
	saName := resolveServiceAccountName(superset)

	requeueAfter, initComplete, err := r.reconcileInit(ctx, superset, configChecksum, topLevel, saName)
	if err != nil {
		r.Recorder.Eventf(superset, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile Init: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling Init: %w", err)
	}
	if !initComplete {
		// Update status before returning.
		if statusErr := r.Status().Update(ctx, superset); statusErr != nil {
			return ctrl.Result{}, fmt.Errorf("updating status during init: %w", statusErr)
		}
		if requeueAfter < 0 {
			// Terminal failure — only a spec change (watch event) can recover.
			return ctrl.Result{}, nil
		}
		if requeueAfter > 0 {
			return ctrl.Result{RequeueAfter: requeueAfter}, nil
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Phase 3: Resolve and reconcile each component (table-driven).
	for _, desc := range componentDescriptors {
		if err := r.reconcileComponent(ctx, superset, desc, topLevel, configChecksum, saName); err != nil {
			r.Recorder.Eventf(superset, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile %s: %v", desc.componentType, err)
			return ctrl.Result{}, fmt.Errorf("reconciling %s: %w", desc.componentType, err)
		}
	}

	// Phase 4: Reconcile networking, monitoring, network policies.
	if err := r.reconcileNetworking(ctx, superset); err != nil {
		r.Recorder.Eventf(superset, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile Networking: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling Networking: %w", err)
	}

	if err := r.reconcileMonitoring(ctx, superset); err != nil {
		r.Recorder.Eventf(superset, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile Monitoring: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling Monitoring: %w", err)
	}

	if err := r.reconcileNetworkPolicies(ctx, superset); err != nil {
		r.Recorder.Eventf(superset, corev1.EventTypeWarning, "ReconcileError", "Failed to reconcile NetworkPolicies: %v", err)
		return ctrl.Result{}, fmt.Errorf("reconciling NetworkPolicies: %w", err)
	}

	// Phase 5: Update aggregate status.
	if err := r.updateStatus(ctx, superset); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

// applyChildCR creates or updates a child CR with the resolved flat spec.
func (r *SupersetReconciler) applyChildCR(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	childName string,
	componentType naming.ComponentType,
	flat *resolution.FlatSpec,
	renderedConfig, configChecksum, saName string,
	imageOverride *supersetv1alpha1.ImageOverrideSpec,
	newObj func() client.Object,
	applySpec func(client.Object, supersetv1alpha1.FlatComponentSpec, string, string),
) error {
	obj := newObj()
	obj.SetName(childName)
	obj.SetNamespace(superset.Namespace)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		if err := controllerutil.SetControllerReference(superset, obj, r.Scheme); err != nil {
			return err
		}
		obj.SetLabels(mergeLabels(obj.GetLabels(), map[string]string{
			naming.LabelKeyName:      naming.LabelValueApp,
			naming.LabelKeyComponent: string(componentType),
			naming.LabelKeyParent:    superset.Name,
		}))
		flatSpec := flatSpecFromResolution(flat, &superset.Spec.Image, imageOverride, saName)
		applySpec(obj, flatSpec, renderedConfig, configChecksum)
		return nil
	})
	return err
}

// --- Conversion helpers: CRD types -> resolution engine types ---

// defaultInitCommand is the fallback when spec.init is nil or has no
// adminUser/loadExamples options that require dynamic command construction.
var defaultInitCommand = []string{"/bin/sh", "-c", "superset db upgrade && superset init"}

func convertTopLevelSpec(spec *supersetv1alpha1.SupersetSpec) *resolution.SharedInput {
	return &resolution.SharedInput{
		Replicas:            spec.Replicas,
		DeploymentTemplate:  spec.DeploymentTemplate,
		PodTemplate:         spec.PodTemplate,
		Autoscaling:         spec.Autoscaling,
		PodDisruptionBudget: spec.PodDisruptionBudget,
	}
}

// buildInitCommand constructs the init shell command from InitSpec fields.
func buildInitCommand(init *supersetv1alpha1.InitSpec) []string {
	script := "superset db upgrade && superset init"

	if init.AdminUser != nil {
		// Wrap in a subshell with || true: create-admin exits non-zero when
		// the user already exists, which is expected on re-init runs.
		script += ` && (superset fab create-admin` +
			` --username "$SUPERSET_OPERATOR__ADMIN_USERNAME"` +
			` --password "$SUPERSET_OPERATOR__ADMIN_PASSWORD"` +
			` --firstname "$SUPERSET_OPERATOR__ADMIN_FIRST_NAME"` +
			` --lastname "$SUPERSET_OPERATOR__ADMIN_LAST_NAME"` +
			` --email "$SUPERSET_OPERATOR__ADMIN_EMAIL"` +
			` || true)`
	}

	if init.LoadExamples != nil && *init.LoadExamples {
		script += " && superset load-examples"
	}

	return []string{"/bin/sh", "-c", script}
}

func convertInitComponent(init *supersetv1alpha1.InitSpec) *resolution.ComponentInput {
	if init == nil {
		return &resolution.ComponentInput{
			SharedInput: resolution.SharedInput{
				PodTemplate: &supersetv1alpha1.PodTemplate{
					Container: &supersetv1alpha1.ContainerTemplate{
						Command: defaultInitCommand,
					},
				},
			},
		}
	}

	pt := init.PodTemplate

	// Determine effective command: explicit command takes precedence,
	// otherwise build dynamically from adminUser/loadExamples fields.
	cmd := init.Command
	if len(cmd) == 0 {
		cmd = buildInitCommand(init)
	}

	// Inject command into the PodTemplate's container.
	var ct *supersetv1alpha1.ContainerTemplate
	if pt != nil && pt.Container != nil {
		copied := *pt.Container
		ct = &copied
	} else {
		ct = &supersetv1alpha1.ContainerTemplate{}
	}
	ct.Command = cmd

	if pt != nil {
		copied := *pt
		copied.Container = ct
		pt = &copied
	} else {
		pt = &supersetv1alpha1.PodTemplate{Container: ct}
	}

	return &resolution.ComponentInput{
		SharedInput: resolution.SharedInput{
			PodTemplate: pt,
		},
	}
}

// reconcileInit creates or updates the SupersetInit child CR and reads its status
// to determine if initialization is complete. Returns (requeueAfter, initComplete, error).
func (r *SupersetReconciler) reconcileInit(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	configChecksum string,
	topLevel *resolution.SharedInput,
	saName string,
) (time.Duration, bool, error) {
	log := logf.FromContext(ctx)
	childName := superset.Name
	resourceBaseName := naming.ResourceBaseName(childName, naming.ComponentInit)

	// Prune orphaned init CRs (e.g., after a custom name change).
	keepName := childName
	if isInitDisabled(superset) {
		keepName = ""
	}
	if err := r.pruneOrphans(ctx, superset.Namespace, superset.Name,
		naming.ComponentInit,
		func() client.ObjectList { return &supersetv1alpha1.SupersetInitList{} },
		keepName,
	); err != nil {
		return 0, false, fmt.Errorf("pruning orphaned init CRs: %w", err)
	}

	// If init is disabled, mark complete.
	if isInitDisabled(superset) {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionTrue, "InitDisabled", "Initialization is disabled", superset.Generation)
		superset.Status.Init = nil
		return 0, true, nil
	}

	// Build the init child spec using the standard resolution pipeline.
	compConfigInput := buildConfigInput(&superset.Spec)
	if superset.Spec.Init != nil && superset.Spec.Init.Config != nil {
		compConfigInput.ComponentConfig = *superset.Spec.Init.Config
	}

	// Compute init engine options (always NullPool for short-lived init pods).
	var initSQLASpec *supersetv1alpha1.SQLAlchemyEngineOptionsSpec
	if superset.Spec.Init != nil {
		initSQLASpec = superset.Spec.Init.SQLAlchemyEngineOptions
	}
	compConfigInput.EngineOptions = supersetconfig.ComputeEngineOptions(
		naming.ComponentInit, superset.Spec.SQLAlchemyEngineOptions, initSQLASpec, 0, 0,
	)

	comp := convertInitComponent(superset.Spec.Init)
	renderedConfig := supersetconfig.RenderConfig(supersetconfig.ComponentInit, compConfigInput)

	secretEnvVars := collectSecretEnvVars(&superset.Spec)
	initEnvVars := collectInitEnvVars(&superset.Spec)
	operatorInjected := buildOperatorInjected(renderedConfig, resourceBaseName, superset.Spec.ForceReload, append(secretEnvVars, initEnvVars...))

	flat := resolution.ResolveChildSpec(
		resolution.ComponentInit, topLevel, comp,
		podOperatorLabels(string(naming.ComponentInit), childName, superset.Name), operatorInjected,
	)

	var imageOverride *supersetv1alpha1.ImageOverrideSpec
	if superset.Spec.Init != nil {
		imageOverride = superset.Spec.Init.Image
	}
	flatSpec := flatSpecFromResolution(flat, &superset.Spec.Image, imageOverride, saName)
	flatSpec.Autoscaling = nil
	flatSpec.PodDisruptionBudget = nil

	// CreateOrUpdate the SupersetInit child CR.
	child := &supersetv1alpha1.SupersetInit{
		ObjectMeta: metav1.ObjectMeta{Name: childName, Namespace: superset.Namespace},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, child, func() error {
		if err := controllerutil.SetControllerReference(superset, child, r.Scheme); err != nil {
			return err
		}
		child.SetLabels(mergeLabels(child.GetLabels(), map[string]string{
			naming.LabelKeyName:      naming.LabelValueApp,
			naming.LabelKeyComponent: string(naming.ComponentInit),
			naming.LabelKeyParent:    superset.Name,
		}))
		child.Spec.FlatComponentSpec = flatSpec
		child.Spec.Config = renderedConfig
		child.Spec.ConfigChecksum = computeChecksum(struct {
			SharedConfigChecksum string
			Config               string
			FlatSpec             supersetv1alpha1.FlatComponentSpec
		}{
			SharedConfigChecksum: configChecksum,
			Config:               renderedConfig,
			FlatSpec:             flatSpec,
		})

		// Pass init-specific fields from the parent InitSpec.
		if superset.Spec.Init != nil {
			child.Spec.MaxRetries = superset.Spec.Init.MaxRetries
			child.Spec.Timeout = superset.Spec.Init.Timeout
			child.Spec.PodRetention = superset.Spec.Init.PodRetention
		} else {
			child.Spec.MaxRetries = nil
			child.Spec.Timeout = nil
			child.Spec.PodRetention = nil
		}
		return nil
	})
	if err != nil {
		return 0, false, fmt.Errorf("creating/updating SupersetInit: %w", err)
	}

	// Read child status to determine if init is complete.
	// Re-fetch to get the latest status.
	if err := r.Get(ctx, client.ObjectKeyFromObject(child), child); err != nil {
		return 0, false, fmt.Errorf("fetching SupersetInit status: %w", err)
	}

	// Copy child status to parent status.Init.
	superset.Status.Init = &supersetv1alpha1.InitTaskStatus{
		State:       child.Status.State,
		StartedAt:   child.Status.StartedAt,
		CompletedAt: child.Status.CompletedAt,
		Duration:    child.Status.Duration,
		Attempts:    child.Status.Attempts,
		PodName:     child.Status.PodName,
		Image:       child.Status.Image,
		Message:     child.Status.Message,
	}

	switch child.Status.State {
	case initStateComplete:
		if child.Spec.ConfigChecksum != "" && child.Status.ConfigChecksum != child.Spec.ConfigChecksum {
			log.Info("Init complete for previous config, waiting for re-initialization")
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
				metav1.ConditionFalse, "InitConfigChanged", "Config changed, awaiting re-initialization", superset.Generation)
			superset.Status.Phase = phaseInitializing
			return initRequeueInterval, false, nil
		}
		log.Info("Init complete")
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionTrue, "InitComplete", "Initialization completed successfully", superset.Generation)
		return 0, true, nil

	case initStateFailed:
		maxRetries := defaultMaxRetries
		if superset.Spec.Init != nil && superset.Spec.Init.MaxRetries != nil {
			maxRetries = *superset.Spec.Init.MaxRetries
		}
		if child.Status.Attempts >= maxRetries {
			log.Info("Init permanently failed")
			setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
				metav1.ConditionFalse, "InitFailed", child.Status.Message, superset.Generation)
			superset.Status.Phase = phaseInitializing
			return -1, false, nil
		}
		// Still retrying.
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionFalse, "InitRetrying", "Initialization is retrying", superset.Generation)
		superset.Status.Phase = phaseInitializing
		return initRequeueInterval, false, nil

	default:
		// Pending, Running, or empty.
		log.Info("Init not yet complete, gating component deployment")
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeInitComplete,
			metav1.ConditionFalse, "InitInProgress", "Initialization is in progress", superset.Generation)
		superset.Status.Phase = phaseInitializing
		return initRequeueInterval, false, nil
	}
}

// --- Config input building ---

func buildConfigInput(spec *supersetv1alpha1.SupersetSpec) *supersetconfig.ConfigInput {
	input := &supersetconfig.ConfigInput{}

	if spec.Metastore != nil {
		if spec.Metastore.URI != nil || spec.Metastore.URIFrom != nil {
			input.MetastoreMode = supersetconfig.MetastorePassthrough
		} else if spec.Metastore.Host != nil {
			input.MetastoreMode = supersetconfig.MetastoreStructured
			dbType := "postgresql"
			if spec.Metastore.Type != nil {
				dbType = *spec.Metastore.Type
			}
			input.DBDriver = dbType
		}
	}

	if spec.Valkey != nil {
		input.Valkey = buildValkeyInput(spec.Valkey)
	}

	if spec.Config != nil {
		input.Config = *spec.Config
	}

	return input
}

// buildValkeyInput converts the CRD ValkeySpec into a resolved ValkeyInput with defaults applied.
func buildValkeyInput(v *supersetv1alpha1.ValkeySpec) *supersetconfig.ValkeyInput {
	vi := &supersetconfig.ValkeyInput{
		Cache:                resolveValkeyCache(v.Cache, 1, "superset_", 300),
		DataCache:            resolveValkeyCache(v.DataCache, 2, "superset_data_", 86400),
		FilterStateCache:     resolveValkeyCache(v.FilterStateCache, 3, "superset_filter_", 3600),
		ExploreFormDataCache: resolveValkeyCache(v.ExploreFormDataCache, 4, "superset_explore_", 3600),
		ThumbnailCache:       resolveValkeyCache(v.ThumbnailCache, 5, "superset_thumbnail_", 3600),
		CeleryBroker:         resolveValkeyCelery(v.CeleryBroker, 0),
		CeleryResultBackend:  resolveValkeyCelery(v.CeleryResultBackend, 0),
		ResultsBackend:       resolveValkeyResults(v.ResultsBackend, 6, "superset_results_"),
	}

	if v.SSL != nil {
		vi.SSL = true
		if v.SSL.CertRequired != nil {
			vi.SSLCertRequired = *v.SSL.CertRequired
		}
		if v.SSL.KeyFile != nil {
			vi.SSLKeyFile = *v.SSL.KeyFile
		}
		if v.SSL.CertFile != nil {
			vi.SSLCertFile = *v.SSL.CertFile
		}
		if v.SSL.CACertFile != nil {
			vi.SSLCACertFile = *v.SSL.CACertFile
		}
	}

	return vi
}

func resolveValkeyCache(spec *supersetv1alpha1.ValkeyCacheSpec, defaultDB int32, defaultPrefix string, defaultTimeout int32) supersetconfig.ValkeyCacheInput {
	input := supersetconfig.ValkeyCacheInput{
		Database:       defaultDB,
		KeyPrefix:      defaultPrefix,
		DefaultTimeout: defaultTimeout,
	}
	if spec == nil {
		return input
	}
	if spec.Disabled != nil && *spec.Disabled {
		input.Disabled = true
	}
	if spec.Database != nil {
		input.Database = *spec.Database
	}
	if spec.KeyPrefix != nil {
		input.KeyPrefix = *spec.KeyPrefix
	}
	if spec.DefaultTimeout != nil {
		input.DefaultTimeout = *spec.DefaultTimeout
	}
	return input
}

func resolveValkeyCelery(spec *supersetv1alpha1.ValkeyCelerySpec, defaultDB int32) supersetconfig.ValkeyCeleryInput {
	input := supersetconfig.ValkeyCeleryInput{Database: defaultDB}
	if spec == nil {
		return input
	}
	if spec.Disabled != nil && *spec.Disabled {
		input.Disabled = true
	}
	if spec.Database != nil {
		input.Database = *spec.Database
	}
	return input
}

func resolveValkeyResults(spec *supersetv1alpha1.ValkeyResultsBackendSpec, defaultDB int32, defaultPrefix string) supersetconfig.ValkeyResultsInput {
	input := supersetconfig.ValkeyResultsInput{
		Database:  defaultDB,
		KeyPrefix: defaultPrefix,
	}
	if spec == nil {
		return input
	}
	if spec.Disabled != nil && *spec.Disabled {
		input.Disabled = true
	}
	if spec.Database != nil {
		input.Database = *spec.Database
	}
	if spec.KeyPrefix != nil {
		input.KeyPrefix = *spec.KeyPrefix
	}
	return input
}

// --- Secret env var collection ---

// collectSecretEnvVars gathers env vars for SECRET_KEY and metastore fields.
func collectSecretEnvVars(spec *supersetv1alpha1.SupersetSpec) []corev1.EnvVar {
	var envs []corev1.EnvVar
	isDev := spec.Environment != nil && *spec.Environment == naming.EnvironmentDev

	// SUPERSET_OPERATOR__SECRET_KEY — rendered into superset_config.py as SECRET_KEY.
	if isDev && spec.SecretKey != nil {
		envs = append(envs, corev1.EnvVar{
			Name:  naming.EnvSecretKey,
			Value: *spec.SecretKey,
		})
	} else if spec.SecretKeyFrom != nil {
		envs = append(envs, corev1.EnvVar{
			Name:      naming.EnvSecretKey,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.SecretKeyFrom},
		})
	}

	// Metastore env vars.
	if spec.Metastore != nil {
		if spec.Metastore.URI != nil {
			envs = append(envs, corev1.EnvVar{
				Name:  naming.EnvDatabaseURI,
				Value: *spec.Metastore.URI,
			})
		} else if spec.Metastore.URIFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvDatabaseURI,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Metastore.URIFrom},
			})
		} else if spec.Metastore.Host != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBHost, Value: *spec.Metastore.Host})
			port := defaultDBPort(spec.Metastore.Type)
			if spec.Metastore.Port != nil {
				port = *spec.Metastore.Port
			}
			envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPort, Value: fmt.Sprintf("%d", port)})
			if spec.Metastore.Database != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvDBName, Value: *spec.Metastore.Database})
			}
			if spec.Metastore.Username != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvDBUser, Value: *spec.Metastore.Username})
			}
			if spec.Metastore.Password != nil {
				envs = append(envs, corev1.EnvVar{Name: naming.EnvDBPass, Value: *spec.Metastore.Password})
			} else if spec.Metastore.PasswordFrom != nil {
				envs = append(envs, corev1.EnvVar{
					Name:      naming.EnvDBPass,
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Metastore.PasswordFrom},
				})
			}
		}
	}

	// Valkey env vars.
	if spec.Valkey != nil {
		envs = append(envs, corev1.EnvVar{Name: naming.EnvValkeyHost, Value: spec.Valkey.Host})
		port := int32(6379)
		if spec.Valkey.Port != nil {
			port = *spec.Valkey.Port
		}
		envs = append(envs, corev1.EnvVar{Name: naming.EnvValkeyPort, Value: fmt.Sprintf("%d", port)})
		if isDev && spec.Valkey.Password != nil {
			envs = append(envs, corev1.EnvVar{Name: naming.EnvValkeyPass, Value: *spec.Valkey.Password})
		} else if spec.Valkey.PasswordFrom != nil {
			envs = append(envs, corev1.EnvVar{
				Name:      naming.EnvValkeyPass,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: spec.Valkey.PasswordFrom},
			})
		}
	}

	return envs
}

// collectInitEnvVars returns env vars specific to the init pod (admin user credentials).
func collectInitEnvVars(spec *supersetv1alpha1.SupersetSpec) []corev1.EnvVar {
	if spec.Init == nil || spec.Init.AdminUser == nil {
		return nil
	}
	admin := spec.Init.AdminUser
	return []corev1.EnvVar{
		{Name: naming.EnvAdminUsername, Value: derefOrDefault(admin.Username, "admin")},
		{Name: naming.EnvAdminPassword, Value: derefOrDefault(admin.Password, "admin")},
		{Name: naming.EnvAdminFirstName, Value: derefOrDefault(admin.FirstName, "Superset")},
		{Name: naming.EnvAdminLastName, Value: derefOrDefault(admin.LastName, "Admin")},
		{Name: naming.EnvAdminEmail, Value: derefOrDefault(admin.Email, "admin@example.com")},
	}
}

func derefOrDefault(ptr *string, def string) string {
	if ptr != nil {
		return *ptr
	}
	return def
}

func defaultDBPort(driver *string) int32 {
	if driver != nil && *driver == "mysql" {
		return 3306
	}
	return 5432
}

// --- Operator-injected volumes/env/mounts ---

func buildOperatorInjected(renderedConfig, childName, forceReload string, configEnvVars []corev1.EnvVar) *resolution.OperatorInjected {
	injected := &resolution.OperatorInjected{}

	if renderedConfig != "" {
		// Config volume + mount.
		injected.Volumes = append(injected.Volumes, corev1.Volume{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: naming.ConfigMapName(childName),
					},
				},
			},
		})
		injected.VolumeMounts = append(injected.VolumeMounts, corev1.VolumeMount{
			Name:      configVolumeName,
			MountPath: configMountPath,
			ReadOnly:  true,
		})

		injected.Env = append(injected.Env, corev1.EnvVar{
			Name:  naming.EnvPythonPath,
			Value: configMountPath,
		})
	}

	// Add config-derived env vars (secret key, metastore fields, etc.).
	injected.Env = append(injected.Env, configEnvVars...)

	// ForceReload propagated via env var (triggers pod restart on change).
	if forceReload != "" {
		injected.Env = append(injected.Env, corev1.EnvVar{
			Name:  naming.EnvForceReload,
			Value: forceReload,
		})
	}

	return injected
}

// --- Resolution output -> CRD type mapping ---

// flatSpecFromResolution converts a FlatSpec into a FlatComponentSpec.
// When imageOverride is non-nil, its Tag and/or Repository override the parent image.
// saName is set on the FlatComponentSpec so it propagates to Deployment pods.
func flatSpecFromResolution(flat *resolution.FlatSpec, parentImage *supersetv1alpha1.ImageSpec, imageOverride *supersetv1alpha1.ImageOverrideSpec, saName string) supersetv1alpha1.FlatComponentSpec {
	replicas := flat.Replicas
	image := *parentImage
	if imageOverride != nil {
		if imageOverride.Tag != nil {
			image.Tag = *imageOverride.Tag
		}
		if imageOverride.Repository != nil {
			image.Repository = *imageOverride.Repository
		}
	}
	return supersetv1alpha1.FlatComponentSpec{
		Image:               image,
		Replicas:            &replicas,
		DeploymentTemplate:  flat.DeploymentTemplate,
		PodTemplate:         flat.PodTemplate,
		ServiceAccountName:  saName,
		Autoscaling:         flat.Autoscaling,
		PodDisruptionBudget: flat.PodDisruptionBudget,
	}
}

// --- Shared resource reconciliation ---

func (r *SupersetReconciler) reconcileServiceAccount(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	saName := superset.Name
	if superset.Spec.ServiceAccount != nil && superset.Spec.ServiceAccount.Name != "" {
		saName = superset.Spec.ServiceAccount.Name
	}

	keepName := saName
	if !saCreateEnabled(superset.Spec.ServiceAccount) {
		keepName = ""
	}
	if err := r.deleteByLabels(ctx, superset.Namespace, parentLabels(superset.Name),
		func() client.ObjectList { return &corev1.ServiceAccountList{} }, keepName); err != nil {
		return err
	}

	if !saCreateEnabled(superset.Spec.ServiceAccount) {
		return nil
	}

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: superset.Namespace},
	}

	// Guard against adopting a pre-existing ServiceAccount not owned by this CR.
	existing := &corev1.ServiceAccount{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(sa), existing); err == nil {
		if !isOwnedBy(existing, superset) {
			return fmt.Errorf("ServiceAccount %q already exists and is not owned by Superset %q; set serviceAccount.create=false to use a pre-existing ServiceAccount",
				saName, superset.Name)
		}
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		if err := controllerutil.SetControllerReference(superset, sa, r.Scheme); err != nil {
			return err
		}
		sa.Labels = mergeLabels(sa.Labels, parentLabels(superset.Name))
		sa.Annotations = nil
		if superset.Spec.ServiceAccount != nil {
			sa.Annotations = mergeAnnotations(nil, superset.Spec.ServiceAccount.Annotations)
		}
		return nil
	})
	return err
}

// isOwnedBy returns true if obj has a controller ownerReference pointing to owner.
func isOwnedBy(obj, owner client.Object) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == owner.GetUID() {
			return true
		}
	}
	return false
}

// --- Status aggregation ---

func (r *SupersetReconciler) updateStatus(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	superset.Status.ObservedGeneration = superset.Generation
	superset.Status.Version = superset.Spec.Image.Tag

	if superset.Status.Components == nil {
		superset.Status.Components = &supersetv1alpha1.ComponentStatusMap{}
	}

	allReady := true

	// Table-driven status aggregation.
	for _, desc := range componentDescriptors {
		isEnabled := desc.extract(&superset.Spec) != nil
		if isEnabled {
			childName := desc.childName(&superset.Spec, superset.Name)
			status := r.getChildStatus(ctx, superset.Namespace, childName, desc.kind)
			desc.setStatus(superset.Status.Components, status)
			if status != nil && !isReadyString(status.Ready) {
				allReady = false
			}
		} else {
			desc.setStatus(superset.Status.Components, nil)
		}
	}

	if !anyComponentEnabled(superset) {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionTrue, "NoComponentsEnabled", "No components are enabled", superset.Generation)
		superset.Status.Phase = phaseRunning
	} else if allReady {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionTrue, "AllComponentsReady", "All components are ready", superset.Generation)
		superset.Status.Phase = phaseRunning
	} else {
		setCondition(&superset.Status.Conditions, supersetv1alpha1.ConditionTypeAvailable,
			metav1.ConditionFalse, "ComponentsNotReady", "One or more components are not ready", superset.Generation)
		superset.Status.Phase = phaseDegraded
	}

	return r.Status().Update(ctx, superset)
}

// getChildStatus reads a child CR's status using unstructured API.
func (r *SupersetReconciler) getChildStatus(ctx context.Context, namespace, childName, kind string) *supersetv1alpha1.ComponentRefStatus {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "superset.apache.org",
		Version: "v1alpha1",
		Kind:    kind,
	})

	err := r.Get(ctx, types.NamespacedName{Name: childName, Namespace: namespace}, obj)
	if err != nil {
		log := logf.FromContext(ctx)
		log.Info("child CR not found for status", "kind", kind, "name", childName, "error", err)
		return &supersetv1alpha1.ComponentRefStatus{
			Ready: "0/0",
			Ref:   kind + "/" + childName,
		}
	}

	// Read the status.ready field from the unstructured object.
	ready, _, _ := unstructured.NestedString(obj.Object, "status", "ready")

	return &supersetv1alpha1.ComponentRefStatus{
		Ready: ready,
		Ref:   kind + "/" + childName,
	}
}

// --- Utility functions ---

func resolveServiceAccountName(superset *supersetv1alpha1.Superset) string {
	if superset.Spec.ServiceAccount == nil {
		return superset.Name
	}
	if superset.Spec.ServiceAccount.Create != nil && !*superset.Spec.ServiceAccount.Create {
		if superset.Spec.ServiceAccount.Name != "" {
			return superset.Spec.ServiceAccount.Name
		}
		return ""
	}
	if superset.Spec.ServiceAccount.Name != "" {
		return superset.Spec.ServiceAccount.Name
	}
	return superset.Name
}

func anyComponentEnabled(superset *supersetv1alpha1.Superset) bool {
	return superset.Spec.WebServer != nil ||
		superset.Spec.CeleryWorker != nil ||
		superset.Spec.CeleryBeat != nil ||
		superset.Spec.CeleryFlower != nil ||
		superset.Spec.WebsocketServer != nil ||
		superset.Spec.McpServer != nil
}

func computeChecksum(obj any) string {
	data, err := json.Marshal(obj)
	if err != nil {
		data = fmt.Appendf(nil, "%v", obj)
	}
	h := sha256.New()
	h.Write(data)
	return fmt.Sprintf("sha256:%x", h.Sum(nil))
}

func gunicornSpecFrom(ws *supersetv1alpha1.WebServerComponentSpec) *supersetv1alpha1.GunicornSpec {
	if ws == nil {
		return nil
	}
	return ws.Gunicorn
}

func celerySpecFrom(cw *supersetv1alpha1.CeleryWorkerComponentSpec) *supersetv1alpha1.CeleryWorkerProcessSpec {
	if cw == nil {
		return nil
	}
	return cw.Celery
}

func isReadyString(ready string) bool {
	if ready == "" || ready == "0/0" {
		return false
	}
	// Parse "X/Y" and check X == Y and X > 0.
	var readyCount, desiredCount int
	if _, err := fmt.Sscanf(ready, "%d/%d", &readyCount, &desiredCount); err != nil {
		return false
	}
	return readyCount > 0 && readyCount == desiredCount
}

// pruneOrphans lists all resources matching the parent+component labels and deletes
// any whose name does not match keepName. If keepName is empty, all matches are deleted.
func (r *SupersetReconciler) pruneOrphans(
	ctx context.Context,
	ns, parentName string,
	componentType naming.ComponentType,
	newList func() client.ObjectList,
	keepName string,
) error {
	return r.deleteByLabels(ctx, ns, map[string]string{
		naming.LabelKeyParent:    parentName,
		naming.LabelKeyComponent: string(componentType),
	}, newList, keepName)
}

// deleteByLabels lists all resources matching the given labels and deletes any
// whose name does not match keepName. Pass empty keepName to delete all matches.
// Gracefully handles missing CRDs (returns nil for NoMatchError).
func (r *SupersetReconciler) deleteByLabels(
	ctx context.Context,
	ns string,
	labels map[string]string,
	newList func() client.ObjectList,
	keepName string,
) error {
	list := newList()
	if err := r.List(ctx, list,
		client.InNamespace(ns),
		client.MatchingLabels(labels),
	); err != nil {
		if meta.IsNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("listing resources by labels %v: %w", labels, err)
	}
	return deleteMatches(ctx, r.Client, list, keepName)
}

// saCreateEnabled returns true if the ServiceAccount spec says to create one.
func saCreateEnabled(sa *supersetv1alpha1.ServiceAccountSpec) bool {
	if sa == nil {
		return true
	}
	return sa.Create == nil || *sa.Create
}

// SetupWithManager sets up the controller with the Manager.
func (r *SupersetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&supersetv1alpha1.Superset{}).
		Owns(&supersetv1alpha1.SupersetInit{}).
		Owns(&supersetv1alpha1.SupersetWebServer{}).
		Owns(&supersetv1alpha1.SupersetCeleryWorker{}).
		Owns(&supersetv1alpha1.SupersetCeleryBeat{}).
		Owns(&supersetv1alpha1.SupersetCeleryFlower{}).
		Owns(&supersetv1alpha1.SupersetWebsocketServer{}).
		Owns(&supersetv1alpha1.SupersetMcpServer{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Named("superset")

	// Only watch HTTPRoute if the Gateway API CRDs are installed.
	_, err := mgr.GetRESTMapper().RESTMapping(
		schema.GroupKind{Group: "gateway.networking.k8s.io", Kind: "HTTPRoute"},
	)
	if err == nil {
		b = b.Owns(&gatewayv1.HTTPRoute{})
	}

	return b.Complete(r)
}
