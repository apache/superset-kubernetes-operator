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
	"fmt"
	"html"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	naming "github.com/apache/superset-kubernetes-operator/internal/common"
)

const (
	maintenanceDefaultTitle   = "Scheduled Maintenance"
	maintenanceDefaultMessage = "Apache Superset is being upgraded. This page will refresh automatically."
	maintenanceDefaultImage   = "nginx"
	maintenanceDefaultTag     = "alpine"

	maintenanceContainerName = "maintenance-page"

	// maintenanceNonRootUID is the default UID for the maintenance nginx
	// container. The rendered nginx.conf routes the pid file and temp paths to
	// /tmp, so nginx runs correctly as any non-root UID regardless of the
	// image's built-in nginx user.
	maintenanceNonRootUID int64 = 101
)

var maintenanceDeployConfig = DeploymentConfig{
	ContainerName:  maintenanceContainerName,
	DefaultCommand: nil,
}

func isMaintenancePageEnabled(superset *supersetv1alpha1.Superset) bool {
	return superset.Spec.Lifecycle != nil &&
		superset.Spec.Lifecycle.MaintenancePage != nil &&
		superset.Spec.WebServer != nil
}

func webServerDesiredReplicas(superset *supersetv1alpha1.Superset) int32 {
	accessor := webServerDescriptor.extract(&superset.Spec)
	if accessor == nil {
		return 0
	}
	return desiredReplicasForStatus(superset, webServerDescriptor, accessor)
}

func (r *SupersetReconciler) hasExistingWebServerWorkload(ctx context.Context, superset *supersetv1alpha1.Superset) (bool, error) {
	deployName := naming.ResourceBaseName(superset.Name, naming.ComponentWebServer)
	deploy := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: superset.Namespace, Name: deployName}, deploy); err != nil {
		if !errors.IsNotFound(err) {
			return false, fmt.Errorf("getting web-server Deployment: %w", err)
		}
	} else if deploy.DeletionTimestamp == nil && deploymentHasReplicas(deploy) {
		return true, nil
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(superset.Namespace),
		client.MatchingLabels{
			naming.LabelKeyParent:    superset.Name,
			naming.LabelKeyComponent: string(naming.ComponentWebServer),
		},
	); err != nil {
		return false, fmt.Errorf("listing web-server pods: %w", err)
	}
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil {
			return true, nil
		}
	}
	return false, nil
}

func deploymentHasReplicas(deploy *appsv1.Deployment) bool {
	if deploy.Status.Replicas > 0 ||
		deploy.Status.ReadyReplicas > 0 ||
		deploy.Status.AvailableReplicas > 0 ||
		deploy.Status.UpdatedReplicas > 0 ||
		deploy.Status.UnavailableReplicas > 0 {
		return true
	}
	return deploy.Spec.Replicas == nil || *deploy.Spec.Replicas > 0
}

func isCustomMode(spec *supersetv1alpha1.MaintenancePageSpec) bool {
	return spec.Image != nil
}

func maintenanceConfigMapName(parentName string) string {
	return naming.ConfigMapName(naming.ResourceBaseName(parentName, naming.ComponentMaintenancePage))
}

func maintenanceDeploymentName(parentName string) string {
	return naming.ResourceBaseName(parentName, naming.ComponentMaintenancePage)
}

// reconcileMaintenancePageUp ensures the maintenance page Deployment is running
// and ready. Returns ready=true when the maintenance Deployment has ready pods.
// The caller sets MaintenanceActive=true and reconcileWebServerService() picks
// up the flag to switch the Service selector.
func (r *SupersetReconciler) reconcileMaintenancePageUp(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
) (bool, error) {
	log := logf.FromContext(ctx)
	spec := superset.Spec.Lifecycle.MaintenancePage
	port := resolveWebServerPort(superset)

	// Step 1: Reconcile ConfigMap (managed mode only).
	if !isCustomMode(spec) {
		if err := r.reconcileMaintenanceConfigMap(ctx, superset, spec, port); err != nil {
			return false, fmt.Errorf("reconciling maintenance ConfigMap: %w", err)
		}
	}

	// Step 2: CreateOrUpdate maintenance Deployment (parent-owned).
	if err := r.reconcileMaintenanceDeployment(ctx, superset, spec, port); err != nil {
		return false, fmt.Errorf("reconciling maintenance Deployment: %w", err)
	}

	// Step 3: Check if maintenance Deployment is ready.
	deployName := maintenanceDeploymentName(superset.Name)
	deploy := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: superset.Namespace, Name: deployName}, deploy); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting maintenance Deployment: %w", err)
	}
	if deploy.Status.ReadyReplicas < 1 {
		log.V(1).Info("Waiting for maintenance page pod to become ready")
		return false, nil
	}

	if !superset.Status.Lifecycle.MaintenanceActive {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "MaintenanceStarted", "Lifecycle",
			"Maintenance page is ready; routing web-server Service to maintenance")
	}
	superset.Status.Lifecycle.MaintenanceActive = true
	return true, nil
}

// reconcileMaintenanceReturn handles the zero-downtime switchback from
// maintenance to web-server. It waits for the web-server Deployment to be
// ready, then sets MaintenanceActive=false so reconcileWebServerService
// switches the selector on the same reconcile pass.
// Resource cleanup is deferred to the caller (after the Service is reconciled)
// to avoid a failure window where the Service still selects maintenance pods
// whose Deployment has been deleted.
// Returns cleared=true when maintenance is inactive (either already was, or
// was just cleared).
func (r *SupersetReconciler) reconcileMaintenanceReturn(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
) (bool, error) {
	if superset.Status.Lifecycle == nil || !superset.Status.Lifecycle.MaintenanceActive {
		return true, nil
	}
	log := logf.FromContext(ctx)

	// If webServer was removed while maintenance is active, clear immediately
	// rather than waiting forever for a Deployment that won't come.
	if superset.Spec.WebServer == nil {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "MaintenanceEnded", "Lifecycle",
			"Maintenance page disabled because webServer was removed")
		superset.Status.Lifecycle.MaintenanceActive = false
		log.V(1).Info("WebServer removed while maintenance active, clearing maintenance")
		return true, nil
	}
	if webServerDesiredReplicas(superset) == 0 {
		r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "MaintenanceEnded", "Lifecycle",
			"Maintenance page disabled because webServer has zero desired replicas")
		superset.Status.Lifecycle.MaintenanceActive = false
		log.V(1).Info("WebServer scaled to zero while maintenance active, clearing maintenance")
		return true, nil
	}

	// Check web-server Deployment readiness before switching traffic.
	webDeployName := naming.ResourceBaseName(superset.Name, naming.ComponentWebServer)
	deploy := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: superset.Namespace, Name: webDeployName}, deploy); err != nil {
		if errors.IsNotFound(err) {
			log.V(1).Info("Waiting for web-server Deployment to be created before clearing maintenance")
			return false, nil
		}
		return false, fmt.Errorf("getting web-server Deployment: %w", err)
	}
	if deploy.Status.ReadyReplicas < 1 {
		log.V(1).Info("Waiting for web-server pods to become ready before clearing maintenance")
		return false, nil
	}

	// Web-server is ready — mark maintenance as inactive. The caller will
	// reconcile the Service (switching selector) and then clean up resources.
	superset.Status.Lifecycle.MaintenanceActive = false
	r.Recorder.Eventf(superset, nil, corev1.EventTypeNormal, "MaintenanceEnded", "Lifecycle",
		"Web-server is ready; routing web-server Service back to Superset")
	log.V(1).Info("Web-server ready, clearing maintenance page")
	return true, nil
}

// cleanupMaintenanceResources removes all maintenance resources unconditionally.
// Used when lifecycle is disabled or maintenance page config is removed.
func (r *SupersetReconciler) cleanupMaintenanceResources(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	if superset.Status.Lifecycle != nil {
		superset.Status.Lifecycle.MaintenanceActive = false
	}
	return r.deleteMaintenanceResources(ctx, superset)
}

func (r *SupersetReconciler) deleteMaintenanceResources(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      maintenanceDeploymentName(superset.Name),
			Namespace: superset.Namespace,
		},
	}
	if err := r.Delete(ctx, deploy); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting maintenance Deployment: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      maintenanceConfigMapName(superset.Name),
			Namespace: superset.Namespace,
		},
	}
	if err := r.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting maintenance ConfigMap: %w", err)
	}
	return nil
}

// reconcileMaintenanceDeployment creates or updates the maintenance page
// Deployment, owned directly by the parent Superset CR.
func (r *SupersetReconciler) reconcileMaintenanceDeployment(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	spec *supersetv1alpha1.MaintenancePageSpec,
	port int32,
) error {
	deployName := maintenanceDeploymentName(superset.Name)

	flat := buildMaintenanceFlatSpec(superset.Name, spec)
	checksum := computeMaintenanceChecksum(spec)
	selectorLabels := componentLabels(string(naming.ComponentMaintenancePage), superset.Name)
	podAnnotations := map[string]string{
		naming.AnnotationConfigChecksum: checksum,
	}

	cfg := maintenanceDeployConfig
	cfg.DefaultPorts = []corev1.ContainerPort{
		{Name: naming.PortNameHTTP, ContainerPort: port, Protocol: corev1.ProtocolTCP},
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deployName,
				Namespace: superset.Namespace,
			},
		}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
			if err := controllerutil.SetControllerReference(superset, deploy, r.Scheme); err != nil {
				return err
			}
			deploy.Spec = buildDeploymentSpec(&flat, cfg, podAnnotations, selectorLabels)
			deploy.Labels = mergeLabels(nil, componentLabels(string(naming.ComponentMaintenancePage), superset.Name))
			return nil
		})
		return err
	})
}

// reconcileMaintenanceConfigMap creates or updates the ConfigMap containing
// the nginx config and HTML page for managed mode.
func (r *SupersetReconciler) reconcileMaintenanceConfigMap(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	spec *supersetv1alpha1.MaintenancePageSpec,
	port int32,
) error {
	cmName := maintenanceConfigMapName(superset.Name)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: superset.Namespace,
			},
		}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
			if err := controllerutil.SetControllerReference(superset, cm, r.Scheme); err != nil {
				return err
			}
			cm.Data = map[string]string{
				"nginx.conf":   renderMaintenanceNginxMainConf(),
				"default.conf": renderNginxConf(port),
				"index.html":   renderMaintenanceHTML(spec),
			}
			return nil
		})
		return err
	})
}

// buildMaintenanceFlatSpec constructs the FlatComponentSpec for the maintenance page.
func buildMaintenanceFlatSpec(parentName string, spec *supersetv1alpha1.MaintenancePageSpec) supersetv1alpha1.FlatComponentSpec {
	replicas := int32(1)
	if spec.Replicas != nil {
		replicas = *spec.Replicas
	}

	// DeepCopy template pointers so the volume/mount/env injection below cannot
	// mutate the user-provided spec stored in the informer cache.
	flat := supersetv1alpha1.FlatComponentSpec{
		Image:              resolveMaintenanceImage(spec),
		Replicas:           &replicas,
		DeploymentTemplate: spec.DeploymentTemplate.DeepCopy(),
		PodTemplate:        spec.PodTemplate.DeepCopy(),
	}

	// Build env vars from content fields.
	var envVars []corev1.EnvVar
	if spec.Title != nil {
		envVars = append(envVars, corev1.EnvVar{Name: naming.EnvMaintenanceTitle, Value: *spec.Title})
	}
	if spec.Message != nil {
		envVars = append(envVars, corev1.EnvVar{Name: naming.EnvMaintenanceMessage, Value: *spec.Message})
	}
	if spec.Body != nil {
		envVars = append(envVars, corev1.EnvVar{Name: naming.EnvMaintenanceBody, Value: *spec.Body})
	}

	// In managed mode, inject ConfigMap volume and mounts.
	if !isCustomMode(spec) {
		cmName := maintenanceConfigMapName(parentName)
		volumes := []corev1.Volume{
			{
				Name: "maintenance-config",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
					},
				},
			},
		}
		volumeMounts := []corev1.VolumeMount{
			{Name: "maintenance-config", MountPath: "/etc/nginx/nginx.conf", SubPath: "nginx.conf"},
			{Name: "maintenance-config", MountPath: "/etc/nginx/conf.d/default.conf", SubPath: "default.conf"},
			{Name: "maintenance-config", MountPath: "/usr/share/nginx/html/index.html", SubPath: "index.html"},
		}

		if flat.PodTemplate == nil {
			flat.PodTemplate = &supersetv1alpha1.PodTemplate{}
		}
		flat.PodTemplate.Volumes = append(flat.PodTemplate.Volumes, volumes...)
		if flat.PodTemplate.Container == nil {
			flat.PodTemplate.Container = &supersetv1alpha1.ContainerTemplate{}
		}
		flat.PodTemplate.Container.VolumeMounts = append(flat.PodTemplate.Container.VolumeMounts, volumeMounts...)

		// Default the maintenance container to non-root. Stock nginx runs as root
		// by default, so it would be rejected under a runAsNonRoot pod security
		// context (e.g. a hardened maintenancePage.podTemplate). The rendered
		// nginx.conf redirects the pid file and temp paths to /tmp, so any
		// non-root UID works. An explicit user securityContext is respected.
		flat.PodTemplate.Container.SecurityContext = maintenanceSecurityContext(
			flat.PodTemplate.Container.SecurityContext, flat.PodTemplate.PodSecurityContext)
	}

	// Inject env vars.
	if len(envVars) > 0 {
		if flat.PodTemplate == nil {
			flat.PodTemplate = &supersetv1alpha1.PodTemplate{}
		}
		if flat.PodTemplate.Container == nil {
			flat.PodTemplate.Container = &supersetv1alpha1.ContainerTemplate{}
		}
		flat.PodTemplate.Container.Env = append(flat.PodTemplate.Container.Env, envVars...)
	}

	return flat
}

func resolveMaintenanceImage(spec *supersetv1alpha1.MaintenancePageSpec) supersetv1alpha1.ImageSpec {
	return resolveContainerImage(spec.Image, maintenanceDefaultImage, maintenanceDefaultTag)
}

func computeMaintenanceChecksum(spec *supersetv1alpha1.MaintenancePageSpec) string {
	h := sha256.New()
	if spec.Title != nil {
		h.Write([]byte(*spec.Title))
	}
	h.Write([]byte{0})
	if spec.Message != nil {
		h.Write([]byte(*spec.Message))
	}
	h.Write([]byte{0})
	if spec.Body != nil {
		h.Write([]byte(*spec.Body))
	}
	h.Write([]byte{0})
	if spec.Image != nil {
		h.Write([]byte(spec.Image.Repository))
		h.Write([]byte(spec.Image.Tag))
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// maintenanceSecurityContext returns the maintenance container's security
// context, defaulting to non-root with privilege escalation disabled and all
// capabilities dropped (so it satisfies restricted Pod Security Standards),
// while respecting any user-provided container securityContext.
func maintenanceSecurityContext(containerSC *corev1.SecurityContext, podSC *corev1.PodSecurityContext) *corev1.SecurityContext {
	sc := helperNonRootSecurityContext(containerSC, podSC, maintenanceNonRootUID)
	if sc.AllowPrivilegeEscalation == nil {
		no := false
		sc.AllowPrivilegeEscalation = &no
	}
	if sc.Capabilities == nil {
		sc.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}
	}
	return sc
}

// renderMaintenanceNginxMainConf renders the main nginx.conf for the managed
// maintenance page. It routes the pid file and all temp paths to /tmp and logs
// to stdout/stderr so nginx runs as an unprivileged, non-root user. The
// per-server configuration lives in conf.d/default.conf (renderNginxConf).
func renderMaintenanceNginxMainConf() string {
	return `worker_processes 1;
pid /tmp/nginx.pid;
error_log /dev/stderr warn;
events {
    worker_connections 1024;
}
http {
    access_log /dev/stdout;
    client_body_temp_path /tmp/client_temp;
    proxy_temp_path /tmp/proxy_temp;
    fastcgi_temp_path /tmp/fastcgi_temp;
    uwsgi_temp_path /tmp/uwsgi_temp;
    scgi_temp_path /tmp/scgi_temp;
    include /etc/nginx/mime.types;
    default_type application/octet-stream;
    include /etc/nginx/conf.d/*.conf;
}`
}

func renderNginxConf(port int32) string {
	return `server {
    listen ` + fmt.Sprintf("%d", port) + `;
    server_name _;

    location = / {
        root /usr/share/nginx/html;
        try_files /index.html =404;
    }

    location / {
        return 302 /;
    }
}`
}

func renderMaintenanceHTML(spec *supersetv1alpha1.MaintenancePageSpec) string {
	if spec.Body != nil {
		return *spec.Body
	}

	title := maintenanceDefaultTitle
	if spec.Title != nil {
		title = *spec.Title
	}
	message := maintenanceDefaultMessage
	if spec.Message != nil {
		message = *spec.Message
	}
	title = html.EscapeString(title)
	message = html.EscapeString(message)

	return `<!DOCTYPE html>
<html>
<head>
    <title>` + title + `</title>
    <meta http-equiv="refresh" content="30">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
               display: flex; align-items: center; justify-content: center;
               min-height: 100vh; margin: 0; background: #f8f9fa; color: #333; }
        .container { text-align: center; padding: 2rem; max-width: 480px; }
        h1 { margin-bottom: 0.5rem; }
        p { color: #666; line-height: 1.6; }
    </style>
</head>
<body>
    <div class="container">
        <h1>` + title + `</h1>
        <p>` + message + `</p>
    </div>
</body>
</html>`
}
