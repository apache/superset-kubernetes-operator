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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
)

func isMaintenancePageEnabled(superset *supersetv1alpha1.Superset) bool {
	return superset.Spec.Lifecycle != nil &&
		superset.Spec.Lifecycle.MaintenancePage != nil &&
		superset.Spec.WebServer != nil
}

func isCustomMode(spec *supersetv1alpha1.MaintenancePageSpec) bool {
	return spec.Image != nil
}

func maintenanceConfigMapName(parentName string) string {
	return naming.ConfigMapName(naming.ResourceBaseName(parentName, naming.ComponentMaintenancePage))
}

// reconcileMaintenancePageUp ensures the maintenance page is running and the
// web-server Service routes to it. Returns ready=true when the maintenance page
// is serving traffic.
func (r *SupersetReconciler) reconcileMaintenancePageUp(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
) (bool, error) {
	log := logf.FromContext(ctx)
	spec := superset.Spec.Lifecycle.MaintenancePage

	// Step 1: Reconcile ConfigMap (managed mode only).
	if !isCustomMode(spec) {
		if err := r.reconcileMaintenanceConfigMap(ctx, superset, spec); err != nil {
			return false, fmt.Errorf("reconciling maintenance ConfigMap: %w", err)
		}
	}

	// Step 2: Ensure SupersetMaintenancePage child CR exists.
	childCR := &supersetv1alpha1.SupersetMaintenancePage{}
	childCR.Name = superset.Name
	childCR.Namespace = superset.Namespace

	if err := r.Get(ctx, client.ObjectKeyFromObject(childCR), childCR); err != nil {
		if !errors.IsNotFound(err) {
			return false, fmt.Errorf("getting maintenance page CR: %w", err)
		}
		if err := r.createMaintenancePageCR(ctx, superset, spec); err != nil {
			return false, fmt.Errorf("creating maintenance page CR: %w", err)
		}
		log.Info("Created SupersetMaintenancePage child CR")
		return false, nil
	}

	// Step 3: Check if maintenance Deployment is ready.
	deployName := naming.ResourceBaseName(superset.Name, naming.ComponentMaintenancePage)
	deploy := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: superset.Namespace, Name: deployName}, deploy); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting maintenance Deployment: %w", err)
	}
	if deploy.Status.ReadyReplicas < 1 {
		log.Info("Waiting for maintenance page pod to become ready")
		return false, nil
	}

	// Step 4: Take over the web-server Service.
	if err := r.takeoverWebServerService(ctx, superset); err != nil {
		return false, fmt.Errorf("taking over web-server Service: %w", err)
	}

	superset.Status.Lifecycle.MaintenanceActive = true
	return true, nil
}

// reconcileMaintenancePageDown releases the web-server Service and removes
// maintenance resources. Called after lifecycle tasks complete.
func (r *SupersetReconciler) reconcileMaintenancePageDown(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
) error {
	log := logf.FromContext(ctx)

	// Step 1: Release the web-server Service (remove parent ownership).
	if err := r.releaseWebServerService(ctx, superset); err != nil {
		return fmt.Errorf("releasing web-server Service: %w", err)
	}

	// Step 2: Delete maintenance page child CR (GC cascades to Deployment).
	childCR := &supersetv1alpha1.SupersetMaintenancePage{}
	childCR.Name = superset.Name
	childCR.Namespace = superset.Namespace
	if err := r.Delete(ctx, childCR); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting maintenance page CR: %w", err)
	}

	// Step 3: Delete maintenance ConfigMap.
	cm := &corev1.ConfigMap{}
	cm.Name = maintenanceConfigMapName(superset.Name)
	cm.Namespace = superset.Namespace
	if err := r.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting maintenance ConfigMap: %w", err)
	}

	superset.Status.Lifecycle.MaintenanceActive = false
	log.Info("Maintenance page cleaned up")
	return nil
}

// cleanupMaintenanceResources removes all maintenance resources unconditionally.
func (r *SupersetReconciler) cleanupMaintenanceResources(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	childCR := &supersetv1alpha1.SupersetMaintenancePage{}
	childCR.Name = superset.Name
	childCR.Namespace = superset.Namespace
	if err := r.Delete(ctx, childCR); err != nil && !errors.IsNotFound(err) {
		return err
	}

	cm := &corev1.ConfigMap{}
	cm.Name = maintenanceConfigMapName(superset.Name)
	cm.Namespace = superset.Namespace
	if err := r.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

// takeoverWebServerService orphan-deletes the SupersetWebServer child CR
// (preserving its Service), then patches the orphaned Service's selector to
// route traffic to maintenance pods. The orphaned Deployment is also deleted
// to terminate web-server pods.
//
// Design rationale: we use propagationPolicy=Orphan (a standard K8s API concept)
// rather than manipulating owner references or swapping Ingress/HTTPRoute backends.
// This gives us instant (~1s) traffic switchover via the endpoints controller,
// works for all access patterns (Ingress, direct Service, port-forward), and
// operates on a legitimately unowned Service (no architectural boundary violation).
// The alternative of swapping Ingress/HTTPRoute backends was rejected because
// propagation latency varies by controller (1s for nginx to 3min for cloud LBs),
// creating an unacceptable error window during drain.
func (r *SupersetReconciler) takeoverWebServerService(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
) error {
	log := logf.FromContext(ctx)
	svcName := naming.ResourceBaseName(superset.Name, naming.ComponentWebServer)
	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: superset.Namespace, Name: svcName}

	maintenanceSelector := naming.ComponentLabels(naming.ComponentMaintenancePage, superset.Name)

	// Check if the Service already points to maintenance pods (idempotent).
	if err := r.Get(ctx, key, svc); err != nil {
		if errors.IsNotFound(err) {
			// Service already gone (operator restart after GC completed) — create fresh.
			return r.createMaintenanceWebServerService(ctx, superset, maintenanceSelector)
		}
		return err
	}
	if selectorMatchesComponent(svc, naming.ComponentMaintenancePage, superset.Name) {
		return nil
	}

	// Orphan-delete the SupersetWebServer child CR so its Service survives.
	webServerCR := &supersetv1alpha1.SupersetWebServer{}
	webServerCR.Name = superset.Name
	webServerCR.Namespace = superset.Namespace
	orphanPolicy := metav1.DeletePropagationOrphan
	if err := r.Delete(ctx, webServerCR, &client.DeleteOptions{
		PropagationPolicy: &orphanPolicy,
	}); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("orphan-deleting web-server child CR: %w", err)
	}

	// Patch the now-orphaned Service selector to route to maintenance pods.
	// Remove stale owner references so the child reconciler can re-adopt later.
	svc.OwnerReferences = nil
	svc.Spec.Selector = maintenanceSelector
	if err := r.Update(ctx, svc); err != nil {
		return fmt.Errorf("patching web-server Service selector: %w", err)
	}

	// Delete the orphaned web-server Deployment to terminate pods.
	deployName := naming.ResourceBaseName(superset.Name, naming.ComponentWebServer)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: superset.Namespace,
		},
	}
	if err := r.Delete(ctx, deploy); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting orphaned web-server Deployment: %w", err)
	}

	log.Info("Took over web-server Service via orphan deletion, routing to maintenance page")
	return nil
}

// createMaintenanceWebServerService creates a fresh web-server Service pointing
// to maintenance pods. Used when the original Service was already GC'd (e.g.,
// operator restart after the child CR was deleted without orphan policy).
func (r *SupersetReconciler) createMaintenanceWebServerService(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	selector map[string]string,
) error {
	svcName := naming.ResourceBaseName(superset.Name, naming.ComponentWebServer)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: superset.Namespace,
			Labels:    naming.ComponentLabels(naming.ComponentWebServer, superset.Name),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selector,
			Ports: []corev1.ServicePort{
				{Name: naming.PortNameHTTP, Port: naming.PortWebServer, Protocol: corev1.ProtocolTCP},
			},
		},
	}
	if err := r.Create(ctx, svc); err != nil {
		return fmt.Errorf("creating web-server Service for maintenance: %w", err)
	}
	return nil
}

// releaseWebServerService ensures the web-server Service has no owner references
// so the child reconciler can adopt it via SetControllerReference on the next
// component reconciliation cycle.
func (r *SupersetReconciler) releaseWebServerService(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
) error {
	svcName := naming.ResourceBaseName(superset.Name, naming.ComponentWebServer)
	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: superset.Namespace, Name: svcName}

	if err := r.Get(ctx, key, svc); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if len(svc.OwnerReferences) == 0 {
		return nil
	}

	svc.OwnerReferences = nil
	return r.Update(ctx, svc)
}

// reconcileMaintenanceConfigMap creates or updates the ConfigMap containing
// the nginx config and HTML page for managed mode.
func (r *SupersetReconciler) reconcileMaintenanceConfigMap(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	spec *supersetv1alpha1.MaintenancePageSpec,
) error {
	cmName := maintenanceConfigMapName(superset.Name)
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
			"default.conf": renderNginxConf(),
			"index.html":   renderMaintenanceHTML(spec),
		}
		return nil
	})
	return err
}

// createMaintenancePageCR builds and creates the SupersetMaintenancePage child CR.
func (r *SupersetReconciler) createMaintenancePageCR(
	ctx context.Context,
	superset *supersetv1alpha1.Superset,
	spec *supersetv1alpha1.MaintenancePageSpec,
) error {
	flat := buildMaintenanceFlatSpec(superset.Name, spec)
	checksum := computeMaintenanceChecksum(spec)

	childCR := &supersetv1alpha1.SupersetMaintenancePage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      superset.Name,
			Namespace: superset.Namespace,
		},
		Spec: supersetv1alpha1.SupersetMaintenancePageSpec{
			FlatComponentSpec: flat,
			ConfigChecksum:    checksum,
		},
	}

	if err := controllerutil.SetControllerReference(superset, childCR, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, childCR)
}

// buildMaintenanceFlatSpec constructs the FlatComponentSpec for the maintenance page.
func buildMaintenanceFlatSpec(parentName string, spec *supersetv1alpha1.MaintenancePageSpec) supersetv1alpha1.FlatComponentSpec {
	replicas := int32(1)
	if spec.Replicas != nil {
		replicas = *spec.Replicas
	}

	flat := supersetv1alpha1.FlatComponentSpec{
		Image:              resolveMaintenanceImage(spec),
		Replicas:           &replicas,
		DeploymentTemplate: spec.DeploymentTemplate,
		PodTemplate:        spec.PodTemplate,
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
	if spec.Image != nil {
		return *spec.Image
	}
	return supersetv1alpha1.ImageSpec{
		Repository: maintenanceDefaultImage,
		Tag:        maintenanceDefaultTag,
	}
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

func renderNginxConf() string {
	return `server {
    listen ` + fmt.Sprintf("%d", naming.PortWebServer) + `;
    server_name _;

    location = / {
        root /usr/share/nginx/html;
        index index.html;
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

// selectorMatchesComponent checks if the Service selector already points to
// the given component type and instance.
func selectorMatchesComponent(svc *corev1.Service, component naming.ComponentType, instance string) bool {
	expected := naming.ComponentLabels(component, instance)
	for k, v := range expected {
		if svc.Spec.Selector[k] != v {
			return false
		}
	}
	return true
}
