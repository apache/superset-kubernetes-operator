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

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete

var serviceMonitorGVK = schema.GroupVersionKind{
	Group:   "monitoring.coreos.com",
	Version: "v1",
	Kind:    "ServiceMonitor",
}

// reconcileMonitoring reconciles monitoring resources (ServiceMonitor).
func (r *SupersetReconciler) reconcileMonitoring(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	enabled := superset.Spec.Monitoring != nil && superset.Spec.Monitoring.ServiceMonitor != nil

	if !enabled {
		return r.deleteServiceMonitors(ctx, superset)
	}

	return r.reconcileServiceMonitor(ctx, superset)
}

func (r *SupersetReconciler) reconcileServiceMonitor(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	sm := superset.Spec.Monitoring.ServiceMonitor

	interval := "30s"
	if sm.Interval != nil && *sm.Interval != "" {
		interval = *sm.Interval
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(serviceMonitorGVK)
	obj.SetName(superset.Name)
	obj.SetNamespace(superset.Namespace)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		if err := controllerutil.SetControllerReference(superset, obj, r.Scheme); err != nil {
			return err
		}

		operatorLabels := parentLabels(superset.Name)
		operatorLabels[common.LabelKeyInstance] = superset.Name
		labels := mergeLabels(sm.Labels, operatorLabels)
		obj.SetLabels(labels)

		endpoint := map[string]interface{}{
			"port":     common.PortNameHTTP,
			"interval": interval,
		}
		if sm.ScrapeTimeout != nil {
			endpoint["scrapeTimeout"] = *sm.ScrapeTimeout
		}

		obj.Object["spec"] = map[string]interface{}{
			"selector": map[string]interface{}{
				"matchLabels": map[string]interface{}{
					common.LabelKeyName:      common.LabelValueApp,
					common.LabelKeyComponent: string(common.ComponentWebServer),
					common.LabelKeyInstance:  webServerDescriptor.childName(&superset.Spec, superset.Name),
				},
			},
			"namespaceSelector": map[string]interface{}{
				"matchNames": []interface{}{superset.Namespace},
			},
			"endpoints": []interface{}{endpoint},
		}

		return nil
	})

	if meta.IsNoMatchError(err) {
		// ServiceMonitor CRD not installed -- log and skip.
		logf.FromContext(ctx).Info("ServiceMonitor CRD not installed, skipping monitoring setup")
		return nil
	}

	return err
}

func (r *SupersetReconciler) deleteServiceMonitors(ctx context.Context, superset *supersetv1alpha1.Superset) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(serviceMonitorGVK)
	return r.deleteByLabels(ctx, superset.Namespace,
		parentLabels(superset.Name),
		func() client.ObjectList { return list }, "")
}
