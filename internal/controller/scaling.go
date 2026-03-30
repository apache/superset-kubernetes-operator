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

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// reconcileHPA creates, updates, or deletes an HPA for the given component.
func reconcileHPA(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	autoscaling *supersetv1alpha1.AutoscalingSpec,
	labels map[string]string,
	deploymentName string,
	namespace string,
) error {
	if autoscaling == nil {
		return deleteByLabels(ctx, c, namespace, labels,
			func() client.ObjectList { return &autoscalingv2.HorizontalPodAutoscalerList{} }, "")
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, c, hpa, func() error {
		if err := controllerutil.SetControllerReference(owner, hpa, scheme); err != nil {
			return err
		}

		hpa.Labels = mergeLabels(hpa.Labels, labels)

		hpa.Spec = autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deploymentName,
			},
			MinReplicas: autoscaling.MinReplicas,
			MaxReplicas: autoscaling.MaxReplicas,
			Metrics:     autoscaling.Metrics,
		}

		return nil
	})
	return err
}

// reconcilePDB creates, updates, or deletes a PDB for the given component.
func reconcilePDB(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	pdbSpec *supersetv1alpha1.PDBSpec,
	labels map[string]string,
	name string,
	namespace string,
) error {
	if pdbSpec == nil {
		return deleteByLabels(ctx, c, namespace, labels,
			func() client.ObjectList { return &policyv1.PodDisruptionBudgetList{} }, "")
	}

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, c, pdb, func() error {
		if err := controllerutil.SetControllerReference(owner, pdb, scheme); err != nil {
			return err
		}

		pdb.Labels = mergeLabels(pdb.Labels, labels)

		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			Selector:       &metav1.LabelSelector{MatchLabels: labels},
			MinAvailable:   pdbSpec.MinAvailable,
			MaxUnavailable: pdbSpec.MaxUnavailable,
		}

		return nil
	})
	return err
}
