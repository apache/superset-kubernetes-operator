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

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// setCondition sets a condition on a conditions slice, replacing any existing
// condition of the same type.
func setCondition(conditions *[]metav1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string, observedGeneration int64) {
	now := metav1.Now()
	for i, c := range *conditions {
		if c.Type == conditionType {
			if c.Status != status || c.Reason != reason || c.ObservedGeneration != observedGeneration {
				transitionTime := c.LastTransitionTime
				if c.Status != status {
					transitionTime = now
				}
				(*conditions)[i] = metav1.Condition{
					Type:               conditionType,
					Status:             status,
					LastTransitionTime: transitionTime,
					Reason:             reason,
					Message:            message,
					ObservedGeneration: observedGeneration,
				}
			}
			return
		}
	}
	*conditions = append(*conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: observedGeneration,
	})
}

// updateComponentStatusFromDeployment updates a ChildComponentStatus from a Deployment's status.
func updateComponentStatusFromDeployment(compStatus *supersetv1alpha1.ChildComponentStatus, deploy *appsv1.Deployment, observedGeneration int64) {
	desired := int32(1)
	if deploy.Spec.Replicas != nil {
		desired = *deploy.Spec.Replicas
	} else if deploy.Status.Replicas > 0 {
		desired = deploy.Status.Replicas
	}

	compStatus.Ready = fmt.Sprintf("%d/%d", deploy.Status.ReadyReplicas, desired)
	compStatus.ObservedGeneration = observedGeneration

	if deploy.Status.ReadyReplicas >= desired && desired > 0 {
		setCondition(&compStatus.Conditions,
			supersetv1alpha1.ConditionTypeReady,
			metav1.ConditionTrue,
			"AllReplicasReady",
			"All replicas are ready",
			observedGeneration,
		)
	} else if deploy.Status.ReadyReplicas > 0 {
		setCondition(&compStatus.Conditions,
			supersetv1alpha1.ConditionTypeReady,
			metav1.ConditionFalse,
			"PartiallyReady",
			"Some replicas are not ready",
			observedGeneration,
		)
	} else {
		setCondition(&compStatus.Conditions,
			supersetv1alpha1.ConditionTypeReady,
			metav1.ConditionFalse,
			"NotReady",
			"No replicas are ready",
			observedGeneration,
		)
	}

	// Check for progressing condition from Deployment.
	for _, c := range deploy.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing {
			if c.Status == corev1.ConditionTrue && c.Reason == "NewReplicaSetAvailable" {
				setCondition(&compStatus.Conditions,
					supersetv1alpha1.ConditionTypeProgressing,
					metav1.ConditionFalse,
					"RolloutComplete",
					"Deployment rollout is complete",
					observedGeneration,
				)
			} else if c.Status == corev1.ConditionTrue {
				setCondition(&compStatus.Conditions,
					supersetv1alpha1.ConditionTypeProgressing,
					metav1.ConditionTrue,
					"RolloutInProgress",
					"Deployment rollout is in progress",
					observedGeneration,
				)
			}
		}
	}
}
