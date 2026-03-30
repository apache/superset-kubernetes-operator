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
	"math"
	"time"

	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

const (
	defaultInitTimeout           = 300 * time.Second
	defaultMaxRetries      int32 = 3
	defaultRetentionPolicy       = retentionDelete

	retentionDelete       = "Delete"
	retentionRetain       = "Retain"
	retentionRetainOnFail = "RetainOnFailure"

	initRequeueInterval = 10 * time.Second

	initTaskName = common.InitTaskInit

	labelInitTask     = common.LabelKeyInitTask
	labelInitInstance = common.LabelKeyInitInstance

	// Init state constants.
	initStatePending  = "Pending"
	initStateRunning  = "Running"
	initStateComplete = "Complete"
	initStateFailed   = "Failed"

	// Phase constants.
	phaseInitializing = "Initializing"
	phaseRunning      = "Running"
	phaseDegraded     = "Degraded"
	phaseSuspended    = "Suspended"
)

// calculateBackoff returns the backoff duration for a given attempt using exponential backoff.
func calculateBackoff(attempt int32) time.Duration {
	// 10s, 20s, 40s, 80s, ... capped at 300s.
	seconds := 10.0 * math.Pow(2, float64(attempt-1))
	if seconds > 300 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}

const maxTerminationMessageLen = 256

func podFailureMessage(pod *corev1.Pod) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil {
			msg := fmt.Sprintf("Exit code %d", cs.State.Terminated.ExitCode)
			if cs.State.Terminated.Reason != "" {
				msg += ": " + cs.State.Terminated.Reason
			}
			if cs.State.Terminated.Message != "" {
				detail := cs.State.Terminated.Message
				if len(detail) > maxTerminationMessageLen {
					detail = detail[:maxTerminationMessageLen] + "..."
				}
				msg += ": " + detail
			}
			return msg
		}
	}
	return "Pod failed"
}

// ShouldDeletePod determines if a pod should be deleted based on retention policy and pod phase.
func ShouldDeletePod(policy string, phase corev1.PodPhase) bool {
	switch policy {
	case retentionDelete:
		return true
	case retentionRetainOnFail:
		return phase != corev1.PodFailed
	case retentionRetain:
		return false
	default:
		return true
	}
}

// isInitDisabled checks if initialization is disabled on the parent Superset CR.
func isInitDisabled(superset *supersetv1alpha1.Superset) bool {
	if superset.Spec.Init == nil {
		return false
	}
	return superset.Spec.Init.Disabled != nil && *superset.Spec.Init.Disabled
}
