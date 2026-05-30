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
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// terminalStartupWaitingReasons are container "waiting" reasons that mean the
// container will never start on its own without a change to the Pod or its
// dependencies (a bad image reference, an invalid container config such as a
// runAsNonRoot violation, or an unpullable image). They are distinct from the
// benign transient reasons (ContainerCreating, PodInitializing) a Pod passes
// through during normal startup.
var terminalStartupWaitingReasons = map[string]bool{
	"CreateContainerConfigError": true,
	"CreateContainerError":       true,
	"RunContainerError":          true,
	"InvalidImageName":           true,
	"ErrImagePull":               true,
	"ImagePullBackOff":           true,
}

// podStartupError reports whether a Pod is wedged — unable to start its
// containers without external intervention — and returns a human-readable
// reason. A Job-backed task Pod in this state never sets a JobFailed condition
// (the container never runs), so the controller would otherwise wait on it
// until the Job's activeDeadlineSeconds with no actionable signal. Detecting it
// lets the controller surface the blocker and self-heal once the spec changes.
//
// It is intentionally conservative: only container config/image errors and
// definitive scheduling failures count. Transient states (image pulling,
// ContainerCreating) return false so normal startup is never misreported.
func podStartupError(pod *corev1.Pod) (string, bool) {
	if pod.DeletionTimestamp != nil {
		return "", false
	}

	statuses := make([]corev1.ContainerStatus, 0,
		len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses))
	statuses = append(statuses, pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for i := range statuses {
		w := statuses[i].State.Waiting
		if w != nil && terminalStartupWaitingReasons[w.Reason] {
			return formatContainerStartupError(statuses[i].Name, w.Reason, w.Message), true
		}
	}

	if pod.Status.Phase == corev1.PodPending {
		for i := range pod.Status.Conditions {
			c := pod.Status.Conditions[i]
			if c.Type == corev1.PodScheduled &&
				c.Status == corev1.ConditionFalse &&
				c.Reason == corev1.PodReasonUnschedulable {
				return strings.TrimSpace(fmt.Sprintf("pod unschedulable: %s", c.Message)), true
			}
		}
	}

	return "", false
}

func formatContainerStartupError(name, reason, message string) string {
	msg := fmt.Sprintf("container %q: %s", name, reason)
	if trimmed := strings.TrimSpace(message); trimmed != "" {
		msg += ": " + trimmed
	}
	return msg
}
