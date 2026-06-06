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

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/apache/superset-kubernetes-operator/internal/common"
	"github.com/apache/superset-kubernetes-operator/internal/resolution"
)

// createOrUpdateWithRetry wraps controllerutil.CreateOrUpdate with automatic
// retry on optimistic-lock conflicts. Such conflicts are routine rather than
// exceptional: another actor (e.g. the built-in Deployment controller writing
// rollout status) can bump an object's resourceVersion between our Get and
// Update, which the API server then rejects with a Conflict. Without a retry
// this surfaces as a noisy error-level "the object has been modified" reconcile
// failure that self-heals on the next requeue. Retrying re-Gets the latest
// version and re-applies the mutation inline, so the conflict stays invisible.
//
// Use this in place of controllerutil.CreateOrUpdate for every parent-owned
// resource so the benefit applies uniformly.
func createOrUpdateWithRetry(
	ctx context.Context,
	c client.Client,
	obj client.Object,
	mutate controllerutil.MutateFn,
) (controllerutil.OperationResult, error) {
	var op controllerutil.OperationResult
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var innerErr error
		op, innerErr = controllerutil.CreateOrUpdate(ctx, c, obj, mutate)
		return innerErr
	})
	return op, err
}

// parentLabels returns the operator-managed labels for parent-owned resources
// (ServiceAccount, Ingress, HTTPRoute, ServiceMonitor). These labels enable
// label-based discovery for cleanup.
func parentLabels(parentName string) map[string]string {
	return map[string]string{
		common.LabelKeyName:   common.LabelValueApp,
		common.LabelKeyParent: parentName,
	}
}

// componentLabels returns the standard labels for a Superset component.
// This delegates to common.ComponentLabels for the canonical implementation.
func componentLabels(component string, instance string) map[string]string {
	return common.ComponentLabels(common.ComponentType(component), instance)
}

// podOperatorLabels returns the operator-managed labels applied to pod templates.
// Includes the standard component labels plus the parent label, which is needed
// for instance-scoped NetworkPolicy selectors.
func podOperatorLabels(component string, instance string, parentName string) map[string]string {
	labels := componentLabels(component, instance)
	labels[common.LabelKeyParent] = parentName
	return labels
}

// mergeLabels merges base and extra labels. Extra labels override base labels
// with the same key. Always returns a non-nil map (needed for label selectors).
func mergeLabels(base, extra map[string]string) map[string]string {
	merged := resolution.MergeMaps(base, extra)
	if merged == nil {
		return map[string]string{}
	}
	return merged
}

// mergeAnnotations merges base and extra annotations. Extra annotations override
// base annotations with the same key. Returns nil when both inputs are empty
// (omits empty annotations from serialized YAML).
func mergeAnnotations(base, extra map[string]string) map[string]string {
	return resolution.MergeMaps(base, extra)
}
