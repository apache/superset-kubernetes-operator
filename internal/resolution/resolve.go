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

package resolution

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// ResolveScalar returns the first non-nil value from the provided pointers.
// Returns the zero value of T if all are nil.
func ResolveScalar[T any](values ...*T) T {
	for _, v := range values {
		if v != nil {
			return *v
		}
	}
	var zero T
	return zero
}

// ResolveResource returns the first non-nil ResourceRequirements.
func ResolveResource(values ...*corev1.ResourceRequirements) corev1.ResourceRequirements {
	for _, v := range values {
		if v != nil {
			return *v
		}
	}
	return corev1.ResourceRequirements{}
}

// ResolveOverridableMap returns the override if it's non-nil (even if empty),
// otherwise returns the default. This enables overriding with an empty map.
func ResolveOverridableMap(override, defaultVal map[string]string) map[string]string {
	if override != nil {
		return override
	}
	return defaultVal
}

// ResolveOverridableSlice returns the override if it's non-nil (even if empty),
// otherwise returns the default. This enables overriding with an empty slice.
func ResolveOverridableSlice[T any](override, defaultVal []T) []T {
	if override != nil {
		return override
	}
	return defaultVal
}

// ResolveOverridableValue returns the override if non-nil, otherwise the default.
func ResolveOverridableValue[T any](override, defaultVal *T) *T {
	if override != nil {
		return override
	}
	return defaultVal
}

// Ptr returns a pointer to the given value. Delegates to common.Ptr.
func Ptr[T any](v T) *T { return common.Ptr(v) }
