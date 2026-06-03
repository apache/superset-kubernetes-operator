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
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

func TestIsOwnedBy(t *testing.T) {
	owner := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "default", UID: types.UID("owner-uid")},
	}

	t.Run("matching owner UID returns true", func(t *testing.T) {
		obj := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{
					{UID: types.UID("someone-else")},
					{UID: types.UID("owner-uid")},
				},
			},
		}
		assert.True(t, isOwnedBy(obj, owner))
	})

	t.Run("non-matching owner UID returns false", func(t *testing.T) {
		obj := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{{UID: types.UID("someone-else")}},
			},
		}
		assert.False(t, isOwnedBy(obj, owner))
	})

	t.Run("no owner references returns false", func(t *testing.T) {
		obj := &appsv1.Deployment{}
		assert.False(t, isOwnedBy(obj, owner))
	})
}

func TestComputeChecksum_Stability(t *testing.T) {
	type payload struct {
		A string
		B int
	}
	p := payload{A: "x", B: 1}

	// Same input -> same hash.
	assert.Equal(t, computeChecksum(p), computeChecksum(p))
	// Output carries the sha256 prefix.
	assert.Contains(t, computeChecksum(p), "sha256:")
	// Different input -> different hash.
	assert.NotEqual(t, computeChecksum(p), computeChecksum(payload{A: "y", B: 1}))
	assert.NotEqual(t, computeChecksum(p), computeChecksum(payload{A: "x", B: 2}))
}
