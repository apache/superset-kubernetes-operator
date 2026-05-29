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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

func TestResolveServiceAccountName(t *testing.T) {
	tests := []struct {
		name string
		sa   *supersetv1alpha1.ServiceAccountSpec
		want string
	}{
		{
			name: "nil spec defaults to parent name",
			sa:   nil,
			want: "test",
		},
		{
			name: "create=true with explicit name uses the name",
			sa:   &supersetv1alpha1.ServiceAccountSpec{Create: boolPtr(true), Name: "custom-sa"},
			want: "custom-sa",
		},
		{
			name: "create=true without name defaults to parent name",
			sa:   &supersetv1alpha1.ServiceAccountSpec{Create: boolPtr(true)},
			want: "test",
		},
		{
			name: "create unset with name uses the name",
			sa:   &supersetv1alpha1.ServiceAccountSpec{Name: "custom-sa"},
			want: "custom-sa",
		},
		{
			name: "create=false with name references the existing SA",
			sa:   &supersetv1alpha1.ServiceAccountSpec{Create: boolPtr(false), Name: "external-sa"},
			want: "external-sa",
		},
		{
			name: "create=false without name yields empty (pod default SA)",
			sa:   &supersetv1alpha1.ServiceAccountSpec{Create: boolPtr(false)},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			superset := &supersetv1alpha1.Superset{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec:       supersetv1alpha1.SupersetSpec{ServiceAccount: tt.sa},
			}
			assert.Equal(t, tt.want, resolveServiceAccountName(superset))
		})
	}
}
