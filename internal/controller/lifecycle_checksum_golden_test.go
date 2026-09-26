/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package controller

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// goldenCascadeSupersets returns representative Superset shapes whose
// lifecycle task checksums are pinned by TestLifecycleCascadeChecksumsGolden.
func goldenCascadeSupersets() map[string]*supersetv1alpha1.Superset {
	structured := func() *supersetv1alpha1.MetastoreSpec {
		return &supersetv1alpha1.MetastoreSpec{
			Host:         new("db.example.com"),
			Database:     new("superset"),
			Username:     new("superset"),
			PasswordFrom: &corev1.SecretKeySelector{Name: "db", Key: "password"},
		}
	}
	newSuperset := func(spec supersetv1alpha1.SupersetSpec) *supersetv1alpha1.Superset {
		return &supersetv1alpha1.Superset{
			Name: "golden", Namespace: "default", UID: "golden-uid",
			Spec: spec,
		}
	}

	defaults := minimalSupersetSpec()
	defaults.Image.Tag = "6.0.0"
	defaults.Metastore = structured()
	defaults.Lifecycle = &supersetv1alpha1.LifecycleSpec{}

	full := minimalSupersetSpec()
	full.Image.Tag = "6.0.0"
	full.Metastore = structured()
	full.Metastore.CreateDatabase = new(true)
	full.PreviousSecretKeyFrom = &corev1.SecretKeySelector{Name: "app-secret", Key: "previous"}
	full.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Migrate: &supersetv1alpha1.MigrateTaskSpec{Trigger: new("t1")},
		Rotate:  &supersetv1alpha1.RotateTaskSpec{},
		Init:    &supersetv1alpha1.InitTaskSpec{Command: []string{"sh", "-c", "superset init"}},
	}

	seeded := minimalSupersetSpec()
	seeded.Image.Tag = "6.0.0"
	seeded.Environment = new(common.EnvironmentStaging)
	seeded.Metastore = structured()
	seeded.Lifecycle = &supersetv1alpha1.LifecycleSpec{
		Seed: &supersetv1alpha1.SeedTaskSpec{
			Source: supersetv1alpha1.SeedSourceSpec{
				Host:         "prod-db.example.com",
				Database:     "superset",
				Username:     "reader",
				PasswordFrom: &corev1.SecretKeySelector{Name: "prod", Key: "password"},
			},
			ExcludeTableData: []string{"logs"},
		},
	}

	return map[string]*supersetv1alpha1.Superset{
		"defaults": newSuperset(defaults),
		"full":     newSuperset(full),
		"seeded":   newSuperset(seeded),
	}
}

// TestLifecycleCascadeChecksumsGolden pins the lifecycle task checksums of
// representative Superset resources. A change here means every existing
// installation re-runs the affected tasks (and drains components) on the
// next reconcile after an operator upgrade, so it must be deliberate. Run
// with GOLDEN_PRINT=1 to print the current values.
func TestLifecycleCascadeChecksumsGolden(t *testing.T) {
	golden := map[string]map[string]string{
		"defaults": {
			"Migrate": "sha256:22963c378ac5e4ede0164d52de4f619a268a5d13f97a83a582852415ac640526",
			"Init":    "sha256:2e16634ae7b636bead7a841bc8fef8143e3de59304f0c3f7d705b4aa38aee160",
		},
		"full": {
			"Migrate": "sha256:d41346b569c96ff91e1f432cefb7d556507a2fe7170b6f16c2f3559f05a1915e",
			"Rotate":  "sha256:eb553de2bd316bc60a34ddac66f139be926aa9e91bdfb9121276171dc9dfd7fc",
			"Init":    "sha256:a28957858ada1b369553e125d8ce8cc2106de8325ca2aeaf59da1a70179e7adc",
		},
		"seeded": {
			"Seed":    "sha256:bf647c124968b71f01689bbcabc30fe7d730b302a85ee2af5b33d60f80cfea6d",
			"Migrate": "sha256:07d0e18d962d9f7ba1c4112b8581ece983249a301564781f92eb709dc21ac6aa",
			"Init":    "sha256:a094f614d2eae6ac4ce479a20d51cefb18229df931f49e9a20de4ce17b2fe402",
		},
	}

	r := &SupersetReconciler{}
	for name, superset := range goldenCascadeSupersets() {
		got := map[string]string{}
		for _, step := range r.walkLifecycleCascade(superset, "golden-config") {
			got[step.Desc.TaskType] = step.TaskChecksum
		}
		if os.Getenv("GOLDEN_PRINT") != "" {
			t.Logf("%s: %#v", name, got)
			continue
		}
		assert.Equal(t, golden[name], got, name)
	}
}
