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
	"os"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// TestReconcile_SecretsNeverLeakIntoConfigMaps guards the flagship threat-model
// guarantee (docs/reference/security.md, "Secret Handling"): secret values are
// resolved at runtime from environment variables and never appear in a
// ConfigMap. The renderer emits `os.environ[...]` references rather than
// literals, and config_builder injects the real values as container env vars.
//
// Development mode is the strict case to test: inline secrets are permitted, so
// the sentinel values are actually present in the spec and *could* leak if a
// future change rendered them into superset_config.py instead of referencing
// the env var. websocketServer is deliberately excluded — its inline config is
// written to a ConfigMap by design (the documented dev-only exception), so it
// would invalidate this invariant. See TestReconcile_WebsocketInlineConfig*.
func TestReconcile_SecretsNeverLeakIntoConfigMaps(t *testing.T) {
	scheme := testScheme(t)

	// Distinctive sentinels unlikely to collide with any non-secret config text.
	const (
		sentinelSecretKey = "SENTINEL0SECRET0KEY"
		sentinelPrevKey   = "SENTINEL0PREVIOUS0KEY"
		sentinelDBPass    = "SENTINEL0DATABASE0PASSWORD"
		sentinelValkey    = "SENTINEL0VALKEY0PASSWORD"
	)
	sentinels := []string{sentinelSecretKey, sentinelPrevKey, sentinelDBPass, sentinelValkey}

	spec := minimalSupersetSpec()
	spec.Environment = common.Ptr("Development")
	// Inline secrets (dev-mode) so the values are present in the spec.
	spec.SecretKeyFrom = nil
	spec.SecretKey = common.Ptr(sentinelSecretKey)
	spec.PreviousSecretKey = common.Ptr(sentinelPrevKey)
	spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:     common.Ptr("postgres"),
		Database: common.Ptr("superset"),
		Username: common.Ptr("superset"),
		Password: common.Ptr(sentinelDBPass),
	}
	spec.Valkey = &supersetv1alpha1.ValkeySpec{
		Host:     "valkey",
		Password: common.Ptr(sentinelValkey),
	}
	// Exercise every Python config consumer (web-server is already set).
	spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{}
	spec.CeleryBeat = &supersetv1alpha1.CeleryBeatComponentSpec{}
	spec.McpServer = &supersetv1alpha1.McpServerComponentSpec{}

	superset := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
		Spec:       spec,
	}

	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r)

	ctx := context.Background()

	// 1. No sentinel secret value may appear in ANY owned ConfigMap.
	cms := &corev1.ConfigMapList{}
	if err := c.List(ctx, cms, client.InNamespace("default")); err != nil {
		t.Fatalf("list ConfigMaps: %v", err)
	}
	if len(cms.Items) == 0 {
		t.Fatal("expected at least one ConfigMap to be reconciled")
	}
	for _, cm := range cms.Items {
		for key, val := range cm.Data {
			for _, s := range sentinels {
				if strings.Contains(val, s) {
					t.Errorf("secret leak: ConfigMap %s data[%s] contains sentinel %q", cm.Name, key, s)
				}
			}
		}
	}

	// 2. Positive control: the rendered config references the secrets via env
	//    vars. Without this, a regression that simply dropped the secrets would
	//    pass assertion (1) vacuously.
	webCM := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-web-server-config", Namespace: "default"}, webCM); err != nil {
		t.Fatalf("get web-server ConfigMap: %v", err)
	}
	pyConfig := webCM.Data["superset_config.py"]
	for _, want := range []string{common.EnvSecretKey, common.EnvPreviousSecretKey, common.EnvDBPass, common.EnvValkeyPass} {
		if !strings.Contains(pyConfig, want) {
			t.Errorf("expected superset_config.py to reference env var %q (os.environ), got:\n%s", want, pyConfig)
		}
	}

	// 3. Cross-check: the secrets DO flow through the pod environment (inline
	//    Values in dev mode), confirming the env-var transport is what carries
	//    them — not the ConfigMap.
	webDeploy := &appsv1.Deployment{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-web-server", Namespace: "default"}, webDeploy); err != nil {
		t.Fatalf("get web-server Deployment: %v", err)
	}
	envValues := map[string]string{}
	for _, container := range webDeploy.Spec.Template.Spec.Containers {
		for _, e := range container.Env {
			envValues[e.Name] = e.Value
		}
	}
	if got := envValues[common.EnvSecretKey]; got != sentinelSecretKey {
		t.Errorf("expected %s env Value %q, got %q", common.EnvSecretKey, sentinelSecretKey, got)
	}
	if got := envValues[common.EnvDBPass]; got != sentinelDBPass {
		t.Errorf("expected %s env Value %q, got %q", common.EnvDBPass, sentinelDBPass, got)
	}
	if got := envValues[common.EnvValkeyPass]; got != sentinelValkey {
		t.Errorf("expected %s env Value %q, got %q", common.EnvValkeyPass, sentinelValkey, got)
	}
}

// TestManagerRole_GrantsNoSecretsAccessOrWildcards guards the RBAC least-
// privilege claims in docs/reference/security.md ("The operator does not
// request: `*` (wildcard) on any resource or verb ... Kubernetes Secret read or
// write permissions"). It reads the generated ClusterRole — the source-of-truth
// artifact that is actually deployed — so an accidental `+kubebuilder:rbac`
// marker that broadens scope is caught by `make codegen` + this test.
func TestManagerRole_GrantsNoSecretsAccessOrWildcards(t *testing.T) {
	// Tests run from the package directory; the role manifest lives at repo root.
	const rolePath = "../../config/rbac/role.yaml"

	raw, err := os.ReadFile(rolePath)
	if err != nil {
		t.Fatalf("read %s: %v", rolePath, err)
	}

	role := &rbacv1.ClusterRole{}
	if err := yaml.Unmarshal(raw, role); err != nil {
		t.Fatalf("unmarshal ClusterRole: %v", err)
	}
	if len(role.Rules) == 0 {
		t.Fatal("expected manager ClusterRole to define rules")
	}

	for i, rule := range role.Rules {
		for _, res := range rule.Resources {
			if res == "secrets" {
				t.Errorf("rule[%d] grants access to 'secrets'; the operator must never read or write Secrets (it injects secretKeyRef env vars instead)", i)
			}
			if res == "*" {
				t.Errorf("rule[%d] uses a wildcard resource '*'; the operator must request explicit resources only", i)
			}
		}
		for _, verb := range rule.Verbs {
			if verb == "*" {
				t.Errorf("rule[%d] uses a wildcard verb '*'; the operator must request explicit verbs only", i)
			}
		}
		for _, group := range rule.APIGroups {
			if group == "*" {
				t.Errorf("rule[%d] uses a wildcard apiGroup '*'; the operator must request explicit API groups only", i)
			}
		}
	}
}
