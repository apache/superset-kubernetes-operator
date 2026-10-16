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
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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
// the marker values are actually present in the spec and *could* leak if a
// future change rendered them into superset_config.py instead of referencing
// the env var. That includes the realtime websocket JWT secret, which reaches
// both the app and the Node.js websocket server through env vars only. Valkey
// connects either to a fixed host or through Sentinel, which are mutually
// exclusive, so each mode runs separately; Sentinel adds its own password.
func TestReconcile_SecretsNeverLeakIntoConfigMaps(t *testing.T) {
	scheme := testScheme(t)

	// Distinctive markers unlikely to collide with any non-secret config text.
	const (
		markerSecretKey    = "MARKER0SECRET0KEY"
		markerPrevKey      = "MARKER0PREVIOUS0KEY"
		markerDBPass       = "MARKER0DATABASE0PASSWORD"
		markerValkey       = "MARKER0VALKEY0PASSWORD"
		markerSentinelPass = "MARKER0VALKEY0SENTINEL0PASSWORD"
		markerWSJWT        = "MARKER0WEBSOCKET0JWT0SECRET"
	)

	tests := []struct {
		name   string
		valkey *supersetv1alpha1.ValkeySpec
		// Secret markers and the env vars that carry them, beyond the ones
		// shared by both modes.
		markers   []string
		pyEnvVars []string
		webEnv    map[string]string
		wsEnv     map[string]string
	}{
		{
			name:   "valkey host",
			valkey: &supersetv1alpha1.ValkeySpec{Host: "valkey", Password: new(markerValkey)},
		},
		{
			name: "valkey sentinel",
			valkey: &supersetv1alpha1.ValkeySpec{
				Password: new(markerValkey),
				Sentinel: &supersetv1alpha1.ValkeySentinelSpec{
					MasterName: "mymaster",
					Nodes:      []supersetv1alpha1.ValkeySentinelNode{{Host: "sentinel-0"}},
					Password:   new(markerSentinelPass),
				},
			},
			markers:   []string{markerSentinelPass},
			pyEnvVars: []string{common.EnvValkeySentinelPass},
			webEnv:    map[string]string{common.EnvValkeySentinelPass: markerSentinelPass},
			wsEnv:     map[string]string{"REDIS_SENTINEL_PASSWORD": markerSentinelPass},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			markers := append([]string{markerSecretKey, markerPrevKey, markerDBPass, markerValkey, markerWSJWT}, tt.markers...)

			spec := minimalSupersetSpec()
			spec.Environment = new("Development")
			// Inline secrets (dev-mode) so the values are present in the spec.
			spec.SecretKeyFrom = nil
			spec.SecretKey = new(markerSecretKey)
			spec.PreviousSecretKey = new(markerPrevKey)
			spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				Host:     new("postgres"),
				Database: new("superset"),
				Username: new("superset"),
				Password: new(markerDBPass),
			}
			spec.Valkey = tt.valkey
			// Exercise every Python config consumer (web-server is already set).
			spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{}
			spec.CeleryBeat = &supersetv1alpha1.CeleryBeatComponentSpec{}
			spec.McpServer = &supersetv1alpha1.McpServerComponentSpec{}
			spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{}
			spec.Realtime = &supersetv1alpha1.RealtimeSpec{
				WebSocket: &supersetv1alpha1.WebSocketTransportSpec{JWTSecret: new(markerWSJWT), URL: new("ws://superset.example.com/ws")},
			}

			superset := &supersetv1alpha1.Superset{
				Name: "test", Namespace: "default", UID: "uid-1",
				Spec: spec,
			}

			c := reconcileOnce(t, scheme, superset).Build()
			r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
			doReconcile(t, r)

			ctx := context.Background()

			// 1. No secret marker may appear in ANY owned ConfigMap.
			cms := &corev1.ConfigMapList{}
			if err := c.List(ctx, cms, client.InNamespace("default")); err != nil {
				t.Fatalf("list ConfigMaps: %v", err)
			}
			if len(cms.Items) == 0 {
				t.Fatal("expected at least one ConfigMap to be reconciled")
			}
			for _, cm := range cms.Items {
				for key, val := range cm.Data {
					for _, m := range markers {
						if strings.Contains(val, m) {
							t.Errorf("secret leak: ConfigMap %s data[%s] contains marker %q", cm.Name, key, m)
						}
					}
				}
			}

			// 2. Positive control: the rendered config references the secrets via
			//    env vars. Without this, a regression that simply dropped the
			//    secrets would pass assertion (1) vacuously.
			webCM := &corev1.ConfigMap{}
			if err := c.Get(ctx, types.NamespacedName{Name: "test-web-server-config", Namespace: "default"}, webCM); err != nil {
				t.Fatalf("get web-server ConfigMap: %v", err)
			}
			pyConfig := webCM.Data["superset_config.py"]
			pyEnvVars := append([]string{common.EnvSecretKey, common.EnvPreviousSecretKey, common.EnvDBPass, common.EnvValkeyPass, common.EnvWSJWTSecret}, tt.pyEnvVars...)
			for _, want := range pyEnvVars {
				if !strings.Contains(pyConfig, want) {
					t.Errorf("expected superset_config.py to reference env var %q (os.environ), got:\n%s", want, pyConfig)
				}
			}

			// 3. Cross-check: the secrets DO flow through the pod environment
			//    (inline Values in dev mode), confirming the env-var transport is
			//    what carries them — not the ConfigMap.
			webEnv := map[string]string{
				common.EnvSecretKey:   markerSecretKey,
				common.EnvDBPass:      markerDBPass,
				common.EnvValkeyPass:  markerValkey,
				common.EnvWSJWTSecret: markerWSJWT,
			}
			maps.Copy(webEnv, tt.webEnv)
			assertDeploymentEnv(t, c, "test-web-server", webEnv)
			wsEnv := map[string]string{"JWT_SECRET": markerWSJWT}
			maps.Copy(wsEnv, tt.wsEnv)
			assertDeploymentEnv(t, c, "test-websocket-server", wsEnv)
		})
	}
}

// assertDeploymentEnv checks that the named Deployment's main container sets
// each env var to the given inline value.
func assertDeploymentEnv(t *testing.T, c client.Client, name string, want map[string]string) {
	t.Helper()
	deploy := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, deploy); err != nil {
		t.Fatalf("get Deployment %s: %v", name, err)
	}
	got := envSliceToMap(deploy.Spec.Template.Spec.Containers[0].Env)
	for k, v := range want {
		if got[k] != v {
			t.Errorf("Deployment %s: expected env %s Value %q, got %q", name, k, v, got[k])
		}
	}
}

// TestManagerRole_GrantsNoSecretsAccessOrWildcards guards the RBAC least-
// privilege claims in docs/reference/security.md ("The operator does not
// request: `*` (wildcard) on any resource or verb ... Kubernetes Secret read or
// write permissions"). It reads the generated ClusterRole — the source-of-truth
// artifact that is actually deployed — so an accidental `+kubebuilder:rbac`
// marker that broadens scope is caught by `make codegen` + this test.
func TestManagerRole_GrantsNoSecretsAccessOrWildcards(t *testing.T) {
	role := loadManagerClusterRole(t)

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

// TestManagerRole_GrantsFinalizersForOwnerReferences guards against a
// regression of issue #357. The operator stamps blockOwnerDeletion owner
// references on every child object via controllerutil.SetControllerReference;
// the OwnerReferencesPermissionEnforcement admission plugin (enabled by default
// on OpenShift) rejects those unless the operator can write the owning CR's
// finalizers subresource, which requires this RBAC grant.
func TestManagerRole_GrantsFinalizersForOwnerReferences(t *testing.T) {
	role := loadManagerClusterRole(t)

	if !ruleGrants(role.Rules, "superset.apache.org", "supersets/finalizers", "update") {
		t.Errorf("manager ClusterRole must grant 'update' on supersets/finalizers so blockOwnerDeletion "+
			"owner references are accepted (issue #357); rules: %+v", role.Rules)
	}
}

// TestHelmManagerRulesMatchGeneratedClusterRole guards against RBAC drift
// between the two independent sources of the operator's manager rules: the
// generated config/rbac/role.yaml (from +kubebuilder:rbac markers) and the
// hand-maintained Helm helper. They render into the deployed ClusterRole and
// per-namespace Roles respectively, so a rule added to one but not the other
// silently under- or over-privileges one install path.
func TestHelmManagerRulesMatchGeneratedClusterRole(t *testing.T) {
	generated := loadManagerClusterRole(t)

	const helpersPath = "../../charts/superset-operator/templates/_helpers.tpl"
	raw, err := os.ReadFile(helpersPath)
	if err != nil {
		t.Fatalf("read %s: %v", helpersPath, err)
	}

	// The managerRules define block is static YAML with no template directives,
	// so it can be parsed directly without rendering the chart.
	re := regexp.MustCompile(`(?s){{- define "superset-operator\.managerRules" -}}\n(.*?)\n{{- end }}`)
	m := re.FindStringSubmatch(string(raw))
	if m == nil {
		t.Fatal("could not locate superset-operator.managerRules define block in _helpers.tpl")
	}

	var helmRules []rbacv1.PolicyRule
	if err := yaml.Unmarshal([]byte(m[1]), &helmRules); err != nil {
		t.Fatalf("unmarshal Helm managerRules: %v", err)
	}

	want := normalizeRules(generated.Rules)
	got := normalizeRules(helmRules)
	if !slices.Equal(want, got) {
		t.Errorf("Helm managerRules drifted from the generated ClusterRole; run `make manifests` and reconcile _helpers.tpl.\ngenerated: %v\nhelm:      %v", want, got)
	}
}

// loadManagerClusterRole reads the generated manager ClusterRole — the
// source-of-truth RBAC artifact that is actually deployed. Tests run from the
// package directory; the role manifest lives at repo root.
func loadManagerClusterRole(t *testing.T) *rbacv1.ClusterRole {
	t.Helper()

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
	return role
}

// ruleGrants reports whether any rule grants verb on group/resource.
func ruleGrants(rules []rbacv1.PolicyRule, group, resource, verb string) bool {
	for _, r := range rules {
		if slices.Contains(r.APIGroups, group) && slices.Contains(r.Resources, resource) && slices.Contains(r.Verbs, verb) {
			return true
		}
	}
	return false
}

// normalizeRules renders rules into a set-comparable form, sorting fields within
// each rule and the rules themselves so ordering differences (controller-gen
// sorts; the Helm helper preserves authored order) don't cause false diffs.
func normalizeRules(rules []rbacv1.PolicyRule) []string {
	out := make([]string, 0, len(rules))
	for _, r := range rules {
		groups := slices.Clone(r.APIGroups)
		resources := slices.Clone(r.Resources)
		verbs := slices.Clone(r.Verbs)
		sort.Strings(groups)
		sort.Strings(resources)
		sort.Strings(verbs)
		out = append(out, fmt.Sprintf("apiGroups=%v resources=%v verbs=%v", groups, resources, verbs))
	}
	sort.Strings(out)
	return out
}
