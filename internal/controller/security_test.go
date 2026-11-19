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
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// TestReconcile_SecretsNeverLeakIntoConfigMaps guards the flagship threat-model
// guarantee (docs/reference/security.md, "Secret Handling"): secret values are
// resolved at runtime from environment variables and never appear in a
// ConfigMap, the parent status, or Events. The renderer emits `os.environ[...]`
// references rather than literals, and config_builder injects the real values
// as container env vars.
//
// Development mode is the strict case to test: inline secrets are permitted, so
// the marker values are actually present in the spec and *could* leak if a
// future change rendered them into superset_config.py instead of referencing
// the env var. Each case drives the lifecycle pipeline to completion so the
// task ConfigMaps are rendered alongside the component ones. The realtime cases
// include the websocket JWT secret, which reaches both the app and the Node.js
// websocket server through env vars only. Valkey connects either to a fixed host
// or through Sentinel, which are mutually exclusive, so each mode runs
// separately; Sentinel adds its own password.
func TestReconcile_SecretsNeverLeakIntoConfigMaps(t *testing.T) {
	// Distinctive markers unlikely to collide with any non-secret config text.
	const (
		markerSecretKey    = "MARKER0SECRET0KEY"
		markerPrevKey      = "MARKER0PREVIOUS0KEY"
		markerDBPass       = "MARKER0DATABASE0PASSWORD"
		markerValkey       = "MARKER0VALKEY0PASSWORD"
		markerSentinelPass = "MARKER0VALKEY0SENTINEL0PASSWORD"
		markerWSJWT        = "MARKER0WEBSOCKET0JWT0SECRET"
	)

	t.Run("structured metastore with all lifecycle tasks", func(t *testing.T) {
		const (
			markerSeedPass  = "MARKER0SEED0SOURCE0PASSWORD"
			markerAdminPass = "MARKER0ADMIN0PASSWORD"
		)
		markers := []string{markerSecretKey, markerPrevKey, markerDBPass, markerValkey, markerSeedPass, markerAdminPass}

		spec := devLeakTestSpec(markerSecretKey)
		spec.PreviousSecretKey = new(markerPrevKey)
		spec.Metastore = &supersetv1alpha1.MetastoreSpec{
			Host:     new("postgres"),
			Database: new("superset"),
			Username: new("superset"),
			Password: new(markerDBPass),
		}
		spec.Valkey = &supersetv1alpha1.ValkeySpec{
			Host:     "valkey",
			Password: new(markerValkey),
		}
		spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
			Seed: &supersetv1alpha1.SeedTaskSpec{
				Source: supersetv1alpha1.SeedSourceSpec{
					Host:     "source-postgres",
					Database: "superset_src",
					Username: "reader",
					Password: new(markerSeedPass),
				},
			},
			Rotate: &supersetv1alpha1.RotateTaskSpec{},
			Init: &supersetv1alpha1.InitTaskSpec{
				AdminUser: &supersetv1alpha1.AdminUserSpec{Password: new(markerAdminPass)},
			},
		}

		c, superset, recorded := reconcileLeakTestToCompletion(t, spec)
		lc := superset.Status.Lifecycle
		if lc == nil || lc.Seed == nil || lc.Migrate == nil || lc.Rotate == nil || lc.Init == nil {
			t.Fatalf("expected seed, migrate, rotate, and init to have run; status.lifecycle=%+v", lc)
		}
		for task, ref := range map[string]*supersetv1alpha1.TaskRefStatus{"seed": lc.Seed, "migrate": lc.Migrate, "rotate": lc.Rotate, "init": lc.Init} {
			if ref.State != "Complete" {
				t.Errorf("expected %s task state Complete, got %q", task, ref.State)
			}
		}
		assertNoMarkerLeak(t, c, superset, recorded, markers,
			[]string{"test-web-server-config", "test-migrate-config", "test-rotate-config", "test-init-config"})

		// Positive control: the rendered config references the secrets via env
		// vars. Without this, a regression that simply dropped the secrets would
		// pass the leak assertions vacuously.
		assertConfigReferencesEnv(t, c, "test-web-server-config",
			"SUPERSET_OPERATOR__SECRET_KEY", "SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY",
			"SUPERSET_OPERATOR__DB_PASS", "SUPERSET_OPERATOR__VALKEY_PASS")
		assertConfigReferencesEnv(t, c, "test-rotate-config",
			"SUPERSET_OPERATOR__SECRET_KEY", "SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY",
			"SUPERSET_OPERATOR__DB_PASS")

		// Cross-check: the secrets DO flow through the pod environment (inline
		// Values in dev mode), confirming the env-var transport is what carries
		// them — not the ConfigMap.
		env := deploymentEnv(t, c, "test-web-server")
		for name, want := range map[string]string{
			"SUPERSET_OPERATOR__SECRET_KEY":  markerSecretKey,
			"SUPERSET_OPERATOR__DB_PASS":     markerDBPass,
			"SUPERSET_OPERATOR__VALKEY_PASS": markerValkey,
		} {
			if got := env[name].Value; got != want {
				t.Errorf("expected %s env Value %q, got %q", name, want, got)
			}
		}
	})

	t.Run("passthrough metastore uri", func(t *testing.T) {
		const markerURIPass = "MARKER0URI0PASSWORD"
		markerURI := "postgresql+psycopg2://superset:" + markerURIPass + "@postgres:5432/superset"

		spec := devLeakTestSpec(markerSecretKey)
		spec.Metastore = &supersetv1alpha1.MetastoreSpec{URI: new(markerURI)}
		spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{}

		c, superset, recorded := reconcileLeakTestToCompletion(t, spec)
		assertNoMarkerLeak(t, c, superset, recorded,
			[]string{markerSecretKey, markerURIPass},
			[]string{"test-web-server-config", "test-migrate-config", "test-init-config"})

		assertConfigReferencesEnv(t, c, "test-web-server-config", "SUPERSET_OPERATOR__DB_URI")
		assertConfigReferencesEnv(t, c, "test-migrate-config", "SUPERSET_OPERATOR__DB_URI")

		env := deploymentEnv(t, c, "test-web-server")
		if got := env["SUPERSET_OPERATOR__DB_URI"].Value; got != markerURI {
			t.Errorf("expected SUPERSET_OPERATOR__DB_URI env Value %q, got %q", markerURI, got)
		}
	})

	realtimeCases := []struct {
		name   string
		valkey *supersetv1alpha1.ValkeySpec
		// Markers and env vars carried in addition to those both modes share.
		markers   []string
		pyEnvVars []string
		webEnv    map[string]string
		wsEnv     map[string]string
	}{
		{
			name:   "realtime websocket with valkey host",
			valkey: &supersetv1alpha1.ValkeySpec{Host: "valkey", Password: new(markerValkey)},
		},
		{
			name: "realtime websocket with valkey sentinel",
			valkey: &supersetv1alpha1.ValkeySpec{
				Password: new(markerValkey),
				Sentinel: &supersetv1alpha1.ValkeySentinelSpec{
					MasterName: "mymaster",
					Nodes:      []supersetv1alpha1.ValkeySentinelNode{{Host: "sentinel-0"}},
					Password:   new(markerSentinelPass),
				},
			},
			markers:   []string{markerSentinelPass},
			pyEnvVars: []string{"SUPERSET_OPERATOR__VALKEY_SENTINEL_PASS"},
			webEnv:    map[string]string{"SUPERSET_OPERATOR__VALKEY_SENTINEL_PASS": markerSentinelPass},
			wsEnv:     map[string]string{"REDIS_SENTINEL_PASSWORD": markerSentinelPass},
		},
	}
	for _, tt := range realtimeCases {
		t.Run(tt.name, func(t *testing.T) {
			markers := append([]string{markerSecretKey, markerPrevKey, markerDBPass, markerValkey, markerWSJWT}, tt.markers...)

			spec := devLeakTestSpec(markerSecretKey)
			spec.PreviousSecretKey = new(markerPrevKey)
			spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				Host:     new("postgres"),
				Database: new("superset"),
				Username: new("superset"),
				Password: new(markerDBPass),
			}
			spec.Valkey = tt.valkey
			spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{}
			spec.Realtime = &supersetv1alpha1.RealtimeSpec{
				WebSocket: &supersetv1alpha1.WebSocketTransportSpec{JWTSecret: new(markerWSJWT), URL: new("ws://superset.example.com/ws")},
			}
			spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{}

			c, superset, recorded := reconcileLeakTestToCompletion(t, spec)
			assertNoMarkerLeak(t, c, superset, recorded, markers, []string{"test-web-server-config"})

			// Positive control: the rendered config references the secrets via env
			// vars, so a regression that dropped them cannot pass vacuously.
			pyEnvVars := append([]string{
				"SUPERSET_OPERATOR__SECRET_KEY", "SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY",
				"SUPERSET_OPERATOR__DB_PASS", "SUPERSET_OPERATOR__VALKEY_PASS", "SUPERSET_OPERATOR__WS_JWT_SECRET",
			}, tt.pyEnvVars...)
			assertConfigReferencesEnv(t, c, "test-web-server-config", pyEnvVars...)

			// Cross-check: the secrets DO flow through the pod environment of both
			// the app and the Node.js websocket server.
			webEnv := map[string]string{
				"SUPERSET_OPERATOR__SECRET_KEY":    markerSecretKey,
				"SUPERSET_OPERATOR__DB_PASS":       markerDBPass,
				"SUPERSET_OPERATOR__VALKEY_PASS":   markerValkey,
				"SUPERSET_OPERATOR__WS_JWT_SECRET": markerWSJWT,
			}
			maps.Copy(webEnv, tt.webEnv)
			wsEnv := map[string]string{"JWT_SECRET": markerWSJWT}
			maps.Copy(wsEnv, tt.wsEnv)
			for deployment, want := range map[string]map[string]string{"test-web-server": webEnv, "test-websocket-server": wsEnv} {
				env := deploymentEnv(t, c, deployment)
				for name, value := range want {
					if got := env[name].Value; got != value {
						t.Errorf("Deployment %s: expected %s env Value %q, got %q", deployment, name, value, got)
					}
				}
			}
		})
	}
}

// TestReconcile_ProductionSecretRefsUseSecretKeyRef guards the Staging/Production
// half of "Secret Handling": each operator-managed *From reference becomes a
// valueFrom.secretKeyRef env var, so the value is resolved by the kubelet and
// never copied by the operator.
func TestReconcile_ProductionSecretRefsUseSecretKeyRef(t *testing.T) {
	scheme := testScheme(t)
	spec := minimalSupersetSpec()
	spec.Environment = new(supersetv1alpha1.EnvironmentProduction)
	spec.SecretKeyFrom = &corev1.SecretKeySelector{Name: "app-secret", Key: "secret-key"}
	spec.PreviousSecretKeyFrom = &corev1.SecretKeySelector{Name: "app-secret", Key: "previous-secret-key"}
	spec.Metastore = &supersetv1alpha1.MetastoreSpec{
		Host:         new("postgres"),
		Database:     new("superset"),
		Username:     new("superset"),
		PasswordFrom: &corev1.SecretKeySelector{Name: "db-secret", Key: "password"},
	}
	spec.Valkey = &supersetv1alpha1.ValkeySpec{
		Host:         "valkey",
		PasswordFrom: &corev1.SecretKeySelector{Name: "valkey-secret", Key: "password"},
	}

	superset := &supersetv1alpha1.Superset{
		Name: "test", Namespace: "default", UID: "uid-1",
		Spec: spec,
	}
	c := reconcileOnce(t, scheme, superset).Build()
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	doReconcile(t, r)

	env := deploymentEnv(t, c, "test-web-server")
	for name, want := range map[string]corev1.SecretKeySelector{
		"SUPERSET_OPERATOR__SECRET_KEY":          {Name: "app-secret", Key: "secret-key"},
		"SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY": {Name: "app-secret", Key: "previous-secret-key"},
		"SUPERSET_OPERATOR__DB_PASS":             {Name: "db-secret", Key: "password"},
		"SUPERSET_OPERATOR__VALKEY_PASS":         {Name: "valkey-secret", Key: "password"},
	} {
		e, ok := env[name]
		if !ok {
			t.Errorf("expected env var %s on the web-server container", name)
			continue
		}
		if e.Value != "" {
			t.Errorf("expected %s to carry no literal Value, got %q", name, e.Value)
		}
		if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
			t.Errorf("expected %s to use valueFrom.secretKeyRef, got %+v", name, e.ValueFrom)
			continue
		}
		if got := e.ValueFrom.SecretKeyRef; got.Name != want.Name || got.Key != want.Key {
			t.Errorf("expected %s secretKeyRef %s/%s, got %s/%s", name, want.Name, want.Key, got.Name, got.Key)
		}
	}
}

// devLeakTestSpec returns a Development-mode spec with an inline secret key and
// every Python config consumer enabled.
func devLeakTestSpec(secretKey string) supersetv1alpha1.SupersetSpec {
	spec := minimalSupersetSpec()
	spec.Environment = new(supersetv1alpha1.EnvironmentDevelopment)
	spec.SecretKeyFrom = nil
	spec.SecretKey = new(secretKey)
	spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{}
	spec.CeleryBeat = &supersetv1alpha1.CeleryBeatComponentSpec{}
	spec.McpServer = &supersetv1alpha1.McpServerComponentSpec{}
	return spec
}

// reconcileLeakTestToCompletion reconciles a Superset built from spec, marking
// each lifecycle task Job succeeded as it appears, until the web-server
// Deployment exists. It returns the client, the latest parent, and every Event
// recorded along the way.
func reconcileLeakTestToCompletion(t *testing.T, spec supersetv1alpha1.SupersetSpec) (client.Client, *supersetv1alpha1.Superset, []string) {
	t.Helper()
	ctx := context.Background()
	scheme := testScheme(t)
	superset := &supersetv1alpha1.Superset{
		Name: "test", Namespace: "default", UID: "uid-1",
		Spec: spec,
	}
	c := reconcileOnce(t, scheme, superset).Build()
	recorder := events.NewFakeRecorder(100)
	r := &SupersetReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	var recorded []string
	drainEvents := func() {
		for {
			select {
			case e := <-recorder.Events:
				recorded = append(recorded, e)
			default:
				return
			}
		}
	}

	deployed := false
	for range 20 {
		doReconcile(t, r)
		drainEvents()

		if err := c.Get(ctx, types.NamespacedName{Name: "test-web-server", Namespace: "default"}, &appsv1.Deployment{}); err == nil {
			deployed = true
			break
		}
		jobs := &batchv1.JobList{}
		if err := c.List(ctx, jobs, client.InNamespace("default")); err != nil {
			t.Fatalf("list Jobs: %v", err)
		}
		for i := range jobs.Items {
			if !jobComplete(&jobs.Items[i]) {
				markJobSucceeded(t, c, &jobs.Items[i])
			}
		}
	}
	if !deployed {
		t.Fatal("web-server Deployment was not created; lifecycle did not complete within the reconcile budget")
	}

	got := &supersetv1alpha1.Superset{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, got); err != nil {
		t.Fatalf("get Superset: %v", err)
	}
	return c, got, recorded
}

// assertNoMarkerLeak fails if any marker appears in an owned ConfigMap, the
// JSON-marshalled parent status, or a recorded Event. wantConfigMaps names
// ConfigMaps that must exist so the ConfigMap scan is not vacuous.
func assertNoMarkerLeak(t *testing.T, c client.Client, superset *supersetv1alpha1.Superset, recorded, markers, wantConfigMaps []string) {
	t.Helper()

	cms := &corev1.ConfigMapList{}
	if err := c.List(context.Background(), cms, client.InNamespace("default")); err != nil {
		t.Fatalf("list ConfigMaps: %v", err)
	}
	names := map[string]bool{}
	for _, cm := range cms.Items {
		names[cm.Name] = true
		for key, val := range cm.Data {
			for _, s := range markers {
				if strings.Contains(val, s) {
					t.Errorf("secret leak: ConfigMap %s data[%s] contains marker %q", cm.Name, key, s)
				}
			}
		}
	}
	for _, name := range wantConfigMaps {
		if !names[name] {
			t.Errorf("expected ConfigMap %s to be reconciled; got %v", name, names)
		}
	}

	if superset.Status.Lifecycle == nil {
		t.Error("expected status.lifecycle to be populated")
	}
	status, err := json.Marshal(superset.Status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	for _, s := range markers {
		if strings.Contains(string(status), s) {
			t.Errorf("secret leak: Superset status contains marker %q: %s", s, status)
		}
	}

	if len(recorded) == 0 {
		t.Error("expected reconciliation to record at least one Event")
	}
	for _, e := range recorded {
		for _, s := range markers {
			if strings.Contains(e, s) {
				t.Errorf("secret leak: Event %q contains marker %q", e, s)
			}
		}
	}
}

// assertConfigReferencesEnv asserts the named ConfigMap's superset_config.py
// references each env var.
func assertConfigReferencesEnv(t *testing.T, c client.Client, cmName string, envNames ...string) {
	t.Helper()
	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: cmName, Namespace: "default"}, cm); err != nil {
		t.Fatalf("get ConfigMap %s: %v", cmName, err)
	}
	pyConfig := cm.Data["superset_config.py"]
	for _, want := range envNames {
		if !strings.Contains(pyConfig, want) {
			t.Errorf("expected %s superset_config.py to reference env var %q (os.environ), got:\n%s", cmName, want, pyConfig)
		}
	}
}

// deploymentEnv returns the env vars of the named Deployment's containers, keyed by name.
func deploymentEnv(t *testing.T, c client.Client, name string) map[string]corev1.EnvVar {
	t.Helper()
	deploy := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, deploy); err != nil {
		t.Fatalf("get Deployment %s: %v", name, err)
	}
	env := map[string]corev1.EnvVar{}
	for _, container := range deploy.Spec.Template.Spec.Containers {
		for _, e := range container.Env {
			env[e.Name] = e
		}
	}
	return env
}

// TestManagerRole_GrantsNoSecretsAccessOrWildcards guards the RBAC least-
// privilege claims in docs/reference/security.md ("The operator does not
// request: `*` (wildcard) on any resource or verb; `impersonate` or RBAC
// management permissions; ... Kubernetes Secret read or write permissions").
// It reads the generated ClusterRole — the source-of-truth artifact that is
// actually deployed — so an accidental `+kubebuilder:rbac` marker that broadens
// scope is caught by `make codegen` + this test.
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
			switch verb {
			case "*":
				t.Errorf("rule[%d] uses a wildcard verb '*'; the operator must request explicit verbs only", i)
			case "impersonate", "escalate", "bind":
				t.Errorf("rule[%d] grants the privilege-escalation verb %q; the operator must not request it", i, verb)
			}
		}
		for _, group := range rule.APIGroups {
			switch group {
			case "*":
				t.Errorf("rule[%d] uses a wildcard apiGroup '*'; the operator must request explicit API groups only", i)
			case "rbac.authorization.k8s.io":
				t.Errorf("rule[%d] grants access to rbac.authorization.k8s.io %v; the operator must not manage RBAC", i, rule.Resources)
			}
		}
	}

	// supersets is pinned exactly: patch exists only to consume the supervised
	// upgrade approval annotation, and create/update/delete are never needed.
	var supersetVerbs []string
	for _, rule := range role.Rules {
		if slices.Contains(rule.APIGroups, "superset.apache.org") && slices.Contains(rule.Resources, "supersets") {
			supersetVerbs = append(supersetVerbs, rule.Verbs...)
		}
	}
	sort.Strings(supersetVerbs)
	if want := []string{"get", "list", "patch", "watch"}; !slices.Equal(supersetVerbs, want) {
		t.Errorf("expected supersets verbs %v, got %v", want, supersetVerbs)
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
