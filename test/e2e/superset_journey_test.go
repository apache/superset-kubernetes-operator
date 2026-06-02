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

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This is the suite's single comprehensive deployment journey. Standing up the
// operator and a full Superset is the expensive part of an e2e run, so rather
// than deploy a fresh CR per concern we deploy ONE rich CR in BeforeAll and walk
// it through its lifecycle with many focused checkpoints (components, services,
// rendered config, optional resources, networking, supervised upgrade, deletion).
// Using an Ordered container with per-checkpoint It blocks keeps the costly
// deploy amortized while preserving granular, independently-named assertions.
//
// Assertions are reconciliation/status-level (resources rendered, status fields,
// owner-ref GC) — they do not wait on pod readiness, which never settles in Kind
// without a real Superset database and images.
//
// Narrower, fixture-specific scenarios (failed-migration recovery, secret-key
// rotation, CEL validation) stay in their own specs — they need distinct CRs and
// would only add coupling here.
var _ = Describe("Superset deployment lifecycle", Ordered, func() {
	const crName = "test-journey"
	const secretName = crName + "-secret"
	const webName = crName + "-web-server"
	const npName = webName + "-netpol"

	BeforeAll(func() {
		DeferCleanup(func() {
			deleteSuperset(crName)
			deleteSecret(secretName)
		})

		// A comprehensive CR: every deployment component, a referenced secret,
		// per-component config + replica overrides, autoscaling/PDB, networking,
		// monitoring, a NetworkPolicy, and supervised upgrades. The image starts
		// at a semver tag so the later upgrade is detected as an Upgrade.
		cr := fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %[2]s
  namespace: %[3]s
stringData:
  secret-key: test-secret-key-not-for-production
---
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %[1]s
  namespace: %[3]s
spec:
  image:
    repository: apache/superset
    tag: "1.0.0"
  environment: Development
  secretKeyFrom:
    name: %[2]s
    key: secret-key
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  featureFlags:
    ENABLE_TEMPLATE_PROCESSING: true
  webServer:
    autoscaling:
      minReplicas: 1
      maxReplicas: 3
    podDisruptionBudget:
      minAvailable: 1
  celeryWorker:
    replicas: 2
    config: |
      CELERY_ANNOTATIONS = {}
  celeryBeat: {}
  celeryFlower: {}
  websocketServer:
    image:
      repository: oneacrefund/superset-websocket
      tag: latest
  mcpServer: {}
  networking:
    ingress:
      className: e2e
      host: superset-e2e.example.com
      labels:
        e2e.apache.org/case: rendering
  monitoring:
    serviceMonitor:
      interval: 15s
      scrapeTimeout: 10s
      labels:
        release: e2e-prometheus
  networkPolicy:
    extraIngress:
    - from:
      - namespaceSelector:
          matchLabels:
            e2e.apache.org/network: allowed
  lifecycle:
    upgradeMode: Supervised
    migrate:
      disabled: true
    init:
      disabled: true
`, crName, secretName, namespace)

		By("applying a comprehensive Superset CR")
		applyYAML(crName, cr)

		By("waiting for the initial install to settle (supervised, no enabled tasks)")
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", 2*time.Minute)
		expectJSONPath("superset", crName, "{.status.lastLifecycleImage}", "apache/superset:1.0.0", time.Minute)
	})

	It("reconciles every component into a Deployment", func() {
		componentDeployments := []string{
			crName + "-web-server",
			crName + "-celery-worker",
			crName + "-celery-beat",
			crName + "-celery-flower",
			crName + "-websocket-server",
			crName + "-mcp-server",
		}
		for _, deploy := range componentDeployments {
			expectResourceExists("deployment", deploy, time.Minute)
		}

		By("verifying parent status reports the web-server Deployment")
		expectJSONPath("superset", crName,
			"{.status.components.webServer.resources[?(@.kind=='Deployment')].name}",
			webName, time.Minute)
	})

	It("reports per-component replica overrides in status", func() {
		expectJSONPath("superset", crName,
			"{.status.components.celeryWorker.replicas}", "2", time.Minute)
	})

	It("creates Services only for user-facing components", func() {
		for _, svc := range []string{
			crName + "-web-server",
			crName + "-celery-flower",
			crName + "-websocket-server",
			crName + "-mcp-server",
		} {
			expectResourceExists("service", svc, time.Minute)
		}

		By("verifying internal components have no Service")
		Consistently(func(g Gomega) {
			for _, svc := range []string{crName + "-celery-worker", crName + "-celery-beat"} {
				_, err := runKubectl("get", "service", svc, "-n", namespace)
				g.Expect(err).To(HaveOccurred(), "Service %s should NOT exist", svc)
			}
		}, 5*time.Second, time.Second).Should(Succeed())
	})

	It("renders per-component Python config and skips the Node.js websocket", func() {
		By("verifying the web-server config is operator-generated")
		expectJSONPathContains("configmap", crName+"-web-server-config",
			"{.data.superset_config\\.py}", "import os")
		expectJSONPathContains("configmap", crName+"-web-server-config",
			"{.data.superset_config\\.py}", "SUPERSET_WEBSERVER_PORT")

		By("verifying the celery-worker config merges top-level and component config")
		expectJSONPathContains("configmap", crName+"-celery-worker-config",
			"{.data.superset_config\\.py}", "FEATURE_FLAGS")
		expectJSONPathContains("configmap", crName+"-celery-worker-config",
			"{.data.superset_config\\.py}", "CELERY_ANNOTATIONS")

		By("verifying every Python component has a ConfigMap")
		for _, cm := range []string{
			crName + "-web-server-config",
			crName + "-celery-worker-config",
			crName + "-celery-beat-config",
			crName + "-celery-flower-config",
			crName + "-mcp-server-config",
		} {
			expectResourceExists("configmap", cm, time.Minute)
		}

		By("verifying the Node.js websocket server has no ConfigMap")
		Consistently(func(g Gomega) {
			_, err := runKubectl("get", "configmap", crName+"-websocket-server-config", "-n", namespace)
			g.Expect(err).To(HaveOccurred(), "WebsocketServer should have no ConfigMap")
		}, 5*time.Second, time.Second).Should(Succeed())
	})

	It("renders autoscaling and disruption resources for the web server", func() {
		expectResourceExists("horizontalpodautoscaler", webName, time.Minute)
		expectJSONPath("horizontalpodautoscaler", webName, "{.spec.scaleTargetRef.name}", webName, time.Minute)
		expectJSONPath("horizontalpodautoscaler", webName, "{.spec.minReplicas}", "1", time.Minute)
		expectJSONPath("horizontalpodautoscaler", webName, "{.spec.maxReplicas}", "3", time.Minute)

		expectResourceExists("poddisruptionbudget", webName, time.Minute)
		expectJSONPath("poddisruptionbudget", webName, "{.spec.minAvailable}", "1", time.Minute)
	})

	It("renders a NetworkPolicy scoped to the instance", func() {
		expectResourceExists("networkpolicy", npName, time.Minute)
		expectJSONPathContains("networkpolicy", npName, "{.spec.podSelector.matchLabels}", "web-server")
		expectJSONPathContains("networkpolicy", npName, "{.spec.ingress[*].ports[*].port}", "8088")
	})

	It("renders a ServiceMonitor for the web server", func() {
		expectResourceExists("servicemonitors.monitoring.coreos.com", crName, time.Minute)
		expectJSONPath("servicemonitors.monitoring.coreos.com", crName,
			"{.metadata.labels.release}", "e2e-prometheus", time.Minute)
		expectJSONPath("servicemonitors.monitoring.coreos.com", crName,
			"{.spec.endpoints[0].interval}", "15s", time.Minute)
		expectJSONPath("servicemonitors.monitoring.coreos.com", crName,
			"{.spec.endpoints[0].scrapeTimeout}", "10s", time.Minute)
	})

	It("renders an Ingress, then switches to a Gateway HTTPRoute", func() {
		By("verifying Ingress rendering")
		expectResourceExists("ingress", crName, time.Minute)
		expectJSONPath("ingress", crName, "{.spec.ingressClassName}", "e2e", time.Minute)
		expectJSONPath("ingress", crName, "{.spec.rules[0].host}", "superset-e2e.example.com", time.Minute)
		for _, path := range []string{"/ws", "/mcp", "/flower"} {
			expectJSONPathContains("ingress", crName, "{.spec.rules[0].http.paths[*].path}", path)
		}

		By("switching networking from Ingress to Gateway")
		patchSuperset(crName, "json", `[
  {"op":"remove","path":"/spec/networking/ingress"},
  {"op":"add","path":"/spec/networking/gateway","value":{
    "gatewayRef":{"name":"e2e-gateway"},
    "hostnames":["superset-e2e.example.com"],
    "labels":{"e2e.apache.org/case":"gateway"}
  }}
]`)

		expectResourceGone("ingress", crName)
		expectResourceExists("httproutes.gateway.networking.k8s.io", crName, time.Minute)
		expectJSONPath("httproutes.gateway.networking.k8s.io", crName,
			"{.spec.parentRefs[0].name}", "e2e-gateway", time.Minute)
		expectJSONPath("httproutes.gateway.networking.k8s.io", crName,
			"{.spec.hostnames[0]}", "superset-e2e.example.com", time.Minute)
		expectJSONPathContains("httproutes.gateway.networking.k8s.io", crName,
			"{.spec.rules[*].matches[*].path.value}", "/ws")
	})

	It("detects an image change and gates the supervised upgrade", func() {
		By("patching the image tag")
		patchSuperset(crName, "merge", `{"spec":{"image":{"tag":"1.1.0"}}}`)

		By("verifying the upgrade is awaiting approval")
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "AwaitingApproval", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.upgrade.fromVersion}", "1.0.0", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.upgrade.toVersion}", "1.1.0", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.upgrade.direction}", "Upgrade", time.Minute)

		By("approving the supervised upgrade with the recorded token")
		token, err := jsonPath("superset", crName, "{.status.lifecycle.upgrade.approvalToken}")
		Expect(err).NotTo(HaveOccurred())
		Expect(token).NotTo(BeEmpty())
		_, err = runKubectl("annotate", "superset", crName, "-n", namespace,
			"superset.apache.org/approve-upgrade="+token, "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the upgrade settles and the consumed annotation is removed")
		expectJSONPath("superset", crName, "{.status.lastLifecycleImage}", "apache/superset:1.1.0", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", time.Minute)
		Eventually(func(g Gomega) {
			output, err := jsonPath("superset", crName, "{.metadata.annotations}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).NotTo(ContainSubstring("superset.apache.org/approve-upgrade"))
		}, time.Minute, time.Second).Should(Succeed())
	})

	It("garbage-collects parent-owned resources on delete but preserves referenced Secrets", func() {
		ownedResources := []struct{ resource, name string }{
			{"serviceaccount", crName},
			{"deployment", webName},
			{"service", webName},
			{"configmap", webName + "-config"},
			{"horizontalpodautoscaler", webName},
			{"poddisruptionbudget", webName},
			{"networkpolicy", npName},
			{"httproutes.gateway.networking.k8s.io", crName},
			{"servicemonitors.monitoring.coreos.com", crName},
		}

		By("confirming the owned resources and referenced Secret exist before deletion")
		for _, r := range ownedResources {
			expectResourceExists(r.resource, r.name, time.Minute)
		}
		expectResourceExists("secret", secretName, time.Minute)

		By("deleting the Superset CR")
		_, err := runKubectl("delete", "superset", crName, "-n", namespace, "--timeout=60s")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the CR does not get stuck terminating")
		expectResourceGone("superset", crName)

		By("verifying parent-owned resources are garbage-collected")
		for _, r := range ownedResources {
			expectResourceGone(r.resource, r.name)
		}

		By("verifying the referenced Secret is preserved")
		expectResourceExists("secret", secretName, time.Minute)
	})
})
