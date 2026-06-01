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

var _ = Describe("Superset rendered resources", func() {
	It("renders and removes networking, policy, monitoring, HPA, and PDB resources", func() {
		const crName = "test-rendering"
		DeferCleanup(deleteSuperset, crName)

		cr := fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %s
  namespace: %s
spec:
  image:
    tag: "latest"
  environment: Development
  secretKey: test-secret-key-not-for-production
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  webServer:
    replicas: 2
    autoscaling:
      minReplicas: 1
      maxReplicas: 3
    podDisruptionBudget:
      minAvailable: 1
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
    disabled: true
`, crName, namespace)

		By("applying a Superset CR with optional resources enabled")
		applyYAML(crName, cr)

		webName := crName + "-web-server"
		npName := webName + "-netpol"

		By("verifying autoscaling and disruption resources")
		expectResourceExists("horizontalpodautoscaler", webName, time.Minute)
		expectJSONPath("horizontalpodautoscaler", webName, "{.spec.scaleTargetRef.name}", webName, time.Minute)
		expectJSONPath("horizontalpodautoscaler", webName, "{.spec.minReplicas}", "1", time.Minute)
		expectJSONPath("horizontalpodautoscaler", webName, "{.spec.maxReplicas}", "3", time.Minute)

		expectResourceExists("poddisruptionbudget", webName, time.Minute)
		expectJSONPath("poddisruptionbudget", webName, "{.spec.minAvailable}", "1", time.Minute)

		By("verifying NetworkPolicy rendering")
		expectResourceExists("networkpolicy", npName, time.Minute)
		expectJSONPathContains("networkpolicy", npName,
			"{.spec.podSelector.matchLabels}", "web-server")
		expectJSONPathContains("networkpolicy", npName,
			"{.spec.ingress[*].ports[*].port}", "8088")

		By("verifying Ingress rendering")
		expectResourceExists("ingress", crName, time.Minute)
		expectJSONPath("ingress", crName, "{.spec.ingressClassName}", "e2e", time.Minute)
		expectJSONPath("ingress", crName, "{.spec.rules[0].host}", "superset-e2e.example.com", time.Minute)
		expectJSONPathContains("ingress", crName, "{.spec.rules[0].http.paths[*].path}", "/ws")
		expectJSONPathContains("ingress", crName, "{.spec.rules[0].http.paths[*].path}", "/mcp")
		expectJSONPathContains("ingress", crName, "{.spec.rules[0].http.paths[*].path}", "/flower")

		By("verifying ServiceMonitor rendering")
		expectResourceExists("servicemonitors.monitoring.coreos.com", crName, time.Minute)
		expectJSONPath("servicemonitors.monitoring.coreos.com", crName,
			"{.metadata.labels.release}", "e2e-prometheus", time.Minute)
		expectJSONPath("servicemonitors.monitoring.coreos.com", crName,
			"{.spec.endpoints[0].interval}", "15s", time.Minute)
		expectJSONPath("servicemonitors.monitoring.coreos.com", crName,
			"{.spec.endpoints[0].scrapeTimeout}", "10s", time.Minute)

		By("switching from Ingress to Gateway HTTPRoute")
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

		By("disabling optional resources")
		patchSuperset(crName, "merge", `{
  "spec": {
    "networking": null,
    "monitoring": null,
    "networkPolicy": null,
    "webServer": {
      "autoscaling": null,
      "podDisruptionBudget": null
    }
  }
}`)

		expectResourceGone("httproutes.gateway.networking.k8s.io", crName)
		expectResourceGone("servicemonitors.monitoring.coreos.com", crName)
		expectResourceGone("networkpolicy", npName)
		expectResourceGone("horizontalpodautoscaler", webName)
		expectResourceGone("poddisruptionbudget", webName)

		By("cleaning up")
		deleteSuperset(crName)
		Eventually(func(g Gomega) {
			_, err := runKubectl("get", "superset", crName, "-n", namespace)
			g.Expect(err).To(HaveOccurred())
		}, time.Minute, time.Second).Should(Succeed())
	})
})
