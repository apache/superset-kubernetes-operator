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

var _ = Describe("Superset deletion", func() {
	It("removes parent-owned resources without deleting referenced Secrets", func() {
		const crName = "test-delete"
		secretName := crName + "-user-secret"
		DeferCleanup(deleteSecret, secretName)

		cr := fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %[1]s-user-secret
  namespace: %[2]s
stringData:
  secret-key: test-secret-key-not-for-production
---
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  image:
    tag: "latest"
  environment: Development
  secretKeyFrom:
    name: %[1]s-user-secret
    key: secret-key
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  webServer:
    autoscaling:
      minReplicas: 1
      maxReplicas: 2
    podDisruptionBudget:
      maxUnavailable: 1
  networking:
    gateway:
      gatewayRef:
        name: e2e-gateway
      hostnames:
      - superset-delete.example.com
  monitoring:
    serviceMonitor: {}
  networkPolicy: {}
  lifecycle:
    disabled: true
`, crName, namespace)

		By("applying a Superset CR with several parent-owned resources")
		applyYAML(crName, cr)

		webName := crName + "-web-server"
		ownedResources := []struct {
			resource string
			name     string
		}{
			{"serviceaccount", crName},
			{"deployment", webName},
			{"service", webName},
			{"configmap", webName + "-config"},
			{"horizontalpodautoscaler", webName},
			{"poddisruptionbudget", webName},
			{"networkpolicy", webName + "-netpol"},
			{"httproutes.gateway.networking.k8s.io", crName},
			{"servicemonitors.monitoring.coreos.com", crName},
		}
		for _, resource := range ownedResources {
			expectResourceExists(resource.resource, resource.name, time.Minute)
		}
		expectResourceExists("secret", secretName, time.Minute)

		By("deleting the Superset CR")
		_, err := runKubectl("delete", "superset", crName, "-n", namespace, "--timeout=60s")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the CR does not get stuck terminating")
		expectResourceGone("superset", crName)

		By("verifying parent-owned resources are garbage-collected")
		for _, resource := range ownedResources {
			expectResourceGone(resource.resource, resource.name)
		}

		By("verifying referenced Secrets are preserved")
		expectResourceExists("secret", secretName, time.Minute)
	})
})
