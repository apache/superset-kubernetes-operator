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

var _ = Describe("Superset lifecycle", func() {
	It("recovers a failed migration after the task spec is corrected", func() {
		const crName = "test-migrate-recovery"
		DeferCleanup(deleteSuperset, crName)

		cr := fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %s
  namespace: %s
spec:
  image:
    repository: apache/superset
    tag: "5.0.0"
  environment: Development
  secretKey: test-secret-key-not-for-production
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  lifecycle:
%s%s    migrate:
      command:
      - /bin/sh
      - -c
      - exit 1
      requiresDrain: false
      maxRetries: 1
      timeout: 30s
    init:
      disabled: true
`, crName, namespace, lifecycleImageYAML("    "), restrictedLifecyclePodTemplateYAML("    "))

		By("applying a Superset CR with a failing migration")
		applyYAML(crName, cr)

		By("waiting for the migrate task to become terminally failed")
		expectJSONPath("superset", crName, "{.status.lifecycle.migrate.state}", "Failed", 3*time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.migrate.attempts}", "1", time.Minute)
		expectJSONPath("superset", crName,
			"{.status.conditions[?(@.type=='LifecycleComplete')].reason}",
			"TaskFailed", time.Minute)

		By("patching the migrate command to succeed")
		patchSuperset(crName, "merge", `{
  "spec": {
    "lifecycle": {
      "migrate": {
        "command": ["/bin/sh", "-c", "exit 0"]
      }
    }
  }
}`)

		By("verifying the lifecycle recovers and completes")
		expectJSONPath("superset", crName, "{.status.lifecycle.migrate.state}", "Complete", 3*time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", time.Minute)
		expectJSONPath("superset", crName, "{.status.conditions[?(@.type=='LifecycleComplete')].status}", "True", time.Minute)
	})

	It("runs the rotate task with current and previous secret keys from Secrets", func() {
		const crName = "test-rotate-secrets"
		secrets := []string{crName + "-current", crName + "-previous", crName + "-db"}
		DeferCleanup(func() {
			deleteSuperset(crName)
			for _, secret := range secrets {
				deleteSecret(secret)
			}
		})

		cr := fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %[1]s-current
  namespace: %[2]s
stringData:
  secret-key: current-key
---
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s-previous
  namespace: %[2]s
stringData:
  secret-key: previous-key
---
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s-db
  namespace: %[2]s
stringData:
  uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
---
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  image:
    repository: apache/superset
    tag: "5.0.0"
  environment: Production
  secretKeyFrom:
    name: %[1]s-current
    key: secret-key
  previousSecretKeyFrom:
    name: %[1]s-previous
    key: secret-key
  metastore:
    uriFrom:
      name: %[1]s-db
      key: uri
  lifecycle:
%[3]s%[4]s    podRetention:
      policy: Retain
    migrate:
      disabled: true
    rotate:
      command:
      - /bin/sh
      - -c
      - 'test -n "$SUPERSET_OPERATOR__SECRET_KEY" && test -n "$SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY"'
      requiresDrain: false
      maxRetries: 1
      timeout: 30s
    init:
      disabled: true
`, crName, namespace, lifecycleImageYAML("    "), restrictedLifecyclePodTemplateYAML("    "))

		By("applying a Superset CR with secret-key rotation enabled")
		applyYAML(crName, cr)

		rotateJob := crName + "-rotate"
		expectResourceExists("job", rotateJob, 2*time.Minute)

		By("verifying the rotate Job receives both key references")
		expectJSONPath("job", rotateJob,
			"{.spec.template.spec.containers[0].env[?(@.name=='SUPERSET_OPERATOR__SECRET_KEY')].valueFrom.secretKeyRef.name}",
			crName+"-current", time.Minute)
		expectJSONPath("job", rotateJob,
			"{.spec.template.spec.containers[0].env"+
				"[?(@.name=='SUPERSET_OPERATOR__PREVIOUS_SECRET_KEY')].valueFrom.secretKeyRef.name}",
			crName+"-previous", time.Minute)

		By("verifying the rotate task completes")
		expectJSONPath("superset", crName, "{.status.lifecycle.rotate.state}", "Complete", 3*time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", time.Minute)
	})

	It("detects image changes and gates supervised upgrades", func() {
		const crName = "test-image-change"
		DeferCleanup(deleteSuperset, crName)

		cr := fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %s
  namespace: %s
spec:
  image:
    repository: apache/superset
    tag: "1.0.0"
  environment: Development
  secretKey: test-secret-key-not-for-production
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  lifecycle:
    upgradeMode: Supervised
    migrate:
      disabled: true
    init:
      disabled: true
`, crName, namespace)

		By("applying a Superset CR with no enabled lifecycle tasks")
		applyYAML(crName, cr)
		expectJSONPath("superset", crName, "{.status.lastLifecycleImage}", "apache/superset:1.0.0", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", time.Minute)

		By("patching the image tag")
		patchSuperset(crName, "merge", `{"spec":{"image":{"tag":"1.1.0"}}}`)

		By("verifying the upgrade is awaiting approval")
		expectJSONPath("superset", crName, "{.status.phase}", "AwaitingApproval", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "AwaitingApproval", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.upgrade.fromVersion}", "1.0.0", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.upgrade.toVersion}", "1.1.0", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.upgrade.direction}", "Upgrade", time.Minute)

		By("approving the supervised upgrade")
		_, err := runKubectl("annotate", "superset", crName, "-n", namespace,
			"superset.apache.org/approve-upgrade=true", "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the image change is settled")
		expectJSONPath("superset", crName, "{.status.lastLifecycleImage}", "apache/superset:1.1.0", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", time.Minute)
		Eventually(func(g Gomega) {
			output, err := jsonPath("superset", crName, "{.metadata.annotations}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).NotTo(ContainSubstring("superset.apache.org/approve-upgrade"))
		}, time.Minute, time.Second).Should(Succeed())
	})
})
