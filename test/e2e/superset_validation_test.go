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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Superset CRD validation", func() {
	// Inline-secret rejection is exhaustively covered at the integration tier
	// (internal/controller/cel_validation_test.go, "rejects inline secrets
	// outside Development"). Here we keep a single representative smoke case to
	// confirm the CEL rules are compiled into the CRD as actually installed and
	// enforced by a real cluster's admission path.
	It("rejects an inline secretKey outside Development", func() {
		cr := fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: invalid-inline-secretkey
  namespace: %s
spec:
  image:
    tag: "latest"
  environment: Production
  secretKey: plain-text-key
  metastore:
    uriFrom:
      name: db-secret
      key: uri
  lifecycle:
    disabled: true
`, namespace)

		output, err := serverDryRunYAML("invalid-inline-secretkey", cr, false)
		Expect(err).To(HaveOccurred())
		Expect(output).To(ContainSubstring("secretKey is only allowed"))
	})

	It("rejects namespace fields on SecretKeySelector references", func() {
		cr := fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: invalid-cross-namespace-secret
  namespace: %s
spec:
  image:
    tag: "latest"
  environment: Production
  secretKeyFrom:
    name: app-secret
    namespace: other-namespace
    key: secret-key
  metastore:
    uriFrom:
      name: db-secret
      key: uri
  lifecycle:
    disabled: true
`, namespace)

		output, err := serverDryRunYAML("invalid-cross-namespace-secret", cr, true)
		Expect(err).To(HaveOccurred())
		Expect(output).To(ContainSubstring("namespace"))
	})
})
