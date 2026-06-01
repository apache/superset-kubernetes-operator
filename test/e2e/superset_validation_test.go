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
	DescribeTable("rejects inline secrets outside Development",
		func(name, spec, want string) {
			cr := fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %s
  namespace: %s
spec:
%s
`, name, namespace, spec)

			output, err := serverDryRunYAML(name, cr, false)
			Expect(err).To(HaveOccurred())
			Expect(output).To(ContainSubstring(want))
		},
		Entry("secretKey", "invalid-inline-secretkey", `  image:
    tag: "latest"
  environment: Production
  secretKey: plain-text-key
  metastore:
    uriFrom:
      name: db-secret
      key: uri
  lifecycle:
    disabled: true
`, "secretKey is only allowed"),
		Entry("metastore.uri", "invalid-inline-db-uri", `  image:
    tag: "latest"
  environment: Production
  secretKeyFrom:
    name: app-secret
    key: secret-key
  metastore:
    uri: postgresql+psycopg2://u:p@postgres:5432/superset
  lifecycle:
    disabled: true
`, "metastore.uri is only allowed"),
		Entry("metastore.password", "invalid-inline-db-password", `  image:
    tag: "latest"
  environment: Production
  secretKeyFrom:
    name: app-secret
    key: secret-key
  metastore:
    host: postgres
    database: superset
    username: superset
    password: plain-text-password
  lifecycle:
    disabled: true
`, "metastore.password is only allowed"),
		Entry("valkey.password", "invalid-inline-valkey-password", `  image:
    tag: "latest"
  environment: Production
  secretKeyFrom:
    name: app-secret
    key: secret-key
  metastore:
    uriFrom:
      name: db-secret
      key: uri
  valkey:
    host: valkey
    password: plain-text-password
  lifecycle:
    disabled: true
`, "valkey.password is only allowed"),
	)

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
