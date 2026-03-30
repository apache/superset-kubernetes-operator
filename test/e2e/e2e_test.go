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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/superset-kubernetes-operator/test/utils"
)

// namespace where the project is deployed in
const namespace = "superset-operator-system"

// serviceAccountName created for the project
const serviceAccountName = "superset-kubernetes-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "superset-kubernetes-operator-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "superset-kubernetes-operator-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=superset-kubernetes-operator-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("waiting for the metrics endpoint to be ready")
			verifyMetricsEndpointReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpoints", metricsServiceName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("8443"), "Metrics endpoint is not ready")
			}
			Eventually(verifyMetricsEndpointReady).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("controller-runtime.metrics\tServing metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted).Should(Succeed())

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image="+curlImage,
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "%s",
							"command": ["/bin/sh", "-c"],
							"args": ["curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics"],
							"securityContext": {
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccount": "%s"
					}
				}`, curlImage, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			metricsOutput := getMetricsOutput()
			Expect(metricsOutput).To(ContainSubstring(
				"controller_runtime_reconcile_total",
			))
		})

		It("should reconcile a minimal dev-mode Superset CR", func() {
			const crName = "test-simple"

			By("applying a minimal dev-mode Superset CR")
			cr := `apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: test-simple
  namespace: superset-operator-system
spec:
  image:
    tag: "latest"
  environment: dev
  secretKey: test-secret-key-not-for-production
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  webServer: {}
  init:
    disabled: true
`
			crFile := filepath.Join("/tmp", "test-simple-superset.yaml")
			Expect(os.WriteFile(crFile, []byte(cr), 0644)).To(Succeed())
			cmd := exec.Command("kubectl", "apply", "-f", crFile)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply Superset CR")

			By("verifying the Superset CR exists and has a status phase")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "superset", crName,
					"-n", namespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).NotTo(BeEmpty(), "status.phase should be set")
			}, 60*time.Second, time.Second).Should(Succeed())

			By("verifying SupersetWebServer child CR exists")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "supersetwebserver",
					crName, "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}, 30*time.Second, time.Second).Should(Succeed())

			By("verifying the child CR config contains operator-generated content")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "supersetwebserver",
					crName, "-n", namespace,
					"-o", "jsonpath={.spec.config}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).NotTo(BeEmpty(), "spec.config should not be empty")
				g.Expect(output).To(ContainSubstring("import os"))
				g.Expect(output).To(ContainSubstring("SUPERSET_WEBSERVER_PORT"))
			}, 30*time.Second, time.Second).Should(Succeed())

			By("verifying Deployment exists for web server")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "deployment",
					crName+"-web-server", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}, 30*time.Second, time.Second).Should(Succeed())

			By("verifying Service exists for web server")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "service",
					crName+"-web-server", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}, 30*time.Second, time.Second).Should(Succeed())

			By("verifying ConfigMap exists for web server")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "configmap",
					crName+"-web-server-config", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}, 30*time.Second, time.Second).Should(Succeed())

			By("cleaning up: deleting the Superset CR")
			cmd = exec.Command("kubectl", "delete", "superset", crName, "-n", namespace,
				"--timeout=60s")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete Superset CR")

			By("verifying cascade deletion removes the web server child CR")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "supersetwebserver",
					crName, "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred(), "SupersetWebServer should be deleted")
			}, 60*time.Second, time.Second).Should(Succeed())

			By("verifying cascade deletion removes the Deployment")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "deployment",
					crName+"-web-server", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred(), "Deployment should be deleted")
			}, 60*time.Second, time.Second).Should(Succeed())
		})

		It("should reconcile a multi-component Superset CR with all components", func() {
			const crName = "test-full"

			By("applying a multi-component Superset CR")
			cr := `apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: test-full
  namespace: superset-operator-system
spec:
  image:
    tag: "latest"
  environment: dev
  secretKey: test-secret-key
  metastore:
    uri: postgresql+psycopg2://superset:superset@postgres:5432/superset
  config: |
    FEATURE_FLAGS = {"DASHBOARD_RBAC": True}
  webServer:
    replicas: 2
  celeryWorker:
    config: |
      CELERY_ANNOTATIONS = {}
  celeryBeat: {}
  celeryFlower: {}
  websocketServer: {}
  mcpServer: {}
  init:
    disabled: true
`
			crFile := filepath.Join("/tmp", "test-full-superset.yaml")
			Expect(os.WriteFile(crFile, []byte(cr), 0644)).To(Succeed())
			cmd := exec.Command("kubectl", "apply", "-f", crFile)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply multi-component Superset CR")

			By("verifying all 6 child CRs exist")
			childKinds := []string{
				"supersetwebserver",
				"supersetceleryworker",
				"supersetcelerybeat",
				"supersetceleryflower",
				"supersetwebsocketserver",
				"supersetmcpserver",
			}
			Eventually(func(g Gomega) {
				for _, kind := range childKinds {
					cmd := exec.Command("kubectl", "get", kind, crName, "-n", namespace)
					_, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred(), "child CR %s/%s should exist", kind, crName)
				}
			}, 60*time.Second, time.Second).Should(Succeed())

			By("verifying web server child CR has replicas=2")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "supersetwebserver",
					crName, "-n", namespace,
					"-o", "jsonpath={.spec.replicas}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("2"))
			}, 30*time.Second, time.Second).Should(Succeed())

			By("verifying WebsocketServer child has empty config (no Python config for Node.js)")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "supersetwebsocketserver",
					crName, "-n", namespace,
					"-o", "jsonpath={.spec.config}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(output)).To(BeEmpty(),
					"WebsocketServer should have no Python config")
			}, 30*time.Second, time.Second).Should(Succeed())

			By("verifying CeleryWorker config contains both base and component config")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "supersetceleryworker",
					crName, "-n", namespace,
					"-o", "jsonpath={.spec.config}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("FEATURE_FLAGS"),
					"CeleryWorker config should contain base FEATURE_FLAGS")
				g.Expect(output).To(ContainSubstring("CELERY_ANNOTATIONS"),
					"CeleryWorker config should contain component CELERY_ANNOTATIONS")
			}, 30*time.Second, time.Second).Should(Succeed())

			By("verifying ConfigMaps exist for all Python components but NOT for WebsocketServer")
			pythonConfigMaps := []string{
				crName + "-web-server-config",
				crName + "-celery-worker-config",
				crName + "-celery-beat-config",
				crName + "-celery-flower-config",
				crName + "-mcp-server-config",
			}
			Eventually(func(g Gomega) {
				for _, cm := range pythonConfigMaps {
					cmd := exec.Command("kubectl", "get", "configmap", cm, "-n", namespace)
					_, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred(), "ConfigMap %s should exist", cm)
				}
			}, 30*time.Second, time.Second).Should(Succeed())

			// WebsocketServer should NOT have a ConfigMap
			Consistently(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "configmap",
					crName+"-websocket-server-config", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred(),
					"WebsocketServer ConfigMap should NOT exist")
			}, 5*time.Second, time.Second).Should(Succeed())

			By("verifying Services exist for web-server, celery-flower, websocket-server, mcp-server")
			serviceComponents := []string{
				crName + "-web-server",
				crName + "-celery-flower",
				crName + "-websocket-server",
				crName + "-mcp-server",
			}
			Eventually(func(g Gomega) {
				for _, svc := range serviceComponents {
					cmd := exec.Command("kubectl", "get", "service", svc, "-n", namespace)
					_, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred(), "Service %s should exist", svc)
				}
			}, 30*time.Second, time.Second).Should(Succeed())

			By("verifying Services do NOT exist for celery-worker and celery-beat")
			noServiceComponents := []string{
				crName + "-celery-worker",
				crName + "-celery-beat",
			}
			Consistently(func(g Gomega) {
				for _, svc := range noServiceComponents {
					cmd := exec.Command("kubectl", "get", "service", svc, "-n", namespace)
					_, err := utils.Run(cmd)
					g.Expect(err).To(HaveOccurred(), "Service %s should NOT exist", svc)
				}
			}, 5*time.Second, time.Second).Should(Succeed())

			By("cleaning up: deleting the Superset CR")
			cmd = exec.Command("kubectl", "delete", "superset", crName, "-n", namespace,
				"--timeout=60s")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete multi-component Superset CR")

			By("verifying cascade deletion removes child CRs")
			Eventually(func(g Gomega) {
				for _, kind := range childKinds {
					cmd := exec.Command("kubectl", "get", kind, crName, "-n", namespace)
					_, err := utils.Run(cmd)
					g.Expect(err).To(HaveOccurred(), "child CR %s/%s should be deleted", kind, crName)
				}
			}, 60*time.Second, time.Second).Should(Succeed())
		})
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	// Temporary file to store the token request
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		// Execute kubectl command to create the token
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		// Parse the JSON output to extract the token
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() string {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	metricsOutput, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
	Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
	return metricsOutput
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
