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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/superset-kubernetes-operator/test/utils"
)

var (
	controllerPodName string
	optionalCRDPaths  []string
)

const podContainerStatusJSONPath = "{range .status.containerStatuses[*]}" +
	"{.name}:{.state.waiting.reason}:{.state.waiting.message}" +
	"{.state.terminated.reason}:{.state.terminated.exitCode}:{.state.terminated.message}" +
	"{end}"

func setupManager() {
	By("creating manager namespace")
	if _, err := utils.Run(exec.Command("kubectl", "get", "ns", namespace)); err != nil {
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to create namespace")
	}

	By("labeling the namespace to enforce the restricted security policy")
	cmd := exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
		"pod-security.kubernetes.io/enforce=restricted")
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

	By("installing CRDs")
	cmd = exec.Command("make", "install")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to install CRDs")

	By("installing optional e2e CRDs")
	ensureOptionalCRD("httproutes.gateway.networking.k8s.io", "test/e2e/testdata/crds/gateway-httproutes.yaml")
	ensureOptionalCRD("servicemonitors.monitoring.coreos.com", "test/e2e/testdata/crds/monitoring-servicemonitors.yaml")

	By("deploying the controller-manager")
	cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectImage))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
}

func teardownManager() {
	By("cleaning up the curl pod for metrics")
	_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace))

	By("cleaning up the metrics ClusterRoleBinding")
	_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found"))

	By("undeploying the controller-manager")
	_, _ = utils.Run(exec.Command("make", "undeploy"))

	By("uninstalling optional e2e CRDs")
	for _, path := range optionalCRDPaths {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", path, "--ignore-not-found"))
	}

	By("uninstalling CRDs")
	_, _ = utils.Run(exec.Command("make", "uninstall"))

	By("removing manager namespace")
	_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", namespace))
}

func ensureOptionalCRD(name, path string) {
	if _, err := utils.Run(exec.Command("kubectl", "get", "crd", name)); err != nil {
		cmd := exec.Command("kubectl", "apply", "-f", path)
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to install optional e2e CRD %s", name)
		optionalCRDPaths = append(optionalCRDPaths, path)
	}

	cmd := exec.Command("kubectl", "wait", "--for=condition=Established", "crd/"+name, "--timeout=60s")
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Optional e2e CRD %s was not established", name)
}

var _ = AfterEach(func() {
	if !CurrentSpecReport().Failed() {
		return
	}
	collectDiagnostics()
})

func collectDiagnostics() {
	podName := controllerPodName
	if podName == "" {
		podName = lookupControllerPodName()
	}

	if podName != "" {
		By("fetching controller manager pod logs")
		cmd := exec.Command("kubectl", "logs", podName, "-n", namespace)
		controllerLogs, err := utils.Run(cmd)
		if err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n%s", controllerLogs)
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get controller logs: %s\n", err)
		}

		By("fetching controller manager pod description")
		cmd = exec.Command("kubectl", "describe", "pod", podName, "-n", namespace)
		podDescription, err := utils.Run(cmd)
		if err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Pod description:\n%s", podDescription)
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to describe controller pod: %s\n", err)
		}
	}

	By("fetching Kubernetes events")
	cmd := exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
	eventsOutput, err := utils.Run(cmd)
	if err == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s\n", err)
	}

	By("fetching curl-metrics logs")
	cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	metricsOutput, err := utils.Run(cmd)
	if err == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n%s", metricsOutput)
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s\n", err)
	}

	By("describing the curl-metrics pod")
	cmd = exec.Command("kubectl", "describe", "pod", "curl-metrics", "-n", namespace)
	metricsPodDescription, err := utils.Run(cmd)
	if err == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "curl-metrics pod description:\n%s", metricsPodDescription)
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to describe curl-metrics pod: %s\n", err)
	}
}

func lookupControllerPodName() string {
	cmd := exec.Command("kubectl", "get",
		"pods", "-l", "control-plane=controller-manager",
		"-o", "go-template={{ range .items }}"+
			"{{ if not .metadata.deletionTimestamp }}"+
			"{{ .metadata.name }}"+
			"{{ \"\\n\" }}{{ end }}{{ end }}",
		"-n", namespace,
	)
	output, err := utils.Run(cmd)
	if err != nil {
		return ""
	}
	podNames := utils.GetNonEmptyLines(output)
	if len(podNames) == 0 {
		return ""
	}
	return podNames[0]
}

func runKubectl(args ...string) (string, error) {
	return utils.Run(exec.Command("kubectl", args...))
}

func applyYAML(name, content string) {
	path := writeYAML(name, content)
	cmd := exec.Command("kubectl", "apply", "-f", path)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to apply %s", name)
}

func serverDryRunYAML(name, content string, strict bool) (string, error) {
	path := writeYAML(name, content)
	args := []string{"apply", "--dry-run=server"}
	if strict {
		args = append(args, "--validate=strict")
	}
	args = append(args, "-f", path)
	return runKubectl(args...)
}

func writeYAML(name, content string) string {
	path := filepath.Join(os.TempDir(), "superset-operator-e2e-"+name+".yaml")
	ExpectWithOffset(1, os.WriteFile(path, []byte(content), 0o644)).To(Succeed())
	return path
}

func jsonPath(resource, name, path string) (string, error) {
	return runKubectl("get", resource, name, "-n", namespace, "-o", "jsonpath="+path)
}

func expectJSONPath(resource, name, path, want string, timeout time.Duration) {
	EventuallyWithOffset(1, func(g Gomega) {
		output, err := jsonPath(resource, name, path)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(output)).To(Equal(want))
	}, timeout, time.Second).Should(Succeed())
}

func expectJSONPathContains(resource, name, path, want string) {
	EventuallyWithOffset(1, func(g Gomega) {
		output, err := jsonPath(resource, name, path)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(ContainSubstring(want))
	}, time.Minute, time.Second).Should(Succeed())
}

func expectResourceExists(resource, name string, timeout time.Duration) {
	EventuallyWithOffset(1, func(g Gomega) {
		_, err := runKubectl("get", resource, name, "-n", namespace)
		g.Expect(err).NotTo(HaveOccurred())
	}, timeout, time.Second).Should(Succeed())
}

func expectResourceGone(resource, name string) {
	EventuallyWithOffset(1, func(g Gomega) {
		_, err := runKubectl("get", resource, name, "-n", namespace)
		g.Expect(err).To(HaveOccurred())
	}, time.Minute, time.Second).Should(Succeed())
}

func patchSuperset(name, patchType, patch string) {
	_, err := runKubectl("patch", "superset", name, "-n", namespace, "--type", patchType, "-p", patch)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to patch Superset %s", name)
}

func deleteSuperset(name string) {
	_, _ = runKubectl("delete", "superset", name, "-n", namespace, "--ignore-not-found", "--timeout=60s")
}

func deleteSecret(name string) {
	_, _ = runKubectl("delete", "secret", name, "-n", namespace, "--ignore-not-found")
}

func lifecycleImageYAML(indent string) string {
	repo, tag := splitImageRef(curlImage)
	return fmt.Sprintf("%simage:\n%s  repository: %s\n%s  tag: %s\n%s  pullPolicy: IfNotPresent\n",
		indent, indent, repo, indent, tag, indent)
}

func describePodStatus(name string) string {
	output, err := runKubectl("get", "pod", name, "-n", namespace,
		"-o", "jsonpath="+podContainerStatusJSONPath)
	if err != nil {
		return err.Error()
	}
	return strings.TrimSpace(output)
}

func splitImageRef(image string) (string, string) {
	taggedImage := image
	if before, digest, ok := strings.Cut(image, "@sha256:"); ok {
		if digest == "" {
			Fail(fmt.Sprintf("image %q must include a non-empty sha256 digest", image))
		}
		taggedImage = before
	}

	lastSlash := strings.LastIndex(taggedImage, "/")
	lastColon := strings.LastIndex(taggedImage, ":")
	if lastColon > lastSlash {
		return taggedImage[:lastColon], image[lastColon+1:]
	}

	Fail(fmt.Sprintf("image %q must include an explicit tag, optionally with a sha256 digest", image))
	return "", ""
}

// restrictedLifecyclePodTemplateYAML renders a restricted-PSS lifecycle
// podTemplate indented for placement under spec.lifecycle.
func restrictedLifecyclePodTemplateYAML() string {
	return `    podTemplate:
      podSecurityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      container:
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL
          runAsNonRoot: true
          runAsUser: 1000
`
}
