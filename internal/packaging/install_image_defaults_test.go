/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package packaging

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const operatorImage = "ghcr.io/apache/superset-kubernetes-operator"

func TestInstallImageDefaults(t *testing.T) {
	t.Run("helm values default to chart appVersion", func(t *testing.T) {
		var values struct {
			Image struct {
				Repository string `yaml:"repository"`
				Tag        string `yaml:"tag"`
			} `yaml:"image"`
		}

		readYAML(t, "charts/superset-operator/values.yaml", &values)

		if values.Image.Repository != operatorImage {
			t.Fatalf("image.repository = %q, want %q", values.Image.Repository, operatorImage)
		}
		if values.Image.Tag != "" {
			t.Fatalf("image.tag = %q, want empty so Helm defaults to .Chart.AppVersion; do not pin the operator image in values.yaml", values.Image.Tag)
		}
	})

	t.Run("kustomize manager image placeholder remains unpinned", func(t *testing.T) {
		var kustomization struct {
			Images []struct {
				Name    string `yaml:"name"`
				NewName string `yaml:"newName"`
				NewTag  string `yaml:"newTag"`
			} `yaml:"images"`
		}

		readYAML(t, "config/manager/kustomization.yaml", &kustomization)

		var found bool
		for _, image := range kustomization.Images {
			if image.Name != operatorImage {
				continue
			}

			found = true
			if image.NewName != operatorImage {
				t.Fatalf("newName for %q = %q, want %q", operatorImage, image.NewName, operatorImage)
			}
			if image.NewTag != "latest" {
				t.Fatalf("newTag for %q = %q, want latest; make targets rewrite this placeholder from IMG", operatorImage, image.NewTag)
			}
			if strings.Contains(image.NewTag, "@sha256:") {
				t.Fatalf("newTag for %q contains a digest pin: %q", operatorImage, image.NewTag)
			}
		}

		if !found {
			t.Fatalf("config/manager/kustomization.yaml does not contain an image entry for %q", operatorImage)
		}
	})

	t.Run("manager deployment image placeholder remains unpinned", func(t *testing.T) {
		deployment := readYAMLDocumentKind(t, "config/manager/manager.yaml", "Deployment")

		const managerContainer = "manager"
		const wantImage = operatorImage + ":latest"
		podSpec := requireNestedMap(t, deployment, "spec", "template", "spec")
		containers := requireSlice(t, podSpec["containers"], "spec.template.spec.containers")
		for _, item := range containers {
			container := requireMap(t, item, "spec.template.spec.containers[]")
			name := requireString(t, container["name"], "spec.template.spec.containers[].name")
			if name != managerContainer {
				continue
			}

			image := requireString(t, container["image"], "spec.template.spec.containers[].image")
			if image != wantImage {
				t.Fatalf("manager container image = %q, want %q; make targets rewrite this placeholder from IMG", image, wantImage)
			}
			if strings.Contains(image, "@sha256:") {
				t.Fatalf("manager container image contains a digest pin: %q", image)
			}
			return
		}

		t.Fatalf("config/manager/manager.yaml does not contain container %q", managerContainer)
	})
}

func readYAMLDocumentKind(t *testing.T, relPath, kind string) map[string]any {
	t.Helper()

	for _, doc := range readYAMLDocuments(t, relPath) {
		if doc["kind"] == kind {
			return doc
		}
	}

	t.Fatalf("%s does not contain a %s document", relPath, kind)
	return nil
}

func readYAMLDocuments(t *testing.T, relPath string) []map[string]any {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(repoRoot(t), relPath))
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var docs []map[string]any
	for {
		var doc map[string]any
		err := decoder.Decode(&doc)
		if err == nil {
			if len(doc) > 0 {
				docs = append(docs, doc)
			}
			continue
		}
		if err == io.EOF {
			return docs
		}
		t.Fatalf("parse %s: %v", relPath, err)
	}
}

func requireNestedMap(t *testing.T, value map[string]any, path ...string) map[string]any {
	t.Helper()

	var current any = value
	currentPath := ""
	for _, key := range path {
		if currentPath == "" {
			currentPath = key
		} else {
			currentPath += "." + key
		}

		m := requireMap(t, current, currentPath)
		current = m[key]
	}

	return requireMap(t, current, currentPath)
}

func requireMap(t *testing.T, value any, path string) map[string]any {
	t.Helper()

	m, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s = %T, want YAML mapping", path, value)
	}
	return m
}

func requireSlice(t *testing.T, value any, path string) []any {
	t.Helper()

	s, ok := value.([]any)
	if !ok {
		t.Fatalf("%s = %T, want YAML sequence", path, value)
	}
	return s
}

func requireString(t *testing.T, value any, path string) string {
	t.Helper()

	s, ok := value.(string)
	if !ok {
		t.Fatalf("%s = %T, want string", path, value)
	}
	return s
}

func readYAML(t *testing.T, relPath string, out any) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(repoRoot(t), relPath))
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}

	if err := yaml.Unmarshal(data, out); err != nil {
		t.Fatalf("parse %s: %v", relPath, err)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(wd, "go.mod")); err == nil {
			return wd
		}

		parent := filepath.Dir(wd)
		if parent == wd {
			t.Fatal("could not find repository root containing go.mod")
		}
		wd = parent
	}
}
