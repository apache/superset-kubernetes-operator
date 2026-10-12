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

// Package samples verifies that the Development-mode sample manifests in
// config/samples run against the backing services in dev-dependencies.yaml.
package samples

import (
	"bufio"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

const (
	samplesDir = "../../config/samples"

	// Runtime defaults the operator applies when a sample omits the port.
	defaultPostgresPort int32 = 5432
	defaultValkeyPort   int32 = 6379
)

// endpoint is the connection tuple a sample must agree on with dev-dependencies.yaml.
type endpoint struct {
	host     string
	port     int32
	user     string
	password string
	database string
}

type devDependencies struct {
	postgres endpoint
	valkey   endpoint
}

func TestDevSamplesMatchDevDependencies(t *testing.T) {
	deps := loadDevDependencies(t)

	files, err := filepath.Glob(filepath.Join(samplesDir, "superset_v1alpha1_*.yaml"))
	require.NoError(t, err)

	checked := 0
	for _, file := range files {
		s := loadSuperset(t, file)
		if s.Spec.Environment == nil || *s.Spec.Environment != common.EnvironmentDev {
			continue
		}
		checked++
		t.Run(filepath.Base(file), func(t *testing.T) {
			// Lean Superset images ship no database drivers; the -dev variant includes psycopg2.
			assert.True(t, strings.HasSuffix(s.Spec.Image.Tag, "-dev"),
				"image tag %q is not a -dev variant, so migrations against PostgreSQL would fail", s.Spec.Image.Tag)

			require.NotNil(t, s.Spec.Metastore, "spec.metastore")
			assert.Equal(t, deps.postgres, metastoreEndpoint(t, s.Spec.Metastore), "spec.metastore")
			if s.Spec.Valkey != nil {
				assert.Equal(t, deps.valkey, valkeyEndpoint(s.Spec.Valkey), "spec.valkey")
			}
		})
	}
	assert.NotZero(t, checked, "no Development-mode samples found in %s", samplesDir)
}

// loadDevDependencies identifies the PostgreSQL and Valkey Deployments by how
// they are configured and resolves each one's host from the Service selecting it.
func loadDevDependencies(t *testing.T) devDependencies {
	t.Helper()
	var services []corev1.Service
	var deployments []appsv1.Deployment
	for _, doc := range readDocuments(t, filepath.Join(samplesDir, "dev-dependencies.yaml")) {
		var obj struct {
			Kind string `json:"kind"`
		}
		require.NoError(t, yaml.Unmarshal(doc, &obj))
		switch obj.Kind {
		case "Service":
			var svc corev1.Service
			require.NoError(t, yaml.Unmarshal(doc, &svc))
			services = append(services, svc)
		case "Deployment":
			var dep appsv1.Deployment
			require.NoError(t, yaml.Unmarshal(doc, &dep))
			deployments = append(deployments, dep)
		}
	}

	var deps devDependencies
	for _, dep := range deployments {
		c := dep.Spec.Template.Spec.Containers[0]
		env := map[string]string{}
		for _, e := range c.Env {
			env[e.Name] = e.Value
		}
		switch {
		case env["POSTGRES_PASSWORD"] != "":
			deps.postgres = serviceEndpoint(t, services, dep)
			deps.postgres.user = env["POSTGRES_USER"]
			deps.postgres.password = env["POSTGRES_PASSWORD"]
			deps.postgres.database = env["POSTGRES_DB"]
		case slices.Contains(c.Command, "--requirepass"):
			deps.valkey = serviceEndpoint(t, services, dep)
			deps.valkey.password = c.Command[slices.Index(c.Command, "--requirepass")+1]
		}
	}
	require.NotEmpty(t, deps.postgres.host, "no PostgreSQL Deployment in dev-dependencies.yaml")
	require.NotEmpty(t, deps.valkey.host, "no password-protected Valkey Deployment in dev-dependencies.yaml")
	return deps
}

func serviceEndpoint(t *testing.T, services []corev1.Service, dep appsv1.Deployment) endpoint {
	t.Helper()
	podLabels := labels.Set(dep.Spec.Template.Labels)
	for _, svc := range services {
		if len(svc.Spec.Selector) > 0 && labels.SelectorFromSet(svc.Spec.Selector).Matches(podLabels) {
			return endpoint{host: svc.Name, port: svc.Spec.Ports[0].Port}
		}
	}
	t.Fatalf("no Service in dev-dependencies.yaml selects Deployment %q", dep.Name)
	return endpoint{}
}

func metastoreEndpoint(t *testing.T, m *v1alpha1.MetastoreSpec) endpoint {
	t.Helper()
	if m.URI == nil {
		assert.Equal(t, "PostgreSQL", deref(m.Type, "PostgreSQL"), "spec.metastore.type")
		return endpoint{
			host:     deref(m.Host, ""),
			port:     deref(m.Port, defaultPostgresPort),
			user:     deref(m.Username, ""),
			password: deref(m.Password, ""),
			database: deref(m.Database, ""),
		}
	}
	u, err := url.Parse(*m.URI)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(u.Scheme, "postgresql"), "spec.metastore.uri scheme %q", u.Scheme)
	port := defaultPostgresPort
	if u.Port() != "" {
		p, err := strconv.ParseInt(u.Port(), 10, 32)
		require.NoError(t, err)
		port = int32(p)
	}
	password, _ := u.User.Password()
	return endpoint{
		host:     u.Hostname(),
		port:     port,
		user:     u.User.Username(),
		password: password,
		database: strings.TrimPrefix(u.Path, "/"),
	}
}

func valkeyEndpoint(v *v1alpha1.ValkeySpec) endpoint {
	return endpoint{
		host:     v.Host,
		port:     deref(v.Port, defaultValkeyPort),
		user:     deref(v.Username, ""),
		password: deref(v.Password, ""),
	}
}

func loadSuperset(t *testing.T, path string) v1alpha1.Superset {
	t.Helper()
	for _, doc := range readDocuments(t, path) {
		var s v1alpha1.Superset
		require.NoError(t, yaml.Unmarshal(doc, &s))
		if s.Kind == "Superset" {
			return s
		}
	}
	t.Fatalf("%s contains no Superset", path)
	return v1alpha1.Superset{}
}

func readDocuments(t *testing.T, path string) [][]byte {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	reader := utilyaml.NewYAMLReader(bufio.NewReader(f))
	var docs [][]byte
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return docs
		}
		require.NoError(t, err)
		docs = append(docs, doc)
	}
}

func deref[T any](p *T, fallback T) T {
	if p == nil {
		return fallback
	}
	return *p
}
