//go:build integration

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

package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

var _ = Describe("Integration", Ordered, func() {
	const (
		timeout  = 10 * time.Second
		interval = 250 * time.Millisecond
	)

	// --- helpers ---

	devEnv := "dev"

	newSuperset := func(name, ns string) *supersetv1alpha1.Superset {
		return &supersetv1alpha1.Superset{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: supersetv1alpha1.SupersetSpec{
				Image:       supersetv1alpha1.ImageSpec{Tag: "latest"},
				Environment: &devEnv,
				SecretKey:   strPtr("dev-test-key"),
				Metastore:   &supersetv1alpha1.MetastoreSpec{URI: strPtr("postgresql+psycopg2://u:p@host/db")},
				Init:        &supersetv1alpha1.InitSpec{Disabled: boolPtr(true)},
			},
		}
	}

	createNamespace := func(name string) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
		err := k8sClient.Create(ctx, ns)
		if err != nil && !errors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	}

	// --- CRD Schema Validation ---

	Describe("CRD Schema Validation", func() {
		const ns = "crd-schema-test"

		BeforeAll(func() {
			createNamespace(ns)
		})

		It("should reject invalid environment enum value", func() {
			cr := newSuperset("env-enum", ns)
			invalid := "staging"
			cr.Spec.Environment = &invalid
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("environment"))
		})

		It("should reject autoscaling maxReplicas above maximum", func() {
			cr := newSuperset("hpa-max", ns)
			cr.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
					Autoscaling: &supersetv1alpha1.AutoscalingSpec{
						MinReplicas: common.Ptr(int32(1)),
						MaxReplicas: 101,
					},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxReplicas"))
		})

		It("should reject invalid metastore type enum value", func() {
			cr := newSuperset("type-enum", ns)
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				Host: strPtr("db.example.com"),
				Type: strPtr("sqlite"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("type"))
		})
	})

	// --- CRD Validation (CEL + kubebuilder defaults) ---

	Describe("CRD Validation", func() {
		const ns = "validation-test"

		BeforeAll(func() {
			createNamespace(ns)
		})

		It("should reject prod-mode CR with inline secretKey", func() {
			cr := newSuperset("prod-reject", ns)
			cr.Spec.Environment = nil // defaults to prod via kubebuilder default
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("secretKey"))
		})

		It("should reject prod-mode CR with inline metastore.uri", func() {
			cr := newSuperset("prod-reject-uri", ns)
			cr.Spec.Environment = nil
			cr.Spec.SecretKey = nil
			cr.Spec.SecretKeyFrom = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "s"},
				Key:                  "k",
			}
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				URI: strPtr("postgresql+psycopg2://u:p@host/db"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("metastore.uri"))
		})

		It("should reject prod-mode CR with inline metastore.password", func() {
			cr := newSuperset("prod-reject-pw", ns)
			cr.Spec.Environment = nil
			cr.Spec.SecretKey = nil
			cr.Spec.SecretKeyFrom = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "s"},
				Key:                  "k",
			}
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				Host:     strPtr("db.example.com"),
				Database: strPtr("superset"),
				Username: strPtr("admin"),
				Password: strPtr("secret"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("metastore.password"))
		})

		It("should reject prod-mode CR with inline valkey.password", func() {
			cr := newSuperset("prod-reject-vk", ns)
			cr.Spec.Environment = nil
			cr.Spec.SecretKey = nil
			cr.Spec.SecretKeyFrom = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "s"},
				Key:                  "k",
			}
			cr.Spec.Valkey = &supersetv1alpha1.ValkeySpec{
				Host:     "valkey",
				Password: strPtr("secret"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("valkey.password"))
		})

		It("should allow dev-mode CR with all inline secrets", func() {
			cr := newSuperset("dev-allow", ns)
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				URI: strPtr("postgresql+psycopg2://u:p@host/db"),
			}
			cr.Spec.Valkey = &supersetv1alpha1.ValkeySpec{
				Host:     "valkey",
				Password: strPtr("dev-pass"),
			}
			cr.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())

			// cleanup
			Expect(k8sClient.Delete(ctx, cr)).To(Succeed())
		})

		It("should apply kubebuilder defaults (repository, pullPolicy, environment)", func() {
			cr := newSuperset("mutate-defaults", ns)
			cr.Spec.Image.Repository = ""
			cr.Spec.Image.PullPolicy = ""
			cr.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())

			fetched := &supersetv1alpha1.Superset{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: "mutate-defaults", Namespace: ns}, fetched)
			}).Should(Succeed())
			Expect(fetched.Spec.Image.Repository).To(Equal("apachesuperset.docker.scarf.sh/apache/superset"))
			Expect(fetched.Spec.Image.PullPolicy).To(Equal(corev1.PullPolicy("IfNotPresent")))

			// cleanup
			Expect(k8sClient.Delete(ctx, cr)).To(Succeed())
		})

		It("should reject CR missing both secretKey and secretKeyFrom", func() {
			cr := newSuperset("no-secret", ns)
			cr.Spec.SecretKey = nil
			cr.Spec.SecretKeyFrom = nil
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("secretKey"))
		})

		It("should reject mutually exclusive metastore fields", func() {
			cr := newSuperset("meta-exclusive", ns)
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				URI:  strPtr("postgresql+psycopg2://u:p@host/db"),
				Host: strPtr("host"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})
	})

	// --- Reconciliation Lifecycle ---

	Describe("Reconciliation Lifecycle", func() {
		const ns = "reconcile-test"

		BeforeAll(func() {
			createNamespace(ns)
		})

		It("should create child CRs when parent Superset CR is created", func() {
			cr := newSuperset("lifecycle", ns)
			cr.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{}
			cr.Spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())

			By("waiting for the SupersetWebServer child CR to be created")
			webServer := &supersetv1alpha1.SupersetWebServer{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name: "lifecycle", Namespace: ns,
				}, webServer)
			}, timeout, interval).Should(Succeed())

			By("verifying the child CR has rendered config")
			Expect(webServer.Spec.Config).To(ContainSubstring("import os"))
			Expect(webServer.Spec.Config).To(ContainSubstring("SUPERSET_WEBSERVER_PORT"))

			By("waiting for the SupersetCeleryWorker child CR to be created")
			worker := &supersetv1alpha1.SupersetCeleryWorker{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name: "lifecycle", Namespace: ns,
				}, worker)
			}, timeout, interval).Should(Succeed())

			By("verifying ConfigMaps are created")
			cm := &corev1.ConfigMap{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name: "lifecycle-web-server-config", Namespace: ns,
				}, cm)
			}, timeout, interval).Should(Succeed())
			Expect(cm.Data).To(HaveKey("superset_config.py"))
		})

		It("should delete child CR when component is removed from parent", func() {
			cr := &supersetv1alpha1.Superset{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lifecycle", Namespace: ns}, cr)).To(Succeed())

			By("removing the celery worker component")
			cr.Spec.CeleryWorker = nil
			Expect(k8sClient.Update(ctx, cr)).To(Succeed())

			By("waiting for the CeleryWorker child CR to be deleted")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name: "lifecycle", Namespace: ns,
				}, &supersetv1alpha1.SupersetCeleryWorker{})
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())
		})

		It("should update child CR config when parent config changes", func() {
			cr := &supersetv1alpha1.Superset{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lifecycle", Namespace: ns}, cr)).To(Succeed())

			webServer := &supersetv1alpha1.SupersetWebServer{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "lifecycle", Namespace: ns,
			}, webServer)).To(Succeed())
			oldChecksum := webServer.Spec.ConfigChecksum

			By("adding user config to the parent CR")
			userConfig := "CUSTOM_FLAG = True"
			cr.Spec.Config = &userConfig
			Expect(k8sClient.Update(ctx, cr)).To(Succeed())

			By("waiting for the child CR config to include the user config")
			Eventually(func() string {
				ws := &supersetv1alpha1.SupersetWebServer{}
				if err := k8sClient.Get(ctx, types.NamespacedName{
					Name: "lifecycle", Namespace: ns,
				}, ws); err != nil {
					return ""
				}
				return ws.Spec.Config
			}, timeout, interval).Should(ContainSubstring("CUSTOM_FLAG"))

			By("verifying the checksum changed")
			Eventually(func() string {
				ws := &supersetv1alpha1.SupersetWebServer{}
				if err := k8sClient.Get(ctx, types.NamespacedName{
					Name: "lifecycle", Namespace: ns,
				}, ws); err != nil {
					return oldChecksum
				}
				return ws.Spec.ConfigChecksum
			}, timeout, interval).ShouldNot(Equal(oldChecksum))
		})

		It("should set status phase on the parent CR", func() {
			Eventually(func() string {
				cr := &supersetv1alpha1.Superset{}
				if err := k8sClient.Get(ctx, types.NamespacedName{
					Name: "lifecycle", Namespace: ns,
				}, cr); err != nil {
					return ""
				}
				return cr.Status.Phase
			}, timeout, interval).ShouldNot(BeEmpty())
		})

		AfterAll(func() {
			cr := &supersetv1alpha1.Superset{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lifecycle", Namespace: ns}, cr); err == nil {
				Expect(k8sClient.Delete(ctx, cr)).To(Succeed())
			}
		})
	})
})
