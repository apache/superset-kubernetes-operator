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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

// These specs exhaustively exercise the CRD's CEL (x-kubernetes-validations)
// rules that the original Integration suite did not already cover. CEL
// validation requires a real API server, so it belongs at the integration tier.
// Each spec starts from a CR that is valid except for the single field under
// test, so the rule named in the assertion is the one that fires.

// celValidationNS is the namespace all CEL-validation specs create CRs in.
const celValidationNS = "cel-validation-test"

// secretRef builds a SecretKeySelector for prod-mode secret references.
func secretRef(name, key string) *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: name},
		Key:                  key,
	}
}

// validDevSuperset returns a minimal valid Development-mode CR: inline secrets
// are permitted, lifecycle tasks disabled, no components. A clean baseline that
// satisfies every CEL rule.
func validDevSuperset(name string) *supersetv1alpha1.Superset {
	env := common.EnvironmentDev
	return &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: celValidationNS},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:       supersetv1alpha1.ImageSpec{Tag: "latest"},
			Environment: &env,
			SecretKey:   strPtr("dev-test-key"),
			Metastore:   &supersetv1alpha1.MetastoreSpec{URI: strPtr("postgresql+psycopg2://u:p@host/db")},
			Lifecycle:   &supersetv1alpha1.LifecycleSpec{Disabled: boolPtr(true)},
		},
	}
}

// validProdSuperset returns a minimal valid Production-mode CR: all secrets are
// referenced from Secrets (no inline), lifecycle disabled, no components.
func validProdSuperset(name string) *supersetv1alpha1.Superset {
	env := common.EnvironmentProd
	return &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: celValidationNS},
		Spec: supersetv1alpha1.SupersetSpec{
			Image:         supersetv1alpha1.ImageSpec{Tag: "latest"},
			Environment:   &env,
			SecretKeyFrom: secretRef("app-secret", "secret-key"),
			Metastore:     &supersetv1alpha1.MetastoreSpec{URIFrom: secretRef("db-secret", "uri")},
			Lifecycle:     &supersetv1alpha1.LifecycleSpec{Disabled: boolPtr(true)},
		},
	}
}

// structuredProdMetastore returns a structured metastore valid for prod/staging
// (host + database + username, password via Secret reference).
func structuredProdMetastore() *supersetv1alpha1.MetastoreSpec {
	return &supersetv1alpha1.MetastoreSpec{
		Host:         strPtr("db.example.com"),
		Database:     strPtr("superset"),
		Username:     strPtr("admin"),
		PasswordFrom: secretRef("db-secret", "password"),
	}
}

var _ = Describe("CEL Validation", Ordered, func() {
	BeforeAll(func() {
		nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: celValidationNS}}
		err := k8sClient.Create(ctx, nsObj)
		if err != nil && !errors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	})

	// --- Metastore field constraints ---

	Describe("Metastore", func() {
		It("rejects uri together with uriFrom", func() {
			cr := validDevSuperset("meta-uri-urifrom")
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				URI:     strPtr("postgresql+psycopg2://u:p@host/db"),
				URIFrom: secretRef("db", "uri"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})

		It("rejects metastore password together with passwordFrom", func() {
			cr := validDevSuperset("meta-pw-pwfrom")
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				Host:         strPtr("db.example.com"),
				Database:     strPtr("superset"),
				Username:     strPtr("admin"),
				Password:     strPtr("secret"),
				PasswordFrom: secretRef("db", "password"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})

		It("rejects uriFrom together with structured fields", func() {
			cr := validDevSuperset("meta-urifrom-struct")
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				URIFrom: secretRef("db", "uri"),
				Host:    strPtr("db.example.com"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})

		It("rejects structured fields without host", func() {
			cr := validDevSuperset("meta-no-host")
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				Database: strPtr("superset"),
				Username: strPtr("admin"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("require host to be set"))
		})

		It("rejects host without database and username", func() {
			cr := validDevSuperset("meta-host-only")
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				Host: strPtr("db.example.com"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("requires database and username"))
		})

		It("rejects createDatabase without structured metastore", func() {
			cr := validDevSuperset("meta-createdb-uri")
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				URI:            strPtr("postgresql+psycopg2://u:p@host/db"),
				CreateDatabase: boolPtr(true),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("createDatabase requires structured metastore"))
		})
	})

	// --- Valkey ---

	Describe("Valkey", func() {
		It("rejects valkey password together with passwordFrom", func() {
			cr := validDevSuperset("vk-pw-pwfrom")
			cr.Spec.Valkey = &supersetv1alpha1.ValkeySpec{
				Host:         "valkey",
				Password:     strPtr("secret"),
				PasswordFrom: secretRef("vk", "password"),
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})
	})

	// --- Gunicorn / Celery preset constraints ---

	Describe("Worker presets", func() {
		It("rejects gunicorn threads > 1 with a non-gthread worker class", func() {
			cr := validDevSuperset("gunicorn-threads")
			cr.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{
				Gunicorn: &supersetv1alpha1.GunicornSpec{
					Threads:     int32Ptr(4),
					WorkerClass: strPtr("sync"),
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("workerClass=gthread"))
		})

		It("rejects celery maxTasksPerChild with a non-prefork pool", func() {
			cr := validDevSuperset("celery-maxtasks")
			cr.Spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{
				Celery: &supersetv1alpha1.CeleryWorkerProcessSpec{
					Pool:             strPtr("threads"),
					MaxTasksPerChild: int32Ptr(100),
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxTasksPerChild only applies to pool=prefork"))
		})

		It("rejects celery maxMemoryPerChild with a non-prefork pool", func() {
			cr := validDevSuperset("celery-maxmem")
			cr.Spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{
				Celery: &supersetv1alpha1.CeleryWorkerProcessSpec{
					Pool:              strPtr("gevent"),
					MaxMemoryPerChild: int32Ptr(500000),
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxMemoryPerChild only applies to pool=prefork"))
		})
	})

	// --- Autoscaling / PDB ---

	Describe("Scaling", func() {
		It("rejects autoscaling maxReplicas below minReplicas", func() {
			cr := validDevSuperset("hpa-min-max")
			cr.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
					Autoscaling: &supersetv1alpha1.AutoscalingSpec{
						MinReplicas: int32Ptr(5),
						MaxReplicas: 3,
					},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxReplicas must be >= minReplicas"))
		})

		It("rejects PDB with both minAvailable and maxUnavailable", func() {
			cr := validDevSuperset("pdb-both")
			cr.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{
				ScalableComponentSpec: supersetv1alpha1.ScalableComponentSpec{
					PodDisruptionBudget: &supersetv1alpha1.PDBSpec{
						MinAvailable:   common.Ptr(intstr.FromInt32(1)),
						MaxUnavailable: common.Ptr(intstr.FromInt32(1)),
					},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})
	})

	// --- Networking / Monitoring component requirements ---

	Describe("Networking", func() {
		It("rejects gateway and ingress set together", func() {
			cr := validDevSuperset("net-gw-ing")
			cr.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{}
			cr.Spec.Networking = &supersetv1alpha1.NetworkingSpec{
				Gateway: &supersetv1alpha1.GatewaySpec{
					GatewayRef: gatewayv1.ParentReference{Name: "gw"},
				},
				Ingress: &supersetv1alpha1.IngressSpec{Host: "superset.example.com"},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("gateway and ingress are mutually exclusive"))
		})

		It("rejects ingress without a web server", func() {
			cr := validDevSuperset("net-ing-nows")
			cr.Spec.Networking = &supersetv1alpha1.NetworkingSpec{
				Ingress: &supersetv1alpha1.IngressSpec{Host: "superset.example.com"},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.networking.ingress requires spec.webServer"))
		})

		It("rejects gateway without any routable component", func() {
			cr := validDevSuperset("net-gw-nocomp")
			cr.Spec.Networking = &supersetv1alpha1.NetworkingSpec{
				Gateway: &supersetv1alpha1.GatewaySpec{
					GatewayRef: gatewayv1.ParentReference{Name: "gw"},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("routable service"))
		})

		It("rejects serviceMonitor without a web server", func() {
			cr := validDevSuperset("mon-noweb")
			cr.Spec.Monitoring = &supersetv1alpha1.MonitoringSpec{
				ServiceMonitor: &supersetv1alpha1.ServiceMonitorSpec{},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.monitoring.serviceMonitor requires spec.webServer"))
		})
	})

	// --- Lifecycle clone constraints ---

	Describe("Clone", func() {
		It("rejects clone in Production mode", func() {
			cr := validProdSuperset("clone-prod")
			cr.Spec.Metastore = structuredProdMetastore()
			cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					Source: supersetv1alpha1.CloneSourceSpec{
						Host:         "prod-db",
						Database:     "superset",
						Username:     "readonly",
						PasswordFrom: secretRef("clone-src", "password"),
					},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Development or Staging"))
		})

		It("rejects inline clone source password outside Development", func() {
			staging := common.EnvironmentStaging
			cr := validProdSuperset("clone-staging-pw")
			cr.Spec.Environment = &staging
			cr.Spec.Metastore = structuredProdMetastore()
			cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					Source: supersetv1alpha1.CloneSourceSpec{
						Host:     "prod-db",
						Database: "superset",
						Username: "readonly",
						Password: strPtr("plain"),
					},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("lifecycle.clone.source.password is only allowed when environment is Development"))
		})

		It("rejects clone without a structured metastore", func() {
			cr := validDevSuperset("clone-no-struct")
			// baseline metastore is a plain URI (not structured)
			cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					Source: supersetv1alpha1.CloneSourceSpec{
						Host:     "prod-db",
						Database: "superset",
						Username: "readonly",
						Password: strPtr("plain"),
					},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("requires structured metastore"))
		})

		It("rejects clone source with both password and passwordFrom", func() {
			cr := validDevSuperset("clone-pw-both")
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				Host:     strPtr("db.example.com"),
				Database: strPtr("superset"),
				Username: strPtr("admin"),
			}
			cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					Source: supersetv1alpha1.CloneSourceSpec{
						Host:         "prod-db",
						Database:     "superset",
						Username:     "readonly",
						Password:     strPtr("plain"),
						PasswordFrom: secretRef("clone-src", "password"),
					},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})

		It("rejects clone source with neither password nor passwordFrom", func() {
			cr := validDevSuperset("clone-pw-none")
			cr.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{
				Host:     strPtr("db.example.com"),
				Database: strPtr("superset"),
				Username: strPtr("admin"),
			}
			cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
				Clone: &supersetv1alpha1.CloneTaskSpec{
					Source: supersetv1alpha1.CloneSourceSpec{
						Host:     "prod-db",
						Database: "superset",
						Username: "readonly",
					},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("one of password or passwordFrom must be set"))
		})
	})

	// --- Secret-key rotation constraints ---

	Describe("Rotation", func() {
		It("rejects inline previousSecretKey outside Development", func() {
			cr := validProdSuperset("rotate-prevkey-prod")
			cr.Spec.PreviousSecretKey = strPtr("old-key")
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("previousSecretKey is only allowed when environment is Development"))
		})

		It("rejects previousSecretKey together with previousSecretKeyFrom", func() {
			cr := validDevSuperset("rotate-prevkey-both")
			cr.Spec.PreviousSecretKey = strPtr("old-key")
			cr.Spec.PreviousSecretKeyFrom = secretRef("prev", "key")
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})

		It("rejects rotate task without a previous secret key", func() {
			cr := validDevSuperset("rotate-no-prevkey")
			cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
				Rotate: &supersetv1alpha1.RotateTaskSpec{},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("lifecycle.rotate requires previousSecretKey"))
		})
	})

	// --- Init task (dev-only fields) ---

	Describe("Init", func() {
		It("rejects adminUser outside Development", func() {
			cr := validProdSuperset("init-admin-prod")
			cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
				Init: &supersetv1alpha1.InitTaskSpec{
					AdminUser: &supersetv1alpha1.AdminUserSpec{},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("lifecycle.init.adminUser is only allowed when environment is Development"))
		})

		It("rejects loadExamples outside Development", func() {
			cr := validProdSuperset("init-examples-prod")
			cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
				Init: &supersetv1alpha1.InitTaskSpec{
					LoadExamples: boolPtr(true),
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("lifecycle.init.loadExamples is only allowed when environment is Development"))
		})

		It("rejects init.command together with adminUser", func() {
			cr := validDevSuperset("init-cmd-admin")
			cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
				Init: &supersetv1alpha1.InitTaskSpec{
					BaseTaskSpec: supersetv1alpha1.BaseTaskSpec{Command: []string{"superset", "init"}},
					AdminUser:    &supersetv1alpha1.AdminUserSpec{},
				},
			}
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("init.command is mutually exclusive"))
		})
	})

	// --- Name length / DNS-label limits ---
	//
	// Rule 952 caps the name at 48 chars whenever lifecycle is enabled, which
	// would shadow the looser per-task limits (e.g. rotate's 49). The baseline
	// has lifecycle.disabled=true (exempting 952), so each entry that needs a
	// length above 48 keeps lifecycle disabled to isolate its own rule.

	Describe("Name length", func() {
		It("rejects a name longer than 63 characters", func() {
			cr := validDevSuperset(strings.Repeat("a", 64))
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("at most 63 characters"))
		})

		It("rejects a name that is not a valid DNS label", func() {
			cr := validDevSuperset("Invalid_Name")
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("valid DNS label"))
		})

		DescribeTable("rejects names exceeding the component-specific limit",
			func(name string, mutate func(*supersetv1alpha1.Superset), want string) {
				cr := validDevSuperset(name)
				mutate(cr)
				err := k8sClient.Create(ctx, cr)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(want))
			},
			Entry("webServer (52)", strings.Repeat("a", 53),
				func(s *supersetv1alpha1.Superset) {
					s.Spec.WebServer = &supersetv1alpha1.WebServerComponentSpec{}
				}, "at most 52 characters when webServer is enabled"),
			Entry("celeryWorker (49)", strings.Repeat("a", 50),
				func(s *supersetv1alpha1.Superset) {
					s.Spec.CeleryWorker = &supersetv1alpha1.CeleryWorkerComponentSpec{}
				}, "at most 49 characters when celeryWorker is enabled"),
			Entry("celeryBeat (51)", strings.Repeat("a", 52),
				func(s *supersetv1alpha1.Superset) {
					s.Spec.CeleryBeat = &supersetv1alpha1.CeleryBeatComponentSpec{}
				}, "at most 51 characters when celeryBeat is enabled"),
			Entry("celeryFlower (49)", strings.Repeat("a", 50),
				func(s *supersetv1alpha1.Superset) {
					s.Spec.CeleryFlower = &supersetv1alpha1.CeleryFlowerComponentSpec{}
				}, "at most 49 characters when celeryFlower is enabled"),
			Entry("websocketServer (46)", strings.Repeat("a", 47),
				func(s *supersetv1alpha1.Superset) {
					s.Spec.WebsocketServer = &supersetv1alpha1.WebsocketServerComponentSpec{
						ComponentSpec: supersetv1alpha1.ComponentSpec{
							Image: &supersetv1alpha1.ImageOverrideSpec{Repository: strPtr("example.com/ws")},
						},
					}
				}, "at most 46 characters when websocketServer is enabled"),
			Entry("mcpServer (52)", strings.Repeat("a", 53),
				func(s *supersetv1alpha1.Superset) {
					s.Spec.McpServer = &supersetv1alpha1.McpServerComponentSpec{}
				}, "at most 52 characters when mcpServer is enabled"),
			Entry("maintenancePage (46)", strings.Repeat("a", 47),
				func(s *supersetv1alpha1.Superset) {
					s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
						Disabled:        boolPtr(true),
						MaintenancePage: &supersetv1alpha1.MaintenancePageSpec{},
					}
				}, "at most 46 characters when lifecycle.maintenancePage is enabled"),
			Entry("rotate (49)", strings.Repeat("a", 50),
				func(s *supersetv1alpha1.Superset) {
					s.Spec.PreviousSecretKey = strPtr("old-key")
					s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
						Disabled: boolPtr(true),
						Rotate:   &supersetv1alpha1.RotateTaskSpec{},
					}
				}, "at most 49 characters when lifecycle.rotate is enabled"),
			Entry("lifecycle enabled (48)", strings.Repeat("a", 49),
				func(s *supersetv1alpha1.Superset) {
					s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{}
				}, "at most 48 characters when lifecycle is enabled"),
		)
	})
})
