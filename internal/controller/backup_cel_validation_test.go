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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// volumeBackup returns a minimal valid Volume-destination backup task.
func volumeBackupSpec() *supersetv1alpha1.BackupTaskSpec {
	return &supersetv1alpha1.BackupTaskSpec{
		Destination: supersetv1alpha1.BackupDestinationSpec{
			Type:   "Volume",
			Volume: &supersetv1alpha1.VolumeBackupSpec{ClaimName: "backups"},
		},
	}
}

var _ = Describe("CEL Validation: backup and restore", Ordered, func() {
	BeforeAll(func() {
		nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: celValidationNS}}
		err := k8sClient.Create(ctx, nsObj)
		if err != nil && !errors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("accepts a backup with a structured metastore and a volume destination", func() {
		cr := validProdSuperset("backup-valid")
		cr.Spec.Metastore = structuredProdMetastore()
		cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{Backup: volumeBackupSpec()}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
	})

	It("accepts a restore paired with a backup", func() {
		cr := validProdSuperset("restore-valid")
		cr.Spec.Metastore = structuredProdMetastore()
		cr.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
			Backup:  volumeBackupSpec(),
			Restore: &supersetv1alpha1.RestoreTaskSpec{Source: supersetv1alpha1.RestoreSourceSpec{Type: "Latest"}},
		}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
	})

	DescribeTable("rejects invalid backup/restore configuration",
		func(name string, mutate func(*supersetv1alpha1.Superset), want string) {
			cr := validProdSuperset(name)
			cr.Spec.Metastore = structuredProdMetastore()
			mutate(cr)
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(want))
		},
		Entry("backup without structured metastore", "backup-no-host",
			func(s *supersetv1alpha1.Superset) {
				s.Spec.Metastore = &supersetv1alpha1.MetastoreSpec{URIFrom: secretRef("db-secret", "uri")}
				s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{Backup: volumeBackupSpec()}
			}, "lifecycle.backup requires structured metastore"),
		Entry("destination type Volume without volume field", "backup-vol-missing",
			func(s *supersetv1alpha1.Superset) {
				s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{Backup: &supersetv1alpha1.BackupTaskSpec{
					Destination: supersetv1alpha1.BackupDestinationSpec{Type: "Volume"},
				}}
			}, "destination.volume is required"),
		Entry("destination with both volume and objectStore", "backup-both-dests",
			func(s *supersetv1alpha1.Superset) {
				s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{Backup: &supersetv1alpha1.BackupTaskSpec{
					Destination: supersetv1alpha1.BackupDestinationSpec{
						Type:        "Volume",
						Volume:      &supersetv1alpha1.VolumeBackupSpec{ClaimName: "backups"},
						ObjectStore: &supersetv1alpha1.ObjectStoreBackupSpec{URL: "s3://bucket/x"},
					},
				}}
			}, "volume and objectStore are mutually exclusive"),
		Entry("restore without a backup configured", "restore-no-backup",
			func(s *supersetv1alpha1.Superset) {
				s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
					Restore: &supersetv1alpha1.RestoreTaskSpec{Source: supersetv1alpha1.RestoreSourceSpec{Type: "Latest"}},
				}
			}, "lifecycle.restore requires lifecycle.backup"),
		Entry("restore source BackupID without backupID", "restore-no-id",
			func(s *supersetv1alpha1.Superset) {
				s.Spec.Lifecycle = &supersetv1alpha1.LifecycleSpec{
					Backup:  volumeBackupSpec(),
					Restore: &supersetv1alpha1.RestoreTaskSpec{Source: supersetv1alpha1.RestoreSourceSpec{Type: "BackupID"}},
				}
			}, "source.backupID is required"),
	)
})
