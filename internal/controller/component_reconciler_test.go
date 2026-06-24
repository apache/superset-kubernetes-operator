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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

func TestPreserveServiceAllocatedFields(t *testing.T) {
	t.Parallel()

	familyPolicy := corev1.IPFamilyPolicySingleStack
	tests := []struct {
		name     string
		desired  corev1.ServiceSpec
		existing corev1.ServiceSpec
		want     corev1.ServiceSpec
	}{
		{
			name: "preserves cluster allocation fields",
			desired: corev1.ServiceSpec{
				Type:  corev1.ServiceTypeClusterIP,
				Ports: []corev1.ServicePort{{Port: 9090}},
			},
			existing: corev1.ServiceSpec{
				ClusterIP:      "10.0.0.12",
				ClusterIPs:     []string{"10.0.0.12"},
				IPFamilies:     []corev1.IPFamily{corev1.IPv4Protocol},
				IPFamilyPolicy: &familyPolicy,
			},
			want: corev1.ServiceSpec{
				Type:           corev1.ServiceTypeClusterIP,
				Ports:          []corev1.ServicePort{{Port: 9090}},
				ClusterIP:      "10.0.0.12",
				ClusterIPs:     []string{"10.0.0.12"},
				IPFamilies:     []corev1.IPFamily{corev1.IPv4Protocol},
				IPFamilyPolicy: &familyPolicy,
			},
		},
		{
			name: "preserves health check node port",
			desired: corev1.ServiceSpec{
				Type: corev1.ServiceTypeLoadBalancer,
			},
			existing: corev1.ServiceSpec{
				HealthCheckNodePort: 31000,
			},
			want: corev1.ServiceSpec{
				Type:                corev1.ServiceTypeLoadBalancer,
				HealthCheckNodePort: 31000,
			},
		},
		{
			name: "clears cluster allocation fields for ExternalName",
			desired: corev1.ServiceSpec{
				Type:         corev1.ServiceTypeExternalName,
				ExternalName: "superset.example.com",
			},
			existing: corev1.ServiceSpec{
				ClusterIP:      "10.0.0.5",
				ClusterIPs:     []string{"10.0.0.5"},
				IPFamilies:     []corev1.IPFamily{corev1.IPv4Protocol},
				IPFamilyPolicy: &familyPolicy,
			},
			want: corev1.ServiceSpec{
				Type:         corev1.ServiceTypeExternalName,
				ExternalName: "superset.example.com",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preserveServiceAllocatedFields(&tt.desired, tt.existing)
			assert.Equal(t, tt.want, tt.desired)
		})
	}
}

func TestReconcileComponentResources_NoServiceNoScaling(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	owner := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()
	recorder := events.NewFakeRecorder(20)

	one := int32(1)
	spec := &supersetv1alpha1.FlatComponentSpec{
		Image:    supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
		Replicas: &one,
	}
	// CeleryBeat: defaultPort=0 (no Service), hasScaling=false.
	cfg, ok := componentResourceConfig(common.ComponentCeleryBeat)
	require.True(t, ok)

	err := reconcileComponentResources(ctx, c, scheme, recorder, owner, spec, cfg, "", nil, nil, nil)
	require.NoError(t, err)

	resourceBaseName := common.ResourceBaseName("test", common.ComponentCeleryBeat)
	// Deployment created.
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: resourceBaseName, Namespace: "default"}, &appsv1.Deployment{}))
	// No Service created (defaultPort 0).
	err = c.Get(ctx, client.ObjectKey{Name: resourceBaseName, Namespace: "default"}, &corev1.Service{})
	assert.True(t, errors.IsNotFound(err), "celery-beat should not create a Service")
}

func TestReconcileComponentService_PreservesClusterIPAcrossUpdate(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	owner := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()

	resourceBaseName := common.ResourceBaseName("test", common.ComponentCeleryFlower)
	componentName := string(common.ComponentCeleryFlower)

	// First reconcile creates the Service.
	require.NoError(t, reconcileComponentService(ctx, c, scheme, owner, nil, componentName, common.PortCeleryFlower, common.PortCeleryFlower, resourceBaseName))

	// Simulate the API server allocating a ClusterIP.
	svc := &corev1.Service{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: resourceBaseName, Namespace: "default"}, svc))
	svc.Spec.ClusterIP = "10.20.30.40"
	svc.Spec.ClusterIPs = []string{"10.20.30.40"}
	require.NoError(t, c.Update(ctx, svc))

	// Second reconcile must preserve the allocated ClusterIP.
	require.NoError(t, reconcileComponentService(ctx, c, scheme, owner, nil, componentName, common.PortCeleryFlower, common.PortCeleryFlower, resourceBaseName))

	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: resourceBaseName, Namespace: "default"}, svc))
	assert.Equal(t, "10.20.30.40", svc.Spec.ClusterIP)
	// Selector is the operator-managed component label set.
	assert.Equal(t, string(common.ComponentCeleryFlower), svc.Spec.Selector[common.LabelKeyComponent])
	// Owned by the parent.
	assert.True(t, isOwnedBy(svc, owner))
}

func TestReconcileScaling_NoAutoscalingOrPDB(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	owner := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()

	resourceBaseName := common.ResourceBaseName("test", common.ComponentWebServer)
	// With nil autoscaling/pdb, reconcileScaling should be a clean no-op (it
	// reconciles "absent" HPA/PDB which means deleting any that exist).
	err := reconcileScaling(ctx, c, scheme, owner, nil, nil, string(common.ComponentWebServer), resourceBaseName)
	assert.NoError(t, err)
}

func TestReconcileComponentResources_CreatesDeploymentAndService(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	owner := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()
	recorder := events.NewFakeRecorder(20)

	one := int32(1)
	spec := &supersetv1alpha1.FlatComponentSpec{
		Image:    supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
		Replicas: &one,
	}
	cfg, ok := componentResourceConfig(common.ComponentCeleryFlower)
	require.True(t, ok)

	err := reconcileComponentResources(ctx, c, scheme, recorder, owner, spec, cfg, "cfg-sum", nil, nil, nil)
	require.NoError(t, err)

	// Deployment created.
	resourceBaseName := common.ResourceBaseName("test", common.ComponentCeleryFlower)
	deploy := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: resourceBaseName, Namespace: "default"}, deploy))
	assert.True(t, isOwnedBy(deploy, owner))
	// Config checksum stamped as a pod annotation.
	assert.Equal(t, "cfg-sum", deploy.Spec.Template.Annotations[common.AnnotationConfigChecksum])

	// Service created (flower has defaultPort > 0).
	svc := &corev1.Service{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: resourceBaseName, Namespace: "default"}, svc))
	assert.Equal(t, common.PortCeleryFlower, svc.Spec.Ports[0].Port)
}

func TestPreserveDeploymentReplicasWhenUnmanaged(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		desiredReplicas  *int32
		existingReplicas *int32
		wantReplicas     *int32
	}{
		{
			name:             "unmanaged desired preserves existing replicas",
			desiredReplicas:  nil,
			existingReplicas: int32Ptr(3),
			wantReplicas:     int32Ptr(3),
		},
		{
			name:             "unmanaged desired remains nil on create path",
			desiredReplicas:  nil,
			existingReplicas: nil,
			wantReplicas:     nil,
		},
		{
			name:             "managed desired ignores existing drift",
			desiredReplicas:  int32Ptr(2),
			existingReplicas: int32Ptr(3),
			wantReplicas:     int32Ptr(2),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desired := appsv1.DeploymentSpec{Replicas: tt.desiredReplicas}
			existing := appsv1.DeploymentSpec{Replicas: tt.existingReplicas}

			preserveDeploymentReplicasWhenUnmanaged(&desired, existing)

			if tt.wantReplicas == nil {
				assert.Nil(t, desired.Replicas)
				return
			}
			require.NotNil(t, desired.Replicas)
			assert.Equal(t, *tt.wantReplicas, *desired.Replicas)
		})
	}
}

func TestReconcileComponentDeployment_PreservesReplicasWhenAutoscaling(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	owner := &supersetv1alpha1.Superset{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()
	recorder := events.NewFakeRecorder(20)

	one := int32(1)
	minReplicas := int32(2)
	autoscaling := &supersetv1alpha1.AutoscalingSpec{
		MinReplicas: &minReplicas,
		MaxReplicas: 5,
	}
	spec := &supersetv1alpha1.FlatComponentSpec{
		Image:       supersetv1alpha1.ImageSpec{Repository: "apache/superset", Tag: "latest"},
		Replicas:    &one,
		Autoscaling: autoscaling,
	}
	cfg, ok := componentResourceConfig(common.ComponentWebServer)
	require.True(t, ok)

	resourceBaseName := common.ResourceBaseName("test", common.ComponentWebServer)
	require.NoError(t, reconcileComponentResources(ctx, c, scheme, recorder, owner, spec, cfg, "", nil, autoscaling, nil))

	deploy := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: resourceBaseName, Namespace: "default"}, deploy))

	// Simulate the HPA scaling the target Deployment above the CR's replica value.
	three := int32(3)
	deploy.Spec.Replicas = &three
	require.NoError(t, c.Update(ctx, deploy))

	require.NoError(t, reconcileComponentResources(ctx, c, scheme, recorder, owner, spec, cfg, "", nil, autoscaling, nil))

	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: resourceBaseName, Namespace: "default"}, deploy))
	require.NotNil(t, deploy.Spec.Replicas)
	assert.Equal(t, int32(3), *deploy.Spec.Replicas)
}

func TestDeleteByLabels_KeepsNamedAndDeletesRest(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	labels := map[string]string{"app": "x"}

	keep := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "keep", Namespace: "default", Labels: labels}}
	drop := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "drop", Namespace: "default", Labels: labels}}
	unrelated := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default", Labels: map[string]string{"app": "y"}}}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(keep, drop, unrelated).Build()

	err := deleteByLabels(ctx, c, "default", labels, func() client.ObjectList { return &corev1.ServiceList{} }, "keep")
	require.NoError(t, err)

	// "keep" remains.
	assert.NoError(t, c.Get(ctx, client.ObjectKey{Name: "keep", Namespace: "default"}, &corev1.Service{}))
	// "drop" deleted.
	err = c.Get(ctx, client.ObjectKey{Name: "drop", Namespace: "default"}, &corev1.Service{})
	assert.True(t, errors.IsNotFound(err))
	// Unrelated (different labels) untouched.
	assert.NoError(t, c.Get(ctx, client.ObjectKey{Name: "other", Namespace: "default"}, &corev1.Service{}))
}

func TestDeleteByLabels_EmptyKeepNameDeletesAll(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	labels := map[string]string{"app": "x"}
	a := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "default", Labels: labels}}
	b := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "default", Labels: labels}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(a, b).Build()

	require.NoError(t, deleteByLabels(ctx, c, "default", labels, func() client.ObjectList { return &corev1.ServiceList{} }, ""))

	for _, name := range []string{"a", "b"} {
		err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: "default"}, &corev1.Service{})
		assert.True(t, errors.IsNotFound(err), "expected %s deleted", name)
	}
}
