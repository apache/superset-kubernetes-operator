# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Rewrite kubernetes.io API reference links to pkg.go.dev.
# crd-ref-docs hardcodes kubernetes.io links for k8s.io types and checks
# them before knownTypes, so config alone cannot override them.

# core/v1
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#affinity-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#Affinity|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#container-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#Container|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#containerport-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#ContainerPort|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#dnspolicy-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#DNSPolicy|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#envfromsource-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#EnvFromSource|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#envvar-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#EnvVar|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#hostalias-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#HostAlias|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#lifecycle-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#Lifecycle|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#localobjectreference-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#LocalObjectReference|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#poddnsconfig-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#PodDNSConfig|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#podsecuritycontext-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#PodSecurityContext|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#probe-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#Probe|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#pullpolicy-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#PullPolicy|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#resourcerequirements-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#ResourceRequirements|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#secretkeyselector-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#SecretKeySelector|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#securitycontext-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#SecurityContext|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#servicetype-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#ServiceType|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#toleration-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#Toleration|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#topologyspreadconstraint-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#TopologySpreadConstraint|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#volume-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#Volume|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#volumemount-v1-core|https://pkg.go.dev/k8s.io/api/core/v1#VolumeMount|g

# apps/v1
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#deploymentstrategy-v1-apps|https://pkg.go.dev/k8s.io/api/apps/v1#DeploymentStrategy|g

# autoscaling/v2
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#metricspec-v2-autoscaling|https://pkg.go.dev/k8s.io/api/autoscaling/v2#MetricSpec|g

# networking/v1
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#ingresstls-v1-networking|https://pkg.go.dev/k8s.io/api/networking/v1#IngressTLS|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#networkpolicyegressrule-v1-networking|https://pkg.go.dev/k8s.io/api/networking/v1#NetworkPolicyEgressRule|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#networkpolicyingressrule-v1-networking|https://pkg.go.dev/k8s.io/api/networking/v1#NetworkPolicyIngressRule|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#pathtype-v1-networking|https://pkg.go.dev/k8s.io/api/networking/v1#PathType|g

# meta/v1
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#condition-v1-meta|https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#duration-v1-meta|https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#objectmeta-v1-meta|https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta|g
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#time-v1-meta|https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Time|g

# util/intstr
s|https://kubernetes.io/docs/reference/generated/kubernetes-api/v[0-9.]*/#intorstring-intstr-util|https://pkg.go.dev/k8s.io/apimachinery/pkg/util/intstr#IntOrString|g
