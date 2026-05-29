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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestCalculateBackoff(t *testing.T) {
	tests := []struct {
		name    string
		attempt int32
		want    time.Duration
	}{
		{name: "attempt 1", attempt: 1, want: 10 * time.Second},
		{name: "attempt 2", attempt: 2, want: 20 * time.Second},
		{name: "attempt 3", attempt: 3, want: 40 * time.Second},
		{name: "attempt 4", attempt: 4, want: 80 * time.Second},
		{name: "attempt 5", attempt: 5, want: 160 * time.Second},
		{name: "attempt 6 caps at 300s", attempt: 6, want: 300 * time.Second},
		{name: "attempt 10 stays capped", attempt: 10, want: 300 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, calculateBackoff(tt.attempt))
		})
	}
}

func TestShouldDeletePod(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		phase  corev1.PodPhase
		want   bool
	}{
		{name: "Delete always deletes succeeded", policy: retentionDelete, phase: corev1.PodSucceeded, want: true},
		{name: "Delete always deletes failed", policy: retentionDelete, phase: corev1.PodFailed, want: true},
		{name: "Retain never deletes succeeded", policy: retentionRetain, phase: corev1.PodSucceeded, want: false},
		{name: "Retain never deletes failed", policy: retentionRetain, phase: corev1.PodFailed, want: false},
		{name: "RetainOnFailure deletes succeeded", policy: retentionRetainOnFail, phase: corev1.PodSucceeded, want: true},
		{name: "RetainOnFailure keeps failed", policy: retentionRetainOnFail, phase: corev1.PodFailed, want: false},
		{name: "RetainOnFailure deletes running", policy: retentionRetainOnFail, phase: corev1.PodRunning, want: true},
		{name: "unknown policy deletes (safe default)", policy: "Bogus", phase: corev1.PodFailed, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ShouldDeletePod(tt.policy, tt.phase))
		})
	}
}
