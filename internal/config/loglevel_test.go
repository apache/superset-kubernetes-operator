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
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

func TestWinstonLogLevel(t *testing.T) {
	tests := map[v1alpha1.LogLevel]string{
		v1alpha1.LogLevelDebug:    "debug",
		v1alpha1.LogLevelInfo:     "info",
		v1alpha1.LogLevelWarning:  "warn",
		v1alpha1.LogLevelError:    "error",
		v1alpha1.LogLevelCritical: "error",
	}
	for in, want := range tests {
		t.Run(string(in), func(t *testing.T) {
			assert.Equal(t, want, WinstonLogLevel(in))
		})
	}
}
