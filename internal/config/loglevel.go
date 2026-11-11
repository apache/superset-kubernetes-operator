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
	"fmt"
	"strings"

	v1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

// DefaultLogLevel applies when neither the component nor spec.logLevel sets a level.
const DefaultLogLevel = v1alpha1.LogLevelInfo

// ResolveLogLevel returns the effective lowercase level: the component value,
// then the top-level value, then DefaultLogLevel.
func ResolveLogLevel(component, topLevel *v1alpha1.LogLevel) v1alpha1.LogLevel {
	if component != nil {
		return *component
	}
	if topLevel != nil {
		return *topLevel
	}
	return DefaultLogLevel
}

// IsLogLevelExplicit returns true if log level was set explicitly in
// either the component or top-level spec.
func IsLogLevelExplicit(component, topLevel *v1alpha1.LogLevel) bool {
	return component != nil || topLevel != nil
}

// PythonLogLevel returns the level name Python's logging module expects.
func PythonLogLevel(level v1alpha1.LogLevel) string {
	return strings.ToUpper(string(level))
}

// CeleryLogLevelFlag returns the Celery worker/beat --loglevel argument.
func CeleryLogLevelFlag(level v1alpha1.LogLevel) string {
	return fmt.Sprintf("--loglevel=%s", level)
}

// WinstonLogLevel returns the level name Winston's logging module expects.
// "warning" returns "warn", "critical" returns "error", all other levels returned unchanged.
func WinstonLogLevel(level v1alpha1.LogLevel) string {
	switch level {
	case v1alpha1.LogLevelWarning:
		return "warn"
	case v1alpha1.LogLevelCritical:
		return "error"
	default:
		return string(level)
	}
}
