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
	"fmt"
	"strings"

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
	"github.com/apache/superset-kubernetes-operator/internal/common"
)

const (
	bootstrapShell     = "/bin/sh"
	bootstrapScriptKey = "superset_bootstrap.sh"
)

func effectiveBootstrapScript(parent, override *string) string {
	if override != nil {
		return *override
	}
	if parent != nil {
		return *parent
	}
	return ""
}

func effectiveLifecycleBootstrapScript(spec *supersetv1alpha1.SupersetSpec) string {
	var lifecycleOverride *string
	if spec.Lifecycle != nil {
		lifecycleOverride = spec.Lifecycle.BootstrapScript
	}
	return effectiveBootstrapScript(spec.BootstrapScript, lifecycleOverride)
}

func withBootstrapScript(command []string, script string) []string {
	if script == "" || len(command) == 0 {
		return command
	}
	if len(command) >= 3 && command[0] == bootstrapShell && command[1] == "-c" {
		wrapped := fmt.Sprintf(". %s; %s", shellQuote(common.ConfigMountPath+"/"+bootstrapScriptKey), command[2])
		out := append([]string{command[0], command[1], wrapped}, command[3:]...)
		return out
	}
	return []string{
		bootstrapShell,
		"-c",
		". " + shellQuote(common.ConfigMountPath+"/"+bootstrapScriptKey) + "; exec " + shellJoin(command),
	}
}

func shellJoin(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, shellQuote(arg))
	}
	return strings.Join(quoted, " ")
}

func shellQuote(arg string) string {
	if arg == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(arg, "'", "'\"'\"'") + "'"
}
