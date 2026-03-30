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

// Package common provides shared types and utilities used across
// the resolution and config packages.
package common

// ComponentType identifies a Superset component.
type ComponentType string

const (
	ComponentWebServer       ComponentType = "web-server"
	ComponentCeleryWorker    ComponentType = "celery-worker"
	ComponentCeleryBeat      ComponentType = "celery-beat"
	ComponentCeleryFlower    ComponentType = "celery-flower"
	ComponentWebsocketServer ComponentType = "websocket-server"
	ComponentMcpServer       ComponentType = "mcp-server"
	ComponentInit            ComponentType = "init"
)

// Environment mode values.
const (
	EnvironmentDev  = "dev"
	EnvironmentProd = "prod"
)

// Ptr returns a pointer to the given value.
func Ptr[T any](v T) *T {
	return &v
}
