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

	"github.com/stretchr/testify/assert"
)

func TestRedactCredentials(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "plain message unchanged",
			in:   "Job has reached the specified backoff limit",
			want: "Job has reached the specified backoff limit",
		},
		{
			name: "empty message",
			in:   "",
			want: "",
		},
		{
			name: "postgres URI password",
			in:   `connection to server at "postgresql://superset:hunter2@db:5432/superset" failed`,
			want: `connection to server at "postgresql://superset:***@db:5432/superset" failed`,
		},
		{
			name: "redis URI with empty user",
			in:   "cannot connect to redis://:s3cr3t@valkey:6379/0",
			want: "cannot connect to redis://:***@valkey:6379/0",
		},
		{
			name: "URI without userinfo unchanged",
			in:   "GET http://superset-web-server:8088/health failed",
			want: "GET http://superset-web-server:8088/health failed",
		},
		{
			name: "password assignment with equals",
			in:   "FATAL: authentication failed, password=hunter2 rejected",
			want: "FATAL: authentication failed, password=*** rejected",
		},
		{
			name: "env-style assignment with colon and prefix",
			in:   "PGPASSWORD: hunter2 is invalid",
			want: "PGPASSWORD: *** is invalid",
		},
		{
			name: "secret key assignment",
			in:   "SECRET_KEY = thisIsNotSecure",
			want: "SECRET_KEY = ***",
		},
		{
			name: "token and api key assignments",
			in:   "token=abc123 api_key: xyz789",
			want: "token=*** api_key: ***",
		},
		{
			name: "multiple URIs in one message",
			in:   "tried postgresql://a:p1@h1/db then postgresql://b:p2@h2/db",
			want: "tried postgresql://a:***@h1/db then postgresql://b:***@h2/db",
		},
		{
			name: "URI password containing a raw at-sign is masked in full",
			in:   "connect to postgresql://user:p@ss@db:5432/superset failed",
			want: "connect to postgresql://user:***@db:5432/superset failed",
		},
		{
			name: "authorization header with bearer scheme",
			in:   `request rejected: Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig`,
			want: `request rejected: Authorization: Bearer ***`,
		},
		{
			name: "authorization assignment with basic scheme",
			in:   "authorization=Basic dXNlcjpwYXNz failed",
			want: "authorization=Basic *** failed",
		},
		{
			name: "authorization header without scheme",
			in:   "Authorization: abc123xyz",
			want: "Authorization: ***",
		},
		{
			name: "bare bearer token",
			in:   "server returned 401 for bearer eyJhbGciOiJIUzI1NiJ9",
			want: "server returned 401 for bearer ***",
		},
		{
			name: "bearer followed by prose is not mangled",
			in:   "bearer authentication failed for request",
			want: "bearer authentication failed for request",
		},
		{
			name: "passphrase assignment",
			in:   "ssl passphrase=opensesame rejected",
			want: "ssl passphrase=*** rejected",
		},
		{
			name: "idempotent on already-redacted output",
			in:   "postgresql://superset:***@db:5432/superset password=*** Authorization: Bearer ***",
			want: "postgresql://superset:***@db:5432/superset password=*** Authorization: Bearer ***",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, redactCredentials(tt.in))
		})
	}
}
