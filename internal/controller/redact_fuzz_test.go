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

import "testing"

// FuzzRedactCredentials exercises redactCredentials with arbitrary task
// output. The properties checked are robustness (no panic on any input) and
// idempotence: redacting already-redacted output must be a no-op, since the
// same message can pass through the function more than once across reconciles.
func FuzzRedactCredentials(f *testing.F) {
	f.Add("connection to postgresql://superset:hunter2@db:5432/superset failed")
	f.Add("redis://:s3cr3t@valkey:6379/0")
	f.Add("password=hunter2 PGPASSWORD: x SECRET_KEY = y token=z")
	f.Add("Job has reached the specified backoff limit")
	f.Add("")
	f.Add("://:@ password= : = @")
	f.Add("postgresql://user:p@ss@db:5432/superset")
	f.Add("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.p.s bearer abc123")
	f.Add("authorization=Basic dXNlcjpwYXNz passphrase: x")

	f.Fuzz(func(t *testing.T, msg string) {
		once := redactCredentials(msg)
		twice := redactCredentials(once)
		if once != twice {
			t.Errorf("redactCredentials is not idempotent:\n input: %q\n once:  %q\n twice: %q", msg, once, twice)
		}
	})
}
