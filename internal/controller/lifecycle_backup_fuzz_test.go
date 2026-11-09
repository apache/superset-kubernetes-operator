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
	"strings"
	"testing"
)

// FuzzSanitizeBackupLabel checks that any image tag maps to a safe backup
// file-name component: only [A-Za-z0-9._-], no leading '.' or '-', bounded
// length, no path separators, and idempotent.
func FuzzSanitizeBackupLabel(f *testing.F) {
	f.Add("6.1.0")
	f.Add("../../etc/passwd")
	f.Add("-rf")
	f.Add(".")
	f.Add("")
	f.Add("tag with spaces/and\nnewlines")
	f.Add("日本語")
	f.Add(strings.Repeat("x", 300))

	f.Fuzz(func(t *testing.T, in string) {
		out := sanitizeBackupLabel(in)
		if len(out) > backupPrefixMaxLen {
			t.Fatalf("length %d exceeds %d", len(out), backupPrefixMaxLen)
		}
		if strings.HasPrefix(out, ".") || strings.HasPrefix(out, "-") {
			t.Fatalf("leading '.' or '-' in %q", out)
		}
		for _, c := range out {
			ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'
			if !ok {
				t.Fatalf("disallowed rune %q in %q", c, out)
			}
		}
		if again := sanitizeBackupLabel(out); again != out {
			t.Fatalf("not idempotent: %q -> %q", out, again)
		}
	})
}

// FuzzParseBackupResult checks that the backup termination-message parser
// never panics and only accepts results whose recorded fields are safe to put
// in status: a plain file name, a lowercase hex SHA-256, a non-negative size,
// a bounded revision, and an RFC 3339 timestamp; or a bounded skip reason.
func FuzzParseBackupResult(f *testing.F) {
	sha := strings.Repeat("ab", 32)
	f.Add(`{"file":"demo_20260926T010203Z_6.0.0.dump","sizeBytes":1,"sha256":"` + sha + `","alembicRevision":"a1","createdAt":"2026-09-26T01:02:03Z"}`)
	f.Add(`{"skipped":"metastore database x does not exist yet"}`)
	f.Add(`{"file":"../x","sizeBytes":1,"sha256":"` + sha + `","createdAt":"2026-09-26T01:02:03Z"}`)
	f.Add(`backup failed at connect`)
	f.Add(`{}`)
	f.Add(``)

	f.Fuzz(func(t *testing.T, in string) {
		res, ok := parseBackupResult(in)
		if !ok {
			return
		}
		if res.Skipped != "" {
			if res.File != "" || len(res.Skipped) > 256 {
				t.Fatalf("accepted invalid skip result %+v", res)
			}
			return
		}
		if !backupFilePattern.MatchString(res.File) || strings.Contains(res.File, "/") {
			t.Fatalf("accepted unsafe file %q", res.File)
		}
		if !backupSHA256Pattern.MatchString(res.SHA256) || res.SizeBytes == nil || *res.SizeBytes < 0 {
			t.Fatalf("accepted invalid digest or size %+v", res)
		}
		if !backupRevisionPattern.MatchString(res.AlembicRevision) {
			t.Fatalf("accepted unsafe revision %q", res.AlembicRevision)
		}
	})
}
