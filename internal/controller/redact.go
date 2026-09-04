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

import "regexp"

// redactedPlaceholder replaces credential values matched by the redaction
// patterns below.
const redactedPlaceholder = "***"

var (
	// uriUserinfoRe matches the password component of a URI userinfo section
	// (scheme://user:password@host). Task failure output commonly embeds full
	// database connection URIs in driver error messages, e.g.
	// "connection to postgresql://superset:hunter2@db:5432/superset failed".
	// The user component may be empty (redis://:password@host). The password
	// match is greedy up to the last @ in the whitespace-delimited token, so a
	// malformed password containing a raw @ is masked in full rather than
	// leaking its tail after the first @.
	uriUserinfoRe = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^/@\s:]*):([^\s]+)@`)

	// authorizationHeaderRe matches HTTP Authorization header values, with or
	// without an auth scheme: "Authorization: Bearer eyJ...", "authorization=
	// Basic dXNlcg==". The scheme word is kept; only the credential is masked.
	authorizationHeaderRe = regexp.MustCompile(`(?i)\b(authorization\s*[=:]\s*(?:(?:bearer|basic|token|digest|negotiate)\s+)?)(\S+)`)

	// bareBearerRe matches a bearer token without an Authorization prefix
	// ("bearer eyJhbGci..."). The token must contain at least one digit so
	// prose like "bearer authentication failed" is not mangled; real tokens
	// (JWTs, base64, hex) virtually always contain digits.
	bareBearerRe = regexp.MustCompile(`(?i)\b(bearer\s+)([A-Za-z0-9._~+/=-]*[0-9][A-Za-z0-9._~+/=-]*)`)

	// credentialAssignmentRe matches free-form credential assignments such as
	// "password=hunter2", "PGPASSWORD: hunter2", or "secret_key = hunter2".
	// The key may carry prefixes/suffixes (DB_PASSWORD, api-key-prod).
	credentialAssignmentRe = regexp.MustCompile(`(?i)([A-Za-z0-9_-]*(?:password|passwd|pwd|secret|token|api[_-]?key|credential|passphrase)[A-Za-z0-9_-]*\s*[=:]\s*)(\S+)`)

	// quotedKeyCredentialRe matches quoted-key credential assignments as emitted
	// by JSON payloads and Python dict/traceback reprs, e.g.
	// {"password": "hunter2"} or {'db_password': 'hunter2'}. credentialAssignmentRe
	// cannot match these — the closing quote sits between the keyword and the
	// separator, which its key class does not allow — so quoted-key forms are a
	// whole serialization class it systematically misses. The value is matched as
	// a quoted string of either style (RE2 has no backreferences, so both quote
	// styles are spelled out) or as a delimiter-bounded bare value, and masked in
	// full, including embedded whitespace for quoted values.
	// "authorization" is included because authorizationHeaderRe is equally
	// quote-blind. The value is replaced with a bare placeholder so a re-run
	// finds no quoted value to match (idempotent).
	quotedKeyCredentialRe = regexp.MustCompile(`(?i)(["'][A-Za-z0-9_-]*(?:password|passwd|pwd|secret|token|api[_-]?key|credential|passphrase|authorization)[A-Za-z0-9_-]*["']\s*[=:]\s*)("[^"]*"|'[^']*'|[^,\s}\]]+)`)
)

// redactCredentials masks credential-shaped substrings in free-form task
// output before it is persisted to status fields or Events. It is a
// best-effort, pattern-based defense: the operator never reads Secret values,
// so it cannot scrub by exact value. Callers must apply it BEFORE truncation —
// truncating first could split a credential mid-pattern, leaving a fragment
// that no longer matches but still leaks.
func redactCredentials(msg string) string {
	msg = uriUserinfoRe.ReplaceAllString(msg, "${1}:"+redactedPlaceholder+"@")
	// authorizationHeaderRe must run before bareBearerRe so "Authorization:
	// Bearer x" keeps its scheme word instead of being matched as a bare token.
	msg = authorizationHeaderRe.ReplaceAllString(msg, "${1}"+redactedPlaceholder)
	msg = bareBearerRe.ReplaceAllString(msg, "${1}"+redactedPlaceholder)
	msg = credentialAssignmentRe.ReplaceAllString(msg, "${1}"+redactedPlaceholder)
	// Quoted-key forms (JSON, Python dict repr) must run too — the unquoted
	// assignment pattern above cannot match them. Mask the whole quoted value
	// with a bare placeholder (not a re-quoted string) so a second pass finds no
	// quoted value to match, keeping redaction idempotent even on adversarial,
	// unbalanced-quote input.
	msg = quotedKeyCredentialRe.ReplaceAllString(msg, "${1}"+redactedPlaceholder)
	return msg
}
