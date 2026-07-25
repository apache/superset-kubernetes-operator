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

package schedule

import (
	"fmt"
	"time"

	"github.com/adhocore/gronx"
)

// CurrentTick returns the most recent time matching the cron expression that is
// at or before now, formatted as RFC3339 UTC (e.g., "2026-05-12T02:00:00Z").
// Returns "" if the expression is invalid or no matching tick exists.
func CurrentTick(expr string, now time.Time) string {
	prev, err := gronx.PrevTickBefore(expr, now, true)
	if err != nil {
		return ""
	}
	return prev.UTC().Format(time.RFC3339)
}

// NextTick returns the next future time matching the cron expression (strictly
// after now). Returns zero time if the expression is invalid.
func NextTick(expr string, now time.Time) time.Time {
	next, err := gronx.NextTickAfter(expr, now, false)
	if err != nil {
		return time.Time{}
	}
	return next
}

// Validate checks whether a cron expression is parseable.
// Returns an error describing the problem, or nil if valid.
func Validate(expr string) error {
	if !gronx.IsValid(expr) {
		return fmt.Errorf("invalid cron expression %q", expr)
	}
	return nil
}
