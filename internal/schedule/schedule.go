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

	"github.com/robfig/cron/v3"
)

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// CurrentTick returns the most recent past time matching the cron expression,
// formatted as RFC3339 UTC (e.g., "2026-05-12T02:00:00Z").
// Returns "" if the expression is invalid or no tick exists within the
// lookback window (~2 years).
func CurrentTick(expr string, now time.Time) string {
	sched, err := parser.Parse(expr)
	if err != nil {
		return ""
	}
	prev := findPrevTick(sched, now)
	if prev.IsZero() {
		return ""
	}
	return prev.UTC().Format(time.RFC3339)
}

// NextTick returns the next future time matching the cron expression.
// Returns zero time if the expression is invalid.
func NextTick(expr string, now time.Time) time.Time {
	sched, err := parser.Parse(expr)
	if err != nil {
		return time.Time{}
	}
	return sched.Next(now)
}

// Validate checks whether a cron expression is parseable.
// Returns an error describing the problem, or nil if valid.
func Validate(expr string) error {
	_, err := parser.Parse(expr)
	if err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", expr, err)
	}
	return nil
}

// findPrevTick finds the most recent time matching the schedule that is <= now.
// Uses geometric doubling of the lookback window for efficiency.
func findPrevTick(sched cron.Schedule, now time.Time) time.Time {
	lookback := time.Minute
	for range 20 {
		start := now.Add(-lookback)
		t := sched.Next(start)
		if !t.After(now) {
			// Found a tick in the window. Iterate forward to find the latest one <= now.
			prev := t
			for {
				t = sched.Next(t)
				if t.After(now) {
					return prev
				}
				prev = t
			}
		}
		lookback *= 2
	}
	return time.Time{}
}
