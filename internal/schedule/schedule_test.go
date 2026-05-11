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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCurrentTick_Hourly(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	tick := CurrentTick("0 * * * *", now)
	assert.Equal(t, "2026-05-11T14:00:00Z", tick)
}

func TestCurrentTick_Daily(t *testing.T) {
	now := time.Date(2026, 5, 11, 10, 0, 0, 0, time.UTC)
	tick := CurrentTick("0 2 * * *", now)
	assert.Equal(t, "2026-05-11T02:00:00Z", tick)
}

func TestCurrentTick_BeforeFirstDailyTick(t *testing.T) {
	now := time.Date(2026, 5, 11, 1, 0, 0, 0, time.UTC)
	tick := CurrentTick("0 2 * * *", now)
	assert.Equal(t, "2026-05-10T02:00:00Z", tick)
}

func TestCurrentTick_EveryMinute(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 45, 0, time.UTC)
	tick := CurrentTick("* * * * *", now)
	assert.Equal(t, "2026-05-11T14:30:00Z", tick)
}

func TestCurrentTick_ExactlyOnBoundary(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 0, 0, 0, time.UTC)
	tick := CurrentTick("0 * * * *", now)
	// At exactly the boundary, the tick is the previous occurrence
	// because Next(now-lookback) finds 14:00 which is not AFTER now (it equals now).
	assert.Equal(t, "2026-05-11T14:00:00Z", tick)
}

func TestCurrentTick_Monthly(t *testing.T) {
	now := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	tick := CurrentTick("0 0 1 * *", now)
	assert.Equal(t, "2026-05-01T00:00:00Z", tick)
}

func TestCurrentTick_MonthlyBeforeFirstDay(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	// At midnight on the 1st — the tick is exactly now.
	tick := CurrentTick("0 0 1 * *", now)
	assert.Equal(t, "2026-06-01T00:00:00Z", tick)
}

func TestCurrentTick_Weekly(t *testing.T) {
	// 2026-05-11 is a Monday (day 1)
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC) // Thursday
	tick := CurrentTick("30 1 * * 1", now)               // Mondays at 1:30
	assert.Equal(t, "2026-05-11T01:30:00Z", tick)
}

func TestCurrentTick_InvalidExpr(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	tick := CurrentTick("not a cron", now)
	assert.Equal(t, "", tick)
}

func TestCurrentTick_EmptyExpr(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	tick := CurrentTick("", now)
	assert.Equal(t, "", tick)
}

func TestCurrentTick_EveryFiveMinutes(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 37, 0, 0, time.UTC)
	tick := CurrentTick("*/5 * * * *", now)
	assert.Equal(t, "2026-05-11T14:35:00Z", tick)
}

func TestCurrentTick_EverySixHours(t *testing.T) {
	now := time.Date(2026, 5, 11, 7, 0, 0, 0, time.UTC)
	tick := CurrentTick("0 */6 * * *", now)
	assert.Equal(t, "2026-05-11T06:00:00Z", tick)
}

func TestCurrentTick_StableWithinWindow(t *testing.T) {
	// Two times within the same cron window should return the same tick.
	expr := "0 2 * * *"
	t1 := time.Date(2026, 5, 11, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 11, 23, 59, 59, 0, time.UTC)
	assert.Equal(t, CurrentTick(expr, t1), CurrentTick(expr, t2))
}

func TestCurrentTick_ChangesAcrossBoundary(t *testing.T) {
	expr := "0 2 * * *"
	before := time.Date(2026, 5, 11, 1, 59, 59, 0, time.UTC)
	after := time.Date(2026, 5, 11, 2, 0, 1, 0, time.UTC)
	assert.NotEqual(t, CurrentTick(expr, before), CurrentTick(expr, after))
}

func TestNextTick_Hourly(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	next := NextTick("0 * * * *", now)
	expected := time.Date(2026, 5, 11, 15, 0, 0, 0, time.UTC)
	assert.Equal(t, expected, next)
}

func TestNextTick_Daily(t *testing.T) {
	now := time.Date(2026, 5, 11, 3, 0, 0, 0, time.UTC)
	next := NextTick("0 2 * * *", now)
	expected := time.Date(2026, 5, 12, 2, 0, 0, 0, time.UTC)
	assert.Equal(t, expected, next)
}

func TestNextTick_InvalidExpr(t *testing.T) {
	now := time.Date(2026, 5, 11, 14, 30, 0, 0, time.UTC)
	next := NextTick("invalid", now)
	assert.True(t, next.IsZero())
}

func TestValidate_Valid(t *testing.T) {
	assert.NoError(t, Validate("0 2 * * *"))
	assert.NoError(t, Validate("*/5 * * * *"))
	assert.NoError(t, Validate("0 */6 * * 1-5"))
}

func TestValidate_Invalid(t *testing.T) {
	require.Error(t, Validate("not valid"))
	require.Error(t, Validate(""))
	require.Error(t, Validate("* * *")) // too few fields
}
