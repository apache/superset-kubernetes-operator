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

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPtr(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		s := "hello"
		p := Ptr(s)
		assert.NotNil(t, p)
		assert.Equal(t, s, *p)
	})

	t.Run("int32", func(t *testing.T) {
		var n int32 = 7
		p := Ptr(n)
		assert.NotNil(t, p)
		assert.Equal(t, n, *p)
	})

	t.Run("bool zero value is still addressable", func(t *testing.T) {
		b := false
		p := Ptr(b)
		assert.NotNil(t, p)
		assert.False(t, *p)
	})

	t.Run("returns a distinct pointer per call", func(t *testing.T) {
		n := 1
		a := Ptr(n)
		b := Ptr(n)
		assert.NotSame(t, a, b)
	})
}
