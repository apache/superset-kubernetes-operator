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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSetIf documents that setIf overwrites the destination only when the
// source pointer is non-nil, leaving the existing value untouched otherwise.
func TestSetIf(t *testing.T) {
	t.Run("nil source leaves dst unchanged", func(t *testing.T) {
		dst := int32(7)
		setIf(&dst, nil)
		assert.Equal(t, int32(7), dst)
	})

	t.Run("non-nil source overwrites dst", func(t *testing.T) {
		dst := int32(7)
		setIf(&dst, ptr(int32(42)))
		assert.Equal(t, int32(42), dst)
	})

	t.Run("generic over string", func(t *testing.T) {
		dst := "default"
		setIf(&dst, nil)
		assert.Equal(t, "default", dst)
		setIf(&dst, ptr("override"))
		assert.Equal(t, "override", dst)
	})
}
