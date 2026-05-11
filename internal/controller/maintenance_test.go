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

	supersetv1alpha1 "github.com/apache/superset-kubernetes-operator/api/v1alpha1"
)

func TestRenderMaintenanceHTML_EscapesTitle(t *testing.T) {
	title := `<script>alert("xss")</script>`
	spec := &supersetv1alpha1.MaintenancePageSpec{Title: &title}
	html := renderMaintenanceHTML(spec)

	if strings.Contains(html, "<script>") {
		t.Error("title should be HTML-escaped but contains raw <script> tag")
	}
	if !strings.Contains(html, "&lt;script&gt;") {
		t.Error("expected escaped title in output")
	}
}

func TestRenderMaintenanceHTML_EscapesMessage(t *testing.T) {
	msg := `<img src=x onerror="alert('xss')">`
	spec := &supersetv1alpha1.MaintenancePageSpec{Message: &msg}
	html := renderMaintenanceHTML(spec)

	if strings.Contains(html, "<img") {
		t.Error("message should be HTML-escaped but contains raw <img tag")
	}
	if !strings.Contains(html, "&lt;img") {
		t.Error("expected escaped message in output")
	}
}

func TestRenderMaintenanceHTML_BodyPassesThrough(t *testing.T) {
	body := `<html><body><h1>Custom</h1><script>ok()</script></body></html>`
	spec := &supersetv1alpha1.MaintenancePageSpec{Body: &body}
	result := renderMaintenanceHTML(spec)

	if result != body {
		t.Errorf("body should be returned as-is, got: %s", result)
	}
}

func TestRenderMaintenanceHTML_DefaultsAreEscaped(t *testing.T) {
	spec := &supersetv1alpha1.MaintenancePageSpec{}
	html := renderMaintenanceHTML(spec)

	if !strings.Contains(html, maintenanceDefaultTitle) {
		t.Error("expected default title in output")
	}
	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("expected full HTML document")
	}
}
