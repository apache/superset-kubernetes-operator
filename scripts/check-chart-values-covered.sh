#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Chart-test coverage gate.
#
# Fails when a knob documented in the chart's values.yaml is not exercised by
# the comprehensive chart test values (tests/values/full-options.yaml). This
# catches the case a snapshot cannot: a new value that defaults to null and is
# consumed via a guarded block ({{- with .Values.newKnob }}) renders nothing
# under defaults, so both the minimal and an under-specified comprehensive test
# would stay green while the knob ships untested.
#
# Limitation: coverage is measured against values.yaml, so a template that
# references an undocumented .Values.x is not caught here. That already violates
# the chart's documented-values contract (README is generated from values.yaml
# via helm-docs) and is expected to be caught in review.

set -euo pipefail

CHART_DIR="${CHART_DIR:-charts/superset-operator}"
VALUES="${CHART_DIR}/values.yaml"
FULL="${CHART_DIR}/tests/values/full-options.yaml"

for f in "${VALUES}" "${FULL}"; do
  [ -f "${f}" ] || { echo "error: ${f} not found" >&2; exit 2; }
done

# Emit the dotted key paths of every "knob" in a values file: scalars plus empty
# maps/seqs (e.g. `affinity: {}`), which are leaves an author is expected to set.
# Numeric (array-index) path segments are stripped so coverage is index-agnostic.
leaf_paths() {
  yq '.. | select(
        (tag == "!!map" and length == 0) or
        (tag == "!!seq" and length == 0) or
        (tag != "!!map" and tag != "!!seq")
      ) | path | join(".")' "$1" |
    sed -E 's/\.[0-9]+//g' | grep -v '^$' | sort -u
}

# Emit every path in a values file (intermediate nodes included), index-stripped.
# A values.yaml leaf counts as covered when it appears anywhere in this set.
all_paths() {
  yq '.. | path | join(".")' "$1" |
    sed -E 's/\.[0-9]+//g' | grep -v '^$' | sort -u
}

uncovered="$(comm -23 <(leaf_paths "${VALUES}") <(all_paths "${FULL}"))"

if [ -n "${uncovered}" ]; then
  {
    echo "ERROR: values.yaml keys not exercised by ${FULL}:"
    echo "${uncovered}" | awk '{ print "  - " $0 }'
    echo
    echo "Add them to the comprehensive chart test values so the chart tests"
    echo "cover every documented knob."
  } >&2
  exit 1
fi

echo "OK: every values.yaml knob is exercised by ${FULL}"
