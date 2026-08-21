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

# Collapse <br /> tags to spaces everywhere except the Validation column.
#
# crd-ref-docs turns every newline in a Go doc comment into a <br />, which
# renders as awkward mid-sentence breaks in the Description column (a comment
# wrapped at 80 columns becomes several stacked lines in one table cell). The
# Validation column, by contrast, uses <br /> intentionally to stack distinct
# constraints (Minimum / Maximum / Optional / ...) and reads well that way, so
# it is left untouched.
#
# Member tables have four columns -- Field | Description | Default | Validation
# -- which split on "|" into six fields (empty leading and trailing splits
# included), so Validation is the second-to-last field. Cells never contain a
# literal "|" (crd-ref-docs escapes pipes, and none occur in practice), so
# field splitting is unambiguous.

BEGIN { FS = OFS = "|" }

# Table data rows start with a backtick-quoted field name.
/^\| `/ {
	# Preserve <br /> in the Validation column (second-to-last field); collapse
	# it, together with any surrounding spaces, everywhere else. Enum tables
	# lack a Validation column, so all their content cells are collapsed.
	last = (NF >= 6) ? NF - 2 : NF - 1
	for (i = 2; i <= last; i++) gsub(/ *<br \/> */, " ", $i)
	print
	next
}

{ print }
