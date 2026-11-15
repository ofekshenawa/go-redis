package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"unicode"
)

// Delta is scoped to the two captured contracts of one workflow run. It is not
// an allowlist or an approval record and is never checked into the repository.
type Delta struct {
	Version    int      `json:"version"`
	HasChanges bool     `json:"has_changes"`
	Changes    []Change `json:"changes"`
}
type Change struct {
	Entity   string `json:"entity"`
	Identity string `json:"identity"`
	Action   string `json:"action"`
	Before   any    `json:"before"`
	After    any    `json:"after"`
}

func jsonBytes(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	return append(data, '\n'), err
}
func readContract(path string) (Contract, error) {
	var c Contract
	file, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer file.Close()
	const maxSize = 16 << 20
	data, err := io.ReadAll(io.LimitReader(file, maxSize+1))
	if err != nil {
		return c, err
	}
	if len(data) > maxSize {
		return c, fmt.Errorf("contract exceeds 16 MiB")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err = dec.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("contract must contain exactly one JSON object")
	}
	// Empty units are valid; an omitted unit is a broken collector output. Other
	// mandatory fields are also checked below for nonempty/non-null values.
	var raw struct {
		Metrics []map[string]json.RawMessage `json:"metrics"`
	}
	if err = json.Unmarshal(data, &raw); err != nil {
		return c, err
	}
	for i, m := range raw.Metrics {
		for _, key := range []string{"package", "name", "instrument", "unit", "attributes"} {
			value, ok := m[key]
			if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return c, fmt.Errorf("metric %d requires %s", i, key)
			}
		}
	}
	if err = normalizeContract(&c); err != nil {
		return c, err
	}
	return c, nil
}
func normalizeContract(c *Contract) error {
	if c.Version != 1 {
		return fmt.Errorf("unsupported contract version %d", c.Version)
	}
	if len(c.Metrics) == 0 {
		return fmt.Errorf("empty telemetry inventory")
	}
	if c.Attributes == nil {
		return fmt.Errorf("attribute_declarations must be an array")
	}
	if err := validateAttrs(c.Attributes); err != nil {
		return fmt.Errorf("attribute declarations: %w", err)
	}
	metrics := map[string]bool{}
	for i := range c.Metrics {
		m := &c.Metrics[i]
		if strings.TrimSpace(m.Package) == "" || strings.TrimSpace(m.Name) == "" || strings.TrimSpace(m.Instrument) == "" {
			return fmt.Errorf("metric %d requires package, name and instrument", i)
		}
		// Keep identity encoding unambiguous, including the scanner's internal key
		// separator. Display strings are independently HTML escaped in the report.
		for _, v := range []string{m.Package, m.Name, m.Instrument, m.Unit} {
			if strings.ContainsRune(v, '\x00') {
				return fmt.Errorf("metric identity contains a NUL byte")
			}
		}
		id := metricKey(*m)
		if metrics[id] {
			return fmt.Errorf("duplicate metric identity: %s", metricID(*m))
		}
		metrics[id] = true
		if m.Attributes == nil {
			return fmt.Errorf("metric %s attributes must be an array", m.Name)
		}
		if err := validateAttrs(m.Attributes); err != nil {
			return fmt.Errorf("metric %s: %w", m.Name, err)
		}
		sortAttrs(m.Attributes)
	}
	sortAttrs(c.Attributes)
	sort.Slice(c.Metrics, func(i, j int) bool { return metricKey(c.Metrics[i]) < metricKey(c.Metrics[j]) })
	return nil
}
func validateAttrs(attrs []Attribute) error {
	validTypes := map[string]bool{"string": true, "int64": true, "float64": true, "bool": true, "string[]": true, "int64[]": true, "float64[]": true, "bool[]": true}
	seen := map[Attribute]bool{}
	for _, a := range attrs {
		if strings.TrimSpace(a.Name) == "" || strings.ContainsRune(a.Name, '\x00') || !validTypes[a.Type] {
			return fmt.Errorf("invalid attribute name/type: %q (%q)", a.Name, a.Type)
		}
		if seen[a] {
			return fmt.Errorf("duplicate attribute: %s (%s)", a.Name, a.Type)
		}
		seen[a] = true
	}
	return nil
}
func metricKey(m Metric) string {
	return strings.Join([]string{m.Package, m.Name, m.Instrument, m.Unit}, "\x00")
}
func compareContracts(before, after Contract) (Delta, []byte) {
	delta := Delta{Version: 1, Changes: []Change{}}
	oldMetrics, newMetrics := map[string]Metric{}, map[string]Metric{}
	for _, m := range before.Metrics {
		oldMetrics[metricKey(m)] = m
	}
	for _, m := range after.Metrics {
		newMetrics[metricKey(m)] = m
	}
	for key, old := range oldMetrics {
		current, ok := newMetrics[key]
		if !ok {
			delta.Changes = append(delta.Changes, Change{"metric", metricID(old), "remove", old, nil})
		} else if !reflect.DeepEqual(old, current) {
			delta.Changes = append(delta.Changes, Change{"metric", metricID(old), "modify", old, current})
		}
	}
	for key, current := range newMetrics {
		if _, ok := oldMetrics[key]; !ok {
			delta.Changes = append(delta.Changes, Change{"metric", metricID(current), "add", nil, current})
		}
	}
	oldAttrs, newAttrs := map[Attribute]bool{}, map[Attribute]bool{}
	for _, a := range before.Attributes {
		oldAttrs[a] = true
	}
	for _, a := range after.Attributes {
		newAttrs[a] = true
	}
	for a := range oldAttrs {
		if !newAttrs[a] {
			delta.Changes = append(delta.Changes, Change{"attribute_declaration", a.Name + " (" + a.Type + ")", "remove", a, nil})
		}
	}
	for a := range newAttrs {
		if !oldAttrs[a] {
			delta.Changes = append(delta.Changes, Change{"attribute_declaration", a.Name + " (" + a.Type + ")", "add", nil, a})
		}
	}
	sort.Slice(delta.Changes, func(i, j int) bool {
		a, b := delta.Changes[i], delta.Changes[j]
		return a.Entity+"\x00"+a.Identity+"\x00"+a.Action < b.Entity+"\x00"+b.Identity+"\x00"+b.Action
	})
	delta.HasChanges = len(delta.Changes) > 0
	return delta, []byte(renderReport(buildReview(before, after, delta), delta.HasChanges))
}

// The review presentation may pair a single removed/added variant of the same
// metric to show instrument/unit changes together. The structured Delta stays
// unchanged; ambiguous multi-variant changes remain separate additions/removals.
type metricReview struct {
	Before, After *Metric
	Attributes    []attributeReview
}
type attributeReview struct {
	Name          string
	Before, After []string
}
type contractReview struct {
	Metrics []metricReview
	Other   []attributeReview
}

func metricNameKey(m Metric) string { return m.Package + "\x00" + m.Name }
func buildReview(before, after Contract, delta Delta) contractReview {
	oldCounts, newCounts := map[string]int{}, map[string]int{}
	added := map[string]Metric{}
	for _, m := range before.Metrics {
		oldCounts[metricNameKey(m)]++
	}
	for _, m := range after.Metrics {
		newCounts[metricNameKey(m)]++
	}
	for _, change := range delta.Changes {
		if change.Entity == "metric" && change.Action == "add" {
			m := change.After.(Metric)
			added[metricNameKey(m)] = m
		}
	}
	review := contractReview{}
	shownRemoved, shownAdded := map[Attribute]bool{}, map[Attribute]bool{}
	for _, change := range delta.Changes {
		if change.Entity != "metric" {
			continue
		}
		var row metricReview
		if change.Before != nil {
			m := change.Before.(Metric)
			row.Before = &m
		}
		if change.After != nil {
			m := change.After.(Metric)
			row.After = &m
		}
		m := reviewMetric(row)
		key := metricNameKey(m)
		if oldCounts[key] == 1 && newCounts[key] == 1 {
			if current, ok := added[key]; ok {
				if change.Action == "add" {
					continue
				}
				if change.Action == "remove" {
					row.After = &current
				}
			}
		}
		var oldAttrs, newAttrs []Attribute
		if row.Before != nil {
			oldAttrs = row.Before.Attributes
		}
		if row.After != nil {
			newAttrs = row.After.Attributes
		}
		row.Attributes = attributeDifferences(oldAttrs, newAttrs)
		for _, attr := range row.Attributes {
			for _, typ := range attr.Before {
				if !slices.Contains(attr.After, typ) {
					shownRemoved[Attribute{attr.Name, typ}] = true
				}
			}
			for _, typ := range attr.After {
				if !slices.Contains(attr.Before, typ) {
					shownAdded[Attribute{attr.Name, typ}] = true
				}
			}
		}
		review.Metrics = append(review.Metrics, row)
	}
	sort.Slice(review.Metrics, func(i, j int) bool {
		return metricKey(reviewMetric(review.Metrics[i])) < metricKey(reviewMetric(review.Metrics[j]))
	})
	var oldOther, newOther []Attribute
	for _, change := range delta.Changes {
		if change.Entity != "attribute_declaration" {
			continue
		}
		if change.Before != nil {
			a := change.Before.(Attribute)
			if !shownRemoved[a] {
				oldOther = append(oldOther, a)
			}
		}
		if change.After != nil {
			a := change.After.(Attribute)
			if !shownAdded[a] {
				newOther = append(newOther, a)
			}
		}
	}
	review.Other = attributeDifferences(oldOther, newOther)
	return review
}
func reviewMetric(row metricReview) Metric {
	if row.After != nil {
		return *row.After
	}
	return *row.Before
}
func attributeDifferences(before, after []Attribute) []attributeReview {
	oldTypes, newTypes := map[string][]string{}, map[string][]string{}
	names := map[string]bool{}
	for _, a := range before {
		oldTypes[a.Name] = append(oldTypes[a.Name], a.Type)
		names[a.Name] = true
	}
	for _, a := range after {
		newTypes[a.Name] = append(newTypes[a.Name], a.Type)
		names[a.Name] = true
	}
	var rows []attributeReview
	for name := range names {
		old, current := oldTypes[name], newTypes[name]
		sort.Strings(old)
		sort.Strings(current)
		if !slices.Equal(old, current) {
			rows = append(rows, attributeReview{name, old, current})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows
}
func metricAction(row metricReview) string {
	if row.Before == nil {
		return "Added"
	}
	if row.After == nil {
		return "Removed"
	}
	return "Changed"
}
func attributeAction(row attributeReview) string {
	if len(row.Before) == 0 {
		return "Added"
	}
	if len(row.After) == 0 {
		return "Removed"
	}
	return "Type changed"
}
func renderReport(review contractReview, changed bool) string {
	var report strings.Builder
	report.WriteString("# Telemetry contract\n\n")
	if changed {
		report.WriteString("**Telemetry changes detected — approval required.**\n\n")
	} else {
		report.WriteString("**No telemetry contract changes — no approval required.**\n\n")
	}
	report.WriteString("**Scope:** The full PR merge candidate versus its target base at collection time. This is not a comparison with the previous PR commit.\n\n")
	if changed {
		counts := map[string]int{}
		attrs := 0
		for _, row := range review.Metrics {
			counts[metricAction(row)]++
			attrs += len(row.Attributes)
		}
		report.WriteString("| Metrics added | Metrics removed | Metrics changed | Attribute/label changes |\n| ---: | ---: | ---: | ---: |\n")
		fmt.Fprintf(&report, "| %d | %d | %d | %d |\n\n", counts["Added"], counts["Removed"], counts["Changed"], attrs)
		report.WriteString("Attribute/label counts are per metric. Only changed attributes/labels are listed below.\n\n")
	}
	for _, row := range review.Metrics {
		m := reviewMetric(row)
		fmt.Fprintf(&report, "## %s metric: %s\n\n**Package:** %s\n\n", metricAction(row), markdownValue(m.Name), markdownValue(m.Package))
		report.WriteString("| Field | Target base | PR merge candidate |\n| --- | --- | --- |\n")
		fmt.Fprintf(&report, "| Instrument | %s | %s |\n", metricField(row.Before, "instrument"), metricField(row.After, "instrument"))
		fmt.Fprintf(&report, "| Unit | %s | %s |\n", metricField(row.Before, "unit"), metricField(row.After, "unit"))
		if (row.Before != nil && row.Before.ExternalAttributes) || (row.After != nil && row.After.ExternalAttributes) {
			fmt.Fprintf(&report, "| Caller-provided attributes | %s | %s |\n", metricField(row.Before, "external"), metricField(row.After, "external"))
		}
		report.WriteString("\n")
		if len(row.Attributes) > 0 {
			writeAttributeTable(&report, row.Attributes)
		} else if len(m.Attributes) > 0 {
			report.WriteString("Attributes/labels unchanged.\n\n")
		} else {
			report.WriteString("Attributes/labels: none.\n\n")
		}
	}
	if len(review.Other) > 0 {
		report.WriteString("## Other attribute declarations\n\nThese changes are not already shown with a metric above.\n\n")
		writeAttributeTable(&report, review.Other)
	}
	report.WriteString("The complete structured changes are in `delta.json`. This report compares captured source-level telemetry contracts; it does not verify runtime emission or extend the scanner's documented coverage.\n")
	return report.String()
}
func metricField(m *Metric, field string) string {
	if m == nil {
		return "—"
	}
	switch field {
	case "instrument":
		return markdownValue(m.Instrument)
	case "unit":
		return markdownValue(m.Unit)
	default:
		if m.ExternalAttributes {
			return "Yes"
		}
		return "No"
	}
}
func writeAttributeTable(out *strings.Builder, rows []attributeReview) {
	out.WriteString("| Change | Attribute / label | Target base type | PR merge candidate type |\n| --- | --- | --- | --- |\n")
	for _, row := range rows {
		fmt.Fprintf(out, "| %s | %s | %s | %s |\n", attributeAction(row), markdownValue(row.Name), typeNames(row.Before), typeNames(row.After))
	}
	out.WriteString("\n")
}
func typeNames(types []string) string {
	if len(types) == 0 {
		return "—"
	}
	return markdownValue(strings.Join(types, ", "))
}

// Source identifiers must not introduce Markdown rows, links, mentions or HTML.
func markdownValue(value string) string {
	if value == "" {
		return "(none)"
	}
	var out strings.Builder
	for _, r := range value {
		switch r {
		case '&':
			out.WriteString("&amp;")
		case '<':
			out.WriteString("&lt;")
		case '>':
			out.WriteString("&gt;")
		default:
			if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '\u2028' || r == '\u2029' {
				fmt.Fprintf(&out, "\\\\u%04x", r)
				continue
			}
			if strings.ContainsRune("\\`*_[]()#!|~:@", r) {
				out.WriteByte('\\')
			}
			out.WriteRune(r)
		}
	}
	return out.String()
}
func reviewOutputs(before, after Contract, delta Delta) (string, []byte) {
	review := buildReview(before, after, delta)
	var lines []string
	for _, row := range review.Metrics {
		m := reviewMetric(row)
		sign := map[string]string{"Added": "+", "Removed": "-", "Changed": "~"}[metricAction(row)]
		definition := m.Instrument + ", unit=" + m.Unit
		if row.Before != nil && row.After != nil {
			instrument, unit := row.After.Instrument, row.After.Unit
			if row.Before.Instrument != instrument {
				instrument = row.Before.Instrument + "→" + instrument
			}
			if row.Before.Unit != unit {
				unit = row.Before.Unit + "→" + unit
			}
			definition = instrument + ", unit=" + unit
		}
		line := sign + " metric " + m.Name + " (" + definition + ")"
		var attrs []string
		for _, a := range row.Attributes {
			attrs = append(attrs, attributeSummary(a))
		}
		if len(attrs) > 0 {
			line += "; attrs " + strings.Join(attrs, ", ")
		}
		if (row.Before != nil && row.Before.ExternalAttributes) != (row.After != nil && row.After.ExternalAttributes) {
			line += "; caller-provided attributes changed"
		}
		lines = append(lines, line)
	}
	for _, a := range review.Other {
		lines = append(lines, "declaration "+attributeSummary(a))
	}
	if len(lines) == 0 {
		lines = append(lines, "No telemetry contract changes.")
	}
	// Sanitize each line before concatenating: source text cannot forge new lines
	// in either the notice artifact or GitHub's single-line output protocol.
	for i, line := range lines {
		lines[i] = strings.Join(strings.FieldsFunc(line, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) }), " ")
	}
	short := limitText(strings.Join(lines, "; "), 180, "… see report")
	full := "Full PR merge candidate versus target base:\n" + strings.Join(lines, "\n")
	full = limitText(full, 2999, "\n… Truncated; see report.md and delta.json for all changes.") + "\n"
	return short, []byte(full)
}
func attributeSummary(a attributeReview) string {
	switch attributeAction(a) {
	case "Added":
		return "+" + a.Name + " (" + strings.Join(a.After, ", ") + ")"
	case "Removed":
		return "-" + a.Name + " (" + strings.Join(a.Before, ", ") + ")"
	default:
		return "~" + a.Name + " (" + strings.Join(a.Before, ", ") + "→" + strings.Join(a.After, ", ") + ")"
	}
}
func limitText(value string, max int, suffix string) string {
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max-len([]rune(suffix))]) + suffix
}
