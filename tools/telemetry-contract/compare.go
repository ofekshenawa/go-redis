package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
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

// jsonBytes formats a value for the report artifact, including its final newline.
func jsonBytes(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	return append(data, '\n'), err
}

// readContract reads a single inventory from disk, validates its fields, and
// sorts it for comparison. A malformed inventory stops collection.
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

// normalizeContract checks the inventory version, metric identities, and
// attributes, then sorts them in place so source order cannot change the diff.
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
		// separator. Source strings are rendered only by the trusted PR presenter.
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

// validateAttrs checks names and supported types and rejects duplicate name/type pairs.
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

// metricKey identifies a metric by package, name, instrument, and unit. A change
// to one of these fields appears as a removal and an addition in the report.
func metricKey(m Metric) string {
	return strings.Join([]string{m.Package, m.Name, m.Instrument, m.Unit}, "\x00")
}

// compareContracts compares the normalized target and proposed-merge inventories.
// It returns a sorted list of metric and attribute-declaration changes plus a
// Markdown summary. For a matching metric identity, changed attributes produce
// one modification with the complete before and after values.
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
	return delta, summaryReport(delta)
}

// summaryReport produces the collector's Markdown counts. The trusted publisher
// renders the detailed diff from the before and after values in delta.json.
func summaryReport(delta Delta) []byte {
	if !delta.HasChanges {
		return []byte("# Telemetry contract unchanged\n\nNo telemetry contract changes.\n")
	}
	metrics, declarations := 0, 0
	for _, change := range delta.Changes {
		if change.Entity == "metric" {
			metrics++
		} else {
			declarations++
		}
	}
	return []byte(fmt.Sprintf("# Telemetry contract — approval required\n\n"+
		"Comparison: full PR merge candidate versus its target base.\n\n"+
		"- Metric changes: %d\n- Attribute declaration changes: %d\n\n"+
		"See the PR review comment for the colored diff, or `delta.json` in this artifact for all before/after values.\n",
		metrics, declarations))
}
