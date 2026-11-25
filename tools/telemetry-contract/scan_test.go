package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const fixtureHeader = `package fixture
import (
 "context"
 "os"
 "go.opentelemetry.io/otel/attribute"
 "go.opentelemetry.io/otel/metric"
 "go.opentelemetry.io/otel/semconv/v1.38.0/dbconv"
 "github.com/prometheus/client_golang/prometheus"
)
var _ = context.Background
var _ = os.Getenv
var _ = attribute.String
var _ metric.Meter
var _ = dbconv.NewClientOperationDuration
var _ = prometheus.NewDesc
// A source scanner must never execute this.
func init() { panic("do not execute the target package") }
`
const basic = `
func unused(m metric.Meter) {
 c, _ := m.Int64Counter("requests", metric.WithUnit("{request}"))
 c.Add(context.Background(), 1, metric.WithAttributes(attribute.String("status", "ok")))
}
`

func fixture(t *testing.T, source string) string {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"go.mod", "go.sum"} {
		data, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	writeSource(t, root, source)
	return root
}
func writeSource(t *testing.T, root, source string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, "fixture.go"), []byte(fixtureHeader+source), 0644); err != nil {
		t.Fatal(err)
	}
}
func scan(t *testing.T, root string) Contract {
	t.Helper()
	c, err := collect(root)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func findMetric(t *testing.T, c Contract, name string) Metric {
	t.Helper()
	for _, m := range c.Metrics {
		if m.Name == name {
			return m
		}
	}
	t.Fatalf("metric %q missing: %+v", name, c)
	return Metric{}
}
func assertAttrs(t *testing.T, m Metric, want ...Attribute) {
	t.Helper()
	if want == nil {
		want = []Attribute{}
	}
	sortAttrs(want)
	if !reflect.DeepEqual(m.Attributes, want) {
		t.Fatalf("%s attributes: got %+v, want %+v", m.Name, m.Attributes, want)
	}
}
func TestStaticInventory(t *testing.T) {
	root := fixture(t, basic)
	before := scan(t, root)
	if len(before.Metrics) != 1 {
		t.Fatal(before)
	}
	m := findMetric(t, before, "requests")
	if m.Instrument != "Int64Counter" || m.Unit != "{request}" {
		t.Fatal(m)
	}
	assertAttrs(t, m, Attribute{"status", "string"})
	again := scan(t, root)
	if !reflect.DeepEqual(before, again) {
		t.Fatal("nondeterministic output")
	}
	writeSource(t, root, strings.ReplaceAll(basic, "1, metric", "999, metric")+"\n// Formatting and sample values do not affect the schema.\n")
	if got := scan(t, root); !reflect.DeepEqual(before, got) {
		t.Fatal("sample values changed contract")
	}
	cases := []struct{ name, source, report string }{
		{"unreachable metric", basic + `func hidden(m metric.Meter) {if false {_, _ = m.Float64Histogram("dormant")}}`, "+ metric:"},
		{"new attribute", strings.Replace(basic, `attribute.String("status", "ok")`, `attribute.String("status", "ok"), attribute.Int("retries", 3)`, 1), "retries (int64)"},
		{"attribute type", strings.Replace(basic, `attribute.String("status", "ok")`, `attribute.Int64("status", 1)`, 1), "status (int64)"},
		{"rename", strings.ReplaceAll(basic, `"requests"`, `"renamed"`), "- metric:"},
		{"instrument", strings.ReplaceAll(basic, "Int64Counter", "Int64UpDownCounter"), "Int64UpDownCounter"},
		{"unit", strings.ReplaceAll(basic, "{request}", "{operation}"), "unit={operation}"},
		{"remove attribute", strings.ReplaceAll(basic, `, metric.WithAttributes(attribute.String("status", "ok"))`, ""), "- attribute:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeSource(t, root, tc.source)
			after := scan(t, root)
			report := strings.Join(describe(before, after), "\n")
			if !strings.Contains(report, tc.report) {
				t.Fatalf("wanted %q in %s", tc.report, report)
			}
		})
	}
	// Removing a declaration is a reviewable diff, not a collection failure.
	writeSource(t, root, basic+`func hidden(m metric.Meter) {_, _ = m.Float64Histogram("dormant")}`)
	added := scan(t, root)
	if report := strings.Join(describe(added, before), "\n"); !strings.Contains(report, "- metric: example.com/telemetry-fixture / dormant") {
		t.Fatal(report)
	}
}
func TestHelpersAndMetricAttributeAssociation(t *testing.T) {
	root := fixture(t, `
 const prefix = "requests."
 func attrs(a []attribute.KeyValue) []attribute.KeyValue {return append(a,attribute.Bool("shared",true))}
 func pair() (a,b []attribute.KeyValue) {return []attribute.KeyValue{attribute.Int("first",1)}, []attribute.KeyValue{attribute.Int64("second",2)}}
 func unused(m metric.Meter) {
  x,_:=m.Int64Counter(prefix+"x"); y,_:=m.Int64Counter(prefix+"y")
  a,b:=pair()
  x.Add(context.Background(),1,metric.WithAttributes(attrs(a)...))
  y.Add(context.Background(),1,metric.WithAttributeSet(attribute.NewSet(attrs(b)...)))
 }
 `)
	c := scan(t, root)
	assertAttrs(t, findMetric(t, c, "requests.x"), Attribute{"first", "int64"}, Attribute{"shared", "bool"})
	assertAttrs(t, findMetric(t, c, "requests.y"), Attribute{"second", "int64"}, Attribute{"shared", "bool"})
	data, _ := os.ReadFile(filepath.Join(root, "fixture.go"))
	data = []byte(strings.Replace(string(data), "attrs(b)...", `attrs(append(b,attribute.Int("first",1)))...`, 1))
	if err := os.WriteFile(filepath.Join(root, "fixture.go"), data, 0644); err != nil {
		t.Fatal(err)
	}
	after := scan(t, root)
	report := strings.Join(describe(c, after), "\n")
	if !strings.Contains(report, "requests.y") || !strings.Contains(report, "first (int64)") {
		t.Fatal(report)
	}
}
func TestStandardSliceHelpersPreserveMetricAttributes(t *testing.T) {
	root := fixture(t, "")
	source := strings.Replace(fixtureHeader, `"context"`, `"context"
 "slices"`, 1) + `
 func unused(m metric.Meter) {
  first,_:=m.Int64Counter("first"); second,_:=m.Int64Counter("second")
  left:=[]attribute.KeyValue{attribute.String("left","value")}
  right:=[]attribute.KeyValue{attribute.Int("right",1)}
  merged:=slices.Concat(left,slices.Clone(right))
  first.Add(context.Background(),1,metric.WithAttributes(merged...))
  second.Add(context.Background(),1,metric.WithAttributes(slices.Clone(left)...))
 }
 `
	if err := os.WriteFile(filepath.Join(root, "fixture.go"), []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	c := scan(t, root)
	assertAttrs(t, findMetric(t, c, "first"), Attribute{"left", "string"}, Attribute{"right", "int64"})
	assertAttrs(t, findMetric(t, c, "second"), Attribute{"left", "string"})
}
func TestSemanticConventionsAndLiteralAttributes(t *testing.T) {
	root := fixture(t, `
 func unused(m metric.Meter) {
  h,_:=dbconv.NewClientOperationDuration(m)
  h.Inst().Record(context.Background(),1,metric.WithAttributes(
   attribute.KeyValue{Key:"literal",Value:attribute.BoolValue(true)},
   attribute.Key("typed").Int64(2),
  ))
 }
 `)
	c := scan(t, root)
	m := findMetric(t, c, "db.client.operation.duration")
	if m.Unit != "s" {
		t.Fatal(m)
	}
	assertAttrs(t, m, Attribute{"literal", "bool"}, Attribute{"typed", "int64"})
	if !reflect.DeepEqual(c.Attributes, m.Attributes) {
		t.Fatalf("global declarations: %+v", c.Attributes)
	}
}
func TestPrometheusLabels(t *testing.T) {
	root := fixture(t, `
 func NewCollector(namespace, subsystem string) {
  d:=prometheus.NewDesc(prometheus.BuildFQName(namespace,subsystem,"requests"),"description",[]string{"state"},prometheus.Labels{"library":"redis"})
  _=prometheus.MustNewConstMetric(d,prometheus.CounterValue,1,"ok")
 }
 `)
	c := scan(t, root)
	m := findMetric(t, c, "{namespace}_{subsystem}_requests")
	if m.Instrument != "PrometheusCounter" {
		t.Fatal(m)
	}
	assertAttrs(t, m, Attribute{"state", "string"}, Attribute{"library", "string"})
}
func TestUnsupportedSourcesFailClosed(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"dynamic metric", `func unused(m metric.Meter) {_,_=m.Int64Counter(os.Getenv("NAME"))}`, "metric name is not statically resolvable"},
		{"mixed dynamic metric", `func unused(m metric.Meter) {name:="known";if false {name=os.Getenv("NAME")};_,_=m.Int64Counter(name)}`, "metric name is not statically resolvable"},
		{"dynamic attribute", basic + `var hidden=attribute.String(os.Getenv("KEY"),"value")`, "not statically resolvable"},
		{"dynamic literal", basic + `var hidden=attribute.KeyValue{Key:attribute.Key(os.Getenv("KEY")),Value:attribute.BoolValue(true)}`, "not statically resolvable"},
		{"attribute mutation", basic + `func hidden() {var kv attribute.KeyValue; kv.Key="hidden"; kv.Value=attribute.StringValue("value")}`, "mutating attribute.KeyValue"},
		{"untyped literal", basic + `var hidden=attribute.KeyValue{Key:"hidden", Value:attribute.Value{}}`, "literal type is not statically resolvable"},
		{"dynamic unit", strings.ReplaceAll(basic, `"{request}"`, `os.Getenv("UNIT")`), "not statically resolvable"},
		{"indirect constructor", `func unused(m metric.Meter) {makeCounter:=m.Int64Counter;_,_=makeCounter("hidden")}`, "indirect telemetry call"},
		{"unknown instrument", `func unused(c metric.Int64Counter) {c.Add(context.Background(),1)}`, "could not be linked"},
		{"dynamic labels", `func NewCollector(labels []string) {_=prometheus.NewDesc("x","help",labels,nil)}`, "labels must be static"},
		{"dynamic suffix", `func NewCollector(name string) {_=prometheus.NewDesc(prometheus.BuildFQName("ns","ss",name),"help",nil,nil)}`, "not statically resolvable"},
		{"empty", ``, "empty telemetry inventory"},
		{"type error", basic + `var invalid int="bad"`, "source/type analysis failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := collect(fixture(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %q, got %v", tc.want, err)
			}
		})
	}
}
func TestBuildConstraintsFailClosed(t *testing.T) {
	root := fixture(t, basic)
	source := "//go:build telemetry_hidden\n\n" + fixtureHeader + `func hidden(m metric.Meter) {_,_=m.Int64Counter("hidden")}`
	if err := os.WriteFile(filepath.Join(root, "hidden.go"), []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := collect(root)
	if err == nil || !strings.Contains(err.Error(), "excluded by build constraints") {
		t.Fatal(err)
	}
}
func TestDiscoveryOfNewModule(t *testing.T) {
	root := fixture(t, basic)
	newModule := filepath.Join(root, "extra", "new")
	if err := os.MkdirAll(newModule, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go.mod", "go.sum"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if name == "go.mod" {
			data = []byte(strings.Replace(string(data), "example.com/telemetry-fixture", "example.com/added", 1))
		}
		if err = os.WriteFile(filepath.Join(newModule, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	writeSource(t, newModule, strings.ReplaceAll(basic, `"requests"`, `"new.module"`))
	c := scan(t, root)
	if len(c.Metrics) != 2 || findMetric(t, c, "new.module").Package != "example.com/added" {
		t.Fatal(c)
	}
}
func TestReportIncludesTypesAndUnits(t *testing.T) {
	a := Contract{}
	b := Contract{Metrics: []Metric{{Package: "p", Name: "m", Instrument: "Float64Histogram", Unit: "s", Attributes: []Attribute{{"reason", "string"}}}}}
	for _, line := range []string{"Float64Histogram", "unit=s", "reason (string)"} {
		if !strings.Contains(strings.Join(describe(a, b), "\n"), line) {
			t.Fatal(line)
		}
	}
	// JSON must encode an empty attribute list as [] rather than null.
	data, err := json.Marshal(Metric{Attributes: []Attribute{}})
	if err != nil || !strings.Contains(string(data), `"attributes":[]`) {
		t.Fatal(string(data), err)
	}
}

func TestIndexedAttributesAndMultiResultDeclaration(t *testing.T) {
	root := fixture(t, `
 func pair() ([]attribute.KeyValue, []attribute.KeyValue) {return nil, []attribute.KeyValue{attribute.String("second", "v")}}
 func unused(m metric.Meter) {
  c,_:=m.Int64Counter("requests")
  var _, attrs = pair()
  keys:=make([]attribute.KeyValue,1); keys[0]=attribute.Bool("indexed",true)
  c.Add(context.Background(),1,metric.WithAttributes(append(attrs,keys...)...))
 }
 `)
	assertAttrs(t, findMetric(t, scan(t, root), "requests"), Attribute{"indexed", "bool"}, Attribute{"second", "string"})
}

func TestPrometheusAssignedLabels(t *testing.T) {
	root := fixture(t, `
 func unused() {
  labels:=prometheus.Labels{"library":"redis"};labels["state"]="ok"
  d:=prometheus.NewDesc("requests","help",nil,labels)
  _=prometheus.MustNewConstMetric(d,prometheus.CounterValue,1)
 }
 `)
	assertAttrs(t, findMetric(t, scan(t, root), "requests"), Attribute{"library", "string"}, Attribute{"state", "string"})
}

// Test-only inventory view for scanner assertions.

func describe(before, after Contract) []string {
	rows := func(c Contract) map[string]string {
		out := map[string]string{}
		for _, m := range c.Metrics {
			id := metricID(m)
			out["metric: "+id] = ""
			for _, a := range m.Attributes {
				out["attribute: "+id+" / "+a.Name+" ("+a.Type+")"] = ""
			}
			if m.ExternalAttributes {
				out["caller-provided attributes: "+id] = ""
			}
		}
		for _, a := range c.Attributes {
			out["attribute declaration: "+a.Name+" ("+a.Type+")"] = ""
		}
		return out
	}
	a, b := rows(before), rows(after)
	var lines []string
	for k := range a {
		if _, ok := b[k]; !ok {
			lines = append(lines, "- "+k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			lines = append(lines, "+ "+k)
		}
	}
	sort.Strings(lines)
	if len(lines) == 0 {
		lines = append(lines, "No telemetry contract changes.")
	}
	return lines
}
