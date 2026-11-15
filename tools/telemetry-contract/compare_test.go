package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sampleContract() Contract {
	return Contract{Version: 1, Metrics: []Metric{{Package: "example.com/client", Name: "requests", Instrument: "Int64Counter", Unit: "{request}", Attributes: []Attribute{{"status", "string"}}}}, Attributes: []Attribute{{"status", "string"}}}
}
func putContract(t *testing.T, path string, c Contract) {
	t.Helper()
	data, err := jsonBytes(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}
func TestCompareCLIChangesAreSuccessful(t *testing.T) {
	root := t.TempDir()
	base, candidate := filepath.Join(root, "base.json"), filepath.Join(root, "candidate.json")
	putContract(t, base, sampleContract())
	next := sampleContract()
	next.Metrics[0].Attributes = append(next.Metrics[0].Attributes, Attribute{"retries", "int64"})
	next.Attributes = append(next.Attributes, Attribute{"retries", "int64"})
	putContract(t, candidate, next)
	out, reportDir := filepath.Join(root, "outputs"), filepath.Join(root, "report")
	var stdout bytes.Buffer
	if err := runCLI([]string{"-base", base, "-candidate", candidate, "-report-dir", reportDir}, &stdout, out); err != nil {
		t.Fatal(err)
	}
	var delta Delta
	deltaData, err := os.ReadFile(filepath.Join(reportDir, "delta.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(deltaData, &delta); err != nil {
		t.Fatal(err)
	}
	if !delta.HasChanges || len(delta.Changes) != 2 {
		t.Fatalf("unexpected delta: %+v", delta)
	}
	var modified *Change
	for i := range delta.Changes {
		if delta.Changes[i].Entity == "metric" {
			modified = &delta.Changes[i]
		}
	}
	if modified == nil || modified.Action != "modify" || modified.Before == nil || modified.After == nil {
		t.Fatalf("missing before/after modification: %+v", delta)
	}
	report, err := os.ReadFile(filepath.Join(reportDir, "report.md"))
	if err != nil {
		t.Fatal(err)
	}
	outputs, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"has_changes=true", fmt.Sprintf("report_sha256=%x", sha256.Sum256(report)), fmt.Sprintf("delta_sha256=%x", sha256.Sum256(deltaData))} {
		if !strings.Contains(string(outputs), line+"\n") {
			t.Fatalf("missing %s in %s", line, outputs)
		}
	}
	if !strings.Contains(stdout.String(), "approval required") || !strings.Contains(stdout.String(), "| Added | retries | — | int64 |") {
		t.Fatal(stdout.String())
	}
	if !strings.Contains(string(outputs), "review_summary=~ metric requests (Int64Counter, unit={request}); attrs +retries (int64)\n") {
		t.Fatal(string(outputs))
	}
	review, err := os.ReadFile(filepath.Join(reportDir, "review.txt"))
	if err != nil || !bytes.Contains(review, []byte("Full PR merge candidate versus target base:")) || !bytes.Contains(review, []byte("+retries (int64)")) {
		t.Fatalf("review artifact: %s, %v", review, err)
	}
}
func TestComparisonNormalizesOrderingAndWhitespace(t *testing.T) {
	root := t.TempDir()
	base, candidate := filepath.Join(root, "base.json"), filepath.Join(root, "candidate.json")
	c := sampleContract()
	c.Metrics[0].Attributes = append(c.Metrics[0].Attributes, Attribute{"attempt", "int64"})
	c.Attributes = append(c.Attributes, Attribute{"attempt", "int64"})
	c.Metrics = append(c.Metrics, Metric{Package: "example.com/client", Name: "duration", Instrument: "Float64Histogram", Unit: "s", Attributes: []Attribute{}})
	putContract(t, base, c)
	c.Metrics[0].Attributes[0], c.Metrics[0].Attributes[1] = c.Metrics[0].Attributes[1], c.Metrics[0].Attributes[0]
	c.Attributes[0], c.Attributes[1] = c.Attributes[1], c.Attributes[0]
	c.Metrics[0], c.Metrics[1] = c.Metrics[1], c.Metrics[0]
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(candidate, data, 0644); err != nil {
		t.Fatal(err)
	}
	var firstDelta, firstReport []byte
	for i := 0; i < 3; i++ {
		dir := filepath.Join(root, fmt.Sprint(i))
		if err = runCLI([]string{"-base", base, "-candidate", candidate, "-report-dir", dir}, &bytes.Buffer{}, ""); err != nil {
			t.Fatal(err)
		}
		delta, _ := os.ReadFile(filepath.Join(dir, "delta.json"))
		report, _ := os.ReadFile(filepath.Join(dir, "report.md"))
		if !strings.Contains(string(delta), `"has_changes": false`) || !strings.Contains(string(delta), `"changes": []`) {
			t.Fatal(string(delta))
		}
		if i == 0 {
			firstDelta, firstReport = delta, report
		} else if !bytes.Equal(firstDelta, delta) || !bytes.Equal(firstReport, report) {
			t.Fatal("report is not deterministic")
		}
	}
}
func TestMetricRemovalAndTypeUnitChanges(t *testing.T) {
	before := sampleContract()
	before.Metrics = append(before.Metrics, Metric{Package: "p", Name: "removed", Instrument: "Float64Histogram", Unit: "s", Attributes: []Attribute{}})
	after := sampleContract()
	after.Metrics[0].Unit = "{operation}"
	after.Metrics[0].Instrument = "Int64UpDownCounter"
	delta, _ := compareContracts(before, after)
	if !delta.HasChanges || len(delta.Changes) != 3 {
		t.Fatal(delta)
	}
	removed, added := 0, 0
	for _, c := range delta.Changes {
		switch c.Action {
		case "remove":
			removed++
			if c.Before == nil || c.After != nil {
				t.Fatal(c)
			}
		case "add":
			added++
			if c.Before != nil || c.After == nil {
				t.Fatal(c)
			}
		}
	}
	if removed != 2 || added != 1 {
		t.Fatal(delta)
	}
}
func TestMalformedContractsFailBeforePublishing(t *testing.T) {
	goodData, _ := jsonBytes(sampleContract())
	good := string(goodData)
	cases := map[string]string{
		"syntax":                 "{",
		"second object":          good + "{}",
		"unknown field":          strings.Replace(good, `"version": 1`, `"version": 1, "allow_all": true`, 1),
		"version":                strings.Replace(good, `"version": 1`, `"version": 2`, 1),
		"missing version":        strings.Replace(good, `"version": 1,`, "", 1),
		"missing unit":           strings.Replace(good, `"unit": "{request}",`, "", 1),
		"null unit":              strings.Replace(good, `"unit": "{request}"`, `"unit": null`, 1),
		"empty package":          strings.Replace(good, `"example.com/client"`, `""`, 1),
		"unknown attribute type": strings.ReplaceAll(good, `"type": "string"`, `"type": "object"`),
		"empty inventory":        `{"version":1,"metrics":[],"attribute_declarations":[]}`,
		"null declarations":      `{"version":1,"metrics":[],"attribute_declarations":null}`,
	}
	duplicate := sampleContract()
	duplicate.Metrics = append(duplicate.Metrics, duplicate.Metrics[0])
	data, _ := jsonBytes(duplicate)
	cases["duplicate metric"] = string(data)
	duplicate = sampleContract()
	duplicate.Metrics[0].Attributes = append(duplicate.Metrics[0].Attributes, duplicate.Metrics[0].Attributes[0])
	data, _ = jsonBytes(duplicate)
	cases["duplicate attribute"] = string(data)
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			base, candidate := filepath.Join(root, "base.json"), filepath.Join(root, "candidate.json")
			putContract(t, base, sampleContract())
			if err := os.WriteFile(candidate, []byte(bad), 0644); err != nil {
				t.Fatal(err)
			}
			dir, outputs := filepath.Join(root, "report"), filepath.Join(root, "outputs")
			err := runCLI([]string{"-base", base, "-candidate", candidate, "-report-dir", dir}, &bytes.Buffer{}, outputs)
			if err == nil {
				t.Fatal("invalid contract accepted")
			}
			if _, err = os.Stat(dir); !os.IsNotExist(err) {
				t.Fatal("published a report on failure")
			}
			if _, err = os.Stat(outputs); !os.IsNotExist(err) {
				t.Fatal("published successful outputs on failure")
			}
		})
	}
}
func TestCaptureCLIHasNoGoldenFile(t *testing.T) {
	root := fixture(t, basic)
	output := filepath.Join(t.TempDir(), "captured.json")
	if err := runCLI([]string{"-root", root, "-output", output}, &bytes.Buffer{}, ""); err != nil {
		t.Fatal(err)
	}
	c, err := readContract(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Metrics) != 1 || c.Metrics[0].Name != "requests" {
		t.Fatal(c)
	}
	if _, err = os.Stat(filepath.Join(root, "contract.json")); !os.IsNotExist(err) {
		t.Fatal("created a golden file")
	}
	writeSource(t, root, `func bad(m metric.Meter) {_,_=m.Int64Counter(os.Getenv("DYNAMIC_NAME"))}`)
	invalidOutput := filepath.Join(t.TempDir(), "invalid.json")
	if err = runCLI([]string{"-root", root, "-output", invalidOutput}, &bytes.Buffer{}, ""); err == nil {
		t.Fatal("capture should fail")
	}
	if _, err = os.Stat(invalidOutput); !os.IsNotExist(err) {
		t.Fatal("wrote failed capture")
	}
}
func TestReportEscapesSourceText(t *testing.T) {
	before := sampleContract()
	after := sampleContract()
	after.Metrics[0].Attributes = append(after.Metrics[0].Attributes, Attribute{"</pre><script>bad</script>", "string"})
	_, report := compareContracts(before, after)
	if bytes.Contains(report, []byte("<script>")) || !bytes.Contains(report, []byte("&lt;script&gt;")) {
		t.Fatal(string(report))
	}
}
func TestCLIRequiresExplicitModes(t *testing.T) {
	for _, args := range [][]string{{}, {"-update"}, {"-output", "capture.json"}, {"-base", "base.json"}, {"-base", "base", "-candidate", "candidate", "-report-dir", "report", "-output", "file"}, {"-base", "base", "-candidate", "candidate", "-report-dir", "report", "-root", "source"}, {"unexpected"}} {
		if err := runCLI(args, &bytes.Buffer{}, ""); err == nil {
			t.Fatalf("accepted invalid arguments: %v", args)
		}
	}
}

func TestReportExplainsDefinitionsAndAttributeChanges(t *testing.T) {
	before, after := sampleContract(), sampleContract()
	before.Metrics[0].Attributes = append(before.Metrics[0].Attributes, Attribute{"removed", "bool"}, Attribute{"unchanged", "int64"})
	after.Metrics[0].Instrument = "Float64Counter"
	after.Metrics[0].Unit = "{operation}"
	after.Metrics[0].ExternalAttributes = true
	after.Metrics[0].Attributes = []Attribute{{"status", "int64"}, {"added", "string"}, {"unchanged", "int64"}}
	before.Attributes = append(append([]Attribute{}, before.Metrics[0].Attributes...), Attribute{"unattached", "bool"})
	after.Attributes = append(append([]Attribute{}, after.Metrics[0].Attributes...), Attribute{"unattached", "int64"})
	delta, report := compareContracts(before, after)
	text := string(report)
	for _, want := range []string{
		"full PR merge candidate versus its target base", "not a comparison with the previous PR commit",
		"| 0 | 0 | 1 | 3 |", "## Changed metric: requests",
		"| Instrument | Int64Counter | Float64Counter |", "| Unit | {request} | {operation} |",
		"| Caller-provided attributes | No | Yes |", "| Type changed | status | string | int64 |",
		"| Added | added | — | string |", "| Removed | removed | bool | — |",
		"## Other attribute declarations", "| Type changed | unattached | bool | int64 |",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %s", want, text)
		}
	}
	if strings.Count(text, "| status |") != 1 || strings.Contains(text, "| unchanged |") {
		t.Fatal("duplicated declaration or included unchanged attribute:", text)
	}
	// Presentation pairing must not alter the original structured remove/add delta.
	actions := map[string]int{}
	for _, change := range delta.Changes {
		if change.Entity == "metric" {
			actions[change.Action]++
		}
	}
	if actions["remove"] != 1 || actions["add"] != 1 || actions["modify"] != 0 {
		t.Fatal(actions)
	}
}
func TestReportShowsEveryRemovedMetric(t *testing.T) {
	before := sampleContract()
	before.Metrics = append(before.Metrics, Metric{Package: "p", Name: "latency", Instrument: "Float64Histogram", Unit: "s", Attributes: []Attribute{}})
	after := Contract{Version: 1, Metrics: []Metric{}, Attributes: []Attribute{}}
	_, report := compareContracts(before, after)
	for _, want := range []string{"| 0 | 2 | 0 | 1 |", "## Removed metric: requests", "## Removed metric: latency", "| Instrument | Float64Histogram | — |", "| Unit | s | — |", "| Removed | status | string | — |"} {
		if !strings.Contains(string(report), want) {
			t.Fatalf("missing %q in %s", want, report)
		}
	}
	if strings.Contains(string(report), "Other attribute declarations") {
		t.Fatal("removed attribute duplicated", string(report))
	}
}
func TestChangedReportsAreDeterministic(t *testing.T) {
	before, after := sampleContract(), sampleContract()
	after.Metrics[0].Attributes = append(after.Metrics[0].Attributes, Attribute{"new", "string"})
	after.Metrics = append(after.Metrics, Metric{Package: "p", Name: "added", Instrument: "Float64Histogram", Unit: "s", Attributes: []Attribute{}})
	after.Attributes = append(after.Attributes, Attribute{"global", "bool"}, Attribute{"new", "string"})
	d, want := compareContracts(before, after)
	wantDelta, _ := jsonBytes(d)
	for i := 0; i < 5; i++ {
		after.Metrics[0], after.Metrics[1] = after.Metrics[1], after.Metrics[0]
		after.Attributes[0], after.Attributes[2] = after.Attributes[2], after.Attributes[0]
		delta, got := compareContracts(before, after)
		data, _ := jsonBytes(delta)
		if !bytes.Equal(want, got) || !bytes.Equal(wantDelta, data) {
			t.Fatal("non-deterministic report or delta")
		}
	}
}
func TestReportCannotInjectMarkdownRowsOrLinks(t *testing.T) {
	before, after := sampleContract(), sampleContract()
	after.Metrics[0].Name = "evil|name\n## forged [click](https://evil.example)"
	after.Metrics[0].Unit = "</table><script>bad</script>"
	after.Metrics[0].Attributes = append(after.Metrics[0].Attributes, Attribute{"key|`name\r\n@reviewer", "string"})
	delta, report := compareContracts(before, after)
	for _, bad := range []string{"<script>", "\n## forged", "[click](https://evil.example)", "key|`name", "\r", "@reviewer"} {
		if strings.Contains(string(report), bad) && bad != "@reviewer" {
			t.Fatalf("unescaped %q: %s", bad, report)
		}
	}
	for _, want := range []string{"&lt;script&gt;", `evil\|name`, `\[click\]\(https\://evil.example\)`, `key\|\` + "`" + `name`, `\@reviewer`} {
		if !strings.Contains(string(report), want) {
			t.Fatalf("missing escaped %q in %s", want, report)
		}
	}
	short, full := reviewOutputs(before, after, delta)
	if strings.ContainsAny(short, "\r\n") || bytes.Contains(full, []byte("\n## forged")) {
		t.Fatalf("unsanitized summary %q / %s", short, full)
	}
}
func TestReviewSummariesAreBoundedAndTruncationIsExplicit(t *testing.T) {
	before, after := sampleContract(), sampleContract()
	for i := 0; i < 180; i++ {
		after.Metrics[0].Attributes = append(after.Metrics[0].Attributes, Attribute{fmt.Sprintf("attribute.%03d.שלום", i), "string"})
	}
	delta, report := compareContracts(before, after)
	short, full := reviewOutputs(before, after, delta)
	if len([]rune(short)) > 180 || len([]rune(string(full))) > 3000 {
		t.Fatalf("oversized summaries: %d/%d", len([]rune(short)), len([]rune(string(full))))
	}
	if !strings.HasSuffix(short, "… see report") || !bytes.Contains(full, []byte("Truncated; see report.md and delta.json for all changes.")) {
		t.Fatalf("silent truncation: %q / %s", short, full)
	}
	if !bytes.Contains(report, []byte("attribute.179.שלום")) || bytes.Contains(report, []byte("Truncated")) {
		t.Fatal("full Markdown report was truncated")
	}
}
