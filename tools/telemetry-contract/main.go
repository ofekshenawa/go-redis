// telemetry-contract extracts the source-level telemetry schema without running
// Redis, package initializers, tests, or instrumentation callbacks.
package main

import (
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type Attribute struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type Metric struct {
	Package            string      `json:"package"`
	Name               string      `json:"name"`
	Instrument         string      `json:"instrument"`
	Unit               string      `json:"unit"`
	Attributes         []Attribute `json:"attributes"`
	ExternalAttributes bool        `json:"external_attributes,omitempty"`
}
type Contract struct {
	Version    int         `json:"version"`
	Metrics    []Metric    `json:"metrics"`
	Attributes []Attribute `json:"attribute_declarations"`
}

func main() {
	if err := runCLI(os.Args[1:], os.Stdout, os.Getenv("GITHUB_OUTPUT")); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "telemetry-contract:", err)
		os.Exit(2)
	}
}

// Differences are a successful comparison. The workflow decides whether that
// result needs approval; collection/input failures must never reach that gate.
func runCLI(args []string, stdout io.Writer, githubOutput string) error {
	flags := flag.NewFlagSet("telemetry-contract", flag.ContinueOnError)
	flags.SetOutput(stdout)
	root := flags.String("root", "", "repository root to analyze")
	output := flags.String("output", "", "write the collected contract JSON")
	base := flags.String("base", "", "captured base contract JSON")
	candidate := flags.String("candidate", "", "captured merge-candidate contract JSON")
	reportDir := flags.String("report-dir", "", "write delta.json and report.md here")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	comparing := *base != "" || *candidate != "" || *reportDir != ""
	if comparing {
		if *base == "" || *candidate == "" || *reportDir == "" || *output != "" || *root != "" {
			return fmt.Errorf("comparison requires -base, -candidate and -report-dir; do not use -root or -output")
		}
		before, err := readContract(*base)
		if err != nil {
			return fmt.Errorf("base: %w", err)
		}
		after, err := readContract(*candidate)
		if err != nil {
			return fmt.Errorf("candidate: %w", err)
		}
		delta, report := compareContracts(before, after)
		reviewSummary, reviewText := reviewOutputs(before, after, delta)
		deltaData, err := jsonBytes(delta)
		if err != nil {
			return err
		}
		if err = os.MkdirAll(*reportDir, 0755); err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(*reportDir, "delta.json"), deltaData, 0644); err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(*reportDir, "report.md"), report, 0644); err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(*reportDir, "review.txt"), reviewText, 0644); err != nil {
			return err
		}
		if githubOutput != "" {
			file, err := os.OpenFile(githubOutput, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
			if err != nil {
				return err
			}
			_, writeErr := fmt.Fprintf(file, "has_changes=%t\nreport_sha256=%x\ndelta_sha256=%x\nreview_summary=%s\n", delta.HasChanges, sha256.Sum256(report), sha256.Sum256(deltaData), reviewSummary)
			closeErr := file.Close()
			if writeErr != nil {
				return writeErr
			}
			if closeErr != nil {
				return closeErr
			}
		}
		_, err = stdout.Write(report)
		return err
	}
	if *output == "" || *root == "" {
		return fmt.Errorf("capture requires -root PATH -output FILE")
	}
	abs, err := filepath.Abs(*root)
	if err != nil {
		return err
	}
	c, err := collect(abs)
	if err != nil {
		return err
	}
	if err = normalizeContract(&c); err != nil {
		return err
	}
	data, err := jsonBytes(c)
	if err != nil {
		return err
	}
	if err = os.WriteFile(*output, data, 0644); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "Captured %d metric variants and %d attribute declarations in %s.\n", len(c.Metrics), len(c.Attributes), *output)
	return err
}

func metricID(m Metric) string {
	return m.Package + " / " + m.Name + " [" + m.Instrument + ", unit=" + m.Unit + "]"
}
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
