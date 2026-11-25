package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/packages"
)

const otel = "go.opentelemetry.io/otel/"
const prom = "github.com/prometheus/client_golang/prometheus"

// Discover production modules, including modules added by a PR. Parsing imports
// also sees build-tagged files: an unanalysed telemetry file is an error.
func load(root string) ([]*packages.Package, error) {
	type module struct {
		dir, path string
		imports   map[string]bool
		files     []string
		selected  bool
	}
	var modules []*module
	var source []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root {
				switch d.Name() {
				case "vendor", "example", "examples", "doctests", "fuzz", "testdata", "tools", "e2e":
					return filepath.SkipDir
				}
				if strings.HasPrefix(d.Name(), ".") {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in production source: %s", path)
		}
		if d.Name() == "go.mod" {
			data, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			for _, line := range strings.Split(string(data), "\n") {
				f := strings.Fields(line)
				if len(f) == 2 && f[0] == "module" {
					modules = append(modules, &module{dir: filepath.Dir(path), path: f[1], imports: map[string]bool{}})
					break
				}
			}
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			source = append(source, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(modules, func(i, j int) bool { return len(modules[i].dir) > len(modules[j].dir) })
	for _, path := range source {
		f, e := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if e != nil {
			return nil, e
		}
		for _, m := range modules {
			if strings.HasPrefix(path, m.dir+string(filepath.Separator)) {
				m.files = append(m.files, path)
				for _, im := range f.Imports {
					p, _ := strconv.Unquote(im.Path.Value)
					m.imports[p] = true
					if strings.HasPrefix(p, otel) || p == prom {
						m.selected = true
					}
				}
				break
			}
		}
	}
	// Include callers of local telemetry wrappers, even if they never import OTel.
	for changed := true; changed; {
		changed = false
		for _, m := range modules {
			if !m.selected {
				for _, dep := range modules {
					if dep.selected {
						for p := range m.imports {
							if p == dep.path || strings.HasPrefix(p, dep.path+"/") {
								m.selected = true
								changed = true
							}
						}
					}
				}
			}
		}
	}
	var all []*packages.Package
	for _, m := range modules {
		if m.selected {
			cfg := &packages.Config{Dir: m.dir, Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports | packages.NeedDeps | packages.NeedTypes | packages.NeedSyntax | packages.NeedTypesInfo | packages.NeedModule, Tests: false, Env: append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly", "CGO_ENABLED=0")}
			dirs := map[string]bool{}
			for _, file := range m.files {
				rel, e := filepath.Rel(m.dir, filepath.Dir(file))
				if e != nil {
					return nil, e
				}
				dirs["./"+filepath.ToSlash(rel)] = true
			}
			patterns := make([]string, 0, len(dirs))
			for dir := range dirs {
				patterns = append(patterns, dir)
			}
			sort.Strings(patterns)
			pkgs, e := packages.Load(cfg, patterns...)
			if e != nil {
				return nil, e
			}
			if len(pkgs) == 0 {
				return nil, fmt.Errorf("no packages in %s", m.dir)
			}
			var failures []string
			packages.Visit(pkgs, nil, func(p *packages.Package) {
				for _, e := range p.Errors {
					failures = append(failures, e.Error())
				}
			})
			if len(failures) > 0 {
				sort.Strings(failures)
				return nil, fmt.Errorf("source/type analysis failed:\n%s", strings.Join(failures, "\n"))
			}
			seen := map[string]bool{}
			for _, p := range pkgs {
				for _, f := range p.CompiledGoFiles {
					seen[f] = true
				}
			}
			for _, f := range m.files {
				if !seen[f] {
					return nil, fmt.Errorf("production file excluded by build constraints: %s; add a supported build profile before accepting its telemetry", f)
				}
			}
			all = append(all, pkgs...)
		}
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("no production telemetry modules found")
	}
	return all, nil
}

func collect(root string) (Contract, error) {
	pkgs, err := load(root)
	if err != nil {
		return Contract{}, err
	}
	result := Contract{Version: 1, Metrics: []Metric{}, Attributes: []Attribute{}}
	attr := map[Attribute]bool{}
	for _, p := range pkgs {
		if strings.Contains(p.PkgPath, "/example/") {
			continue
		}
		g := newGraph(p)
		if err = g.build(); err != nil {
			return Contract{}, err
		}
		if err = g.solve(); err != nil {
			return Contract{}, err
		}
		ms, as, err := g.contract()
		if err != nil {
			return Contract{}, err
		}
		result.Metrics = append(result.Metrics, ms...)
		for _, a := range as {
			attr[a] = true
		}
	}
	for a := range attr {
		result.Attributes = append(result.Attributes, a)
	}
	sortAttrs(result.Attributes)
	sort.Slice(result.Metrics, func(i, j int) bool { return metricID(result.Metrics[i]) < metricID(result.Metrics[j]) })
	if len(result.Metrics) == 0 {
		return Contract{}, fmt.Errorf("empty telemetry inventory; refusing to publish a capture")
	}
	return result, nil
}
func sortAttrs(a []Attribute) {
	sort.Slice(a, func(i, j int) bool {
		if a[i].Name == a[j].Name {
			return a[i].Type < a[j].Type
		}
		return a[i].Name < a[j].Name
	})
}
