package main

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// A finite, flow-insensitive may-analysis. Every branch and function body is
// inspected, including unreachable ones. Fields are joined by their declared
// Go object; thus the result can over-approximate attributes, never promises
// runtime reachability or correlations between configuration branches.
type node struct {
	values  map[string]bool
	edges   []*node
	results []*node
}
type frame struct {
	objects map[types.Object]*node
	exprs   map[ast.Expr]*node
}
type function struct {
	pkg             *packages.Package
	decl            *ast.FuncDecl
	params, returns []*node
	built           bool
}
type registration struct {
	name, opts, out, labels *node
	kind                    string
	at                      string
}
type observation struct {
	instrument, attrs *node
	kind              string
	at                string
}
type attrSite struct {
	key     *node
	typ, at string
}
type graph struct {
	root            *packages.Package
	nodes           []*node
	objects         map[types.Object]*node
	exprs           map[ast.Expr]*node
	funcs           map[*types.Func]*function
	builtGlobals    map[*packages.Package]bool
	current         *frame
	active          map[*ast.FuncDecl]bool
	used            map[*types.Func]bool
	stringsRequired []attrSite
	literalValues   []attrSite
	imported        map[types.Object]bool
	ops             []func() bool
	regs            []registration
	observations    []observation
	attrs           []attrSite
	errors          []string
}

// newGraph indexes functions in the root package and its go-redis and semantic
// convention dependencies so helper calls can be traced to their declarations.
func newGraph(p *packages.Package) *graph {
	g := &graph{root: p, objects: map[types.Object]*node{}, exprs: map[ast.Expr]*node{}, funcs: map[*types.Func]*function{}, builtGlobals: map[*packages.Package]bool{}, active: map[*ast.FuncDecl]bool{}, used: map[*types.Func]bool{}, imported: map[types.Object]bool{}}
	packages.Visit([]*packages.Package{p}, nil, func(q *packages.Package) {
		if q != p && !strings.Contains(q.PkgPath, "/semconv/") && !strings.HasPrefix(q.PkgPath, "github.com/redis/go-redis/") {
			return
		}
		for _, file := range q.Syntax {
			for _, d := range file.Decls {
				if fd, ok := d.(*ast.FuncDecl); ok {
					if f, ok := q.TypesInfo.Defs[fd.Name].(*types.Func); ok {
						g.funcs[f] = &function{pkg: q, decl: fd}
					}
				}
			}
		}
	})
	return g
}

// new allocates a graph node and seeds it with any already-known values.
func (g *graph) new(values ...string) *node {
	n := &node{values: map[string]bool{}}
	for _, s := range values {
		n.values[s] = true
	}
	g.nodes = append(g.nodes, n)
	return n
}

// add records a possible value and reports whether the node changed.
// When a node accumulates too many values, new string literals widen to an unknown string.
func (n *node) add(s string) bool {
	if strings.HasPrefix(s, "s:") {
		if n.values["?:string"] {
			return false
		}
		if len(n.values) >= 32 {
			s = "?:string"
		}
	}
	if n.values[s] {
		return false
	}
	n.values[s] = true
	return true
}

// edge connects two nodes so propagation can copy possible values from source to destination.
func (g *graph) edge(from, to *node) {
	if from != nil && to != nil && from != to {
		from.edges = append(from.edges, to)
	}
}

// obj returns the node for a Go declaration, seeding string constants immediately.
// Local variables belong to the current function frame. Fields are shared across frames.
func (g *graph) obj(o types.Object) *node {
	if o == nil {
		return g.new()
	}
	objects := g.objects
	if v, ok := o.(*types.Var); ok && !v.IsField() && (v.Pkg() == nil || v.Pkg().Scope().Lookup(v.Name()) != v) && g.current != nil {
		objects = g.current.objects
	}
	if n := objects[o]; n != nil {
		return n
	}
	n := g.new()
	objects[o] = n
	if c, ok := o.(*types.Const); ok && c.Val().Kind() == constant.String {
		n.add("s:" + constant.StringVal(c.Val()))
	}
	return n
}

// fail saves the file and line of an unsupported construct for the scanner's diagnostics.
func (g *graph) fail(p *packages.Package, n ast.Node, msg string) {
	g.errors = append(g.errors, p.Fset.Position(n.Pos()).String()+": "+msg)
}

// build inspects package initializers and root function bodies, including functions
// with no callers. Calls to telemetry helpers can be analyzed with their arguments.
func (g *graph) build() error {
	g.globals(g.root)
	for _, file := range g.root.Syntax {
		for _, d := range file.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok {
				f, _ := g.root.TypesInfo.Defs[fd.Name].(*types.Func)
				if !telemetrySignature(f) {
					g.buildFunc(g.funcs[f])
				}
			}
		}
	}
	for f, fn := range g.funcs {
		if fn.pkg == g.root && !fn.built && !g.used[f] {
			g.buildFunc(fn)
		}
	}
	return g.err()
}

// telemetrySignature identifies functions whose type mentions OTel attributes or
// metrics so the scanner can trace each call's arguments separately.
func telemetrySignature(f *types.Func) bool {
	if f == nil {
		return false
	}
	s := typeName(f.Type())
	return strings.Contains(s, otel+"attribute.") || strings.Contains(s, otel+"metric.")
}

// err combines all unsupported-source diagnostics in stable order, or returns nil.
func (g *graph) err() error {
	if len(g.errors) == 0 {
		return nil
	}
	sort.Strings(g.errors)
	return fmt.Errorf("unsupported telemetry source:\n%s", strings.Join(g.errors, "\n"))
}

// globals scans initializers in the root package once, outside any function frame.
func (g *graph) globals(p *packages.Package) {
	if g.builtGlobals[p] {
		return
	}
	g.builtGlobals[p] = true
	if p != g.root {
		return
	}
	previous := g.current
	g.current = nil
	defer func() { g.current = previous }()
	for _, file := range p.Syntax {
		for _, d := range file.Decls {
			if gd, ok := d.(*ast.GenDecl); ok {
				for _, s := range gd.Specs {
					if v, ok := s.(*ast.ValueSpec); ok {
						g.valueSpec(p, v)
					}
				}
			}
		}
	}
}

// valueSpec connects the values in a declaration to its named variables,
// including individual results from a single multi-value call.
func (g *graph) valueSpec(p *packages.Package, v *ast.ValueSpec) {
	for i, e := range v.Values {
		val := g.expr(p, e)
		if len(v.Values) == 1 && len(val.results) > 1 {
			for j, name := range v.Names {
				if j < len(val.results) {
					g.edge(val.results[j], g.obj(p.TypesInfo.Defs[name]))
				}
			}
			continue
		}
		if i < len(v.Names) {
			g.edge(val, g.obj(p.TypesInfo.Defs[v.Names[i]]))
		}
	}
}

// buildFunc analyzes one function in its own frame and records the nodes used
// for its parameters and returns. Exported functions can receive caller-defined dimensions.
func (g *graph) buildFunc(f *function) {
	if f == nil || f.built {
		return
	}
	f.built = true
	if g.active[f.decl] {
		g.fail(f.pkg, f.decl, "recursive telemetry helper is unsupported")
		return
	}
	g.active[f.decl] = true
	defer delete(g.active, f.decl)
	previous := g.current
	g.current = &frame{objects: map[types.Object]*node{}, exprs: map[ast.Expr]*node{}}
	defer func() { g.current = previous }()
	g.globals(f.pkg)
	sig := f.pkg.TypesInfo.Defs[f.decl.Name].Type().(*types.Signature)
	for i := 0; i < sig.Params().Len(); i++ {
		v := sig.Params().At(i)
		n := g.obj(v)
		f.params = append(f.params, n)
		// Public extension points may accept caller-defined dimensions. Preserve
		// this openness explicitly, instead of inventing a finite allowlist.
		if ast.IsExported(f.decl.Name.Name) && (typeName(v.Type()) == "string" || typeName(v.Type()) == "[]string" || isStringMap(v.Type())) {
			n.add("?:string")
		}
		if ast.IsExported(f.decl.Name.Name) && isAttribute(v.Type()) {
			n.add("x:caller")
		}
	}
	for i := 0; i < sig.Results().Len(); i++ {
		f.returns = append(f.returns, g.obj(sig.Results().At(i)))
	}
	if f.decl.Body != nil {
		g.walk(f.pkg, f.decl.Body, f.returns)
	}
}

// walk records how values move through assignments, returns and loops in a
// function body. It visits every branch, including code with no runtime test coverage.
func (g *graph) walk(p *packages.Package, n ast.Node, returns []*node) {
	ast.Inspect(n, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.FuncLit:
			g.expr(p, v)
			return false
		case *ast.AssignStmt:
			for _, lhs := range v.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok && typeName(p.TypesInfo.TypeOf(sel.X)) == otel+"attribute.KeyValue" {
					g.fail(p, lhs, "mutating attribute.KeyValue fields is unsupported; use an attribute constructor")
				}
				if idx, ok := lhs.(*ast.IndexExpr); ok {
					if isStringMap(p.TypesInfo.TypeOf(idx.X)) {
						g.edge(g.expr(p, idx.Index), g.expr(p, idx.X))
					} else {
						g.edge(g.expr(p, idx), g.expr(p, idx.X))
					}
				}
			}
			for i, e := range v.Rhs {
				value := g.expr(p, e)
				if len(v.Rhs) == 1 && len(value.results) > 1 {
					for j, lhs := range v.Lhs {
						if j < len(value.results) {
							g.edge(value.results[j], g.expr(p, lhs))
						}
					}
					continue
				}
				if i < len(v.Lhs) {
					g.edge(value, g.expr(p, v.Lhs[i]))
				}
			}
		case *ast.ValueSpec:
			g.valueSpec(p, v)
		case *ast.ReturnStmt:
			for i, e := range v.Results {
				if i < len(returns) {
					g.edge(g.expr(p, e), returns[i])
				}
			}
		case *ast.RangeStmt:
			if v.Value != nil {
				g.edge(g.expr(p, v.X), g.expr(p, v.Value))
			}
		case ast.Expr:
			g.expr(p, v)
		}
		return true
	})
}

// called resolves a named function or method call to its Go declaration.
// Indirect calls and built-ins can have no function object.
func called(p *packages.Package, c *ast.CallExpr) *types.Func {
	switch f := c.Fun.(type) {
	case *ast.Ident:
		v, _ := p.TypesInfo.ObjectOf(f).(*types.Func)
		return v
	case *ast.SelectorExpr:
		v, _ := p.TypesInfo.ObjectOf(f.Sel).(*types.Func)
		return v
	}
	return nil
}

// pkgName returns a function's import path, which distinguishes SDK methods
// from client helpers that happen to share their names.
func pkgName(f *types.Func) string {
	if f == nil || f.Pkg() == nil {
		return ""
	}
	return f.Pkg().Path()
}

// typeName renders Go types using full import paths so package aliases do not
// affect how the scanner recognizes telemetry APIs.
func typeName(t types.Type) string {
	if t == nil {
		return ""
	}
	return types.TypeString(t, func(p *types.Package) string { return p.Path() })
}

// isStringMap recognizes map[string]string declarations whose keys can supply
// metric labels.
func isStringMap(t types.Type) bool {
	if t == nil {
		return false
	}
	m, ok := t.Underlying().(*types.Map)
	return ok && typeName(m.Key()) == "string" && typeName(m.Elem()) == "string"
}

// isAttribute checks whether the rendered type refers to the OTel attribute package.
func isAttribute(t types.Type) bool { return strings.Contains(typeName(t), otel+"attribute.") }

// expr builds and caches a node for an expression in the current frame.
// Its edges and deferred operations describe which telemetry values can reach it.
func (g *graph) expr(p *packages.Package, e ast.Expr) *node {
	if e == nil {
		return g.new()
	}
	exprs := g.exprs
	if g.current != nil {
		exprs = g.current.exprs
	}
	if n := exprs[e]; n != nil {
		return n
	}
	n := g.new()
	exprs[e] = n
	if tv, ok := p.TypesInfo.Types[e]; ok && tv.Value != nil && tv.Value.Kind() == constant.String {
		n.add("s:" + constant.StringVal(tv.Value))
		return n
	}
	switch v := e.(type) {
	case *ast.Ident:
		o := p.TypesInfo.ObjectOf(v)
		if variable, ok := o.(*types.Var); ok && p != g.root && variable.Pkg() != nil && variable.Pkg().Scope().Lookup(variable.Name()) == variable {
			g.importValue(p, o)
		}
		g.edge(g.obj(p.TypesInfo.ObjectOf(v)), n)
		g.edge(n, g.obj(p.TypesInfo.ObjectOf(v)))
	case *ast.SelectorExpr:
		o := p.TypesInfo.ObjectOf(v.Sel)
		g.edge(g.obj(o), n)
		g.edge(n, g.obj(o))
		if _, ok := o.(*types.Var); ok { // Resolve imported semantic-convention values.
			if o.Pkg() != nil && strings.Contains(o.Pkg().Path(), "/semconv/") {
				for _, q := range p.Imports {
					if q.PkgPath == o.Pkg().Path() {
						g.importValue(q, o)
					}
				}
			}
		}
	case *ast.ParenExpr:
		g.edge(g.expr(p, v.X), n)
	case *ast.StarExpr:
		g.edge(g.expr(p, v.X), n)
	case *ast.UnaryExpr:
		g.edge(g.expr(p, v.X), n)
	case *ast.TypeAssertExpr:
		g.edge(g.expr(p, v.X), n)
	case *ast.SliceExpr:
		g.edge(g.expr(p, v.X), n)
	case *ast.IndexExpr:
		if isStringMap(p.TypesInfo.TypeOf(v.X)) {
			n.add("?:string")
		} else {
			g.edge(g.expr(p, v.X), n)
		}
	case *ast.BinaryExpr:
		if v.Op == token.ADD {
			a, b := g.expr(p, v.X), g.expr(p, v.Y)
			g.ops = append(g.ops, func() bool {
				changed := false
				if a.values["?:string"] || b.values["?:string"] {
					return n.add("?:string")
				}
				for x := range a.values {
					for y := range b.values {
						if strings.HasPrefix(x, "s:") && strings.HasPrefix(y, "s:") {
							changed = n.add("s:"+x[2:]+y[2:]) || changed
						}
					}
				}
				return changed
			})
		}
	case *ast.CompositeLit:
		t := p.TypesInfo.TypeOf(v)
		if ptr, ok := t.(*types.Pointer); ok {
			t = ptr.Elem()
		}
		if t != nil {
			t = t.Underlying()
		}
		var literalKey, literalValue *node
		for i, el := range v.Elts {
			val := el
			var field *types.Var
			if kv, ok := el.(*ast.KeyValueExpr); ok {
				val = kv.Value
				if id, ok := kv.Key.(*ast.Ident); ok {
					field, _ = p.TypesInfo.ObjectOf(id).(*types.Var)
				}
				if _, ok := t.(*types.Map); ok {
					if isStringMap(t) {
						g.edge(g.expr(p, kv.Key), n)
					} else {
						g.edge(g.expr(p, kv.Value), n)
					}
					continue
				}
			} else if st, ok := t.(*types.Struct); ok && i < st.NumFields() {
				field = st.Field(i)
			}
			child := g.expr(p, val)
			if field != nil {
				if typeName(p.TypesInfo.TypeOf(v)) == otel+"attribute.KeyValue" {
					if field.Name() == "Key" {
						literalKey = child
					} else if field.Name() == "Value" {
						literalValue = child
					}
				} else {
					g.edge(child, g.obj(field))
				}
			} else {
				g.edge(child, n)
			}
		}
		if typeName(p.TypesInfo.TypeOf(v)) == otel+"attribute.KeyValue" {
			key, value := literalKey, literalValue
			if key == nil || value == nil {
				g.fail(p, v, "attribute literal requires a static key and typed value")
				break
			}
			g.attrs = append(g.attrs, attrSite{key, "", p.Fset.Position(v.Pos()).String()})
			g.literalValues = append(g.literalValues, attrSite{value, "", p.Fset.Position(v.Pos()).String()})
			g.ops = append(g.ops, func() bool {
				change := false
				for k := range key.values {
					for t := range value.values {
						if strings.HasPrefix(k, "s:") && strings.HasPrefix(t, "v:") {
							change = n.add("a:"+k[2:]+"\x00"+t[2:]) || change
						}
					}
				}
				return change
			})
		}
	case *ast.FuncLit:
		var ret []*node
		if sig, ok := p.TypesInfo.TypeOf(v).(*types.Signature); ok {
			for i := 0; i < sig.Results().Len(); i++ {
				ret = append(ret, g.obj(sig.Results().At(i)))
			}
		}
		g.walk(p, v.Body, ret)
	case *ast.CallExpr:
		g.call(p, v, n)
	}
	return n
}

var instrumentNames = map[string]bool{}

// init lists the numeric OTel instrument constructors recognized by the scanner.
func init() {
	for _, number := range []string{"Int64", "Float64"} {
		for _, kind := range []string{"Counter", "UpDownCounter", "Histogram", "Gauge", "ObservableCounter", "ObservableUpDownCounter", "ObservableGauge"} {
			instrumentNames[number+kind] = true
		}
	}
}

// call models how a call contributes telemetry data. Constructors create
// registrations, recording calls capture handles and attributes, and recognized
// helpers connect arguments and results across function boundaries.
func (g *graph) call(p *packages.Package, c *ast.CallExpr, out *node) {
	args := make([]*node, len(c.Args))
	for i, e := range c.Args {
		args[i] = g.expr(p, e)
	}
	f := called(p, c)
	path := pkgName(f)
	name := ""
	if f != nil {
		name = f.Name()
	}
	at := p.Fset.Position(c.Pos()).String()
	var recv *node
	if se, ok := c.Fun.(*ast.SelectorExpr); ok {
		recv = g.expr(p, se.X)
	}
	if f == nil {
		if id, ok := c.Fun.(*ast.Ident); ok {
			switch id.Name {
			case "append":
				for _, a := range args {
					g.edge(a, out)
				}
				return
			case "copy":
				if len(args) == 2 {
					g.edge(args[1], args[0])
				}
				return
			case "len", "cap", "close", "delete", "recover", "panic", "min", "max", "clear", "print", "println", "make", "new":
				return
			}
		}
		if p.TypesInfo.Types[c.Fun].IsType() {
			if len(args) == 1 {
				g.edge(args[0], out)
			}
			return
		}
		if strings.Contains(typeName(p.TypesInfo.TypeOf(c.Fun)), prom+".") || strings.Contains(typeName(p.TypesInfo.TypeOf(c.Fun)), otel+"metric.") || strings.Contains(typeName(p.TypesInfo.TypeOf(c.Fun)), otel+"attribute.") {
			g.fail(p, c, "indirect telemetry call is unsupported; use a named helper")
		}
		return
	}
	// These standard-library helpers preserve the slice elements. Model their
	// value flow explicitly so moving attribute assembly into slices.Clone or
	// slices.Concat does not drop attributes or turn a refactor into an error.
	if path == "slices" && (name == "Clone" || name == "Concat") {
		for _, a := range args {
			g.edge(a, out)
		}
		return
	}
	if path == otel+"metric" && instrumentNames[name] {
		if len(args) == 0 {
			g.fail(p, c, "metric constructor without name")
			return
		}
		opts := g.new()
		for _, a := range args[1:] {
			g.edge(a, opts)
		}
		g.regs = append(g.regs, registration{name: args[0], opts: opts, out: out, kind: name, at: at})
		return
	}
	if path == otel+"metric" {
		switch name {
		case "WithUnit":
			if len(args) > 0 {
				a := args[0]
				g.stringsRequired = append(g.stringsRequired, attrSite{a, "unit", at})
				g.ops = append(g.ops, func() bool {
					change := false
					for s := range a.values {
						if strings.HasPrefix(s, "s:") {
							change = out.add("u:"+s[2:]) || change
						}
					}
					return change
				})
			}
			return
		case "WithAttributes", "WithAttributeSet":
			for _, a := range args {
				g.edge(a, out)
			}
			return
		case "WithDescription", "WithExplicitBucketBoundaries", "WithInstrumentationVersion", "WithSchemaURL", "WithInstrumentationAttributes":
			return
		case "Add", "Record":
			attrs := g.new()
			for _, a := range args[2:] {
				g.edge(a, attrs)
			}
			g.observations = append(g.observations, observation{recv, attrs, "", at})
			return
		case "ObserveInt64", "ObserveFloat64":
			if len(args) >= 2 {
				attrs := g.new()
				for _, a := range args[2:] {
					g.edge(a, attrs)
				}
				g.observations = append(g.observations, observation{args[0], attrs, "", at})
			}
			return
		case "RegisterCallback", "Unregister", "Meter":
			return
		}
		g.fail(p, c, "unhandled OTel metric API: "+name)
		return
	}
	if path == otel+"attribute" {
		typ := map[string]string{"String": "string", "Int": "int64", "Int64": "int64", "Float64": "float64", "Bool": "bool", "StringSlice": "string[]", "IntSlice": "int64[]", "Int64Slice": "int64[]", "Float64Slice": "float64[]", "BoolSlice": "bool[]"}
		if t := typ[name]; t != "" {
			var key *node
			if f.Type().(*types.Signature).Recv() != nil {
				key = recv
			} else if len(args) > 0 {
				key = args[0]
			}
			if key == nil {
				g.fail(p, c, "attribute without key")
				return
			}
			g.attrs = append(g.attrs, attrSite{key, t, at})
			g.ops = append(g.ops, func() bool {
				change := false
				for k := range key.values {
					if strings.HasPrefix(k, "s:") {
						change = out.add("a:"+k[2:]+"\x00"+t) || change
					}
				}
				return change
			})
			return
		}
		if t := typ[strings.TrimSuffix(name, "Value")]; strings.HasSuffix(name, "Value") && t != "" {
			out.add("v:" + t)
			return
		}
		switch name {
		case "NewSet":
			for _, a := range args {
				g.edge(a, out)
			}
			return
		case "ToSlice", "ToSliceCopy":
			g.edge(recv, out)
			return
		}
		g.fail(p, c, "unhandled attribute API: "+name)
		return
	}
	if path == prom {
		switch name {
		case "BuildFQName":
			if len(args) == 3 {
				a, b, d := args[0], args[1], args[2]
				g.stringsRequired = append(g.stringsRequired, attrSite{d, "Prometheus metric suffix", at})
				g.ops = append(g.ops, func() bool {
					changed := false
					ns, ss := stringsOf(a), stringsOf(b)
					if len(ns) == 0 || a.values["?:string"] {
						ns = []string{"{namespace}"}
					}
					if len(ss) == 0 || b.values["?:string"] {
						ss = []string{"{subsystem}"}
					}
					for _, x := range ns {
						for _, y := range ss {
							for _, z := range stringsOf(d) {
								parts := []string{}
								for _, v := range []string{x, y, z} {
									if v != "" {
										parts = append(parts, v)
									}
								}
								changed = out.add("s:"+strings.Join(parts, "_")) || changed
							}
						}
					}
					return changed
				})
			}
			return
		case "NewDesc":
			if len(args) == 4 {
				labels := g.new()
				g.edge(args[2], labels)
				g.edge(args[3], labels)
				g.regs = append(g.regs, registration{name: args[0], opts: g.new(), out: out, labels: labels, kind: "PrometheusDesc", at: at})
			}
			return
		case "MustNewConstMetric", "NewConstMetric":
			if len(args) >= 3 {
				kind := ""
				if se, ok := c.Args[1].(*ast.SelectorExpr); ok {
					kind = se.Sel.Name
				}
				if kind != "CounterValue" && kind != "GaugeValue" {
					g.fail(p, c, "unhandled Prometheus value type")
				} else {
					g.observations = append(g.observations, observation{args[0], g.new(), "Prometheus" + strings.TrimSuffix(kind, "Value"), at})
				}
			}
			return
		}
		g.fail(p, c, "unhandled Prometheus API: "+name)
		return
	}
	// Attribute caches are conservative unions of all values stored in that map.
	if path == "sync" && recv != nil {
		switch name {
		case "Store", "LoadOrStore":
			if len(args) > 1 {
				g.edge(args[1], recv)
				g.edge(recv, out)
			}
			return
		case "Load":
			g.edge(recv, out)
			return
		}
	}
	if fun := g.funcs[f]; fun != nil && (fun.pkg == g.root || strings.Contains(path, "/semconv/") || strings.Contains(typeName(f.Type()), otel+"attribute.") || strings.Contains(typeName(f.Type()), otel+"metric.")) {
		g.used[f] = true
		if telemetrySignature(f) {
			fun = &function{pkg: fun.pkg, decl: fun.decl}
		}
		g.buildFunc(fun)
		for i, a := range args {
			j := i
			if j >= len(fun.params) {
				j = len(fun.params) - 1
			}
			if j >= 0 {
				g.edge(a, fun.params[j])
			}
		}
		out.results = fun.returns
		if len(fun.returns) > 0 {
			g.edge(fun.returns[0], out)
		}
		return
	}
	if typeName(p.TypesInfo.TypeOf(c)) == "string" {
		out.add("?:string")
	}
	// Unknown external factories returning telemetry objects must be supported
	// explicitly. Standard-library calls producing sample values are irrelevant.
	if strings.Contains(typeName(p.TypesInfo.TypeOf(c)), prom+".") || isAttribute(p.TypesInfo.TypeOf(c)) || strings.Contains(typeName(p.TypesInfo.TypeOf(c)), otel+"metric.") {
		if (path == "go.opentelemetry.io/otel" && name == "GetMeterProvider") || strings.Contains(path, "/trace") {
			return
		}
		g.fail(p, c, "external telemetry helper requires an adapter: "+path+"."+name)
	}
}

// stringsOf returns a node's known string literals in sorted order.
func stringsOf(n *node) []string {
	var s []string
	if n != nil {
		for v := range n.values {
			if strings.HasPrefix(v, "s:") {
				s = append(s, v[2:])
			}
		}
	}
	sort.Strings(s)
	return s
}

// propagate moves possible values across edges and evaluates derived values
// until the graph stops changing. It returns an error if analysis exceeds its limits.
func (g *graph) propagate() error {
	for round := 0; round < 1000; round++ {
		changed := false
		for _, n := range g.nodes {
			if len(n.values) > 10000 {
				return fmt.Errorf("analysis value limit exceeded; simplify dynamic telemetry definitions")
			}
			for _, to := range n.edges {
				for v := range n.values {
					changed = to.add(v) || changed
				}
			}
		}
		for _, op := range g.ops {
			changed = op() || changed
		}
		if !changed {
			return nil
		}
	}
	return fmt.Errorf("static telemetry analysis did not converge")
}

// solve resolves names, units and attribute keys, then propagates the resulting
// metric identities to the handles used at recording sites.
func (g *graph) solve() error {
	if err := g.err(); err != nil {
		return err
	}
	if err := g.propagate(); err != nil {
		return err
	}
	for _, r := range g.regs {
		names := stringsOf(r.name)
		if len(names) == 0 || r.name.values["?:string"] {
			g.errors = append(g.errors, r.at+": metric name is not statically resolvable")
			continue
		}
		var units []string
		for v := range r.opts.values {
			if strings.HasPrefix(v, "u:") {
				units = append(units, v[2:])
			}
		}
		if len(units) == 0 {
			units = []string{""}
		}
		for _, name := range names {
			if name == "" {
				g.errors = append(g.errors, r.at+": empty metric name")
				continue
			}
			for _, unit := range units {
				r.out.add("m:" + name + "\x00" + r.kind + "\x00" + unit)
			}
		}
	}
	for _, a := range g.literalValues {
		known := false
		for v := range a.key.values {
			if strings.HasPrefix(v, "v:") {
				known = true
			}
		}
		if !known {
			g.errors = append(g.errors, a.at+": attribute literal type is not statically resolvable")
		}
	}
	for _, a := range append(g.attrs, g.stringsRequired...) {
		if len(stringsOf(a.key)) == 0 || a.key.values["?:string"] {
			g.errors = append(g.errors, a.at+": telemetry key/unit is not statically resolvable")
		}
	}
	if err := g.err(); err != nil {
		return err
	}
	return g.propagate()
}

// contract builds the package's metric definitions and attribute inventory from
// the solved graph. Each recording contributes keys to the metrics its handle can reach.
func (g *graph) contract() ([]Metric, []Attribute, error) {
	metrics := map[string]*Metric{}
	allAttrs := map[Attribute]bool{}
	for _, r := range g.regs {
		for value := range r.out.values {
			if !strings.HasPrefix(value, "m:") {
				continue
			}
			parts := strings.Split(value[2:], "\x00")
			m := metrics[value]
			if m == nil {
				m = &Metric{Package: g.root.PkgPath, Name: parts[0], Instrument: parts[1], Unit: parts[2], Attributes: []Attribute{}}
				metrics[value] = m
			}
			if r.labels != nil {
				if r.labels.values["?:string"] {
					g.errors = append(g.errors, r.at+": Prometheus labels must be static")
				}
				for _, name := range stringsOf(r.labels) {
					m.Attributes = append(m.Attributes, Attribute{name, "string"})
				}
			}
		}
	}
	for _, n := range g.nodes {
		for value := range n.values {
			if strings.HasPrefix(value, "a:") {
				parts := strings.Split(value[2:], "\x00")
				allAttrs[Attribute{parts[0], parts[1]}] = true
			}
		}
	}
	for _, o := range g.observations {
		found := false
		for value := range o.instrument.values {
			m := metrics[value]
			if m == nil {
				continue
			}
			found = true
			if o.kind != "" {
				if m.Instrument != "PrometheusDesc" && m.Instrument != o.kind {
					g.errors = append(g.errors, o.at+": conflicting Prometheus types for one descriptor")
				}
				m.Instrument = o.kind
			}
			for a := range o.attrs.values {
				if a == "x:caller" {
					m.ExternalAttributes = true
				}
				if strings.HasPrefix(a, "a:") {
					parts := strings.Split(a[2:], "\x00")
					m.Attributes = append(m.Attributes, Attribute{parts[0], parts[1]})
				}
			}
		}
		if !found {
			g.errors = append(g.errors, o.at+": recorded instrument could not be linked to its registration")
		}
	}
	var result []Metric
	for _, m := range metrics {
		seen := map[Attribute]bool{}
		attrs := []Attribute{}
		for _, a := range m.Attributes {
			if !seen[a] {
				seen[a] = true
				attrs = append(attrs, a)
			}
		}
		sortAttrs(attrs)
		m.Attributes = attrs
		result = append(result, *m)
	}
	var attrs []Attribute
	for a := range allAttrs {
		attrs = append(attrs, a)
	}
	sortAttrs(attrs)
	return result, attrs, g.err()
}

// importValue scans the initializer for one referenced declaration in an
// imported package so its value can flow into the calling package.
func (g *graph) importValue(p *packages.Package, o types.Object) {
	if g.imported[o] {
		return
	}
	g.imported[o] = true
	previous := g.current
	g.current = nil
	defer func() { g.current = previous }()
	for _, file := range p.Syntax {
		for _, d := range file.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, sp := range gd.Specs {
				v, ok := sp.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, id := range v.Names {
					if p.TypesInfo.Defs[id] == o {
						g.valueSpec(p, v)
						return
					}
				}
			}
		}
	}
}
