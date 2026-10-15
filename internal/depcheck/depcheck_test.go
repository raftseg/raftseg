// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

// Package depcheck holds tests that keep the module's dependencies to the
// policy in README.md: the standard library, first-party modules (those
// under firstParty, maintained with this one) and the third-party modules
// in thirdParty, each from a tier the core may use (Google, for protobuf).
// go.mod may require nothing else; a first-party module may require only
// allowed third-party modules; go.sum lists nothing else but what their
// go.mod files require; no Go file imports, and no build compiles, a
// package from anywhere else.
package depcheck

import (
	"bufio"
	"encoding/json"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/raftseg/raftseg"

// firstParty is the prefix of the modules maintained with this one, in
// their own repositories (see README.md, Packages).
const firstParty = "github.com/raftseg/"

// thirdParty lists the third-party modules allowed, with the modules their
// go.mod files require in turn, which go.sum lists without compiling them.
var thirdParty = map[string][]string{
	// Google's protobuf runtime: raft's messages are generated for it.
	"google.golang.org/protobuf": {"github.com/google/go-cmp", "github.com/golang/protobuf"},
}

// moduleAllowed reports whether go.mod may require module mod.
func moduleAllowed(mod string) bool {
	_, ok := thirdParty[mod]
	return ok || strings.HasPrefix(mod, firstParty)
}

func moduleRoot(t *testing.T) string {
	dir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		t.Fatal(err)
	}
	return dir
}

// goModLines returns the significant lines of a go.mod file: comments and
// blank lines dropped, require blocks flattened to "require <path> <version>".
func goModLines(t *testing.T, path string) []string {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	block := ""
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if i := strings.Index(line, "//"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		switch {
		case line == "":
		case block != "" && line == ")":
			block = ""
		case block != "":
			out = append(out, block+" "+line)
		case strings.HasSuffix(line, "("):
			block = strings.TrimSpace(strings.TrimSuffix(line, "("))
		default:
			out = append(out, line)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// requiredModules returns the module paths go.mod requires.
func requiredModules(t *testing.T) []string {
	var mods []string
	for _, line := range goModLines(t, filepath.Join(moduleRoot(t), "go.mod")) {
		if f := strings.Fields(line); f[0] == "require" && len(f) == 3 {
			mods = append(mods, f[1])
		}
	}
	return mods
}

// go.mod declares the module and a Go version, and requires first-party
// and allowed third-party modules only: no replace, exclude, retract, tool
// or toolchain lines. go.sum holds those modules and what their go.mod
// files require.
func TestGoModRequiresAllowedOnly(t *testing.T) {
	root := moduleRoot(t)
	for _, line := range goModLines(t, filepath.Join(root, "go.mod")) {
		f := strings.Fields(line)
		switch {
		case f[0] == "module":
			if line != "module "+modulePath {
				t.Errorf("go.mod: %q", line)
			}
		case f[0] == "go":
		case f[0] == "require" && len(f) == 3 && moduleAllowed(f[1]):
		default:
			t.Errorf("go.mod: %q: the module may require only %s modules and %v", line, firstParty, keys(thirdParty))
		}
	}
	sumOK := map[string]bool{}
	for m, deps := range thirdParty {
		sumOK[m] = true
		for _, d := range deps {
			sumOK[d] = true
		}
	}
	if b, err := os.ReadFile(filepath.Join(root, "go.sum")); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			f := strings.Fields(line)
			if line != "" && (len(f) != 3 || !(strings.HasPrefix(f[0], firstParty) || sumOK[f[0]])) {
				t.Errorf("go.sum: %q is not an allowed module", line)
			}
		}
	}
}

func keys(m map[string][]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Every first-party module this one requires may itself require only
// allowed third-party modules.
func TestFirstPartyModulesRequireAllowedOnly(t *testing.T) {
	var mods []string
	for _, m := range requiredModules(t) {
		if strings.HasPrefix(m, firstParty) {
			mods = append(mods, m)
		}
	}
	if len(mods) == 0 {
		t.Skip("no first-party modules required")
	}
	for _, mod := range mods {
		cmd := exec.Command("go", "list", "-m", "-json", mod)
		cmd.Dir = moduleRoot(t)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go list -m %s: %v (the module must be in the module cache or the workspace)", mod, err)
		}
		var m struct{ GoMod string }
		if err := json.Unmarshal(out, &m); err != nil {
			t.Fatal(err)
		}
		for _, line := range goModLines(t, m.GoMod) {
			f := strings.Fields(line)
			switch {
			case f[0] == "module", f[0] == "go":
			case f[0] == "require" && len(f) == 3 && thirdParty[f[1]] != nil:
			default:
				t.Errorf("%s: go.mod: %q: a first-party module may require only %v", mod, line, keys(thirdParty))
			}
		}
	}
}

// Every Go file, tests included and whatever its build tags, imports only
// the standard library, this module, first-party modules and allowed
// third-party modules.
func TestImportsAllowedOnly(t *testing.T) {
	root := moduleRoot(t)
	files := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Go ignores testdata, and directories starting with . or _.
			if path != root && (d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "_")) {
				return filepath.SkipDir
			}
			// A nested module is not part of this one.
			if path != root {
				if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		files++
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if !allowed(p) {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s imports %q", rel, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 50 {
		t.Fatalf("only %d Go files found under %s", files, root)
	}
	t.Logf("%d Go files import only the standard library, %s, %s modules and %v", files, modulePath, firstParty, keys(thirdParty))
}

// allowed reports whether an import path is in the standard library, this
// module, a first-party module or an allowed third-party module. Standard
// library paths have no dot in their first element.
func allowed(p string) bool {
	first, _, _ := strings.Cut(p, "/")
	return allowedModule(p) || !strings.Contains(first, ".")
}

// allowedModule reports whether a package path is in this module, a
// first-party module or an allowed third-party module.
func allowedModule(p string) bool {
	if p == modulePath || strings.HasPrefix(p, modulePath+"/") || strings.HasPrefix(p, firstParty) {
		return true
	}
	for m := range thirdParty {
		if p == m || strings.HasPrefix(p, m+"/") {
			return true
		}
	}
	return false
}

// Every package a build of this module compiles, tests included, is
// allowed: what first-party and third-party modules import counts too. The
// go command says which packages are the standard library's, so any other
// package must be in an allowed module, dot in its path or not (a go.work
// workspace can supply a module named "tools").
func TestCompiledPackagesAllowedOnly(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "-test", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./...")
	cmd.Dir = moduleRoot(t)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v (the module cache must hold the required modules)", err)
	}
	n := 0
	for _, p := range strings.Fields(string(out)) {
		if strings.HasPrefix(p, "[") {
			continue // the test variant of the package before it
		}
		p = strings.TrimSuffix(strings.TrimSuffix(p, ".test"), "_test")
		n++
		if !allowedModule(p) {
			t.Errorf("a build compiles %s", p)
		}
	}
	if n < 10 {
		t.Fatalf("only %d packages listed", n)
	}
}
