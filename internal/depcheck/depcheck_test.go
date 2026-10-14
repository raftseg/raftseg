// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

// Package depcheck holds a test that keeps the module free of third-party
// dependencies. go.mod may require only first-party modules (those under
// firstParty), each of which must itself require nothing, and no Go file
// imports anything outside the standard library, this module and those
// modules.
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
// modules only: no replace, exclude, retract, tool or toolchain lines, and
// nothing third-party. go.sum, if present, holds first-party modules only.
func TestGoModRequiresFirstPartyOnly(t *testing.T) {
	root := moduleRoot(t)
	for _, line := range goModLines(t, filepath.Join(root, "go.mod")) {
		f := strings.Fields(line)
		switch {
		case f[0] == "module":
			if line != "module "+modulePath {
				t.Errorf("go.mod: %q", line)
			}
		case f[0] == "go":
		case f[0] == "require" && len(f) == 3 && strings.HasPrefix(f[1], firstParty):
		default:
			t.Errorf("go.mod: %q: the module may require only %s modules", line, firstParty)
		}
	}
	if b, err := os.ReadFile(filepath.Join(root, "go.sum")); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if line != "" && !strings.HasPrefix(line, firstParty) {
				t.Errorf("go.sum: %q is not a first-party module", line)
			}
		}
	}
}

// Every first-party module this one requires must itself require nothing:
// together they compile in only the standard library.
func TestFirstPartyModulesRequireNothing(t *testing.T) {
	mods := requiredModules(t)
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
			if w := strings.Fields(line)[0]; w != "module" && w != "go" {
				t.Errorf("%s: go.mod: %q: a first-party module must require nothing", mod, line)
			}
		}
	}
}

// Every Go file, tests included and whatever its build tags, imports only
// the standard library, this module and first-party modules.
func TestImportsStandardLibraryOnly(t *testing.T) {
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
	t.Logf("%d Go files import only the standard library, %s and %s modules", files, modulePath, firstParty)
}

// allowed reports whether an import path is in the standard library, this
// module or a first-party module. Standard library paths have no dot in
// their first element.
func allowed(p string) bool {
	if p == modulePath || strings.HasPrefix(p, modulePath+"/") || strings.HasPrefix(p, firstParty) {
		return true
	}
	first, _, _ := strings.Cut(p, "/")
	return !strings.Contains(first, ".")
}
