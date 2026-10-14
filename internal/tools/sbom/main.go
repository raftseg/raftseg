// Copyright 2026 The raftseg authors.

// Command sbom writes a CycloneDX 1.5 software bill of materials for the
// raftseg module, and fails if anything other than the Go standard library,
// the module itself and first-party modules would be compiled into a
// product.
//
//	go run ./internal/tools/sbom -version 0.1.0 > raftseg.cdx.json
//
// First-party modules are those under firstParty (github.com/raftseg/),
// kept in their own repositories. go.mod may require only those, and each
// must itself require nothing. They are resolved as the go command
// resolves them: from the module cache, or from a go.work workspace. A
// workspace copy must be a clean git checkout of exactly the version
// go.mod requires, so that the SBOM describes what go.mod pins. A
// first-party module that is a fork records its upstream in upstream.json,
// which becomes its components' pedigree.
//
// What goes into a product is found with `go list -deps` over the module's
// non-test, non-internal-tool packages. Code used only by tests
// (internal/datadriven, internal/testutil, internal/crashfs,
// internal/raftsim) is listed with scope "excluded".
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type pkg struct {
	ImportPath string
	Standard   bool
	Module     *struct{ Path string }
	Dir        string
	GoFiles    []string
}

// firstParty is the prefix of the modules maintained with this one.
const firstParty = "github.com/raftseg/"

// upstream is a fork's upstream.json: the release it was forked from and
// the upstream commits cherry-picked since.
type upstream struct {
	Module, Version, Repository, Commit, Sum, License, Copyright, Changes string
	CherryPicked                                                          []struct{ Commit, Subject string }
}

// firstPartyModule is a required first-party module, as resolved.
type firstPartyModule struct {
	Path, Version, Dir string
	Upstream           *upstream
}

// Provenance of the test-only datadriven fork.
const (
	datadrivenModule  = "github.com/cockroachdb/datadriven"
	datadrivenVersion = "v1.0.3-0.20250407164829-2945557346d5"
	datadrivenCommit  = "2945557346d519eb302c516517e14dab327bef3a"
)

// testOnly are the module's packages (with those below them) that are never
// compiled into a product.
var testOnly = map[string]string{
	"internal/datadriven": "Fork of github.com/cockroachdb/datadriven (Apache-2.0), used by tests only.",
	"internal/testutil":   "Test assertions, used by tests only.",
	"internal/crashfs":    "Crash-simulating filesystem, used by tests only.",
	"internal/raftsim":    "Deterministic cluster simulator, used by tests only.",
	"internal/depcheck":   "The zero-dependency check, a test.",
	"internal/tools/sbom": "This generator.",
}

// isTestOnly reports whether the package at rel (relative to the module) is
// one of testOnly or below one.
func isTestOnly(rel string) bool {
	for p := range testOnly {
		if rel == p || strings.HasPrefix(rel, p+"/") {
			return true
		}
	}
	return false
}

func main() {
	version := flag.String("version", "0.0.0-dev", "version of the raftseg release")
	dir := flag.String("C", ".", "module root")
	timestamp := flag.String("time", "", "RFC 3339 timestamp (default now; set it for reproducible output)")
	flag.Parse()
	if err := run(*dir, *version, *timestamp, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "sbom:", err)
		os.Exit(1)
	}
}

func goCmd(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	return cmd.Output()
}

func run(dir, version, timestamp string, w io.Writer) error {
	// The module's path, from its go.mod (in a workspace, `go list -m`
	// lists every module of the workspace).
	modJSON, err := goCmd(dir, "mod", "edit", "-json")
	if err != nil {
		return err
	}
	var self struct{ Module struct{ Path string } }
	if err := json.Unmarshal(modJSON, &self); err != nil {
		return err
	}
	module := self.Module.Path
	gov, err := goCmd(dir, "env", "GOVERSION")
	if err != nil {
		return err
	}
	goVersion := strings.TrimSpace(string(gov))

	// The module may require first-party modules only, each requiring
	// nothing itself.
	fps, err := firstPartyModules(dir)
	if err != nil {
		return err
	}
	var requires []string
	for _, m := range fps {
		requires = append(requires, m.Path+"@"+m.Version)
	}
	sort.Strings(requires)

	// Every package of the module, and what products compile in.
	all, err := listPackages(dir, "./...")
	if err != nil {
		return err
	}
	var shipped []string
	for _, p := range all {
		rel := strings.TrimPrefix(strings.TrimPrefix(p.ImportPath, module), "/")
		if isTestOnly(rel) || strings.HasPrefix(rel, "internal/tools/") || strings.Contains(rel, "/testdata/") {
			continue
		}
		shipped = append(shipped, p.ImportPath)
	}
	deps, err := listPackages(dir, append([]string{"-deps"}, shipped...)...)
	if err != nil {
		return err
	}
	var stdPkgs []string
	byPath := map[string]pkg{}
	fpByPath := map[string]pkg{}
	for _, p := range deps {
		switch {
		case p.Standard:
			stdPkgs = append(stdPkgs, p.ImportPath)
		case p.Module != nil && p.Module.Path == module:
			byPath[p.ImportPath] = p
		case p.Module != nil && fps[p.Module.Path] != nil:
			fpByPath[p.ImportPath] = p
		default:
			return fmt.Errorf("package %s is compiled into products but is neither standard library, part of %s, nor part of a required first-party module", p.ImportPath, module)
		}
		if rel := strings.TrimPrefix(strings.TrimPrefix(p.ImportPath, module), "/"); p.Module != nil && isTestOnly(rel) {
			return fmt.Errorf("test-only package %s is compiled into products", p.ImportPath)
		}
	}

	moduleHash, err := dirHash(dir)
	if err != nil {
		return err
	}
	ts := timestamp
	if ts == "" {
		ts = time.Now().UTC().Format(time.RFC3339)
	}

	type hash struct {
		Alg     string `json:"alg"`
		Content string `json:"content"`
	}
	type license struct {
		License map[string]string `json:"license,omitempty"`
	}
	type extRef struct {
		Type    string `json:"type"`
		URL     string `json:"url"`
		Comment string `json:"comment,omitempty"`
	}
	type property struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	type component struct {
		Type        string     `json:"type"`
		BOMRef      string     `json:"bom-ref"`
		Name        string     `json:"name"`
		Version     string     `json:"version,omitempty"`
		Scope       string     `json:"scope,omitempty"`
		Description string     `json:"description,omitempty"`
		Purl        string     `json:"purl,omitempty"`
		Hashes      []hash     `json:"hashes,omitempty"`
		Licenses    []license  `json:"licenses,omitempty"`
		ExtRefs     []extRef   `json:"externalReferences,omitempty"`
		Pedigree    any        `json:"pedigree,omitempty"`
		Properties  []property `json:"properties,omitempty"`
	}
	apache := []license{{License: map[string]string{"id": "Apache-2.0"}}}
	purl := func(path string) string { return fmt.Sprintf("pkg:golang/%s@%s", path, version) }

	// One component per shipped package of the module, with a hash of its
	// source files.
	var pkgComps []component
	var shippedPaths []string
	for path := range byPath {
		shippedPaths = append(shippedPaths, path)
	}
	sort.Strings(shippedPaths)
	for _, path := range shippedPaths {
		p := byPath[path]
		h, err := filesHash(p.Dir, p.GoFiles)
		if err != nil {
			return err
		}
		c := component{
			Type:    "library",
			BOMRef:  purl(path),
			Name:    path,
			Version: version,
			Purl:    purl(path),
			Hashes:  []hash{{"SHA-256", h}},
		}
		pkgComps = append(pkgComps, c)
	}

	// One component per compiled package of a first-party module, at the
	// version go.mod requires, with the module's pedigree if it is a fork.
	var fpPaths []string
	for path := range fpByPath {
		fpPaths = append(fpPaths, path)
	}
	sort.Strings(fpPaths)
	for _, path := range fpPaths {
		p := fpByPath[path]
		m := fps[p.Module.Path]
		h, err := filesHash(p.Dir, p.GoFiles)
		if err != nil {
			return err
		}
		sub := strings.TrimPrefix(strings.TrimPrefix(path, m.Path), "/")
		ref := "pkg:golang/" + m.Path + "@" + m.Version
		if sub != "" {
			ref += "#" + sub
		}
		c := component{
			Type:    "library",
			BOMRef:  ref,
			Name:    path,
			Version: m.Version,
			Purl:    ref,
			Hashes:  []hash{{"SHA-256", h}},
			ExtRefs: []extRef{{Type: "vcs", URL: "https://" + m.Path}},
		}
		if u := m.Upstream; u != nil {
			lic := []license{{License: map[string]string{"id": u.License}}}
			c.Licenses = lic
			c.Description = "Fork of " + u.Module + " " + u.Version + "; every change is listed in " + u.Changes + " of " + m.Path + "."
			uname := u.Module
			if sub != "" {
				uname += "/" + sub
			}
			commits := []map[string]string{{"uid": u.Commit, "url": u.Repository + "/commit/" + u.Commit, "message": "upstream release " + u.Version + ", the fork's base"}}
			for _, cp := range u.CherryPicked {
				commits = append(commits, map[string]string{"uid": cp.Commit, "url": u.Repository + "/commit/" + cp.Commit, "message": "cherry-picked: " + cp.Subject})
			}
			c.Pedigree = map[string]any{
				"ancestors": []component{{
					Type:     "library",
					BOMRef:   "pkg:golang/" + u.Module + "@" + u.Version + "#" + sub,
					Name:     uname,
					Version:  u.Version,
					Purl:     "pkg:golang/" + u.Module + "@" + u.Version,
					Licenses: lic,
					ExtRefs:  []extRef{{Type: "vcs", URL: u.Repository, Comment: "tag " + u.Version + ", commit " + u.Commit}},
					Properties: []property{
						{"go:module:sum", u.Sum},
						{"copyright", u.Copyright},
					},
				}},
				"commits": commits,
				"notes":   "Fork of " + u.Module + " " + u.Version + " with the listed upstream commits cherry-picked; every change is explained in " + u.Changes + " and NOTICE of " + m.Path + ".",
			}
		}
		pkgComps = append(pkgComps, c)
	}

	std := component{
		Type:        "library",
		BOMRef:      "pkg:golang/std@" + goVersion,
		Name:        "std",
		Version:     goVersion,
		Description: "The Go standard library (" + fmt.Sprint(len(stdPkgs)) + " packages compiled in)",
		Purl:        "pkg:golang/std@" + goVersion,
		Licenses:    []license{{License: map[string]string{"id": "BSD-3-Clause"}}},
		Properties:  []property{{"go:packages", strings.Join(stdPkgs, " ")}},
	}

	var excluded []component
	var testPaths []string
	for rel := range testOnly {
		testPaths = append(testPaths, rel)
	}
	sort.Strings(testPaths)
	for _, rel := range testPaths {
		c := component{
			Type:        "library",
			BOMRef:      purl(module + "/" + rel),
			Name:        module + "/" + rel,
			Version:     version,
			Scope:       "excluded",
			Description: testOnly[rel],
		}
		if rel == "internal/datadriven" {
			c.Licenses = apache
			c.Pedigree = map[string]any{
				"ancestors": []component{{
					Type:     "library",
					BOMRef:   "pkg:golang/" + datadrivenModule + "@" + datadrivenVersion,
					Name:     datadrivenModule,
					Version:  datadrivenVersion,
					Purl:     "pkg:golang/" + datadrivenModule + "@" + datadrivenVersion,
					Licenses: apache,
					ExtRefs:  []extRef{{Type: "vcs", URL: "https://github.com/cockroachdb/datadriven", Comment: "commit " + datadrivenCommit}},
				}},
				"notes": "github.com/pmezard/go-difflib replaced by internal/datadriven/diff.go.",
			}
		}
		excluded = append(excluded, c)
	}

	root := component{
		Type:        "library",
		BOMRef:      purl(module),
		Name:        module,
		Version:     version,
		Description: "raftseg: a distributed data fabric for streams, queues and jobs on Raft. No third-party dependencies: products compile in only this module, first-party modules and the Go standard library.",
		Purl:        purl(module),
		Hashes:      []hash{{"SHA-256", moduleHash}},
		Properties: []property{
			{"raftseg:third-party-modules", "0"},
			{"raftseg:go-mod-requires", requiresProperty(requires)},
			{"raftseg:go-version", goVersion},
		},
	}
	var deps2 []map[string]any
	var compRefs []string
	for _, c := range pkgComps {
		compRefs = append(compRefs, c.BOMRef)
	}
	deps2 = append(deps2, map[string]any{"ref": root.BOMRef, "dependsOn": append(compRefs, std.BOMRef)})
	for _, c := range pkgComps {
		deps2 = append(deps2, map[string]any{"ref": c.BOMRef, "dependsOn": []string{std.BOMRef}})
	}
	deps2 = append(deps2, map[string]any{"ref": std.BOMRef, "dependsOn": []string{}})

	bom := map[string]any{
		"bomFormat":    "CycloneDX",
		"specVersion":  "1.5",
		"serialNumber": "urn:uuid:" + uuidFrom(moduleHash+version),
		"version":      1,
		"metadata": map[string]any{
			"timestamp": ts,
			"tools":     map[string]any{"components": []map[string]string{{"type": "application", "name": module + "/internal/tools/sbom", "version": version}}},
			"component": root,
		},
		"components":   append(append(pkgComps, std), excluded...),
		"dependencies": deps2,
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(bom)
}

func requiresProperty(requires []string) string {
	if len(requires) == 0 {
		return "none"
	}
	return strings.Join(requires, " ")
}

// firstPartyModules resolves the modules go.mod requires, which must all be
// first-party and require nothing themselves.
func firstPartyModules(dir string) (map[string]*firstPartyModule, error) {
	modJSON, err := goCmd(dir, "mod", "edit", "-json")
	if err != nil {
		return nil, err
	}
	var gomod struct {
		Require []struct{ Path, Version string }
	}
	if err := json.Unmarshal(modJSON, &gomod); err != nil {
		return nil, err
	}
	out := map[string]*firstPartyModule{}
	for _, r := range gomod.Require {
		if !strings.HasPrefix(r.Path, firstParty) {
			return nil, fmt.Errorf("go.mod requires %s, which is not a first-party module (%s...)", r.Path, firstParty)
		}
		info, err := goCmd(dir, "list", "-m", "-json", r.Path)
		if err != nil {
			return nil, err
		}
		var m struct {
			Version, Dir, GoMod string
			Main                bool
		}
		if err := json.Unmarshal(info, &m); err != nil {
			return nil, err
		}
		if m.Dir == "" {
			return nil, fmt.Errorf("%s@%s is not in the module cache or the workspace", r.Path, r.Version)
		}
		switch {
		case m.Main:
			// A workspace copy: it must be exactly the version required.
			if err := checkCheckout(m.Dir, r.Version); err != nil {
				return nil, fmt.Errorf("%s in the workspace (%s): %v", r.Path, m.Dir, err)
			}
		case m.Version != r.Version:
			return nil, fmt.Errorf("%s resolves to %s, go.mod requires %s", r.Path, m.Version, r.Version)
		}
		sub, err := goCmd(dir, "mod", "edit", "-json", m.GoMod)
		if err != nil {
			return nil, err
		}
		var subMod struct{ Require []any }
		if err := json.Unmarshal(sub, &subMod); err != nil {
			return nil, err
		}
		if len(subMod.Require) != 0 {
			return nil, fmt.Errorf("first-party module %s requires %d modules; it must require none", r.Path, len(subMod.Require))
		}
		fp := &firstPartyModule{Path: r.Path, Version: r.Version, Dir: m.Dir}
		if b, err := os.ReadFile(filepath.Join(m.Dir, "upstream.json")); err == nil {
			var u upstream
			if err := json.Unmarshal(b, &u); err != nil {
				return nil, fmt.Errorf("%s/upstream.json: %v", r.Path, err)
			}
			if u.Module == "" || u.Version == "" || u.Commit == "" || u.License == "" {
				return nil, fmt.Errorf("%s/upstream.json is incomplete", r.Path)
			}
			fp.Upstream = &u
		}
		out[r.Path] = fp
	}
	return out, nil
}

// checkCheckout checks that the git checkout at dir is clean and at the
// commit that version names: the commit of a pseudo-version, or a tag.
func checkCheckout(dir, version string) error {
	git := func(args ...string) (string, error) {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		out, err := cmd.Output()
		return strings.TrimSpace(string(out)), err
	}
	head, err := git("rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("not a git checkout: %v", err)
	}
	if st, err := git("status", "--porcelain"); err != nil || st != "" {
		return fmt.Errorf("the checkout has uncommitted changes")
	}
	// A pseudo-version ends in the 12-character commit prefix.
	if i := strings.LastIndex(version, "-"); i >= 0 && len(version)-i-1 == 12 {
		if want := version[i+1:]; !strings.HasPrefix(head, want) {
			return fmt.Errorf("checked out at %.12s, go.mod requires %s", head, version)
		}
		return nil
	}
	tagged, err := git("rev-parse", version+"^{commit}")
	if err != nil || tagged != head {
		return fmt.Errorf("checked out at %.12s, not at tag %s", head, version)
	}
	return nil
}

func listPackages(dir string, args ...string) ([]pkg, error) {
	out, err := goCmd(dir, append([]string{"list", "-json"}, args...)...)
	if err != nil {
		return nil, err
	}
	var ps []pkg
	dec := json.NewDecoder(bytes.NewReader(out))
	for dec.More() {
		var p pkg
		if err := dec.Decode(&p); err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	return ps, nil
}

// filesHash hashes the named files of a package directory.
func filesHash(dir string, files []string) (string, error) {
	h := sha256.New()
	sorted := append([]string(nil), files...)
	sort.Strings(sorted)
	for _, f := range sorted {
		b, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			return "", err
		}
		s := sha256.Sum256(b)
		fmt.Fprintf(h, "%x  %s\n", s, f)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// dirHash hashes every regular file of the module tree, as Go's "h1:"
// module hash does (sha256 over "sha256(file)  path" lines), returned in
// hex.
func dirHash(dir string) (string, error) {
	var files []string
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		name := info.Name()
		if info.IsDir() && p != dir && (strings.HasPrefix(name, ".") || name == "testdata" && strings.Contains(p, "fuzz")) {
			return filepath.SkipDir
		}
		if info.Mode().IsRegular() {
			rel, _ := filepath.Rel(dir, p)
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(files)
	h := sha256.New()
	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			return "", err
		}
		s := sha256.Sum256(b)
		fmt.Fprintf(h, "%x  %s\n", s, f)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// uuidFrom derives a version-5-style UUID from s, so that the same tree and
// version give the same serial number.
func uuidFrom(s string) string {
	b := sha256.Sum256([]byte(s))
	b[6] = b[6]&0x0f | 0x50
	b[8] = b[8]&0x3f | 0x80
	x := hex.EncodeToString(b[:16])
	return x[:8] + "-" + x[8:12] + "-" + x[12:16] + "-" + x[16:20] + "-" + x[20:32]
}
