// Copyright 2026 The raftseg authors.

// Command sbom writes a CycloneDX 1.5 software bill of materials for the
// raftseg module, and fails if anything other than the Go standard library,
// the module itself, first-party modules and the allowed third-party modules
// would be compiled into a product.
//
//	go run ./internal/tools/sbom -version 0.1.0 > raftseg.cdx.json
//
// First-party modules are those under firstParty (github.com/raftseg/),
// kept in their own repositories. The allowed third-party modules are those
// in thirdParty, the list internal/depcheck enforces (Google's protobuf
// runtime, which raft's messages are generated for). go.mod may require
// only these, may replace none of them, and a first-party module may itself
// require only allowed third-party modules.
//
// Modules are resolved as the go command resolves them: from the module
// cache, or, for a first-party module, from a go.work workspace. A module
// from the module cache must hash to what go.sum records for it; a
// workspace copy must be a clean git checkout of exactly the version go.mod
// requires. Either way the SBOM describes what go.mod and go.sum pin. A
// first-party module that is a fork records its upstream in upstream.json,
// which becomes its components' pedigree.
//
// What goes into a product is found with `go list -deps` over the module's
// non-test, non-internal-tool packages. Each compiled package other than the
// standard library's is a component, which depends on the components of the
// packages it imports. Code used only by tests (internal/datadriven,
// internal/testutil, internal/crashfs, internal/raftsim) is listed with
// scope "excluded".
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	CgoFiles   []string
	SFiles     []string
	HFiles     []string
	EmbedFiles []string
	Imports    []string
}

// sources are the files of p that are compiled or embedded.
func (p pkg) sources() []string {
	var out []string
	for _, fs := range [][]string{p.GoFiles, p.CgoFiles, p.SFiles, p.HFiles, p.EmbedFiles} {
		out = append(out, fs...)
	}
	return out
}

// firstParty is the prefix of the modules maintained with this one.
const firstParty = "github.com/raftseg/"

// thirdPartyInfo is what the SBOM says of an allowed third-party module.
type thirdPartyInfo struct {
	Supplier, License, Copyright, Repository, Notes string
}

// thirdParty are the third-party modules that products may compile in: the
// list internal/depcheck enforces.
var thirdParty = map[string]thirdPartyInfo{
	"google.golang.org/protobuf": {
		Supplier:   "Google LLC",
		License:    "BSD-3-Clause",
		Copyright:  "Copyright (c) 2018 The Go Authors. All rights reserved.",
		Repository: "https://github.com/protocolbuffers/protobuf-go",
		Notes:      "Google's Go protocol buffers runtime, which raft's messages are generated for. Its PATENTS file grants additional patent rights.",
	},
}

// upstream is a fork's upstream.json: the upstream version it was forked
// from (a release, or the pseudo-version of the upstream commit merged
// last) and the upstream commits cherry-picked since.
type upstream struct {
	Module, Version, Repository, Commit, Sum, License, Copyright, Changes string
	CherryPicked                                                          []struct{ Commit, Subject string }
}

// requiredModule is a module go.mod requires, as resolved.
type requiredModule struct {
	Path, Version, Dir string
	// Sum is the module's hash as go.sum records it.
	Sum        string
	FirstParty bool
	// Upstream is a first-party fork's upstream.json.
	Upstream *upstream
	// Info is a third-party module's.
	Info thirdPartyInfo
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
	"internal/depcheck":   "The dependency policy check, a test.",
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

// pseudoVersion matches Go pseudo-versions (as golang.org/x/mod/module).
var pseudoVersion = regexp.MustCompile(`^v[0-9]+\.(0\.0-|\d+\.\d+-([^+]*\.)?0\.)\d{14}-[A-Za-z0-9]+(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)

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

	// The modules go.mod requires: first-party modules and allowed
	// third-party ones, as go.mod and go.sum pin them.
	mods, err := requiredModules(dir)
	if err != nil {
		return err
	}
	var requires, thirdPartyRequires []string
	for _, m := range mods {
		requires = append(requires, m.Path+"@"+m.Version)
		if !m.FirstParty {
			thirdPartyRequires = append(thirdPartyRequires, m.Path+"@"+m.Version)
		}
	}
	sort.Strings(requires)
	sort.Strings(thirdPartyRequires)

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
	own := map[string]pkg{}    // the module's own packages
	others := map[string]pkg{} // packages of required modules
	for _, p := range deps {
		switch {
		case p.Standard:
			stdPkgs = append(stdPkgs, p.ImportPath)
		case p.Module != nil && p.Module.Path == module:
			own[p.ImportPath] = p
		case p.Module != nil && mods[p.Module.Path] != nil:
			others[p.ImportPath] = p
		default:
			return fmt.Errorf("package %s is compiled into products but is neither standard library, part of %s, nor part of a module go.mod may require", p.ImportPath, module)
		}
		if rel := strings.TrimPrefix(strings.TrimPrefix(p.ImportPath, module), "/"); p.Module != nil && p.Module.Path == module && isTestOnly(rel) {
			return fmt.Errorf("test-only package %s is compiled into products", p.ImportPath)
		}
	}
	// Every required module must be compiled in: one that is not belongs
	// out of go.mod.
	used := map[string]bool{}
	for _, p := range others {
		used[p.Module.Path] = true
	}
	for path := range mods {
		if !used[path] {
			return fmt.Errorf("go.mod requires %s, but no product compiles it in", path)
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
	type supplier struct {
		Name string `json:"name"`
	}
	type component struct {
		Type        string     `json:"type"`
		BOMRef      string     `json:"bom-ref"`
		Supplier    *supplier  `json:"supplier,omitempty"`
		Name        string     `json:"name"`
		Version     string     `json:"version,omitempty"`
		Scope       string     `json:"scope,omitempty"`
		Description string     `json:"description,omitempty"`
		Purl        string     `json:"purl,omitempty"`
		Hashes      []hash     `json:"hashes,omitempty"`
		Licenses    []license  `json:"licenses,omitempty"`
		Copyright   string     `json:"copyright,omitempty"`
		ExtRefs     []extRef   `json:"externalReferences,omitempty"`
		Pedigree    any        `json:"pedigree,omitempty"`
		Properties  []property `json:"properties,omitempty"`
	}
	apache := []license{{License: map[string]string{"id": "Apache-2.0"}}}
	purl := func(path string) string { return fmt.Sprintf("pkg:golang/%s@%s", path, version) }
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

	// One component per compiled package of the module, with a hash of its
	// source files.
	var comps []component
	pkgByRef := map[string]pkg{}
	for _, path := range sortedKeys(own) {
		p := own[path]
		h, err := filesHash(p.Dir, p.sources())
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
		comps = append(comps, c)
		pkgByRef[c.BOMRef] = p
	}
	ownRefs := make([]string, 0, len(comps))
	for _, c := range comps {
		ownRefs = append(ownRefs, c.BOMRef)
	}

	// One component per compiled package of a required module, at the
	// version go.mod requires: a first-party fork's with its pedigree, a
	// third-party module's with its supplier, license and go.sum hash.
	for _, path := range sortedKeys(others) {
		p := others[path]
		m := mods[p.Module.Path]
		h, err := filesHash(p.Dir, p.sources())
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
		if m.Sum != "" {
			c.Properties = append(c.Properties, property{"go:module:sum", m.Sum})
		}
		if !m.FirstParty {
			c.Supplier = &supplier{Name: m.Info.Supplier}
			c.Licenses = []license{{License: map[string]string{"id": m.Info.License}}}
			c.Copyright = m.Info.Copyright
			c.Description = m.Info.Notes
			c.ExtRefs = []extRef{{Type: "vcs", URL: m.Info.Repository}}
			c.Properties = append(c.Properties, property{"raftseg:third-party", "true"})
		}
		if u := m.Upstream; u != nil {
			lic := []license{{License: map[string]string{"id": u.License}}}
			c.Licenses = lic
			// The fork's base: an upstream release, or the upstream commit
			// merged last (named by its pseudo-version).
			base := "upstream release " + u.Version
			vcs := "tag " + u.Version + ", commit " + u.Commit
			if pseudoVersion.MatchString(u.Version) {
				base = "upstream commit " + u.Commit + " (" + u.Version + ")"
				vcs = "commit " + u.Commit + " (pseudo-version " + u.Version + ")"
			}
			c.Description = "Fork of " + u.Module + " at " + base + "; every change is listed in " + u.Changes + " of " + m.Path + "."
			uname := u.Module
			if sub != "" {
				uname += "/" + sub
			}
			commits := []map[string]string{{"uid": u.Commit, "url": u.Repository + "/commit/" + u.Commit, "message": base + ", the fork's base"}}
			for _, cp := range u.CherryPicked {
				commits = append(commits, map[string]string{"uid": cp.Commit, "url": u.Repository + "/commit/" + cp.Commit, "message": "cherry-picked: " + cp.Subject})
			}
			picked := ""
			if len(u.CherryPicked) > 0 {
				picked = fmt.Sprintf(", with the %d upstream commits listed cherry-picked", len(u.CherryPicked))
			}
			ancestorRef := "pkg:golang/" + u.Module + "@" + u.Version
			if sub != "" {
				ancestorRef += "#" + sub
			}
			c.Pedigree = map[string]any{
				"ancestors": []component{{
					Type:     "library",
					BOMRef:   ancestorRef,
					Name:     uname,
					Version:  u.Version,
					Purl:     "pkg:golang/" + u.Module + "@" + u.Version,
					Licenses: lic,
					ExtRefs:  []extRef{{Type: "vcs", URL: u.Repository, Comment: vcs}},
					Properties: []property{
						{"go:module:sum", u.Sum},
						{"copyright", u.Copyright},
					},
				}},
				"commits": commits,
				"notes":   "Fork of " + u.Module + " at " + base + picked + "; every change is explained in " + u.Changes + " and NOTICE of " + m.Path + ".",
			}
		}
		comps = append(comps, c)
		pkgByRef[c.BOMRef] = p
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
		Description: "raftseg: a distributed data fabric for streams, queues and jobs on Raft. Products compile in only this module, first-party modules, the allowed third-party modules (" + requiresProperty(thirdPartyRequires) + ") and the Go standard library.",
		Purl:        purl(module),
		Hashes:      []hash{{"SHA-256", moduleHash}},
		Properties: []property{
			{"raftseg:third-party-modules", fmt.Sprint(len(thirdPartyRequires))},
			{"raftseg:third-party", requiresProperty(thirdPartyRequires)},
			{"raftseg:go-mod-requires", requiresProperty(requires)},
			{"raftseg:go-version", goVersion},
		},
	}

	// The dependency graph: the module is its packages; each package
	// depends on the packages it imports (the standard library as one
	// component).
	refByPath := map[string]string{}
	for _, c := range comps {
		refByPath[c.Name] = c.BOMRef
	}
	stdByPath := map[string]bool{}
	for _, p := range stdPkgs {
		stdByPath[p] = true
	}
	graph := []map[string]any{{"ref": root.BOMRef, "dependsOn": ownRefs}}
	for _, c := range comps {
		set := map[string]bool{}
		for _, imp := range pkgByRef[c.BOMRef].Imports {
			switch {
			case stdByPath[imp]:
				set[std.BOMRef] = true
			case refByPath[imp] != "":
				set[refByPath[imp]] = true
			default:
				return fmt.Errorf("%s imports %s, which is not compiled in", c.Name, imp)
			}
		}
		on := sortedKeys(set)
		if on == nil {
			on = []string{}
		}
		graph = append(graph, map[string]any{"ref": c.BOMRef, "dependsOn": on})
	}
	graph = append(graph, map[string]any{"ref": std.BOMRef, "dependsOn": []string{}})

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
		"components":   append(append(comps, std), excluded...),
		"dependencies": graph,
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(bom)
}

func sortedKeys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func requiresProperty(requires []string) string {
	if len(requires) == 0 {
		return "none"
	}
	return strings.Join(requires, " ")
}

type goMod struct {
	Require []struct{ Path, Version string }
	Replace []struct{ Old struct{ Path string } }
}

func readGoMod(dir, file string) (*goMod, error) {
	args := []string{"mod", "edit", "-json"}
	if file != "" {
		args = append(args, file)
	}
	b, err := goCmd(dir, args...)
	if err != nil {
		return nil, err
	}
	var m goMod
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// readGoSum returns the module hashes (not the go.mod hashes) go.sum
// records, by "path version".
func readGoSum(file string) (map[string]string, error) {
	f, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, err
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 3 || strings.HasSuffix(fields[1], "/go.mod") {
			continue
		}
		out[fields[0]+" "+fields[1]] = fields[2]
	}
	return out, sc.Err()
}

// requiredModules resolves the modules go.mod requires. Each must be
// first-party or an allowed third-party module, and none replaced. A module
// from the module cache must hash to what go.sum records; a first-party
// module from the workspace must be a clean checkout of the version
// required. A first-party module may require only allowed third-party
// modules.
func requiredModules(dir string) (map[string]*requiredModule, error) {
	gomod, err := readGoMod(dir, "")
	if err != nil {
		return nil, err
	}
	for _, r := range gomod.Replace {
		return nil, fmt.Errorf("go.mod replaces %s; products must compile in what go.mod requires", r.Old.Path)
	}
	sums, err := readGoSum(filepath.Join(dir, "go.sum"))
	if err != nil {
		return nil, err
	}
	out := map[string]*requiredModule{}
	for _, r := range gomod.Require {
		first := strings.HasPrefix(r.Path, firstParty)
		info, allowed := thirdParty[r.Path]
		if !first && !allowed {
			return nil, fmt.Errorf("go.mod requires %s, which is neither a first-party module (%s...) nor an allowed third-party module", r.Path, firstParty)
		}
		b, err := goCmd(dir, "list", "-m", "-json", r.Path)
		if err != nil {
			return nil, err
		}
		var m struct {
			Version, Dir, GoMod string
			Main                bool
			Replace             *struct{ Path, Version string }
		}
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		if m.Replace != nil {
			return nil, fmt.Errorf("%s is replaced by %s %s", r.Path, m.Replace.Path, m.Replace.Version)
		}
		if m.Dir == "" {
			return nil, fmt.Errorf("%s@%s is not in the module cache or the workspace", r.Path, r.Version)
		}
		rm := &requiredModule{Path: r.Path, Version: r.Version, Dir: m.Dir, Sum: sums[r.Path+" "+r.Version], FirstParty: first, Info: info}
		switch {
		case m.Main && !first:
			return nil, fmt.Errorf("third-party module %s comes from the workspace (%s); it must come from the module cache", r.Path, m.Dir)
		case m.Main:
			// A workspace copy: it must be exactly the version required.
			if err := checkCheckout(m.Dir, r.Version); err != nil {
				return nil, fmt.Errorf("%s in the workspace (%s): %v", r.Path, m.Dir, err)
			}
		case m.Version != r.Version:
			return nil, fmt.Errorf("%s resolves to %s, go.mod requires %s", r.Path, m.Version, r.Version)
		default:
			if rm.Sum == "" {
				return nil, fmt.Errorf("go.sum has no hash for %s %s", r.Path, r.Version)
			}
			got, err := moduleSum(m.Dir, r.Path+"@"+r.Version)
			if err != nil {
				return nil, err
			}
			if got != rm.Sum {
				return nil, fmt.Errorf("%s@%s in the module cache (%s) hashes to %s; go.sum records %s", r.Path, r.Version, m.Dir, got, rm.Sum)
			}
		}
		if first {
			sub, err := readGoMod(dir, m.GoMod)
			if err != nil {
				return nil, err
			}
			for _, sr := range sub.Require {
				if _, ok := thirdParty[sr.Path]; !ok {
					return nil, fmt.Errorf("first-party module %s requires %s, which is not an allowed third-party module", r.Path, sr.Path)
				}
			}
			if b, err := os.ReadFile(filepath.Join(m.Dir, "upstream.json")); err == nil {
				var u upstream
				if err := json.Unmarshal(b, &u); err != nil {
					return nil, fmt.Errorf("%s/upstream.json: %v", r.Path, err)
				}
				if u.Module == "" || u.Version == "" || u.Commit == "" || u.License == "" || u.Repository == "" || u.Sum == "" {
					return nil, fmt.Errorf("%s/upstream.json is incomplete", r.Path)
				}
				rm.Upstream = &u
			}
		}
		out[r.Path] = rm
	}
	return out, nil
}

// moduleSum computes the "h1:" hash go.sum records for the module
// mv (path@version) extracted at dir, as the go command does for the
// module cache: sha256 over "sha256(file)  mv/path" lines for every file,
// sorted, base64-encoded.
func moduleSum(dir, mv string) (string, error) {
	var names []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		names = append(names, mv+"/"+filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(names)
	h := sha256.New()
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(strings.TrimPrefix(n, mv+"/"))))
		if err != nil {
			return "", err
		}
		fmt.Fprintf(h, "%x  %s\n", sha256.Sum256(b), n)
	}
	return "h1:" + base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
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
