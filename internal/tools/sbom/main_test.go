// Copyright 2026 The raftseg authors.

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The SBOM lists what products compile in (this module's shipped packages,
// the first-party raft module's packages and the allowed third-party
// modules' packages at the versions go.mod requires, and the standard
// library), gives the raft packages their upstream pedigree from
// upstream.json and the third-party packages their supplier, license and
// go.sum hash, has a dependency graph that reaches every component, lists
// test-only code as excluded, and is reproducible.
func TestSBOM(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command")
	}
	const root = "../../.."
	gen := func() []byte {
		var b bytes.Buffer
		if err := run(root, "1.2.3", "2026-01-01T00:00:00Z", &b); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	out := gen()
	if !bytes.Equal(out, gen()) {
		t.Fatal("two runs on the same tree differ")
	}
	type property struct{ Name, Value string }
	var bom struct {
		BOMFormat, SpecVersion string
		Metadata               struct {
			Component struct {
				Name, Version string
				Properties    []property
			}
		}
		Components []struct {
			Name, Version, Scope, Purl, Copyright string
			BOMRef                                string `json:"bom-ref"`
			Supplier                              *struct{ Name string }
			Licenses                              []struct{ License struct{ ID string } }
			Hashes                                []struct{ Alg, Content string }
			Properties                            []property
			Pedigree                              *struct {
				Ancestors []struct {
					Name, Version, Purl string
					BOMRef              string `json:"bom-ref"`
					Properties          []property
				}
				Commits []struct{ UID, Message string }
			}
		}
		Dependencies []struct {
			Ref       string
			DependsOn []string
		}
	}
	if err := json.Unmarshal(out, &bom); err != nil {
		t.Fatal(err)
	}
	if bom.BOMFormat != "CycloneDX" || bom.SpecVersion != "1.5" || bom.Metadata.Component.Version != "1.2.3" {
		t.Fatalf("header %s %s %s", bom.BOMFormat, bom.SpecVersion, bom.Metadata.Component.Version)
	}
	prop := func(ps []property, name string) string {
		for _, p := range ps {
			if p.Name == name {
				return p.Value
			}
		}
		return ""
	}

	// What go.mod, go.sum and raft's upstream.json say.
	gomod, err := readGoMod(root, "")
	if err != nil {
		t.Fatal(err)
	}
	required := map[string]string{}
	for _, r := range gomod.Require {
		required[r.Path] = r.Version
	}
	sums, err := readGoSum(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	const raftModule = "github.com/raftseg/raft"
	const protobuf = "google.golang.org/protobuf"
	if required[raftModule] == "" || required[protobuf] == "" {
		t.Fatalf("go.mod requires %v; want %s and %s", required, raftModule, protobuf)
	}
	b, err := goCmd(root, "list", "-m", "-f", "{{.Dir}}", raftModule)
	if err != nil {
		t.Fatal(err)
	}
	ub, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(b)), "upstream.json"))
	if err != nil {
		t.Fatal(err)
	}
	var up upstream
	if err := json.Unmarshal(ub, &up); err != nil {
		t.Fatal(err)
	}
	if up.Module != "go.etcd.io/raft/v3" || up.Commit == "" || up.Sum == "" {
		t.Fatalf("raft's upstream.json: %+v", up)
	}

	module := bom.Metadata.Component.Name
	moduleOf := func(name string) string {
		for m := range required {
			if name == m || strings.HasPrefix(name, m+"/") {
				return m
			}
		}
		return ""
	}
	shipped := map[string]bool{}
	refs := map[string]bool{}
	thirdPartyPkgs := 0
	for _, c := range bom.Components {
		if refs[c.BOMRef] {
			t.Errorf("bom-ref %s is not unique", c.BOMRef)
		}
		refs[c.BOMRef] = true
		switch {
		case c.Scope == "excluded":
			if !strings.Contains(c.Name, "/internal/") {
				t.Errorf("%s is excluded but not internal", c.Name)
			}
		case c.Name == "std":
			if !strings.HasPrefix(c.Version, "go1.") {
				t.Errorf("std version %q", c.Version)
			}
		default:
			if len(c.Hashes) != 1 || len(c.Hashes[0].Content) != 64 {
				t.Errorf("%s: hashes %v", c.Name, c.Hashes)
			}
			shipped[strings.TrimPrefix(c.Name, module+"/")] = true
			m := moduleOf(c.Name)
			switch {
			case strings.HasPrefix(c.Name, module+"/"):
				if c.Version != "1.2.3" || c.Pedigree != nil || c.Supplier != nil {
					t.Errorf("%s: version %q, pedigree %v, supplier %v", c.Name, c.Version, c.Pedigree, c.Supplier)
				}
				continue
			case m == "":
				t.Errorf("shipped component %s is neither part of %s nor of a module go.mod requires", c.Name, module)
				continue
			}
			// A package of a required module: at the version go.mod
			// requires, with go.sum's hash.
			if c.Version != required[m] || !strings.HasPrefix(c.Purl, "pkg:golang/"+m+"@"+c.Version) {
				t.Errorf("%s: version %q, purl %q: want %s", c.Name, c.Version, c.Purl, required[m])
			}
			if got, want := prop(c.Properties, "go:module:sum"), sums[m+" "+required[m]]; want == "" || got != want {
				t.Errorf("%s: go:module:sum %q, go.sum has %q", c.Name, got, want)
			}
			switch m {
			case raftModule:
				if c.Pedigree == nil {
					t.Errorf("%s: no pedigree", c.Name)
					continue
				}
				a, cs := c.Pedigree.Ancestors, c.Pedigree.Commits
				if len(a) != 1 || a[0].Version != up.Version || a[0].Purl != "pkg:golang/"+up.Module+"@"+up.Version ||
					strings.HasSuffix(a[0].BOMRef, "#") || prop(a[0].Properties, "go:module:sum") != up.Sum ||
					len(cs) != 1+len(up.CherryPicked) || cs[0].UID != up.Commit ||
					!strings.HasPrefix(cs[0].Message, "upstream commit "+up.Commit) {
					t.Errorf("%s: pedigree %+v", c.Name, *c.Pedigree)
				}
			case protobuf:
				thirdPartyPkgs++
				if c.Pedigree != nil || c.Supplier == nil || c.Supplier.Name != "Google LLC" ||
					len(c.Licenses) != 1 || c.Licenses[0].License.ID != "BSD-3-Clause" ||
					!strings.Contains(c.Copyright, "The Go Authors") || prop(c.Properties, "raftseg:third-party") != "true" {
					t.Errorf("%s: %+v", c.Name, c)
				}
			}
		}
	}
	for _, want := range []string{raftModule, raftModule + "/raftpb", raftModule + "/quorum", raftModule + "/tracker", raftModule + "/confchange",
		protobuf + "/proto", protobuf + "/reflect/protoreflect", protobuf + "/runtime/protoimpl", "seglog", "vfs"} {
		if !shipped[want] {
			t.Errorf("%s is not listed as shipped", want)
		}
	}
	if thirdPartyPkgs == 0 {
		t.Error("no third-party packages listed")
	}
	ps := bom.Metadata.Component.Properties
	if v := prop(ps, "raftseg:third-party-modules"); v != "1" {
		t.Errorf("third-party modules: %s", v)
	}
	if v := prop(ps, "raftseg:third-party"); v != protobuf+"@"+required[protobuf] {
		t.Errorf("third-party: %s", v)
	}
	if v := prop(ps, "raftseg:go-mod-requires"); v != raftModule+"@"+required[raftModule]+" "+protobuf+"@"+required[protobuf] {
		t.Errorf("go.mod requires: %s", v)
	}

	// The dependency graph names only components, and reaches every shipped
	// one from the root; seglog depends on raft, raft on protobuf, and
	// protobuf on nothing of raft's or this module's.
	graph := map[string][]string{}
	for _, d := range bom.Dependencies {
		if !refs[d.Ref] && d.Ref != "pkg:golang/"+module+"@1.2.3" {
			t.Errorf("dependency entry for unknown ref %s", d.Ref)
		}
		for _, on := range d.DependsOn {
			if !refs[on] {
				t.Errorf("%s depends on unknown ref %s", d.Ref, on)
			}
		}
		graph[d.Ref] = d.DependsOn
	}
	seen := map[string]bool{}
	var walk func(string)
	walk = func(r string) {
		if seen[r] {
			return
		}
		seen[r] = true
		for _, on := range graph[r] {
			walk(on)
		}
	}
	walk("pkg:golang/" + module + "@1.2.3")
	for _, c := range bom.Components {
		if c.Scope != "excluded" && !seen[c.BOMRef] {
			t.Errorf("%s is not reachable from the root", c.BOMRef)
		}
	}
	dependsOn := func(from, to string) bool {
		for _, on := range graph[from] {
			if strings.HasPrefix(on, to) {
				return true
			}
		}
		return false
	}
	if seglog := "pkg:golang/" + module + "/seglog@1.2.3"; !dependsOn(seglog, "pkg:golang/"+raftModule+"@") {
		t.Errorf("seglog depends on %v", graph[seglog])
	}
	raftpb := "pkg:golang/" + raftModule + "@" + required[raftModule] + "#raftpb"
	if !dependsOn(raftpb, "pkg:golang/"+protobuf+"@") {
		t.Errorf("raftpb depends on %v", graph[raftpb])
	}
	for ref, on := range graph {
		if strings.HasPrefix(ref, "pkg:golang/"+protobuf+"@") {
			for _, o := range on {
				if strings.Contains(o, raftModule) || strings.Contains(o, module) {
					t.Errorf("%s depends on %s", ref, o)
				}
			}
		}
	}
}

// go.mod may require only first-party and allowed third-party modules, and
// replace none: anything else fails before any module is resolved.
func TestRequiredModulesPolicy(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command")
	}
	for _, tc := range []struct{ gomod, want string }{
		{"require example.com/x v1.0.0\n", "neither a first-party module"},
		{"require github.com/golang/protobuf v1.5.0\n", "neither a first-party module"},
		{"require google.golang.org/protobuf v1.36.12\nreplace google.golang.org/protobuf => ../protobuf\n", "replaces google.golang.org/protobuf"},
		{"require github.com/raftseg/raft v0.0.0-20260101000000-0123456789ab\nreplace github.com/raftseg/raft v0.0.0-20260101000000-0123456789ab => example.com/raft v1.0.0\n", "replaces github.com/raftseg/raft"},
	} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/m\n\ngo 1.26\n\n"+tc.gomod), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := requiredModules(dir)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: %v, want an error containing %q", tc.gomod, err, tc.want)
		}
	}
}

// moduleSum computes go.sum's "h1:" hash of a module extracted in the
// module cache: here, the protobuf module this one requires. Any change to
// a file changes it.
func TestModuleSum(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command")
	}
	const root = "../../.."
	sums, err := readGoSum(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	const protobuf = "google.golang.org/protobuf"
	b, err := goCmd(root, "list", "-m", "-f", "{{.Version}} {{.Dir}}", protobuf)
	if err != nil {
		t.Fatal(err)
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		t.Fatalf("go list -m %s: %q", protobuf, b)
	}
	got, err := moduleSum(f[1], protobuf+"@"+f[0])
	if err != nil {
		t.Fatal(err)
	}
	if want := sums[protobuf+" "+f[0]]; got != want {
		t.Fatalf("moduleSum = %s, go.sum has %s", got, want)
	}

	dir := t.TempDir()
	write := func(name, data string) {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module example.com/m\n")
	write("a/b.go", "package a\n")
	sum := func() string {
		s, err := moduleSum(dir, "example.com/m@v1.0.0")
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	base := sum()
	if !strings.HasPrefix(base, "h1:") || base != sum() {
		t.Fatalf("moduleSum %q is not stable", base)
	}
	write("a/b.go", "package a \n")
	if sum() == base {
		t.Error("a changed file left the hash unchanged")
	}
	write("a/b.go", "package a\n")
	write("c.txt", "")
	if sum() == base {
		t.Error("an added file left the hash unchanged")
	}
}

func TestPseudoVersion(t *testing.T) {
	for v, want := range map[string]bool{
		"v3.7.0-rc.0.0.20261009205358-638f42739b77": true,
		"v0.0.0-20261015011057-df50b51ed79a":        true,
		"v1.2.4-0.20260101000000-0123456789ab":      true,
		"v3.6.0":                                    false,
		"v3.7.0-rc.0":                               false,
		"v1.36.12":                                  false,
	} {
		if got := pseudoVersion.MatchString(v); got != want {
			t.Errorf("%s: %v, want %v", v, got, want)
		}
	}
}

// A first-party module taken from a go.work workspace must be a clean
// checkout of exactly the version go.mod requires; otherwise the SBOM would
// describe code other than what go.mod pins.
func TestCheckCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git")
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(cmd.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	if err := os.WriteFile(filepath.Join(dir, "a.go"), []byte("package a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", "a.go")
	git("commit", "-q", "-m", "one")
	head := git("rev-parse", "HEAD")
	git("tag", "v0.1.0")
	pseudo := "v0.0.0-20260101000000-" + head[:12]
	for _, v := range []string{pseudo, "v0.1.0"} {
		if err := checkCheckout(dir, v); err != nil {
			t.Errorf("%s: %v", v, err)
		}
	}
	for _, v := range []string{"v0.0.0-20260101000000-0123456789ab", "v0.2.0"} {
		if err := checkCheckout(dir, v); err == nil {
			t.Errorf("%s accepted for a checkout at %s", v, head[:12])
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "a.go"), []byte("package a // changed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkCheckout(dir, pseudo); err == nil {
		t.Error("a checkout with uncommitted changes accepted")
	}
}
