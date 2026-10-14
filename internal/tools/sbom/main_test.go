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
// the first-party raft module's packages at the version go.mod requires,
// and the standard library), gives the raft packages their upstream
// pedigree from upstream.json, lists test-only code as excluded, and is
// reproducible.
func TestSBOM(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command")
	}
	gen := func() []byte {
		var b bytes.Buffer
		if err := run("../../..", "1.2.3", "2026-01-01T00:00:00Z", &b); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	out := gen()
	if !bytes.Equal(out, gen()) {
		t.Fatal("two runs on the same tree differ")
	}
	var bom struct {
		BOMFormat, SpecVersion string
		Metadata               struct {
			Component struct {
				Name, Version string
				Properties    []struct{ Name, Value string }
			}
		}
		Components []struct {
			Name, Version, Scope, Purl string
			Hashes                     []struct{ Alg, Content string }
			Pedigree                   *struct {
				Ancestors []struct{ Name, Version, Purl string }
				Commits   []struct{ UID string }
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
	module := bom.Metadata.Component.Name
	const raftModule = "github.com/raftseg/raft"
	shipped := map[string]bool{}
	for _, c := range bom.Components {
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
			first := strings.HasPrefix(c.Name, firstParty)
			if !strings.HasPrefix(c.Name, module+"/") && !first {
				t.Errorf("shipped component %s is neither part of %s nor first-party", c.Name, module)
			}
			if len(c.Hashes) != 1 || len(c.Hashes[0].Content) != 64 {
				t.Errorf("%s: hashes %v", c.Name, c.Hashes)
			}
			shipped[strings.TrimPrefix(c.Name, module+"/")] = true
			isRaft := c.Name == raftModule || strings.HasPrefix(c.Name, raftModule+"/")
			if isRaft != (c.Pedigree != nil) {
				t.Errorf("%s: pedigree %v", c.Name, c.Pedigree)
			}
			if isRaft && (c.Version == "1.2.3" || !strings.Contains(c.Purl, raftModule+"@"+c.Version)) {
				t.Errorf("%s: version %q, purl %q: want the version go.mod requires", c.Name, c.Version, c.Purl)
			}
			if c.Pedigree != nil {
				a := c.Pedigree.Ancestors
				cs := c.Pedigree.Commits
				if len(a) != 1 || a[0].Version != "v3.6.0" || !strings.Contains(a[0].Purl, "go.etcd.io/raft/v3") ||
					len(cs) < 3 || cs[0].UID != "f35a02212416d045c574e30aa59d4b07c4b8d732" {
					t.Errorf("%s: pedigree %+v", c.Name, *c.Pedigree)
				}
			}
		}
	}
	for _, want := range []string{raftModule, raftModule + "/raftpb", raftModule + "/quorum", raftModule + "/tracker", raftModule + "/confchange", "seglog", "vfs"} {
		if !shipped[want] {
			t.Errorf("%s is not listed as shipped", want)
		}
	}
	for _, p := range bom.Metadata.Component.Properties {
		if p.Name == "raftseg:third-party-modules" && p.Value != "0" {
			t.Errorf("third-party modules: %s", p.Value)
		}
		if p.Name == "raftseg:go-mod-requires" && !strings.HasPrefix(p.Value, raftModule+"@v") {
			t.Errorf("go.mod requires: %s", p.Value)
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
