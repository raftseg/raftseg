// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package seglog

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft/raftpb"
)

// testdata/v0_log.tar.gz is a log written by seglog before raft moved to
// google.golang.org/protobuf, when raftpb was gogo's generated code; its
// generator is testdata/genv0log.go.txt, and testdata/v0_log.json records
// what that version read back: each group's hard state, configuration,
// indexes and snapshot, and the encoding of each entry. The log holds
// entries of every type, empty and spanning several blocks, an overwritten
// suffix, a compaction, a snapshot in a file and one inline, a removed
// group, a group with only a hard state, and several segments with their
// checkpoints and indexes.
//
// TestReadsV0Log checks that seglog reads the log as that version did,
// each entry encoding to the same bytes, then appends to every group and
// reads the log again after a restart.
func TestReadsV0Log(t *testing.T) {
	var want struct {
		Groups []struct {
			ID                   uint64
			HardState, ConfState []byte
			First, Last          uint64
			Entries              []struct {
				Index, Term uint64
				Type        int32
				Size        int
				Sum         string
			}
			Snapshot struct {
				Metadata []byte
				DataSum  string
			}
		}
		Removed []uint64
	}
	b, err := os.ReadFile("testdata/v0_log.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &want); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	untar(t, "testdata/v0_log.tar.gz", dir)
	opts := Options{SegmentSize: 64 << 10, SnapshotInlineBytes: 512}

	sum := func(b []byte) string {
		s := sha256.Sum256(b)
		return hex.EncodeToString(s[:])
	}
	sameCS := func(a, b *raftpb.ConfState) bool {
		return slices.Equal(a.GetVoters(), b.GetVoters()) && slices.Equal(a.GetLearners(), b.GetLearners()) &&
			slices.Equal(a.GetVotersOutgoing(), b.GetVotersOutgoing()) && slices.Equal(a.GetLearnersNext(), b.GetLearnersNext()) &&
			a.GetAutoLeave() == b.GetAutoLeave()
	}
	// appended[id] are the entries appended to group id since the log was
	// written.
	appended := map[uint64][]*raftpb.Entry{}
	check := func(l *Log) {
		t.Helper()
		var ids []uint64
		for _, g := range want.Groups {
			ids = append(ids, g.ID)
		}
		slices.Sort(ids)
		got := l.Groups() // in no particular order
		slices.Sort(got)
		if !slices.Equal(got, ids) {
			t.Fatalf("groups %v, want %v", got, ids)
		}
		for _, g := range want.Groups {
			s := l.Group(g.ID)
			hs, cs, err := s.InitialState()
			if err != nil {
				t.Fatal(err)
			}
			whs, wcs := &raftpb.HardState{}, &raftpb.ConfState{}
			if err := proto.Unmarshal(g.HardState, whs); err != nil {
				t.Fatal(err)
			}
			if err := proto.Unmarshal(g.ConfState, wcs); err != nil {
				t.Fatal(err)
			}
			if n := len(appended[g.ID]); n > 0 {
				whs = &raftpb.HardState{Term: new(appended[g.ID][n-1].GetTerm()), Vote: whs.Vote, Commit: whs.Commit}
			}
			if hs.GetTerm() != whs.GetTerm() || hs.GetVote() != whs.GetVote() || hs.GetCommit() != whs.GetCommit() {
				t.Errorf("group %d: hard state %v, want %v", g.ID, hs, whs)
			}
			if !sameCS(cs, wcs) {
				t.Errorf("group %d: configuration %v, want %v", g.ID, cs, wcs)
			}
			first, err := s.FirstIndex()
			if err != nil {
				t.Fatal(err)
			}
			last, err := s.LastIndex()
			if err != nil {
				t.Fatal(err)
			}
			wantLast := g.Last + uint64(len(appended[g.ID]))
			if first != g.First || last != wantLast {
				t.Errorf("group %d: indexes [%d, %d], want [%d, %d]", g.ID, first, last, g.First, wantLast)
			}
			if last >= first {
				es, err := s.Entries(first, last+1, math.MaxUint64)
				if err != nil {
					t.Fatal(err)
				}
				if len(es) != len(g.Entries)+len(appended[g.ID]) {
					t.Fatalf("group %d: %d entries, want %d", g.ID, len(es), len(g.Entries)+len(appended[g.ID]))
				}
				for i, we := range g.Entries {
					e := es[i]
					enc, err := proto.Marshal(e)
					if err != nil {
						t.Fatal(err)
					}
					if e.GetIndex() != we.Index || e.GetTerm() != we.Term || int32(e.GetType()) != we.Type || len(enc) != we.Size || sum(enc) != we.Sum {
						t.Errorf("group %d: entry %d (term %d, type %s, %d bytes) does not encode as it did (index %d, term %d, type %d, %d bytes)",
							g.ID, e.GetIndex(), e.GetTerm(), e.GetType(), len(enc), we.Index, we.Term, we.Type, we.Size)
					}
				}
				for i, ae := range appended[g.ID] {
					if e := es[len(g.Entries)+i]; !proto.Equal(e, ae) {
						t.Errorf("group %d: appended entry %v, want %v", g.ID, e, ae)
					}
				}
			}
			snap, err := s.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			wmd := &raftpb.SnapshotMetadata{}
			if err := proto.Unmarshal(g.Snapshot.Metadata, wmd); err != nil {
				t.Fatal(err)
			}
			md := snap.GetMetadata()
			if md.GetIndex() != wmd.GetIndex() || md.GetTerm() != wmd.GetTerm() || !sameCS(md.GetConfState(), wmd.GetConfState()) || sum(snap.GetData()) != g.Snapshot.DataSum {
				t.Errorf("group %d: snapshot %v (data %s), want %v (data %s)", g.ID, md, sum(snap.GetData()), wmd, g.Snapshot.DataSum)
			}
		}
		for _, id := range want.Removed {
			if slices.Contains(l.Groups(), id) {
				t.Errorf("removed group %d is back", id)
			}
		}
		rep, err := l.Scrub()
		if err != nil {
			t.Fatal(err)
		}
		if len(rep.Problems) > 0 || rep.IndexedEntries == 0 {
			t.Errorf("scrub: %+v", rep)
		}
	}

	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	check(l)

	// Append to every group, then restart.
	for _, g := range want.Groups {
		s := l.Group(g.ID)
		var term uint64 = 7
		var ents []*raftpb.Entry
		for i := g.Last + 1; i <= g.Last+3; i++ {
			ents = append(ents, &raftpb.Entry{Term: new(term), Index: new(i), Type: raftpb.EntryNormal.Enum(), Data: []byte("after the move to protobuf")})
		}
		hs, _, err := s.InitialState()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Save(&raftpb.HardState{Term: new(term), Vote: hs.Vote, Commit: hs.Commit}, ents, nil); err != nil {
			t.Fatalf("group %d: %v", g.ID, err)
		}
		appended[g.ID] = ents
	}
	check(l)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	check(l)
}

// untar extracts the regular files of a .tar.gz archive into dir.
func untar(t *testing.T, archive, dir string) {
	t.Helper()
	f, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(zr)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag != tar.TypeReg || !filepath.IsLocal(h.Name) {
			t.Fatalf("unexpected archive entry %q", h.Name)
		}
		p := filepath.Join(dir, filepath.FromSlash(h.Name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
