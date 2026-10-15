// Copyright 2026 The raftseg authors.

package seglog

// A group id may be reused after RemoveGroup, by a raft group with another
// history. Records of the removed group and of the new one can then hold
// the same group, index and term with different data, so neither the
// record's header nor the term check tells them apart. Reads must never
// return a record of the removed group.

import (
	"errors"
	"io"
	"testing"

	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// entryRecords returns the records of entry index of group g in segment
// seg, oldest first.
func entryRecords(t *testing.T, mem *crashfs.FS, seg uint64, key uint32, g, index uint64) []rawRecord {
	t.Helper()
	f, err := mem.Open(segPathN(seg))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc, err := newSegScanner(f, key)
	if err != nil {
		t.Fatal(err)
	}
	var out []rawRecord
	for pos := int64(0); ; {
		rec, err := sc.next(pos)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		pos = rec.end
		typ, id, payload, err := decodeRecord(rec.data)
		if err != nil || typ != recEntry || id != g {
			continue
		}
		if i, _, err := entryHeader(payload); err == nil && i == index {
			out = append(out, rec)
		}
	}
}

// The new group's record of an entry is damaged where the checkpoint says
// the data was durable, and the segment's index sidecar is gone. Open
// rebuilds the index from what it can read, which includes the removed
// group's record of the same entry, index and term. Reading the entry must
// fail, not return the removed group's data. (Found by FuzzCorruptLog.)
func TestRemovedGroupRecordNotReturned(t *testing.T) {
	for _, tt := range []struct {
		name   string
		reopen bool // the new group was created after a restart
		crash  bool // and its first commit was replayed after a crash
	}{
		{"same process", false, false},
		{"after a restart", true, false},
		{"replayed after a crash", true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mem := crashfs.New()
			l := open(t, crashDir, memOpts(mem, Options{}))
			save := func(size int) {
				t.Helper()
				if err := l.Save(Update{Group: 3, Entries: ents(1, 1, 5, size), HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(5))}}); err != nil {
					t.Fatal(err)
				}
			}
			save(100)
			if err := l.RemoveGroup(3); err != nil {
				t.Fatal(err)
			}
			if tt.reopen {
				l.Close()
				l = open(t, crashDir, memOpts(mem, Options{}))
			}
			// Another group 3: the same indexes and terms, other data.
			save(300)
			if tt.crash {
				l.crash()
				l = open(t, crashDir, memOpts(mem, Options{}))
				if st, _ := l.Stats(); st.Recovery.ReplayedCommits == 0 {
					t.Fatal("nothing replayed")
				}
			}
			key := l.key
			seg := l.active
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			recs := entryRecords(t, mem, seg, key, 3, 4)
			if len(recs) != 2 {
				t.Fatalf("%d records of entry 4 in segment %d, want 2", len(recs), seg)
			}
			// Damage the new record of entry 4, and lose the sidecar.
			b := mustRead(t, mem, segPathN(seg))
			b[recs[1].start+chunkHeaderSize+20] ^= 0x55
			rewrite(t, mem, segPathN(seg), b)
			if err := mem.Remove(crashDir + "/segments/" + idxName(seg)); err != nil {
				t.Fatal(err)
			}

			l, err := Open(crashDir, memOpts(mem, Options{}))
			if err != nil {
				t.Fatalf("open: %v", err) // the damage is reported, the rest stays readable
			}
			defer l.Close()
			st, _ := l.Stats()
			if len(st.Recovery.Damage) == 0 {
				t.Fatal("damage not reported")
			}
			s := l.Group(3)
			got, err := s.Entries(4, 5, 1<<20)
			if err == nil {
				t.Fatalf("entry 4 read as %d bytes: the removed group's record", len(got[0].Data))
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("entry 4: %v, want ErrCorrupt", err)
			}
			// The undamaged entries still read, with the new group's data.
			for _, r := range [][2]uint64{{1, 4}, {5, 6}} {
				es, err := s.Entries(r[0], r[1], 1<<20)
				if err != nil {
					t.Fatalf("Entries(%d, %d): %v", r[0], r[1], err)
				}
				for _, e := range es {
					if len(e.Data) != 300 || !entryDataOK(e) {
						t.Fatalf("entry %d: %d bytes", e.GetIndex(), len(e.Data))
					}
				}
			}
			rep, err := l.Scrub()
			if err != nil {
				t.Fatal(err)
			}
			if len(rep.Problems) == 0 {
				t.Fatal("Scrub found nothing")
			}
		})
	}
}
