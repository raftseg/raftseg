// Copyright 2026 The raftseg authors.

package seglog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// forgedChunk frames record as a Full chunk whose checksum is computed with
// key: what a client can put inside an entry's data if it knows the key
// (or if there is none, as with key 0).
func forgedChunk(record []byte, key uint32) []byte {
	c := make([]byte, chunkHeaderSize, chunkHeaderSize+len(record))
	c[6] = chunkFull
	c = append(c, record...)
	binary.LittleEndian.PutUint16(c[4:], uint16(len(record)))
	binary.LittleEndian.PutUint32(c[0:], chunkCRC(key, c[6:]))
	return c
}

// recordAt finds the record of group g, entry index in segment 1, and
// returns its offset.
func recordAt(t *testing.T, fs *crashfs.FS, key uint32, g, index uint64) rawRecord {
	t.Helper()
	f, err := fs.Open(segPathN(1))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc, _ := newSegScanner(f, key)
	for pos := int64(0); ; {
		rec, err := sc.next(pos)
		if err != nil {
			t.Fatalf("record of group %d entry %d not found: %v", g, index, err)
		}
		if typ, gid, payload, _ := decodeRecord(rec.data); typ == recEntry && gid == g {
			if i, _, _ := entryHeader(payload); i == index {
				return rec
			}
		}
		pos = rec.end
	}
}

// A client's entry carries a forged commit marker that claims the segment
// was durable far beyond it. The commit holding that entry is then torn,
// before the entry, as a crash during its write would leave it. Recovery
// must see an interrupted write and recover the commits before it. Without
// the checksum key (key 0), the scanner found the forged marker while
// looking for what follows the damage, took the tear for corruption, and
// refused to open: one client could keep every node of a cluster down after
// a power cut.
func TestForgedMarkerInPayload(t *testing.T) {
	for _, zeroKey := range []bool{false, true} {
		mem := crashfs.New()
		l, err := Open(crashDir, memOpts(mem, Options{testZeroKey: zeroKey, testDropIndexAfter: 1}))
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Save(Update{Group: 1, Entries: ents(1, 1, 3, 400), HardState: raftpb.HardState{Term: 1, Commit: 3}}); err != nil {
			t.Fatal(err)
		}
		// The attacker cannot know the key; key 0 is what the format used.
		marker := encodeRecord(nil, recCommit, 0, commitMarker{seq: 99, nrecs: 1, start: 1 << 40, synced: 1 << 40}.encode(nil))
		payload := append(bytes.Repeat([]byte{0xaa}, 2000), forgedChunk(marker, 0)...)
		if err := l.Save(Update{Group: 2, Entries: []raftpb.Entry{{Index: 1, Term: 1, Data: payload}}, HardState: raftpb.HardState{Term: 1, Commit: 1}}); err != nil {
			t.Fatal(err)
		}
		key := l.key
		l.crash()
		// Tear the last commit: the first whole sector of the entry's record
		// never reached the disk. (The forged marker is further on.)
		rec := recordAt(t, mem, key, 2, 1)
		f, _ := mem.OpenReadWrite(segPathN(1))
		lo := (rec.start + tornSector - 1) &^ (tornSector - 1)
		f.WriteAt(make([]byte, tornSector), lo)
		f.Close()
		l, err = Open(crashDir, memOpts(mem, Options{}))
		if zeroKey {
			if err == nil {
				l.Close()
				t.Fatal("without a key, the forged marker was not found: the test no longer shows the attack")
			}
			continue
		}
		if err != nil {
			t.Fatalf("a forged marker in an entry made Open refuse a torn write: %v", err)
		}
		if last, _ := l.Group(1).LastIndex(); last != 3 {
			t.Fatalf("group 1 last %d", last)
		}
		st, _ := l.Stats()
		if st.Recovery.Tail == nil || !st.Recovery.Tail.TornLike {
			t.Fatalf("tail %+v", st.Recovery.Tail)
		}
		l.Close()
	}
}

// A client's entry carries a forged entry record for another group. Bit rot
// then damages the header of the client's record in data a checkpoint
// covers, and Open looks for the next record at every offset after it.
// Without the key it found the forged record and indexed it: reads of the
// other group's entry returned the client's data. With the key the forged
// record is not a record, and the real entry is read.
func TestForgedEntryAfterDamage(t *testing.T) {
	for _, zeroKey := range []bool{false, true} {
		mem := crashfs.New()
		l, err := Open(crashDir, memOpts(mem, Options{testZeroKey: zeroKey, CacheEntries: -1}))
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Save(Update{Group: 1, Entries: ents(1, 1, 10, 100), HardState: raftpb.HardState{Term: 1, Commit: 10}}); err != nil {
			t.Fatal(err)
		}
		evil := raftpb.Entry{Index: 5, Term: 1, Data: []byte("EVIL")}
		eb, _ := evil.Marshal()
		payload := append(bytes.Repeat([]byte{0xbb}, 100), forgedChunk(encodeRecord(nil, recEntry, 1, eb), 0)...)
		payload = append(payload, bytes.Repeat([]byte{0xcc}, 100)...)
		if err := l.Save(Update{Group: 2, Entries: []raftpb.Entry{{Index: 1, Term: 1, Data: payload}}, HardState: raftpb.HardState{Term: 1, Commit: 1}}); err != nil {
			t.Fatal(err)
		}
		key := l.key
		l.Close() // the checkpoint covers everything
		rec := recordAt(t, mem, key, 2, 1)
		f, _ := mem.OpenReadWrite(segPathN(1))
		b := []byte{0}
		f.ReadAt(b, rec.start)
		b[0] ^= 0xff // the record's checksum
		f.WriteAt(b, rec.start)
		f.Close()
		// Without its index sidecar, Open indexes the segment by scanning it.
		if err := mem.Remove(crashDir + "/segments/" + idxName(1)); err != nil {
			t.Fatal(err)
		}
		l, err = Open(crashDir, memOpts(mem, Options{CacheEntries: -1}))
		if err != nil {
			t.Fatal(err)
		}
		es, err := l.Group(1).Entries(5, 6, 1<<20)
		l.Close()
		if zeroKey {
			if err != nil || len(es) != 1 || string(es[0].Data) != "EVIL" {
				t.Fatalf("without a key, the forged entry was not read (%v): the test no longer shows the attack", err)
			}
			continue
		}
		if err != nil && !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
		if err == nil && (len(es) != 1 || !entryDataOK(es[0])) {
			t.Fatalf("group 1 entry 5 reads %q: a forged record was indexed", es[0].Data)
		}
	}
}
