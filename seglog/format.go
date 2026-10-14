// Copyright 2026 The raftseg authors.

package seglog

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/raftseg/raft/raftpb"
)

// Segment wire format: pebble/record "legacy" chunks. The stream is divided
// into 32 KiB blocks; a record is one or more chunks, and a chunk never
// crosses a block boundary. Each chunk carries a masked CRC-32C.
const (
	blockSize       = 32 << 10
	blockMask       = blockSize - 1
	chunkHeaderSize = 7 // crc(4) length(2) type(1)
)

// formatVersion identifies this on-disk format. Open refuses any other.
// Version 4: no key-value index; every state change is a segment record,
// with a checkpoint file and per-segment index sidecars.
// Version 5: chunk checksums are keyed with a random per-log key, kept in
// the checkpoint (see chunkCRC); the checkpoint records where each group's
// first entry record is (see group.born).
const formatVersion = 5

// Record types stored in segment files. Every change to the log's state is
// one of these; the checkpoint only bounds how far back Open must replay.
const (
	recEntry     byte = 1 // uvarint(group) + raftpb.Entry; may truncate a suffix
	recHardState byte = 2 // uvarint(group) + raftpb.HardState
	recCommit    byte = 3 // uvarint(0) + commit marker; ends every group commit
	recRelocate  byte = 4 // uvarint(group) + raftpb.Entry; moves a live entry, never truncates
	recSnapshot  byte = 5 // uvarint(group) + snapshot record (see encodeSnapRecord)
	recConfState byte = 6 // uvarint(group) + raftpb.ConfState
	recRemove    byte = 7 // uvarint(group); the group's state is deleted
)

// recordStart is where a record that follows a record ending at prevEnd
// begins: if a chunk header does not fit in the rest of the block, the block
// is zero-padded and the record starts at the next block boundary.
func recordStart(prevEnd int64) int64 {
	if int(prevEnd&blockMask)+chunkHeaderSize > blockSize {
		return (prevEnd + blockMask) &^ blockMask
	}
	return prevEnd
}

func encodeRecord(dst []byte, typ byte, group uint64, payload []byte) []byte {
	dst = append(dst[:0], typ)
	dst = binary.AppendUvarint(dst, group)
	return append(dst, payload...)
}

func decodeRecord(b []byte) (typ byte, group uint64, payload []byte, err error) {
	if len(b) < 2 {
		return 0, 0, nil, fmt.Errorf("short record (%d bytes)", len(b))
	}
	typ = b[0]
	group, n := binary.Uvarint(b[1:])
	if n <= 0 {
		return 0, 0, nil, fmt.Errorf("bad group varint")
	}
	return typ, group, b[1+n:], nil
}

// entryHeader reads the index and term of a marshalled raftpb.Entry without
// copying its data.
func entryHeader(b []byte) (index, term uint64, err error) {
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, 0, fmt.Errorf("bad entry field tag")
		}
		b = b[n:]
		field, wire := tag>>3, tag&7
		switch wire {
		case 0:
			v, n := binary.Uvarint(b)
			if n <= 0 {
				return 0, 0, fmt.Errorf("bad entry varint")
			}
			b = b[n:]
			switch field {
			case 2:
				term = v
			case 3:
				index = v
			}
		case 2:
			l, n := binary.Uvarint(b)
			if n <= 0 || l > uint64(len(b)-n) {
				return 0, 0, fmt.Errorf("bad entry field length")
			}
			b = b[n+int(l):]
		default:
			return 0, 0, fmt.Errorf("unexpected entry wire type %d", wire)
		}
	}
	return index, term, nil
}

// commitMarker ends every group commit. Replay applies a commit only once
// its marker is read, so a commit is recovered entirely or not at all.
type commitMarker struct {
	seq   uint64 // global, strictly increasing by one
	nrecs uint64 // records in the commit, excluding the marker
	start int64  // offset of the commit's first record
	// synced is the offset up to which this segment was known to be durable
	// (an fsync had completed) when the commit was written. Commits are
	// pipelined, so several unsynced commits may follow each other; damage
	// below a later marker's synced offset is corruption, damage above it may
	// be an interrupted write.
	synced int64
}

func (m commitMarker) encode(dst []byte) []byte {
	dst = binary.AppendUvarint(dst[:0], m.seq)
	dst = binary.AppendUvarint(dst, m.nrecs)
	dst = binary.AppendUvarint(dst, uint64(m.start))
	return binary.AppendUvarint(dst, uint64(m.synced))
}

func decodeCommitMarker(b []byte) (commitMarker, error) {
	var m commitMarker
	var n int
	if m.seq, n = binary.Uvarint(b); n <= 0 {
		return m, fmt.Errorf("bad commit seq")
	}
	b = b[n:]
	if m.nrecs, n = binary.Uvarint(b); n <= 0 {
		return m, fmt.Errorf("bad commit record count")
	}
	b = b[n:]
	start, n := binary.Uvarint(b)
	if n <= 0 {
		return m, fmt.Errorf("bad commit start")
	}
	b = b[n:]
	synced, n := binary.Uvarint(b)
	if n <= 0 || n != len(b) || synced > start {
		return m, fmt.Errorf("bad commit synced offset")
	}
	m.start, m.synced = int64(start), int64(synced)
	return m, nil
}

// ---- snapshots ----

// Snapshot record flags.
const (
	snapApplied = 1 // received through Save: replaces the group's log
	snapExt     = 2 // data lives in a file; a snapRef precedes the metadata
	snapSetCS   = 4 // CreateSnapshot with a ConfState: it becomes the group's
)

// snapInfo is a group's current snapshot: metadata and inline data, or a
// reference to the file holding the data.
type snapInfo struct {
	meta raftpb.SnapshotMetadata
	data []byte   // inline data
	ref  *snapRef // set if the data is in a file
}

func (s *snapInfo) index() uint64 {
	if s == nil {
		return 0
	}
	return s.meta.Index
}

// snapRef points at snapshot data kept in its own file.
type snapRef struct {
	index uint64
	size  uint64
	crc   uint32
}

func (r snapRef) encode(dst []byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, r.index)
	dst = binary.BigEndian.AppendUint64(dst, r.size)
	return binary.BigEndian.AppendUint32(dst, r.crc)
}

func decodeSnapRef(b []byte) (snapRef, error) {
	if len(b) < 20 {
		return snapRef{}, fmt.Errorf("%w: bad snapshot reference length %d", ErrCorrupt, len(b))
	}
	return snapRef{index: binary.BigEndian.Uint64(b[0:]), size: binary.BigEndian.Uint64(b[8:]), crc: binary.BigEndian.Uint32(b[16:])}, nil
}

func encodeSnapRecord(flags byte, s *snapInfo) ([]byte, error) {
	if s.ref != nil {
		flags |= snapExt
	}
	snap := raftpb.Snapshot{Metadata: s.meta, Data: s.data}
	m, err := snap.Marshal()
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 1+20+len(m))
	out = append(out, flags)
	if s.ref != nil {
		out = s.ref.encode(out)
	}
	return append(out, m...), nil
}

func decodeSnapRecord(b []byte) (flags byte, s *snapInfo, err error) {
	if len(b) < 1 || b[0]&^(snapApplied|snapExt|snapSetCS) != 0 {
		return 0, nil, fmt.Errorf("%w: bad snapshot record flags", ErrCorrupt)
	}
	flags, b = b[0], b[1:]
	s = &snapInfo{}
	if flags&snapExt != 0 {
		ref, err := decodeSnapRef(b)
		if err != nil {
			return 0, nil, err
		}
		s.ref, b = &ref, b[20:]
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(b); err != nil {
		return 0, nil, fmt.Errorf("%w: snapshot record: %v", ErrCorrupt, err)
	}
	s.meta, s.data = snap.Metadata, snap.Data
	if s.ref != nil && (s.ref.index != s.meta.Index || len(s.data) > 0) {
		return 0, nil, fmt.Errorf("%w: snapshot record %d refers to data of %d", ErrCorrupt, s.meta.Index, s.ref.index)
	}
	return flags, s, nil
}

// ---- segment files ----

const (
	segSuffix = ".seg"
	idxSuffix = ".idx" // per-segment entry index (see segindex.go)
)

// maxSegment bounds segment numbers, so that the next number never wraps.
// One is used per SegmentSize bytes written.
const maxSegment = 1<<63 - 1

func segName(n uint64) string { return fmt.Sprintf("%020d%s", n, segSuffix) }
func idxName(n uint64) string { return fmt.Sprintf("%020d%s", n, idxSuffix) }

// listNumbered returns the numbers of the files in dir named <n><suffix>.
func listNumbered(fs FS, dir, suffix string) ([]uint64, error) {
	names, err := fs.List(dir)
	if err != nil {
		return nil, err
	}
	var out []uint64
	for _, name := range names {
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(name, suffix), 10, 64)
		if err != nil {
			continue
		}
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func listSegments(fs FS, dir string) ([]uint64, error) { return listNumbered(fs, dir, segSuffix) }

func syncDir(fs FS, dir string) error {
	d, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// location says where an entry lives in the segment log.
type location struct {
	seg   uint64
	start int64 // offset of the record's first chunk header
	end   int64 // offset just past the record's last chunk
	term  uint64
}

// position is a place in the segment log; positions order records by when
// they were written. The zero position is "none".
type position struct {
	seg uint64
	off int64
}

// after reports whether p is later than the record at loc: the record was
// written before p.
func (p position) after(loc location) bool {
	return loc.seg < p.seg || (loc.seg == p.seg && loc.start < p.off)
}

// seglog supports 64-bit platforms only (as does raftpb, which fails the
// build elsewhere): offsets, sizes and counts are converted to int freely.
const _ uint = strconv.IntSize - 64

// maxSegmentBytes keeps every offset in a segment within 32 bits (the entry
// index stores offsets as uint32): SegmentSize, MaxBatchBytes and the size
// of one request are each capped at 1 GiB, so a segment never reaches 4 GiB.
const maxSegmentBytes = 1 << 30

// maxSegmentFileBytes bounds a segment file's size. A segment is sealed once
// it reaches SegmentSize (at most 1 GiB); the commit that crosses it holds
// at most MaxBatchBytes (1 GiB) plus one more request (1 GiB), and framing
// adds well under a sixteenth.
const maxSegmentFileBytes = 3*maxSegmentBytes + maxSegmentBytes/16

// groupMeta is the shape of a group's log: entries [first, last] are
// available, and compactTerm is the term of entry first-1.
type groupMeta struct {
	first, last, compactTerm uint64
}

// segMeta describes a segment: how far it is reflected in the log's state,
// and whether it is sealed (complete and never written again).
type segMeta struct {
	count   uint64 // records, including commit markers
	entries uint64 // entry and relocation records among them
	end     int64  // offset just past the last commit
	sealed  bool
}
