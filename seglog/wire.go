// Copyright 2026 The raftseg authors.

package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
)

// The segment files use pebble/record's legacy chunk format byte for byte
// (wire_test.go checks this against streams written by Pebble's own Writer,
// kept in testdata). The reader and writer are implemented here because pebble's
// record.Writer can only start at offset 0 of a fresh file and its Reader
// cannot report record offsets, resynchronise after damage, or tell a torn
// tail from corruption. With absolute block alignment, this writer can resume
// a segment at any record boundary after recovery.

const (
	chunkFull   = 1
	chunkFirst  = 2
	chunkMiddle = 3
	chunkLast   = 4
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// chunkCRC is the masked CRC-32C of a chunk's type and payload, computed
// from the log's key instead of from 0. With key 0 it is pebble/record's
// checksum.
//
// The key makes chunks unforgeable from inside a payload. After damage, the
// scanner looks for the next valid chunk at every byte offset, including
// offsets inside entries, whose bytes come from clients. Without a key, a
// client could embed a well-formed chunk in an entry (a commit marker, say,
// or an entry for another group), and a crash that tore the commit before it
// would have recovery read it as the log's own: refusing to open, or
// indexing the forged entry. With a random key that only the log knows,
// such a chunk verifies with probability 2^-32. Files from another log fail
// every checksum too.
func chunkCRC(key uint32, b []byte) uint32 {
	c := crc32.Update(key, crcTable, b)
	return (c>>15 | c<<17) + 0xa282ead8
}

var zeroBlock [blockSize]byte

// segWriter appends records at absolute offsets with 32 KiB block alignment.
type segWriter struct {
	f       File
	key     uint32 // checksum key (see chunkCRC)
	flushed int64  // [0, flushed) is written to the file
	buf     []byte // bytes for [flushed, pos())
}

func newSegWriter(f File, off int64, key uint32) *segWriter {
	return &segWriter{f: f, flushed: off, key: key}
}

func (w *segWriter) pos() int64 { return w.flushed + int64(len(w.buf)) }

// add frames payload as one record and returns its [start, end) offsets.
func (w *segWriter) add(payload []byte) (start, end int64) {
	pos := w.pos()
	if rem := blockSize - int(pos&blockMask); rem < chunkHeaderSize {
		w.buf = append(w.buf, zeroBlock[:rem]...)
		pos += int64(rem)
	}
	start = pos
	first := true
	for {
		avail := blockSize - int(pos&blockMask) - chunkHeaderSize
		n := len(payload)
		if n > avail {
			n = avail
		}
		last := n == len(payload)
		var typ byte
		switch {
		case first && last:
			typ = chunkFull
		case first:
			typ = chunkFirst
		case last:
			typ = chunkLast
		default:
			typ = chunkMiddle
		}
		h := len(w.buf)
		w.buf = append(w.buf, 0, 0, 0, 0, 0, 0, typ)
		w.buf = append(w.buf, payload[:n]...)
		binary.LittleEndian.PutUint16(w.buf[h+4:], uint16(n))
		binary.LittleEndian.PutUint32(w.buf[h:], chunkCRC(w.key, w.buf[h+6:]))
		pos += int64(chunkHeaderSize + n)
		payload = payload[n:]
		first = false
		if last {
			return start, pos
		}
	}
}

// truncate discards buffered (unflushed) bytes beyond pos.
func (w *segWriter) truncate(pos int64) {
	if pos >= w.flushed && pos <= w.pos() {
		w.buf = w.buf[:pos-w.flushed]
	}
}

func (w *segWriter) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	if _, err := w.f.WriteAt(w.buf, w.flushed); err != nil {
		return err
	}
	w.flushed += int64(len(w.buf))
	w.buf = w.buf[:0]
	return nil
}

// decodeChunks parses the chunks of one record laid out in buf, which starts
// at absolute offset start, validating CRCs, chunk order and alignment.
func decodeChunks(buf []byte, start int64, key uint32) ([]byte, error) {
	var out []byte
	pos, i, first := start, 0, true
	for {
		if len(buf)-i < chunkHeaderSize {
			return nil, fmt.Errorf("truncated chunk header at %d", pos)
		}
		h := buf[i:]
		sum := binary.LittleEndian.Uint32(h[0:4])
		n := int(binary.LittleEndian.Uint16(h[4:6]))
		typ := h[6]
		if chunkHeaderSize+n > blockSize-int(pos&blockMask) {
			return nil, fmt.Errorf("chunk at %d crosses a block boundary", pos)
		}
		if chunkHeaderSize+n > len(buf)-i {
			return nil, fmt.Errorf("chunk at %d runs past the record end", pos)
		}
		if chunkCRC(key, h[6:chunkHeaderSize+n]) != sum {
			return nil, fmt.Errorf("checksum mismatch in chunk at %d", pos)
		}
		switch {
		case first && (typ == chunkFull || typ == chunkFirst):
		case !first && (typ == chunkMiddle || typ == chunkLast):
		default:
			return nil, fmt.Errorf("unexpected chunk type %d at %d", typ, pos)
		}
		out = append(out, h[chunkHeaderSize:chunkHeaderSize+n]...)
		i += chunkHeaderSize + n
		pos += int64(chunkHeaderSize + n)
		if typ == chunkFull || typ == chunkLast {
			if i != len(buf) {
				return nil, fmt.Errorf("record ends at %d, index says %d", pos, start+int64(len(buf)))
			}
			return out, nil
		}
		if pos&blockMask != 0 {
			return nil, fmt.Errorf("continued chunk does not end at a block boundary (%d)", pos)
		}
		first = false
	}
}

// readRecord reads the record at loc with one ReadAt and validates it
// independently of the rest of the segment.
func readRecord(f io.ReaderAt, loc location, key uint32) ([]byte, error) {
	buf := make([]byte, loc.end-loc.start)
	if _, err := f.ReadAt(buf, loc.start); err != nil {
		return nil, readErr(loc.seg, loc.start, err)
	}
	payload, err := decodeChunks(buf, loc.start, key)
	if err != nil {
		return nil, fmt.Errorf("%w: segment %d record at %d: %v", ErrCorrupt, loc.seg, loc.start, err)
	}
	return payload, nil
}

// readErr describes a failed read of a record. A record that runs past the
// end of its segment means the file lost data after it was written: that is
// corruption, not an I/O error.
func readErr(seg uint64, at int64, err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: segment %d: record at %d runs past the end of the file", ErrCorrupt, seg, at)
	}
	return fmt.Errorf("seglog: segment %d: read at %d: %w", seg, at, err)
}

// ---- sequential scanning ----

// badData reports invalid bytes at an offset: a torn write, zeroes, or
// corruption. Which one it is depends on what follows (see classify).
type badData struct {
	at  int64
	why string
}

func (b *badData) Error() string { return fmt.Sprintf("invalid data at %d: %s", b.at, b.why) }

type rawRecord struct {
	start, end int64
	data       []byte
}

// segScanner reads a segment sequentially, one block at a time.
type segScanner struct {
	f    File
	key  uint32 // checksum key (see chunkCRC)
	size int64
	blk  []byte
	blkN int64
}

func newSegScanner(f File, key uint32) (*segScanner, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &segScanner{f: f, key: key, size: st.Size(), blk: make([]byte, blockSize), blkN: -1}, nil
}

func (s *segScanner) block(n int64) ([]byte, error) {
	if n == s.blkN {
		return s.blk[:s.blockLen(n)], nil
	}
	l := s.blockLen(n)
	if _, err := s.f.ReadAt(s.blk[:l], n*blockSize); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	s.blkN = n
	return s.blk[:l], nil
}

func (s *segScanner) blockLen(n int64) int {
	rem := s.size - n*blockSize
	if rem > blockSize {
		return blockSize
	}
	if rem < 0 {
		return 0
	}
	return int(rem)
}

// next reads the record that follows a record ending at pos. It returns
// io.EOF at the end of the file, *badData for invalid bytes, or an I/O error.
func (s *segScanner) next(pos int64) (rawRecord, error) {
	start := recordStart(pos)
	if start != pos {
		// Block padding must be zeroes.
		end := start
		if end > s.size {
			end = s.size
		}
		if end > pos {
			blk, err := s.block(pos / blockSize)
			if err != nil {
				return rawRecord{}, err
			}
			for _, c := range blk[int(pos&blockMask):] {
				if c != 0 {
					return rawRecord{}, &badData{pos, "non-zero block padding"}
				}
			}
		}
	}
	if start >= s.size {
		return rawRecord{}, io.EOF
	}
	var data []byte
	p, first := start, true
	for {
		if p >= s.size {
			return rawRecord{}, &badData{start, "record truncated by end of file"}
		}
		blk, err := s.block(p / blockSize)
		if err != nil {
			return rawRecord{}, err
		}
		off := int(p & blockMask)
		if off+chunkHeaderSize > len(blk) {
			return rawRecord{}, &badData{p, "truncated chunk header"}
		}
		h := blk[off:]
		sum := binary.LittleEndian.Uint32(h[0:4])
		n := int(binary.LittleEndian.Uint16(h[4:6]))
		typ := h[6]
		if sum == 0 && n == 0 && typ == 0 {
			return rawRecord{}, &badData{p, "zeroed chunk header"}
		}
		if off+chunkHeaderSize+n > len(blk) {
			return rawRecord{}, &badData{p, "chunk overruns block or file"}
		}
		if chunkCRC(s.key, h[6:chunkHeaderSize+n]) != sum {
			return rawRecord{}, &badData{p, "checksum mismatch"}
		}
		switch {
		case first && (typ == chunkFull || typ == chunkFirst):
		case !first && (typ == chunkMiddle || typ == chunkLast):
		default:
			return rawRecord{}, &badData{p, fmt.Sprintf("unexpected chunk type %d", typ)}
		}
		data = append(data, h[chunkHeaderSize:chunkHeaderSize+n]...)
		p += int64(chunkHeaderSize + n)
		if typ == chunkFull || typ == chunkLast {
			return rawRecord{start: start, end: p, data: data}, nil
		}
		if p&blockMask != 0 {
			return rawRecord{}, &badData{p, "continued chunk does not end at block boundary"}
		}
		first = false
	}
}

// resync finds the first valid record that starts after from. It tries
// every byte offset, so records that start later in the same block as the
// damage are found too (a record may start anywhere in a block). Only Full
// and First chunks with a valid CRC whose whole record parses count.
// It returns -1 if there is none.
func (s *segScanner) resync(from int64) (int64, error) {
	for p := from + 1; p+chunkHeaderSize <= s.size; p++ {
		off := int(p & blockMask)
		if off+chunkHeaderSize > blockSize {
			p = (p | blockMask) // continue at the next block
			continue
		}
		blk, err := s.block(p / blockSize)
		if err != nil {
			return -1, err
		}
		if off+chunkHeaderSize > len(blk) {
			return -1, nil
		}
		h := blk[off:]
		typ := h[6]
		if typ != chunkFull && typ != chunkFirst {
			continue
		}
		n := int(binary.LittleEndian.Uint16(h[4:6]))
		if off+chunkHeaderSize+n > len(blk) {
			continue
		}
		if chunkCRC(s.key, h[6:chunkHeaderSize+n]) != binary.LittleEndian.Uint32(h[0:4]) {
			continue
		}
		if _, err := s.next(p); err == nil {
			return p, nil
		}
	}
	return -1, nil
}

// tornLike reports whether invalid data at bad looks like a torn write
// (missing bytes read back as zeroes, or the file simply ends) rather than
// bit rot (non-zero bytes that fail their checksum).
func (s *segScanner) tornLike(bd *badData) (bool, error) {
	bad := bd.at
	if strings.Contains(bd.why, "end of file") || strings.Contains(bd.why, "truncated") || strings.Contains(bd.why, "zeroed") {
		return true, nil
	}
	if bad+chunkHeaderSize > s.size {
		return true, nil
	}
	blk, err := s.block(bad / blockSize)
	if err != nil {
		return false, err
	}
	off := int(bad & blockMask)
	if off+chunkHeaderSize > len(blk) {
		return true, nil
	}
	h := blk[off:]
	n := int(binary.LittleEndian.Uint16(h[4:6]))
	end := off + chunkHeaderSize + n
	if end > len(blk) {
		end = len(blk)
		if bad-int64(off)+int64(len(blk)) >= s.size {
			return true, nil // the chunk runs off the end of the file
		}
	}
	// Lost writes read back as zeroes (segments are preallocated and their
	// tails zeroed): a long zero run, or a chunk whose tail is zero (the
	// write stopped inside it).
	run := 0
	for _, c := range blk[off:end] {
		if c == 0 {
			if run++; run >= 64 {
				return true, nil
			}
		} else {
			run = 0
		}
	}
	if run >= 16 || (run > 0 && run == end-off) {
		return true, nil
	}
	// Storage loses writes in whole sectors. If the part of the damaged
	// chunk inside some sector is all zero, a write was lost there, however
	// few of the chunk's bytes that sector held (the end of the chunk, or
	// part of its header). Bit rot leaves non-zero bytes behind. (A chunk
	// whose own data has zeroes filling its share of a sector is therefore
	// classed as torn.)
	for s := off; s < end; {
		e := min((s/tornSector+1)*tornSector, end)
		zero := true
		for _, c := range blk[s:e] {
			if c != 0 {
				zero = false
				break
			}
		}
		if zero {
			return true, nil
		}
		s = e
	}
	return false, nil
}

// tornSector is the granularity at which writes are assumed to be lost.
const tornSector = 512

// classify decides what invalid data at bad means, given what follows it.
// Every commit marker records how far the segment was durable when that
// commit was written. If any valid later marker says the segment was
// durable beyond bad, the damaged bytes had been synced: that is corruption.
// Otherwise bad lies in commits that were written but not yet synced when
// the writer stopped (with pipelining there can be several), which is a torn
// tail: none of those commits had been acknowledged.
func (s *segScanner) classify(bad int64) (fatal bool, why string, err error) {
	p, err := s.resync(bad)
	if err != nil {
		return false, "", err
	}
	for p >= 0 && p < s.size {
		rec, err := s.next(p)
		if err == io.EOF {
			break
		}
		var bd *badData
		if errors.As(err, &bd) {
			if p, err = s.resync(bd.at); err != nil {
				return false, "", err
			}
			continue
		}
		if err != nil {
			return false, "", err
		}
		if typ, _, payload, derr := decodeRecord(rec.data); derr == nil && typ == recCommit {
			if m, derr := decodeCommitMarker(payload); derr == nil && m.synced > bad {
				return true, fmt.Sprintf("commit %d at %d says the segment was durable up to %d, past the damage at %d", m.seq, rec.start, m.synced, bad), nil
			}
		}
		p = rec.end
	}
	return false, "", nil
}

// allZero reports whether [from, size) holds only zero bytes.
func (s *segScanner) allZero(from int64) (bool, error) {
	for p := from; p < s.size; {
		blk, err := s.block(p / blockSize)
		if err != nil {
			return false, err
		}
		for _, c := range blk[p&blockMask:] {
			if c != 0 {
				return false, nil
			}
		}
		p = (p/blockSize + 1) * blockSize
	}
	return true, nil
}
