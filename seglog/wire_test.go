// Copyright 2026 The raftseg authors.

package seglog

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"slices"
	"testing"

	"github.com/raftseg/raftseg/internal/crashfs"
)

// payloadSizes exercises the block-boundary cases: exactly 7 bytes left in a
// block (zero-length first chunk), payloads ending on a boundary, padding,
// and multi-block records.
func payloadSizes(rng *rand.Rand, n int) []int {
	sizes := []int{0, 1, blockSize - 2*chunkHeaderSize, blockSize - chunkHeaderSize - 3, 0, 70 << 10, 5}
	for len(sizes) < n {
		switch rng.Intn(4) {
		case 0:
			sizes = append(sizes, rng.Intn(100))
		case 1:
			sizes = append(sizes, rng.Intn(blockSize))
		case 2:
			sizes = append(sizes, rng.Intn(3*blockSize))
		default:
			sizes = append(sizes, blockSize-chunkHeaderSize-rng.Intn(16))
		}
	}
	return sizes
}

func makePayloads(rng *rand.Rand, sizes []int) [][]byte {
	out := make([][]byte, len(sizes))
	for i, n := range sizes {
		out[i] = make([]byte, n)
		rng.Read(out[i])
	}
	return out
}

// wireGolden is testdata/wire_golden.json: streams written by Pebble's
// record.Writer (github.com/cockroachdb/pebble/v2 v2.1.7, legacy chunks) by
// testdata/genwire.go.txt.
type wireGolden struct {
	Generator string       `json:"generator"`
	Random    []wireStream `json:"random"`
	Edge      wireStream   `json:"edge"`
}

type wireStream struct {
	Seed   int64   `json:"seed"`
	N      int     `json:"n"`
	Sizes  []int   `json:"sizes"`
	Ends   []int64 `json:"ends"`
	Size   int     `json:"size"`
	SHA256 string  `json:"sha256"`
}

func loadWireGolden(t *testing.T) wireGolden {
	t.Helper()
	b, err := os.ReadFile("testdata/wire_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var g wireGolden
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatal(err)
	}
	if len(g.Random) == 0 || len(g.Edge.Sizes) == 0 {
		t.Fatal("empty golden file")
	}
	return g
}

// edgePayloads returns the payloads of the golden edge stream, and the stream
// as Pebble wrote it.
func edgePayloads(t *testing.T, g wireGolden) ([][]byte, []byte) {
	t.Helper()
	f, err := os.Open("testdata/wire_edge.bin.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	payloads := make([][]byte, len(g.Edge.Sizes))
	for i, n := range g.Edge.Sizes {
		payloads[i] = bytes.Repeat([]byte{byte(i + 1)}, n)
	}
	return payloads, data
}

// fileBytes returns the contents of a file. With a nil t it panics on error.
func fileBytes(t *testing.T, fs FS, name string) []byte {
	fail := func(err error) {
		if t == nil {
			panic(err)
		}
		t.Helper()
		t.Fatal(err)
	}
	f, err := fs.Open(name)
	if err != nil {
		fail(err)
		return nil
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		fail(err)
		return nil
	}
	b := make([]byte, st.Size())
	if n, err := f.ReadAt(b, 0); n != len(b) || (err != nil && err != io.EOF) {
		fail(fmt.Errorf("read %s: %d of %d bytes: %v", name, n, len(b), err))
	}
	return b
}

// writeStream writes payloads with our writer, flushing at arbitrary points
// (including mid-block), and returns the bytes and each record's end.
func writeStream(t *testing.T, payloads [][]byte, flushEvery int) ([]byte, []int64) {
	t.Helper()
	fs := crashfs.New()
	f, _ := fs.Create("seg")
	w := newSegWriter(f, 0, 0)
	var ends []int64
	for i, p := range payloads {
		_, end := w.add(p)
		ends = append(ends, end)
		if i%flushEvery == 0 {
			if err := w.flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	return fileBytes(t, fs, "seg"), ends
}

// scanStream parses data with our scanner and positioned reader, and checks
// it holds exactly payloads, ending at ends.
func scanStream(t *testing.T, data []byte, payloads [][]byte, ends []int64) {
	t.Helper()
	sc := &segScanner{f: crashfs.NewFile(data), size: int64(len(data)), blk: make([]byte, blockSize), blkN: -1}
	pos := int64(0)
	for i, want := range payloads {
		rec, err := sc.next(pos)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if !bytes.Equal(rec.data, want) || rec.end != ends[i] {
			t.Fatalf("record %d: %d bytes ending %d, want %d ending %d", i, len(rec.data), rec.end, len(want), ends[i])
		}
		got, err := readRecord(sc.f, location{start: rec.start, end: rec.end}, 0)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("readRecord %d: %v", i, err)
		}
		pos = rec.end
	}
	if _, err := sc.next(pos); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// Our writer produces exactly the bytes Pebble's record.Writer produces, with
// the same record offsets, for random streams and for a stream that starts
// records at every offset near a block end. So Pebble's reader reads our
// segments, and ours reads Pebble's.
func TestWireMatchesPebble(t *testing.T) {
	g := loadWireGolden(t)
	for _, s := range g.Random {
		rng := rand.New(rand.NewSource(s.Seed))
		payloads := makePayloads(rng, payloadSizes(rng, s.N))
		for _, every := range []int{1, 17, 1 << 30} {
			data, ends := writeStream(t, payloads, every)
			sum := sha256.Sum256(data)
			if len(data) != s.Size || hex.EncodeToString(sum[:]) != s.SHA256 {
				t.Fatalf("seed %d: our stream (%d bytes, %x) differs from Pebble's (%d bytes, %s)", s.Seed, len(data), sum, s.Size, s.SHA256)
			}
			if !slices.Equal(ends, s.Ends) {
				t.Fatalf("seed %d: record ends differ from Pebble's", s.Seed)
			}
		}
	}
	payloads, pebbleData := edgePayloads(t, g)
	data, ends := writeStream(t, payloads, 17)
	if !bytes.Equal(data, pebbleData) {
		i := 0
		for i < len(data) && i < len(pebbleData) && data[i] == pebbleData[i] {
			i++
		}
		t.Fatalf("edge stream differs from Pebble's at byte %d (%d vs %d bytes)", i, len(data), len(pebbleData))
	}
	if !slices.Equal(ends, g.Edge.Ends) {
		t.Fatal("edge stream: record ends differ from Pebble's")
	}
}

// Our scanner reads Pebble's streams, with exact offsets.
func TestWireReadsPebble(t *testing.T) {
	g := loadWireGolden(t)
	payloads, data := edgePayloads(t, g)
	scanStream(t, data, payloads, g.Edge.Ends)
	for _, s := range g.Random {
		rng := rand.New(rand.NewSource(s.Seed))
		payloads := makePayloads(rng, payloadSizes(rng, s.N))
		data, _ := writeStream(t, payloads, 1<<30) // identical to Pebble's, by TestWireMatchesPebble
		scanStream(t, data, payloads, s.Ends)
	}
}

// A writer resumed at a record boundary continues the same block structure:
// the file is indistinguishable from one written in a single pass.
func TestWireResume(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	payloads := makePayloads(rng, payloadSizes(rng, 200))
	fs := crashfs.New()
	f, _ := fs.Create("a")
	w := newSegWriter(f, 0, 0)
	for _, p := range payloads {
		w.add(p)
	}
	w.flush()
	g, _ := fs.Create("b")
	var off int64
	for i, p := range payloads {
		if i%13 == 0 { // reopen the writer at the current end
			w2 := newSegWriter(g, off, 0)
			_, off = w2.add(p)
			w2.flush()
			continue
		}
		w2 := newSegWriter(g, off, 0)
		_, off = w2.add(p)
		w2.flush()
	}
	da, db := fileBytes(t, fs, "a"), fileBytes(t, fs, "b")
	if !bytes.Equal(da, db) {
		t.Fatalf("resumed stream differs (%d vs %d bytes)", len(da), len(db))
	}
}

// buildCommits writes n commits of k entry-like records each and returns
// the segment bytes and each commit's [start, end).
func buildCommits(n, k, size int) ([]byte, [][2]int64) {
	fs := crashfs.New()
	f, _ := fs.Create("s")
	w := newSegWriter(f, 0, 0)
	var spans [][2]int64
	for c := 0; c < n; c++ {
		var first int64 = -1
		for r := 0; r < k; r++ {
			s, _ := w.add(encodeRecord(nil, recEntry, 1, bytes.Repeat([]byte{byte(c)}, size)))
			if first < 0 {
				first = s
			}
		}
		// As with an unpipelined writer: everything before this commit was
		// synced when it was written.
		_, e := w.add(encodeRecord(nil, recCommit, 0, commitMarker{seq: uint64(c + 1), nrecs: uint64(k), start: first, synced: first}.encode(nil)))
		spans = append(spans, [2]int64{first, e})
	}
	w.flush()
	return fileBytes(nil, fs, "s"), spans
}

func classifyAt(t *testing.T, data []byte, bad int64) bool {
	t.Helper()
	sc := &segScanner{f: crashfs.NewFile(data), size: int64(len(data)), blk: make([]byte, blockSize), blkN: -1}
	fatal, _, err := sc.classify(bad)
	if err != nil {
		t.Fatal(err)
	}
	return fatal
}

func TestClassifyTornVersusCorrupt(t *testing.T) {
	data, spans := buildCommits(12, 5, 9000)
	last := spans[len(spans)-1]

	// Torn final commit: file cut in its middle.
	cut := last[0] + (last[1]-last[0])/2
	if classifyAt(t, data[:cut], cut) {
		t.Fatal("truncated final commit classified as corruption")
	}
	// Damage inside the final commit with its marker intact (pages of an
	// unsynced write landing out of order).
	d := append([]byte(nil), data...)
	d[last[0]+100] ^= 0xff
	if classifyAt(t, d, last[0]+93) {
		t.Fatal("damage confined to the final commit classified as corruption")
	}
	// Damage in an earlier commit that later commits followed: corruption.
	for _, c := range []int{0, 5, len(spans) - 2} {
		d := append([]byte(nil), data...)
		at := spans[c][0] + 50
		d[at] ^= 0xff
		if !classifyAt(t, d, spans[c][0]) {
			t.Fatalf("damage in commit %d of %d not detected", c+1, len(spans))
		}
	}
	// A zeroed (preallocated) tail after the last commit is clean.
	z := append(append([]byte(nil), data...), make([]byte, 3*blockSize)...)
	if classifyAt(t, z, int64(len(data))) {
		t.Fatal("zero tail classified as corruption")
	}
}
