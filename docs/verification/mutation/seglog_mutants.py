# Mutants for raftseg/seglog (paths relative to seglog/). Each breaks one
# thing the storage engine must get right; the test suite must notice.
M = []
def m(label, file, old, new):
    M.append(dict(label=label, file=file, old=old, new=new))

# --- framing and checksums
m("scanner skips chunk checksum", "wire.go",
  '''		if chunkCRC(s.key, h[6:chunkHeaderSize+n]) != sum {
			return rawRecord{}, &badData{p, "checksum mismatch"}''',
  '''		if false && chunkCRC(s.key, h[6:chunkHeaderSize+n]) != sum {
			return rawRecord{}, &badData{p, "checksum mismatch"}''')
m("positioned read skips chunk checksum", "wire.go",
  '''		if chunkCRC(key, h[6:chunkHeaderSize+n]) != sum {
			return nil, fmt.Errorf("checksum mismatch in chunk at %d", pos)''',
  '''		if false && chunkCRC(key, h[6:chunkHeaderSize+n]) != sum {
			return nil, fmt.Errorf("checksum mismatch in chunk at %d", pos)''')
m("record start rule off by one", "format.go",
  "if int(prevEnd&blockMask)+chunkHeaderSize > blockSize {",
  "if int(prevEnd&blockMask)+chunkHeaderSize >= blockSize {")
m("checkpoint checksum not checked", "checkpoint.go",
  "if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(b[len(b)-4:]) {\n\t\treturn nil, errors.New(\"checksum mismatch\")",
  "if false {\n\t\treturn nil, errors.New(\"checksum mismatch\")")
m("sidecar checksum not checked", "segindex.go",
  "if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(b[len(b)-4:]) {\n\t\treturn nil, errBadSidecar",
  "if false {\n\t\treturn nil, errBadSidecar")
m("snapshot file checksum not checked", "snapshot.go",
  "if crc32.Checksum(data, crcTable) != ref.crc {",
  "if false {")

# --- commits and replay
m("replay ignores commit sequence", "recover.go",
  "if m.seq != l.seq+1 {", "if false {")
m("replay ignores the marker's record count", "recover.go",
  "if m.nrecs != uint64(len(pending)) || len(pending) == 0 || m.start != pending[0].start {",
  "if len(pending) == 0 {")
m("damage after a vouching marker treated as torn", "wire.go",
  "if m, derr := decodeCommitMarker(payload); derr == nil && m.synced > bad {",
  "if m, derr := decodeCommitMarker(payload); derr == nil && false && m.synced > bad {")
m("every damaged tail treated as torn-like", "wire.go",
  '''func (s *segScanner) tornLike(bd *badData) (bool, error) {
	bad := bd.at''', '''func (s *segScanner) tornLike(bd *badData) (bool, error) {
	return true, nil
	bad := bd.at''')
m("torn tail not zeroed", "recover.go",
  "if err := zeroRange(l.fs, path, end, sc.size); err != nil {",
  "if err := error(nil); err != nil {")
m("replayed segment not synced", "recover.go",
  "if err := syncFile(l.fs, path); err != nil {\n\t\t\treturn res, err",
  "if err := error(nil); err != nil {\n\t\t\treturn res, err")
m("marker claims durability it does not have", "log.go",
  "if d := l.durable.Load(); d != nil && d.seg == l.active && d.end <= m.start {\n\t\t\tm.synced = d.end",
  "if d := l.durable.Load(); d != nil && d.seg == l.active && d.end <= m.start {\n\t\t\tm.synced = m.start")
m("non-newest segment may end in garbage", "recover.go",
  "if !zero {\n\t\t\t\t\treturn res, corrupt(\"%v in a segment that is not the newest\", bd)",
  "if false && !zero {\n\t\t\t\t\treturn res, corrupt(\"%v in a segment that is not the newest\", bd)")

# --- durability ordering
m("acknowledged before fsync", "log.go",
  "if err := j.f.SyncData(); err != nil {", "if err := error(nil); err != nil {")
m("roll seals without a full sync", "log.go",
  "if err := l.wf.Sync(); err != nil { // full sync: size metadata too",
  "if err := error(nil); err != nil { // full sync: size metadata too")
m("new segment's name not synced", "log.go",
  "if err := syncDir(l.fs, l.segDir); err != nil {\n\t\tf.Close()",
  "if err := error(nil); err != nil {\n\t\tf.Close()")
m("checkpoint file not synced before rename", "checkpoint.go",
  "_, err = f.WriteAt(data, 0)\n\t\tif err == nil {\n\t\t\terr = f.Sync()\n\t\t}",
  "_, err = f.WriteAt(data, 0)")
m("checkpoint rename not synced", "checkpoint.go",
  "if err := syncDir(l.fs, l.dir); err != nil {\n\t\treturn err\n\t}\n\tl.ckptGen++",
  "l.ckptGen++")
m("snapshot file not synced", "snapshot.go",
  "\tif err == nil {\n\t\terr = f.Sync()\n\t}\n\tif cerr := f.Close(); err == nil {\n\t\terr = cerr\n\t}\n\tif err == nil {\n\t\terr = l.fs.Rename(tmp, final)\n\t}\n\tif err == nil {\n\t\terr = syncDir(l.fs, l.snapDir)",
  "\tif cerr := f.Close(); err == nil {\n\t\terr = cerr\n\t}\n\tif err == nil {\n\t\terr = l.fs.Rename(tmp, final)\n\t}\n\tif err == nil {\n\t\terr = syncDir(l.fs, l.snapDir)")
m("snapshot file name not synced", "snapshot.go",
  "\tif err == nil {\n\t\terr = syncDir(l.fs, l.snapDir)\n\t}",
  "")
m("GC deletes before the checkpoint", "gc.go",
  "\tl.drain()\n\tif err := l.checkpoint(skip); err != nil {\n\t\treturn res, l.fail(err)\n\t}",
  "\tl.drain()")
m("GC deletes segments with live entries", "gc.go",
  "if n != l.active && ss.meta.sealed && !ss.deleted && l.live[n] == 0 {",
  "if n != l.active && ss.meta.sealed && !ss.deleted && l.live[n] <= 1 {")
m("replaced snapshot file deleted before the new record is durable", "snapshot.go",
  "\t\t\tj.after = append(j.after, func() { l.dropSnapFile(u.Group, oldIdx) })",
  "\t\t\tl.dropSnapFile(u.Group, oldIdx)")
m("created snapshot's old file deleted at once", "snapshot.go",
  "\t\t\tj.after = append(j.after, func() { s.l.dropSnapFile(s.id, oldIdx) })",
  "\t\t\ts.l.dropSnapFile(s.id, oldIdx)")
m("unsynced writes kept after a failed fsync (fsyncgate)", "log.go",
  "\t\t\t\t\tl.forgetUnsynced(j.f, j.seg)\n", "")
m("recovered segments' names not synced", "recover.go",
  "\tif err := syncDir(l.fs, l.segDir); err != nil {\n\t\treturn err\n\t}\n\tif l.recovery.ReplayedCommits > 0",
  "\tif l.recovery.ReplayedCommits > 0")

# --- validation and recovery checks
m("Save not validated", "log.go",
  "\t\tif err := l.validate(cur.st, r.updates); err != nil {\n\t\t\tcur.add(r, err)\n\t\t\tcontinue\n\t\t}",
  "")
m("commit beyond the last entry accepted", "log.go",
  "if s := &sims[i]; !isEmptyHS(s.hs) && s.hs.Commit > s.meta.last {",
  "if s := &sims[i]; false && s.hs.Commit > s.meta.last {")
m("segment numbers need not be consecutive", "recover.go",
  "\t\tcase n != next:\n\t\t\treturn fmt.Errorf(\"%w: segment %d is missing, but segment %d exists\", ErrCorrupt, next, n)",
  "")
m("a roll may overwrite an existing segment", "log.go",
  "\tif _, err := l.fs.Stat(l.segPath(n)); err == nil {\n\t\treturn fmt.Errorf(\"%w: segment %d already exists\", ErrCorrupt, n)\n\t}",
  "")
m("snapshot size trusted before allocating", "snapshot.go",
  "\tif uint64(st.Size()) != ref.size {\n\t\treturn nil, fmt.Errorf(\"%w: snapshot %d of group %d: %d bytes, expected %d\", ErrCorrupt, ref.index, group, st.Size(), ref.size)\n\t}\n\tdata := make([]byte, ref.size)",
  "\t_ = st\n\tdata := make([]byte, ref.size)")
m("oversized sidecar yields no index", "segindex.go",
  "\t\tif err == nil && x != nil {\n\t\t\treturn x, nil",
  "\t\tif err == nil {\n\t\t\treturn x, nil")
m("missing segment ignored", "recover.go",
  "\t\treturn fmt.Errorf(\"%w: segment %d is missing\", ErrCorrupt, n)",
  "\t\tdelete(l.segs, n)\n\t\tcontinue")
m("commit beyond last not checked at open", "recover.go",
  "\t\tif g.hs.Commit > m.last {", "\t\tif false {")
m("commit below a snapshot not raised", "recover.go",
  "\t\tif m.first > 1 && g.hs.Commit < m.first-1 && !isEmptyHS(g.hs) {",
  "\t\tif false {")
m("orphan segments kept", "recover.go",
  "if _, ok := ck.segs[n]; !ok && n < ck.seg {", "if _, ok := ck.segs[n]; !ok && n < ck.seg && false {")
m("oldest checkpoint copy preferred", "checkpoint.go",
  "if best == nil || c.gen > best.gen {", "if best == nil || c.gen < best.gen {")
m("sidecar's segment number not checked", "segindex.go",
  "if binary.LittleEndian.Uint64(body[4:]) != seg || int64(binary.LittleEndian.Uint64(body[12:])) != end ||",
  "if int64(binary.LittleEndian.Uint64(body[12:])) != end ||")
m("entry's group not checked on read", "storage.go",
  "if err != nil || (typ != recEntry && typ != recRelocate) || gid != id {",
  "if _ = gid; err != nil || (typ != recEntry && typ != recRelocate) {")
m("entry's term not checked on read", "storage.go",
  "\t\tif e.Term != loc.term {", "\t\tif false {")
m("short read not reported as corruption", "wire.go",
  "\tif errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {\n\t\treturn fmt.Errorf(\"%w: segment %d: record at %d runs past the end of the file\", ErrCorrupt, seg, at)\n\t}",
  "")
m("truncated sealed segment not reported", "recover.go",
  "\t\t\t} else if st.Size() < ss.meta.end {", "\t\t\t} else if false {")

# --- log shape
m("term runs truncated one too far", "checkpoint.go",
  "k := sort.Search(len(ts), func(k int) bool { return ts[k].index >= from })",
  "k := sort.Search(len(ts), func(k int) bool { return ts[k].index > from })")
m("term runs compacted one too far", "checkpoint.go",
  "\tif out[0].index < first {\n\t\tout[0].index = first",
  "\tif out[0].index < first+1 {\n\t\tout[0].index = first + 1")
m("overwrite keeps the old suffix's terms", "log.go",
  "\t\tl.runsRemoveFrom(g, ents[0].Index)\n\t\tg.terms = termsTruncate(g.terms, ents[0].Index)\n\t}\n\tfor k := range ents {",
  "\t\tl.runsRemoveFrom(g, ents[0].Index)\n\t}\n\tfor k := range ents {")
m("size limit lets one entry too many through", "storage.go",
  "if len(*out) > 0 && *size+s > maxSize {", "if len(*out) > 0 && *size > maxSize {")
m("Term of FirstIndex-1 off by one", "storage.go",
  "\t\tcase i == m.first-1:\n\t\t\tterm = m.compactTerm", "\t\tcase i == m.first:\n\t\t\tterm = m.compactTerm")
m("compaction past the commit index not raised on open", "recover.go",
  "\t\t\tg.hs.Commit = m.first - 1", "\t\t\tg.hs.Commit = m.first - 2")

# --- concurrency
m("readers do not retry when a group changed", "storage.go",
  "\t\tstable := s.g.version == v && !s.g.removed", "\t\tstable := true || s.g.version == v")
m("publish does not bump the version", "log.go",
  "\t\tg.pruns, g.pterms = d.runs, d.terms\n\t\tfor _, ents := range d.appended {\n\t\t\tl.cacheAppend(g, ents)\n\t\t}\n\t\tg.version++",
  "\t\tg.pruns, g.pterms = d.runs, d.terms\n\t\tfor _, ents := range d.appended {\n\t\t\tl.cacheAppend(g, ents)\n\t\t}")
m("GC deletes files before marking the segment deleted", "gc.go",
  "\t\t// (or Scrub) starts on it and finds them missing.\n\t\tl.filesMu.Lock()",
  "\t\t// (or Scrub) starts on it and finds them missing.\n\t\tif err := l.removeSegmentFiles(n); err != nil {\n\t\t\treturn res, l.fail(err)\n\t\t}\n\t\tl.filesMu.Lock()")

m("Close hides the log's failure", "log.go",
  "\tif f := l.failure(); f != nil && clean && err == nil {\n\t\terr = f\n\t}", "")

# --- the checksum key
m("chunk checksums not keyed", "wire.go",
  "\tc := crc32.Update(key, crcTable, b)", "\tc := crc32.Update(0, crcTable, b)")
m("recovered log forgets its key", "recover.go",
  "\tl.ckptGen, l.seq, l.key = ck.gen, ck.seq, ck.key", "\tl.ckptGen, l.seq = ck.gen, ck.seq")

# --- reused group ids (found by FuzzCorruptLog in the final run)
m("a removed group's record read as the live one", "storage.go",
  "\t\t\t\tif born.after(ls[k]) {", "\t\t\t\tif false && born.after(ls[k]) {")
m("replay forgets where a group's first record is", "recover.go",
  "\t\t\tg.born = position{seg, start} // as writeEntries does", "")
m("checkpoint drops where a group's first record is", "checkpoint.go",
  "\t\tg.born = position{seg: r.uv(), off: int64(r.uv())}", "\t\tr.uv()\n\t\tr.uv()")
