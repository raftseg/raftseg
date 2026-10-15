# seglog

seglog is the durable raft log of a raftseg node: one segmented log on disk shared by every raft group the node hosts. Each group's `*seglog.Storage` implements `raft.Storage` with `raft.MemoryStorage`'s semantics; `Log.Save` persists the Readies of many groups in one group commit.

It imports only the standard library, `raft`, Google's protobuf runtime (which raft's messages are generated for) and `vfs`. There is no key-value store underneath: every change of state is a record in the log, and the log's own index structures say where each entry lives.

```
<dir>/
  LOCK                                  one process at a time (flock)
  CHECKPOINT-A, CHECKPOINT-B            the log's state as of a commit: two checksummed copies
  segments/00000000000000000001.seg     32 KiB blocks of checksummed chunks (pebble/record's layout)
  segments/00000000000000000001.idx     entry index of a sealed segment (a cache: rebuilt if lost)
  snapshots/<group>-<index>.snap        snapshot data larger than SnapshotInlineBytes
```

## How it works

**Writes.** `Save` marshals records in the caller's goroutine. One writer goroutine gathers every pending request into a group commit. It frames the records into the active segment, then appends a **commit marker**: sequence number, record count, start offset, and how far the segment was durable when the commit was written. A syncer goroutine `fdatasync`s the segment, which is the durability point; one fsync covers every commit written so far. Then the commit is published to readers and its callers are answered. Up to `PipelineDepth` commits are written ahead of their fsync.

**Sealing.** When the active segment reaches `SegmentSize`, it is fully synced and its entry index is written to a sidecar file. Then the next segment is created (with a directory sync), a checkpoint is written, and GC runs.

**Recovery.** `Open` loads the newest checkpoint copy that passes its checksum, then replays the commits that follow it. A commit is applied only once its marker checks out, so it is recovered entirely or not at all. Invalid bytes at the end of the newest segment are a **torn tail** when no later marker vouches for them and they look like an interrupted write (zeroes, or a short file). A torn tail is zeroed, reported in `RecoveryStats.Tail`, and the segment is resumed. Anything else is corruption.

**Reads.**
- Recent entries come from a per-group cache with a global byte budget.
- For older entries, the group's runs (in memory) give the segment, the segment's index gives the offset, and one `ReadAt` fetches each run of adjacent records. Every chunk is checksummed again on the way out, and every entry is checked to belong to the group, index and term that was asked for. A record older than the group itself is refused: each group records where its first entry record is, because a group id can be reused after `RemoveGroup` (bug 9 below).
- `Term` never touches the disk.
- Readers never see a half-applied Save, truncation, compaction or relocation.

**Reclaiming space.** GC checkpoints the state without the dead segments first, then deletes them, so no checkpoint, and no replay after one, can need a deleted segment. Cleaning copies the live entries out of old, mostly dead segments.

**Checksums.** Every chunk carries a masked CRC-32C, as in pebble/record, but computed from a random 32-bit key that each log draws when it is created and keeps in its checkpoints. With key 0, the bytes are exactly pebble/record's. `testdata/wire_golden.json` holds streams written by Pebble's own writer, and the tests check against them byte for byte.

The key exists because, after damage, the scanner looks for the next valid chunk at every byte offset, including inside entries, whose bytes come from clients. A client who could compute checksums could plant a well-formed chunk in a message. If a crash then tore the commit before it, recovery would read the planted chunk as the log's own. A forged commit marker would make the node refuse to start. A forged entry, after bit rot, could even be indexed in place of a real one. With the key, a planted chunk verifies with probability 2^-32. `forgery_test.go` shows both attacks succeeding without the key and failing with it. As a side effect, segments of another log fail every checksum.

## Guarantees

- When `Save` returns nil, the update survives a process crash, an OS crash and power loss. `SetConfState`, `RemoveGroup` and `Sync` are durable when they return.
- `CreateSnapshot` becomes durable with the next group commit, or at `Sync`. `Compact` becomes durable with the next checkpoint. A crash before then keeps the previous snapshot or more entries; nothing is lost.
- After a crash, every group recovers exactly to the state after its last acknowledged change, or after the one in flight.
- **Fail-closed I/O errors.** An I/O error is sticky: every later call fails, and `Close` returns the error. A failed fsync is never retried. The unacknowledged writes it covered are overwritten with zeroes, because on Linux they could otherwise stay readable in the page cache without ever reaching the disk ("fsyncgate"). Reopening recovers every acknowledged change.
- **Validated input.** A `Save` that raft would never issue is refused with `ErrInvalidUpdate`, and changes nothing. That covers:
  - a gap in the entries;
  - an index of 0 or above 2^63;
  - a commit index beyond the last entry, including one left behind by an overwrite or a snapshot;
  - a nil entry.

  Before this check, such a Save was accepted, and the next `Open` failed for every group on the node.
- **Damage is never silent.** Damaged data is reported in one of three ways:
  - `Open` refuses with `ErrCorrupt`;
  - reads of the damaged entries fail with `ErrCorrupt`, and `Scrub` reports them;
  - the log discards newest commits that look like an interrupted write and that no later write vouches for, and reports them in `RecoveryStats.Tail`.

  A damaged final commit that does **not** look like an interrupted write may have been acknowledged, and losing an acknowledged raft vote or entry can break raft's safety. So `Open` refuses it, unless `DiscardDamagedTail` is set.
- **Reused group ids are safe.** After `RemoveGroup`, the id may be reused, even by a raft group with another history. Reads never return a record of the removed group, even when damage has destroyed the new group's record of the same index and term.
- **Redundant files may be lost or damaged without loss.** These are index sidecars, either checkpoint copy, and the LOCK file.
- **Operator mistakes fail closed.** A missing segment, a segment out of sequence, or a stray file with a segment's name stops `Open`. A segment is never created over an existing file. Unknown files are ignored, and files of another log fail their checksums.
- **Hostile files cannot crash the process.** Allocations are bounded by the bytes actually present, so a sparse file or a crafted size field can neither crash the process nor make it allocate what the disk does not hold.

What seglog does not defend against:
- **Forgery by someone with write access.** Someone who can write to the log's directory and knows its key (it is in the checkpoint) can forge anything, as with any storage engine.
- **Rewrites raft never makes.** After damage, a read can fall back on an older record of the same entry that is still readable. That is safe because raft never writes two different entries with the same group, index and term (log matching). A client that bypasses raft and rewrites an entry under the same term could, after damage, read the older version.
- **Loss of both checkpoint copies.** Losing both at once stops `Open`. Raft replication restores such a node from its peers.
- **A cut at a commit boundary.** The newest segment cut short exactly at a commit boundary, in the part written since the last checkpoint, cannot be told apart from commits that were never written. `Open` then recovers the log as it was before those commits, without an error. Every write-ahead log has this blind spot: nothing written later vouches for the last commits.

  A cut there is below the filesystem's contract, which keeps a file's size once `fdatasync` returns. It takes a filesystem bug, or someone truncating the file. The exhaustive damage test found it, and checks that it is the only silent case.

  `CheckpointInterval` narrows the window, since each checkpoint records how far the segment reached. The way to close it is segments created at their full size, so that a shorter file is always corruption. That is listed under next steps; it would also save `fdatasync` a metadata update on every commit.

## Testing

Each suite and what it establishes:

| Suite | What it establishes |
|---|---|
| `contract_test.go` | Upstream raft's own MemoryStorage tests pass against seglog. `FuzzStorageContract` (and its deterministic twin) drives seglog and `raft.MemoryStorage` with arbitrary operations, valid or not: appends that overwrite or leave gaps, compactions, snapshots created and received, reads with arbitrary bounds, reopens, group removal. Where MemoryStorage accepts an operation, the observable state must be identical, also after a reopen. Where it panics, seglog must return an error and change nothing. |
| `hostile_disk_test.go` | Every file of a log, clean and after a crash, is damaged in every way storage fails: byte flips and zeroed bytes at every offset (`SEGLOG_EXHAUSTIVE=1`; a prime stride by default), lost and misdirected sectors, truncation at every length, deletion. About 28,000 images by default; the exhaustive run damaged every byte, sector and length: 1,241,449 images. Each must be recovered exactly, refused, or detected on read and by `Scrub`, or have its newest commits discarded as a reported tail. Damage to a redundant file must lose nothing. |
| `hostile_files_test.go` | Stray segments (including numbers near 2^64), segments of an unrelated log, swapped segments, checkpoints rolled back to every earlier point, checkpoints from another log, restored deleted segments, directories where files belong, unknown files. A log that opens must keep working: it is written until segments roll, then reopened and checked. |
| `crafted_test.go` | `FuzzCraftedRecords` writes arbitrary records (entries, HardStates, snapshots with arbitrary file references, removals, relocations, unknown types, commit markers that lie) through the log's own framing, then crashes. `FuzzCraftedCheckpoint` rewrites checkpoints field by field, with valid checksums. `Open` must refuse or cope, never panic or hang, and every API call on an opened log must return. |
| `pb_test.go` | raft's messages are protobuf messages held by pointer, so seglog copies what it keeps and what it hands out (`pb.go`). Changing a message after passing it to seglog, or one seglog returned, changes nothing in seglog. Entries read back equal those saved, unset fields and edge values included, with the same size, from the cache and after a restart. |
| `v0log_test.go` | `testdata/v0_log.tar.gz` is a log written by seglog before raft moved to protobuf, when raftpb was gogo's generated code. It holds entries of every type, an overwrite, a compaction, snapshots in a file and inline, a removed group and several segments. seglog must read it as that version did, each entry encoding to the same bytes, then append to it and reopen it. |
| `alloc_test.go` | Sparse checkpoint, sidecar and segment files, and snapshot references claiming terabytes, cost neither memory nor time. |
| `harness_test.go` | `TestCrashAtEveryOp` simulates a power cut before each of about 670 filesystem operations of a workload. Each cut is tried keeping none, all, or a random torn and reordered part of the unsynced data. Recovery itself is then power-cut, sometimes twice over. `TestErrorAtEveryOp`, `TestMetamorphicOptions`, `FuzzScanner` and `FuzzCorruptLog` are here too. |
| `faultmodel_test.go` | `TestSyncLostAtEveryOp` fails each fsync as Linux does after a writeback error: the data stays readable but never reaches the disk. The run continues across both a power cut and a restart without one. `TestErrorAtAnyOp` fails each operation of every kind (reads, opens, stats, listings, closes, writes, syncs, renames, removals) on every file, with the caches on and off. |
| `chaos_api_test.go` | Many goroutines use every API at once, one owner per group as raft does, while others read any group, run GC and Scrub, and remove groups. The log is closed at a random moment. Run under `-race`. |
| `validate_test.go`, `forgery_test.go`, `wrap_test.go`, `incarnation_test.go` | Regression tests for the bugs below, each failing on the code before its fix. |
| `survivors_test.go` | Tests written for the mutants that survived the first mutation run (see below). Each exercises one safeguard no other test reached on its own. |
| `diff_test.go`, `crash_test.go`, `kill_test.go`, `chaos_test.go`, `cluster_test.go`, … | The suites carried over from raftseg v4: differential runs against MemoryStorage with crashes, random power cuts under concurrent load, real SIGKILLs, and multi-node raft chaos. |
| `internal/raftsim` | `TestSimulationSeglog` and `TestSimulationMixed` run the deterministic raft simulator with every node's log in seglog (or mixed with in-memory nodes). This adds all the raft safety and liveness checks, crashes that strike in the middle of a Save, and several groups sharing a node's log. The simulator also checks that no message raft and seglog share (entries, hard states, configurations, snapshots) changes after it is handed over. |
| `internal/crashfs` | The crash model itself: `TestConformsToOS` checks that, without crashes, it behaves exactly like the operating system's filesystem, op by op. |

**Mutation testing.** 65 mutants each put one mistake back into the engine: a checksum not checked, an fsync or directory sync left out, a deletion moved before the state that stops needing it is durable, a recovery check dropped, an off-by-one in the log's shape, a missing version bump, a message shared where seglog must copy it. 61 are killed by the tests, and 4 are equivalent (the change cannot be observed), each explained. None survives. The table, the catalogue and the runner are in [docs/verification](../docs/verification/seglog-mutation.md).

```
go test ./seglog                      # about 3.5 minutes on 2 cores
go test -short -race ./seglog
SEGLOG_EXHAUSTIVE=1 go test ./seglog -run TestDamageEveryFile -timeout 2h
go test ./seglog -run XXX -fuzz FuzzCraftedRecords      # and FuzzCraftedCheckpoint, FuzzStorageContract, FuzzCorruptLog, FuzzScanner
RAFTSIM_SEEDS=5000 go test ./internal/raftsim -run 'Seglog|Mixed' -timeout 3h
SEGLOG_PERF=1 SEGLOG_PERF_DIR=/data/bench go test ./seglog -run TestPerfReport -v -timeout 2h
```

## Bugs found and fixed while hardening

Each has a regression test that fails without its fix.

1. **A Save raft never issues could stop the whole node at the next restart.** Such a Save had a commit index beyond the last entry (directly, or behind an overwrite or a snapshot), or an index near 2^64. It was accepted, and `Open` then refused the log, taking down every group on the node.
2. **A rejected Save could persist its snapshot.** The snapshot was staged before the entries were checked.
3. **Fsyncgate.** After a failed fsync, a restart without a power cut replayed writes that never reached the disk. Commits acknowledged later then sat behind a hole, and the next power cut left an unopenable log.
4. **A resumed segment's directory entry was never synced.** The segment had been created by a roll whose sync failed. A power cut then lost the file, with acknowledged commits in it.
5. **A stray segment numbered near 2^64 truncated live segments.** It became the active segment, and the next roll wrapped the segment number to 0 and recreated segments 1, 2, … over existing ones.
6. **Forged chunks in client data** (see Checksums above).
7. **A crash and an allocation on hostile input.** A sidecar file over the size bound crashed the process with a nil index. A crafted snapshot reference could make the process allocate up to 1 TiB.
8. **Silent loss of an acknowledged commit.** A damaged final commit that did not look like an interrupted write was discarded by default. It is now refused unless `DiscardDamagedTail` is set.
9. **A removed group's data returned for a reused id.** Found by `FuzzCorruptLog` in the final fuzzing run. A group id was removed and reused by a group with another history, so one segment held records of both with the same index and term but different data. Bit rot then destroyed the new group's record, in data the checkpoint covered, and the segment's index sidecar was lost too. `Open` rebuilds the index from whatever it can still read, which included the removed group's record. The term check could not tell the two apart, so the read returned the removed group's entry without an error. Each group now records where its first entry record is, in its checkpoints and on replay, and reads refuse any record older than that.
10. **Smaller fixes:**
   - `Close` reported success after the log had failed.
   - A truncated sealed segment read back as an I/O error rather than `ErrCorrupt`, and `Open` did not report it.
   - `Term` of an index near 2^64 said "compacted".
   - `Scrub` raised false alarms while GC deleted segments or a roll was sealing one.
11. **Two defects of the port to protobuf messages,** found in review before release:
   - A nil entry passed to `Save` crashed the process: the writer goroutine sized it before validation. It is now refused as an invalid update (`TestNilEntryRefused`).
   - An entry with fields raftpb does not define, which protobuf keeps and encodes again, was saved, and then the log would not open: the entry index read only the wire types raftpb uses. It now skips any field, as protobuf does (`TestEntriesRoundTripExactly`, `TestEntryHeader`).

## Performance

The hardening costs nothing measurable. Measured against raftseg v4 (the same engine before this work), on the same 2-vCPU VM and disk, with the runs interleaved:
- CPU per Save, 128 groups, without fsync: 7.36 µs (v4) and 7.27 µs (seglog), as means of six runs each.
- Saves with fsync, 128 groups: 12.5 µs (v4) and 12.1 µs (seglog) per Save, as means of five runs each.
- The full benchmark report (`TestPerfReport`, four interleaved runs): every configuration differs from v4 by less than runs of the same version differ from each other (up to about 10%).

**After raft's move to protobuf.** raft's messages became protobuf messages held by pointer, encoded by protobuf's runtime instead of gogo's generated code. Measured against seglog before the move (raftseg 3bfbd2c), on the same 2-vCPU VM, with the runs interleaved, four runs each:
- CPU per Save, 128 groups each saving 4 entries of 1 KiB and a hard state, without fsync: 16.5 µs before and 16.3 µs after, as means. The first port took 20.3 µs; encoding each entry straight into its record, instead of encoding it and then copying it into the record, brought it back.
- CPU per read of 100 entries of 1 KiB from disk, entry cache off: 215 µs before and 187 µs after. protobuf's decoder allocates more than gogo's (a pointer per field), which the first port showed as 265 µs; a record that fits in one chunk is now decoded where it was read, without the copy that reassembling chunks makes.

Keyed checksums cost the same as unkeyed ones (CRC-32C with a different initial value), and request validation does not allocate for requests of up to 16 groups. A clean reopen takes about 0.3 ms longer, for the directory sync that makes recovered segment names durable.

## Next steps

- **Segments created at their full size** (`fallocate` without `KEEP_SIZE`). A segment shorter than its size is then always corruption, which closes the one blind spot above. `fdatasync` would also no longer journal a size change on every commit.
- **Partial sidecar reads.** A random read in a sealed segment whose index is not cached loads that segment's whole sidecar, about 8 bytes per entry. With the index cache shrunk to 1 MiB, point reads fall to about 9,500/s. A small per-group directory in the sidecar would let a miss read only the part it needs.
- **Incremental checkpoints.** A checkpoint rewrites every group's state. That is cheap for thousands of groups; for hundreds of thousands, either use larger segments or make checkpoints incremental.

## Platforms

Linux on 64-bit CPUs is the supported platform: durability rests on `fdatasync`, `fallocate` and `flock`. The package builds on other 64-bit Unix systems and on Windows, but Windows has no cross-process lock. It refuses to build on 32-bit platforms, because it converts file offsets, sizes and counts to `int` freely. raft's old wire decoder was a second reason: on 32-bit builds it accepted malformed messages that 64-bit builds reject. raft's protobuf decoder does not, and raft builds on 32-bit platforms again, but seglog's own reason stands.
