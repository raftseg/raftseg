# Mutation testing: seglog

Each mutant puts one mistake into the storage engine that a storage engine must not make:
- a checksum not checked;
- an fsync, or the directory sync that makes a new name durable, left out or moved;
- a file deleted before the state that stops needing it is durable;
- a recovery check dropped;
- an off-by-one in the log's shape;
- a lock or version bump missing.

A mutant is killed when `go test -short ./seglog`, or the seglog raft simulations, fail with it in place. The catalogue is `mutation/seglog_mutants.py`, and `mutation/run.py seglog` reruns it.

**58 mutants: 55 killed, 3 equivalent, none surviving.**

The first run left 14 mutants alive, and 4 were invalid (they did not compile until fixed). Three of the survivors are equivalent, as explained in the table. Each of the other 11, and each of the 2 fixed invalid ones that also survived, got a test for exactly the case it breaks (`seglog/survivors_test.go`, 14 tests). Every one of them is now killed.

Two of those tests uncovered behaviour worth knowing. A segment's sequence check is the only guard against an older HardState (a raft vote) silently replacing a newer one, when two segments that hold different groups' entries are swapped. And a sealed segment's trailing bytes are the only evidence of a lost final commit when the next segment is still empty.

| # | Mutant | File | Outcome | First test to fail, or why it is equivalent |
|---|---|---|---|---|
| 0 | scanner skips chunk checksum | wire.go | killed | TestCrashCloneConcurrent |
| 1 | positioned read skips chunk checksum | wire.go | killed | TestDamageEveryFile |
| 2 | record start rule off by one | format.go | killed | TestSyncLostAtEveryOp |
| 3 | checkpoint checksum not checked | checkpoint.go | killed | TestDamageEveryFile |
| 4 | sidecar checksum not checked | segindex.go | killed | TestDamageEveryFile |
| 5 | snapshot file checksum not checked | snapshot.go | killed | TestDamageEveryFile |
| 6 | replay ignores commit sequence | recover.go | killed | TestSwappedSegmentsRefused |
| 7 | replay ignores the marker's record count | recover.go | killed | TestLyingMarkerRefused |
| 8 | damage after a vouching marker treated as torn | wire.go | killed | TestForgedMarkerInPayload |
| 9 | every damaged tail treated as torn-like | wire.go | killed | TestTailForensics |
| 10 | torn tail not zeroed | recover.go | killed | TestTornTailZeroed |
| 11 | replayed segment not synced | recover.go | killed | TestReplayedCommitsMadeDurable |
| 12 | marker claims durability it does not have | log.go | killed | TestCrashCloneConcurrent |
| 13 | non-newest segment may end in garbage | recover.go | killed | TestDamageAfterSealedSegmentsLastCommit |
| 14 | acknowledged before fsync | log.go | killed | TestChaos |
| 15 | roll seals without a full sync | log.go | equivalent | Equivalent: every commit is already fdatasync'd, which persists the file's size; the full fsync at a roll adds only metadata no reader needs. |
| 16 | new segment's name not synced | log.go | killed | TestCrashCloneSequential |
| 17 | checkpoint file not synced before rename | checkpoint.go | killed | TestChaos |
| 18 | checkpoint rename not synced | checkpoint.go | killed | TestChaos |
| 19 | snapshot file not synced | snapshot.go | killed | TestChaos |
| 20 | snapshot file name not synced | snapshot.go | killed | TestChaos |
| 21 | GC deletes before the checkpoint | gc.go | killed | TestChaos |
| 22 | GC deletes segments with live entries | gc.go | killed | TestAPIChaos |
| 23 | replaced snapshot file deleted before the new record is durable | snapshot.go | killed | TestAPIChaos |
| 24 | created snapshot's old file deleted at once | snapshot.go | killed | TestErrorAtAnyOp |
| 25 | unsynced writes kept after a failed fsync (fsyncgate) | log.go | killed | TestSyncLostAtEveryOp |
| 26 | recovered segments' names not synced | recover.go | killed | TestResumedSegmentNameIsDurable |
| 27 | Save not validated | log.go | killed | TestStorageContract |
| 28 | commit beyond the last entry accepted | log.go | killed | TestStorageContract |
| 29 | segment numbers need not be consecutive | recover.go | killed | TestStraySegmentNumbers |
| 30 | a roll may overwrite an existing segment | log.go | killed | TestRollNeverOverwrites |
| 31 | snapshot size trusted before allocating | snapshot.go | killed | TestSnapshotRefSizeNotTrusted |
| 32 | oversized sidecar yields no index | segindex.go | equivalent | Equivalent: an oversized sidecar now sets the error itself, so the extra nil-index check is a second guard for the same case. |
| 33 | missing segment ignored | recover.go | killed | TestMissingDeadSegmentRefused |
| 34 | commit beyond last not checked at open | recover.go | killed | TestOpenRefusesCommitBeyondLast |
| 35 | commit below a snapshot not raised | recover.go | killed | TestStorageContract |
| 36 | orphan segments kept | recover.go | killed | TestCrashAtEveryOp |
| 37 | oldest checkpoint copy preferred | checkpoint.go | killed | TestHostileFiles |
| 38 | sidecar's segment number not checked | segindex.go | killed | TestSidecarOfLookalikeSegment |
| 39 | entry's group not checked on read | storage.go | killed | TestEntryOfAnotherGroupNotReturned |
| 40 | entry's term not checked on read | storage.go | killed | TestStaleRecordNotReturned |
| 41 | short read not reported as corruption | wire.go | killed | TestDamageEveryFile |
| 42 | truncated sealed segment not reported | recover.go | killed | TestTruncatedSealedSegmentReported |
| 43 | term runs truncated one too far | checkpoint.go | equivalent | Equivalent: the off-by-one keeps one extra term run starting at the truncation point, which the term appended next supersedes; term lookups return the same values. |
| 44 | term runs compacted one too far | checkpoint.go | killed | TestAPIChaos |
| 45 | overwrite keeps the old suffix's terms | log.go | killed | TestUpstreamStorageAppend |
| 46 | size limit lets one entry too many through | storage.go | killed | TestUpstreamStorageEntries |
| 47 | Term of FirstIndex-1 off by one | storage.go | killed | TestAPIChaos |
| 48 | compaction past the commit index not raised on open | recover.go | killed | TestStorageContract |
| 49 | readers do not retry when a group changed | storage.go | killed | TestAPIChaos |
| 50 | publish does not bump the version | log.go | killed | every reader hangs (test timeout) |
| 51 | GC deletes files before marking the segment deleted | gc.go | killed | TestAPIChaos |
| 52 | Close hides the log's failure | log.go | killed | TestCloseReportsFailure |
| 53 | chunk checksums not keyed | wire.go | killed | TestForgedMarkerInPayload |
| 54 | recovered log forgets its key | recover.go | killed | TestSparseFilesNotRead |
| 55 | a removed group's record read as the live one | storage.go | killed | TestRemovedGroupRecordNotReturned |
| 56 | replay forgets where a group's first record is | recover.go | killed | TestChaos |
| 57 | checkpoint drops where a group's first record is | checkpoint.go | killed | TestSnapshotRefSizeNotTrusted |

Mutants 55 to 57 were added on 2026-10-14 with the fix for reused group ids (bug 9 in [seglog/README.md](../../seglog/README.md)). Only `TestRemovedGroupRecordNotReturned` and FuzzCorruptLog's saved input catch mutant 55: the case needs a reused id, damage, and a lost index file at once. Mutants 56 and 57 make `Open` refuse every log whose groups have entries, so the first test that reopens one fails.
