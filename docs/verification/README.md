# Verification of raftseg's raft and storage packages

Status as of 2026-10-14. Everything below can be rerun with the commands given, from this repository and the raft repository ([github.com/raftseg/raft](https://github.com/raftseg/raft)) checked out next to it in a `go.work` workspace (see README.md). None of it needs anything beyond Go 1.24 and the standard library (the mutation runner also needs Python 3).

## What exists

| Package | What it is | Production code | Tests |
|---|---|---|---|
| [`github.com/raftseg/raft`](https://github.com/raftseg/raft) (its own repository) | Fork of etcd raft v3.6.0 (Apache-2.0): protobuf dependencies removed, safety, linearizability and liveness fixes, validation of peer messages, and upstream fixes taken since by cherry-pick. | 11,700 lines (2,700 of them generated wire code) | upstream's 255 tests and 28 data-driven scenarios, plus one upstream test taken with its fix, plus 30 of ours: regression, hostile-input and compatibility tests, and 4 fuzzers |
| `seglog/` | The durable raft log: one segmented log per node shared by all its raft groups, each a `raft.Storage`. | 5,200 lines | 99 tests, 5 fuzzers, about 8,800 lines |
| `vfs/` | The filesystem interface and its OS implementation. | 200 lines | 4 tests, plus the OS side of crashfs's conformance test |
| `internal/raftsim/` | Deterministic cluster simulator with raft's safety and liveness properties as checks. Storage: in memory, seglog, or mixed. | (test only) | |
| `internal/crashfs/` | Filesystem model: power cuts with torn and reordered writes, lost directory operations, writeback errors, fault injection. | (test only) | conformance to the OS filesystem |

Nothing third-party is compiled into a product: `go.mod` requires only the first-party raft module, which requires nothing itself. `internal/depcheck` fails the build otherwise, and `go run ./internal/tools/sbom` writes a CycloneDX SBOM that says so (and fails otherwise).

## raft

**Compatibility with etcd raft.** The wire format is unchanged. 3,609 messages encoded by the original must decode and re-encode byte for byte, with the same size, text and JSON forms. For 21,654 damaged and hostile inputs, the fork must give the original's error or decode the same message. The original's test suite runs unchanged and passes, except where a fix changes documented behaviour; raft's FORK.md lists each such case. The raft repository's history shows this: its first commit on top of upstream's v3.6.0 removes the dependencies and changes no raft code, and upstream's tests pass on it as they are.

**Bugs found in etcd raft v3.6.0 and fixed.** Every change is listed with its reason in raft's FORK.md, and each has a regression test that fails on the original. They include:
- **Split brain.** A node two configuration changes behind could win an election, so two leaders in one term committed different entries at the same index.
- **Silent configuration corruption.** A snapshot restored while older entries were still being applied corrupted the configuration.
- **Lease reads up to two election timeouts stale.** Lease-based reads were served on CheckQuorum alone.
- **Groups left without a leader for good.** This happened after a leader removed itself.
- **A panicking leader.** Malformed proposals from a peer panicked the leader.
- **A killed process.** About 40 MB of nested input exhausted the stack.

**Fixed upstream since v3.6.0, taken by cherry-pick.** Upstream fixed a stale read in March 2026: a read retried with the same request context, while heartbeat responses were delayed, could be confirmed by acknowledgements of an earlier request, so a leader already cut off answered with a stale index. Running upstream's regression test on the fork showed it had the bug too. The fix came in by `git cherry-pick`, keeping its authorship. As written upstream, it let a heartbeat response with a context shorter than 8 bytes panic the leader; the fork's hostile-input test caught that at once, and the fork now checks the context. upstream.json in the raft repository lists every commit taken this way.

**Found after the move to its own repository.** Once the simulator retried reads, its schedules changed, and it found two more defects, both also in upstream's main branch:
- **A crash on a pre-vote.** Raft's storage contract persists entries before the hard state, so a crash between the two can leave a node with entries but no term (0). Such a node panicked when it rejected a pre-vote. It now rejects without answering. `TestPreVoteRejectedWithoutTerm` reproduces it.
- **A group without a leader for good.** Raft ignored vote requests from a lower term, so a candidate behind in term could never learn the higher term from a voter it needed. In a joint configuration, another node's endless campaigns kept raising that voter's term without ever reaching the candidate. A node that knows no leader now rejects such a request with its term, as the Raft paper prescribes. `TestCandidateLearnsHigherTerm` reproduces it.

**Simulation.** `internal/raftsim` runs randomized clusters of 1 to 7 voters plus learners. Faults include message loss, duplication and reordering, partitions, crashes that lose unsynced writes, crashes of and partitions around the leader, membership churn and joint consensus, snapshots, leadership transfers, lease and ReadIndex reads (a quarter of them retries of earlier requests, as clients that time out send), and clock skew. Throughout, it checks:
- election safety;
- that votes persist;
- state machine safety;
- configuration safety;
- snapshot integrity;
- log matching;
- linearizable reads;
- at-most-once delivery;
- liveness once faults stop.

Every run is a pure function of its seed. Before reads were retried, two soaks of 40,000 clusters each found nothing. Together they ran 493 million steps, with 1.4 million crashes, 0.9 million partitions, 730,000 configuration changes, 1.2 million snapshots, 218,000 leadership transfers and 6.3 million reads, 2.1 million of them checked for linearizability.

Retried reads changed the schedules, and the next two soaks of 40,000 clusters found the two defects above, one each. With both fixed, a further 40,000 clusters found nothing: 247 million steps, 821,000 crashes, 573,000 partitions, 438,000 configuration changes (95,000 of them through joint configurations), 514,000 snapshots and 2.4 million reads, 778,000 of them retries and 773,000 checked for linearizability.

The simulator did not reach the read-index bug itself. 40,000 clusters on the code before that fix found no stale read, because the bug needs a leader that is already cut off to receive a retried request and late heartbeat responses in a particular order. Upstream's regression test covers it.

**Mutation testing.** 47 mutants, listed in [raft-mutation.md](raft-mutation.md):
- **26 classic raft bugs.** Upstream's suite catches 23. The fork's tests and the simulator catch 2 more, and the last is a redundant check that no test can observe.
- **21 undone fork fixes.** All 21 are caught, while upstream's suite alone catches 3.

## seglog

The guarantees, and how each is tested, are in [seglog/README.md](../../seglog/README.md). In summary:

| Suite | Scale |
|---|---|
| Differential test against `raft.MemoryStorage`, plus upstream's MemoryStorage tests | 400 deterministic random operation sequences per run, plus 25 minutes of fuzzing (about 275,000 sequences) |
| Damage to every file: byte flips, zeroed bytes, lost and misdirected sectors, truncation, deletion | about 28,000 images per run. The exhaustive run damaged every byte, sector and length of every file: 1,241,449 images, of which 784,153 were recovered intact, 2,876 refused, 453,909 detected on read and 510 had a torn tail reported. Exactly 1 lost data silently: the documented blind spot below. |
| Stray, swapped, stale and foreign files | 68 cases on two images |
| Crafted records and checkpoints with valid checksums | 1,200 cases per run; 10 minutes of fuzzing each in the final run (1.6 million and 218,000 executions) |
| Power cut before every filesystem operation, keeping none, all or a torn part of the unsynced data, and again during recovery | 2,013 crash images |
| Writeback errors that lose data (fsyncgate), with and without a later power cut | each of 100 fsyncs |
| An error at every operation of every kind, caches on and off | 748 operations |
| Concurrent use of every API with a random Close, under the race detector | 12 runs per test run |
| raft simulator on seglog storage, and on mixed seglog and in-memory nodes | 3,000 + 3,000 clusters, rerun on the final raft with retried reads: 124,555 crashes and 112,817 retried reads. The run before, on seglog alone, replayed 1.03 million commits in recovery and discarded 105 torn tails. |
| Mutation testing | 58 mutants: 55 killed, 3 equivalent ([seglog-mutation.md](seglog-mutation.md)) |

**Bugs found and fixed** (each with a regression test that fails before its fix):
1. A Save raft never issues could make the next Open fail for every group on the node.
2. A rejected Save could still persist its snapshot.
3. After a failed fsync (fsyncgate), a restart replayed data that never reached the disk.
4. A resumed segment's name was never synced, so a power cut could lose the file.
5. A stray segment file numbered near 2^64 led a roll to overwrite live segments.
6. Clients could plant checksummed chunks in their entries: recovery after a torn write could refuse to start, or index a forged entry. Chunk checksums are now keyed per log.
7. An oversized index file crashed the process, and a crafted snapshot reference could allocate up to 1 TiB.
8. Damage to the last commit that did not look like an interrupted write was silently discarded, although that commit may have been acknowledged. Open now refuses it, unless told otherwise.
9. A reused group id could return the removed group's data. Fuzzing found this in the final run. One segment held records of a removed group and of a new group with the same id, with the same index and term but different data. If damage made the new group's record unreadable and the segment's index file was also lost, a read returned the removed group's record without an error. Each group now records where its first entry record is, and reads refuse any record older than that.
10. `Close` hid failures. A truncated segment read back as an I/O error, not corruption. `Term` misreported indexes near 2^64. `Scrub` raised false alarms during GC.

**Performance:** unchanged against the engine before this work, within run-to-run noise (seglog/README.md, Performance).

## The crash model

Every crash test trusts `internal/crashfs`, so it is tested too:
- `TestConformsToOS` runs 300 random sequences of 120 filesystem operations against both crashfs and the real filesystem, and compares every result: errors by class, data, sizes and listings. It found 6 places where the model differed from Linux, all fixed. For example, a path through a file gave "does not exist" instead of ENOTDIR, and a directory containing only subdirectories could be removed.
- crashfs's own tests check that a crash keeps synced data and only plausible unsynced data, that renames are atomic, and that a failed sync loses data as Linux does.

## What this does not cover

- **Real power cuts.** Crash testing runs against a model of a POSIX filesystem, checked against Linux's behaviour without crashes. Testing on real hardware with real power cuts, or with a block-level recorder such as `dm-log-writes`, belongs in the target environment.
- **Fuzzing time.** Fuzzers ran for minutes each, not days. Continuous fuzzing belongs in CI.
- **Formal proof.** raft's correctness rests on its published proofs, plus the testing above for this implementation. No model checking of the code was done.
- **Platforms.** Supported: 64-bit Linux. Builds refuse 32-bit platforms. On Windows, two processes are not kept from opening the same log.
- **Someone with write access to a node's data directory.** Such a person can forge anything, as with any storage engine.
- **One silent case.** If the newest segment is cut exactly at a commit boundary, after the last checkpoint, the commits after the cut are gone without an error. No write-ahead log can tell that from commits never written. It needs a fault below the filesystem's fsync contract. The exhaustive damage test checks that this is the only silent case. Fixed-size segment files would close it (seglog/README.md).
