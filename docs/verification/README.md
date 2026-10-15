# Verification of raftseg's raft and storage packages

Status as of 2026-10-15, after raft moved to etcd raft's main branch and google.golang.org/protobuf. Everything below can be rerun with the commands given, from this repository and the raft repository ([github.com/raftseg/raft](https://github.com/raftseg/raft)) checked out next to it in a `go.work` workspace (see README.md). It needs Go 1.26 or later and the modules `go.mod` requires; the mutation runner also needs Python 3.

## What exists

| Package | What it is | Production code | Tests |
|---|---|---|---|
| [`github.com/raftseg/raft`](https://github.com/raftseg/raft) (its own repository) | Fork of etcd raft (Apache-2.0) at upstream's `main` (commit `638f427`, every change of v3.7.0): safety, linearizability and liveness fixes, and validation of peer messages. | 12,400 lines (1,200 of them generated protobuf code) | upstream's 258 tests and 28 data-driven scenarios, plus 37 of ours (regression, hostile-input and compatibility tests) and 4 fuzzers |
| `seglog/` | The durable raft log: one segmented log per node shared by all its raft groups, each a `raft.Storage`. | 5,300 lines | 104 tests, 5 fuzzers, about 9,600 lines |
| `vfs/` | The filesystem interface and its OS implementation. | 200 lines | 4 tests, plus the OS side of crashfs's conformance test |
| `internal/raftsim/` | Deterministic cluster simulator with raft's safety and liveness properties as checks, and a check that nothing raft and storage share changes once handed over. Storage: in memory, seglog, or mixed. | (test only) | |
| `internal/crashfs/` | Filesystem model: power cuts with torn and reordered writes, lost directory operations, writeback errors, fault injection. | (test only) | conformance to the OS filesystem |

One third-party module is compiled into a product: Google's protobuf runtime, google.golang.org/protobuf, which raft's messages are generated for. `go.mod` requires only it and the first-party raft module, which requires only it. `internal/depcheck`'s tests fail otherwise. `go run ./internal/tools/sbom` writes a CycloneDX SBOM that lists every compiled package with a hash of its sources (protobuf's with their supplier and license, raft's with their license and upstream pedigree), checks every module from the module cache against `go.sum`, and fails on anything else.

The full suite passes with Go 1.27.2 (raft from a workspace checkout) and Go 1.26.9 (raft from the module cache), and the short suite under the race detector.

## raft

**Following upstream.** The fork began at upstream's v3.6.0, with its protobuf dependencies (gogo's) removed. In October 2026 it merged upstream's `main`, which had moved to google.golang.org/protobuf: every message and every field became a pointer, read through getters. Both histories are kept whole in the raft repository, and every fork change was ported to the new API. Each fork test still fails on upstream's code, and upstream's own tests run with only their import paths changed (and testify replaced). They pass, except that 11 of upstream's data-driven scenarios record behaviour a fix changes; raft's FORK.md names each.

**Compatibility.** The wire format is unchanged, and the fork checks it against corpora recorded from v3.6.0:
- All 3,609 messages encoded by v3.6.0 decode to what v3.6.0 said they held, and encode again byte for byte.
- Of 21,654 damaged and hostile inputs, 5,911 decode as v3.6.0 decoded them (apart from unknown fields, which protobuf keeps and gogo dropped) and 15,047 are rejected by both. 580 that v3.6.0 rejected now decode, each with unknown fields; 116 that v3.6.0 accepted are now rejected as invalid wire format.
- Text and JSON forms are now protobuf's, as upstream has them.

raftseg's own stored data follows: seglog reads a log written before the move (with gogo's code) as it was read then, each entry encoding to the same bytes (`TestReadsV0Log`).

**Bugs found in etcd raft and fixed.** Every change is listed with its reason in raft's FORK.md, and each has a regression test that fails on upstream's code. They include:
- **Split brain.** A node two configuration changes behind could win an election, so two leaders in one term committed different entries at the same index.
- **Silent configuration corruption.** A snapshot restored while older entries were still being applied corrupted the configuration.
- **Lease reads up to two election timeouts stale.** Lease-based reads were served on CheckQuorum alone.
- **Groups left without a leader for good.** This happened after a leader removed itself.
- **A panicking leader.** Malformed proposals from a peer panicked the leader.
- **A killed process.** About 40 MB of nested input exhausted the stack. That was v3.6.0's gogo decoder; protobuf's decoder bounds the nesting itself.

**Fixed upstream since v3.6.0.** Upstream fixed a stale read in March 2026: a read retried with the same request context, while heartbeat responses were delayed, could be confirmed by acknowledgements of an earlier request, so a leader already cut off answered with a stale index. Running upstream's regression test on the fork showed it had the bug too. The fork first took the fix by `git cherry-pick`, keeping its authorship, and now has it from upstream's `main`. As written upstream, it lets a heartbeat response with a context shorter than 8 bytes panic the leader; the fork's hostile-input test caught that at once, and the fork checks the context. upstream.json in the raft repository records the upstream commit the fork is based on and any commit cherry-picked since.

**Found with the move to protobuf.** Pointers and protobuf's open enums brought defects of their own, all in upstream's `main`; each is fixed with a regression test (raft's FORK.md):
- **A panic on a damaged message type.** protobuf decodes any int32 into an enum field, and raft indexed a table with the message type, so a negative type from a peer panicked `Step` before validation. The hostile-input fuzzer found it once it fed messages decoded from damaged bytes.
- **A caller's message changed under it.** A follower forwarding a proposal, read or transfer readdressed the message it was given, harmless with values but not with pointers: a message a transport delivered twice arrived the second time addressed to the leader. Forwarded requests are now copies.
- **Unchecked pointers from peers.** Nil entries and entries with fields this version does not know are refused, as are snapshots without metadata or configuration.

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
- liveness once faults stop;
- that nothing raft and storage share changes once handed over: the entries, messages, hard state and snapshot of every `Ready`, and everything raft reads from storage. With pointer messages, a change there would rewrite a log behind its owner's back. A storage that changes entries it has handed out is caught in 19 of 20 runs (`TestCatchesSharedMessageChanges`).

Every run is a pure function of its seed. Before reads were retried, two soaks of 40,000 clusters each found nothing. Together they ran 493 million steps, with 1.4 million crashes, 0.9 million partitions, 730,000 configuration changes, 1.2 million snapshots, 218,000 leadership transfers and 6.3 million reads, 2.1 million of them checked for linearizability.

Retried reads changed the schedules, and the next two soaks of 40,000 clusters found the two defects above, one each. With both fixed, a further 40,000 clusters found nothing: 247 million steps, 821,000 crashes, 573,000 partitions, 438,000 configuration changes (95,000 of them through joint configurations), 514,000 snapshots and 2.4 million reads, 778,000 of them retries and 773,000 checked for linearizability.

After the move to upstream's `main` and protobuf, a further 40,000 clusters found nothing: 247 million steps, 819,000 crashes, 572,000 partitions, 439,000 configuration changes (97,000 of them through joint configurations), 521,000 snapshots, 88,000 leadership transfers and 2.4 million reads, 783,000 of them retries and 778,000 checked for linearizability. Throughout, 415 million shared messages were checked unchanged.

The simulator did not reach the read-index bug itself. 40,000 clusters on the code before that fix found no stale read, because the bug needs a leader that is already cut off to receive a retried request and late heartbeat responses in a particular order. Upstream's regression test covers it.

**Mutation testing.** 53 mutants on the fork at upstream's `main`, listed in [raft-mutation.md](raft-mutation.md):
- **26 classic raft bugs.** Upstream's suite catches 23. The simulator catches 2 more, and the last is a redundant check that no test can observe.
- **27 undone fork fixes**, 7 of them added with the move to protobuf. All 27 are caught, while upstream's suite alone catches 3.

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
| raft simulator on seglog storage, and on mixed seglog and in-memory nodes | 3,000 + 3,000 clusters on the fork at upstream's `main` with protobuf: 120,836 crashes and 114,779 retried reads, 63 million shared messages checked unchanged. On seglog alone, recovery replayed 1.02 million commits and discarded 127 torn tails. The runs before, on the v3.6.0 base, found nothing either. |
| A log written before raft's move to protobuf | read back as it was read then, each entry encoding to the same bytes, then appended to and reopened |
| raft's messages as pointers | copies kept and handed out; entries read back exactly as saved, unset and unknown fields included, from the cache, from disk and after index rebuilds; nil entries refused |
| Fuzzing again after raft's move to protobuf | 3 to 4 minutes each: FuzzStorageContract 57,700 executions, FuzzCraftedRecords 1.09 million, FuzzCraftedCheckpoint 138,000, FuzzCorruptLog 29,700, FuzzScanner 2.05 million. Nothing found. |
| Mutation testing | 65 mutants: 61 killed, 4 equivalent ([seglog-mutation.md](seglog-mutation.md)) |

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
11. With raft's move to protobuf, two defects came in with the port and were found in review before release: a nil entry passed to `Save` crashed the process (it is now refused as an invalid update), and an entry with fields raftpb does not define, which protobuf keeps, made the log unopenable (the entry index now skips any field, as protobuf does).

**Performance:** unchanged against the engine before this work, within run-to-run noise. After raft's move to protobuf, a Save costs the same CPU as before (16.3 µs against 16.5 µs) and a read of 100 entries from disk 13% less (seglog/README.md, Performance).

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
