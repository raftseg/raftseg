# Mutation testing: raft

Each mutant puts one bug back into the fork ([github.com/raftseg/raft](https://github.com/raftseg/raft)): either a mistake raft implementations are known to make (an off-by-one in a quorum, a vote not recorded, a response sent before the state it reports is durable), or the undoing of one of the fork's own fixes. A mutant is killed when a test suite fails with it in place. The catalogue is `mutation/raft_mutants.py`, with paths inside the raft repository, and `mutation/run.py raft` reruns it against a raft checkout in the go.work workspace.

This run, on 2026-10-15, is of the fork on upstream's `main` (commit `638f427`, with google.golang.org/protobuf). The run before it, of the fork on v3.6.0, is in `mutation/results-raft-2026-10-14.json`; its catalogue is in this repository's history.

Three suites were run against every mutant:
- **Upstream:** etcd raft's own tests from upstream's `main`, with only their import paths changed and testify replaced by the fork's equivalent (and the 11 data-driven scenarios that record behaviour a fix changes, which raft's FORK.md names).
- **Fork:** the regression, hostile-input and compatibility tests the fork adds.
- **Simulator:** `internal/raftsim`, 150 random clusters per mutant, and the determinism tests.

| Mutants | Count | Upstream suite kills | Fork tests kill | Simulator kills | Killed by any |
|---|---|---|---|---|---|
| Classic raft bugs | 26 | 23 | 8 | 12 | 25 |
| Fork fixes undone | 27 | 3 | 27 | 4 | 27 |

Upstream's suite misses two classic bugs that the simulator catches:
- a conf change judged pending against the commit index rather than the applied index;
- a changed vote not marking the Ready MustSync.

It also misses 24 of the 27 undone fork fixes.

One mutant survives every suite: restoring a snapshot whose index equals the commit index. That case is redundant rather than wrong. Such a snapshot carries nothing the log does not already hold, and raft's next check (whether the log already has the snapshot's last entry) turns the restore into a fast-forward of the commit index.

**What changed with the move to upstream's `main`.**
- The catalogue was retargeted to the new code (fields read through getters, messages built as pointers). Every mutant still changes the same logic.
- One mutant is retired: "unbounded message nesting" undid the nesting limit the fork had added to gogo's decoder. protobuf's decoder enforces its own limit now, which `TestNestingLimit` checks; there is no fork code left to mutate.
- Mutants 46 to 52 are new. They undo what the fork added with the move to protobuf: negative message types (protobuf decodes any int32 into an enum field), nil entries and entries with fields this version does not know, and forwarding a follower's requests as copies now that messages are shared pointers. The fork's tests kill all seven; nothing else does.
- "Transfer cannot override a lease" (17) no longer compiled as written, because the forced flag it leaves unused is used nowhere else now; it was rewritten to keep the flag referenced. The simulator's 150 clusters per mutant no longer kill it, nor "safe read completes without a quorum" (14) or "snapshot Ready not synced" (35), which they killed on the v3.6.0 base; upstream's or the fork's tests still do. They now kill "single-voter read before a commit in its term" (32) and "stale leader not answered" (34), which they did not. Different code paths change the simulator's schedules, so a fixed number of clusters reaches different cases.

Mutants 41 to 45 came with the cherry-picked read-index fix, the check the fork added to it, and two fixes the simulator found once it retried reads: the pre-vote crash and the candidate that never learned the higher term. The simulator kills none of the five in 150 clusters: the first three need hostile or retried input in a particular order, and the last two a crash or a configuration at one particular moment, which the simulator found only after tens of thousands of clusters. Unit tests construct each directly.

| # | Kind | Mutant | Upstream | Fork tests | Simulator |
|---|---|---|---|---|---|
| 0 | classic | isUpToDate accepts a log one entry short | killed | killed | killed |
| 1 | classic | commit entries of earlier terms by count (Figure 8) | killed | - | - |
| 2 | classic | follower commits beyond the entries it got | killed | - | - |
| 3 | classic | vote again in a term when no leader is known | killed | - | killed |
| 4 | classic | do not record a granted vote | killed | - | killed |
| 5 | classic | candidate does not vote for itself | killed | - | killed |
| 6 | classic | quorum index off by one | killed | killed | killed |
| 7 | classic | vote quorum off by one | killed | killed | killed |
| 8 | classic | progress accepts stale match | killed | - | - |
| 9 | classic | append below commit index accepted | killed | - | - |
| 10 | classic | vote response sent before the vote is durable | killed | killed | killed |
| 11 | classic | append response sent before entries are durable | killed | killed | killed |
| 12 | classic | pending conf change judged against commit, not apply | - | - | killed |
| 13 | classic | new leader allows a conf change at once | killed | - | killed |
| 14 | classic | safe read completes without a quorum | killed | killed | - |
| 15 | classic | reads before the leader commits in its term | killed | killed | killed |
| 16 | classic | check quorum never steps down | killed | - | - |
| 17 | classic | transfer cannot override a lease | killed | - | - |
| 18 | classic | campaign with unapplied conf changes | killed | - | - |
| 19 | classic | restore a snapshot at the commit index | - | - | - |
| 20 | classic | truncated unstable entries stay in progress | killed | - | - |
| 21 | classic | flow control never pauses | killed | - | - |
| 22 | classic | vote change not synced | - | - | killed |
| 23 | classic | progress update ignores probing | killed | - | - |
| 24 | classic | candidate ignores lost votes | killed | - | - |
| 25 | fork fix undone | no commit inference from later conf changes | - | killed | - |
| 26 | fork fix undone | no rescue campaigns | - | killed | - |
| 27 | fork fix undone | lease without heartbeat rounds | - | killed | - |
| 28 | fork fix undone | no restart guard | - | killed | - |
| 29 | fork fix undone | removed leader steps down at once | killed | killed | - |
| 30 | fork fix undone | snapshot restored while applying | - | killed | - |
| 31 | fork fix undone | single-voter read without checking the voter | - | killed | killed |
| 32 | fork fix undone | single-voter read before a commit in its term | - | killed | killed |
| 33 | fork fix undone | lease kept after MsgTimeoutNow | - | killed | - |
| 34 | fork fix undone | stale leader not answered | killed | killed | killed |
| 35 | fork fix undone | snapshot Ready not synced | killed | killed | - |
| 36 | fork fix undone | peer messages not validated | - | killed | - |
| 37 | fork fix undone | zero apply limit pauses for good | - | killed | killed |
| 38 | fork fix undone | transfer dropped while conf changes are pending | - | killed | - |
| 39 | fork fix undone | no validation of conf change enums | - | killed | - |
| 40 | fork fix undone | transfer forwarded with its term | - | killed | - |
| 41 | fork fix undone | read acknowledgements of any length decoded | - | killed | - |
| 42 | fork fix undone | read acknowledgement beyond the reads registered accepted | - | killed | - |
| 43 | classic | reads confirmed by the request's own context again | killed | killed | - |
| 44 | fork fix undone | node without a term answers a rejected pre-vote | - | killed | - |
| 45 | fork fix undone | lower-term vote requests ignored again | - | killed | - |
| 46 | fork fix undone | negative message types index the type tables | - | killed | - |
| 47 | fork fix undone | negative message types pass validation | - | killed | - |
| 48 | fork fix undone | nil entries from a peer accepted | - | killed | - |
| 49 | fork fix undone | entry types outside the enum accepted | - | killed | - |
| 50 | fork fix undone | entries with unknown fields accepted | - | killed | - |
| 51 | fork fix undone | forwarded requests change the caller's message | - | killed | - |
| 52 | fork fix undone | shallow copy drops a field | - | killed | - |
