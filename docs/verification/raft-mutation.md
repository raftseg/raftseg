# Mutation testing: raft

Each mutant puts one bug back into the fork ([github.com/raftseg/raft](https://github.com/raftseg/raft)): either a mistake raft implementations are known to make (an off-by-one in a quorum, a vote not recorded, a response sent before the state it reports is durable), or the undoing of one of the fork's own fixes. A mutant is killed when a test suite fails with it in place. The catalogue is `mutation/raft_mutants.py`, with paths inside the raft repository, and `mutation/run.py raft` reruns it against a raft checkout in the go.work workspace.

Three suites were run against every mutant:
- **Upstream:** etcd raft's own tests, carried over unchanged, plus upstream's tests that came with fixes taken by cherry-pick.
- **Fork:** the regression and hostile-input tests the fork adds.
- **Simulator:** `internal/raftsim`, 150 random clusters per mutant.

| Mutants | Count | Upstream suite kills | Fork tests kill | Simulator kills | Killed by any |
|---|---|---|---|---|---|
| Classic raft bugs | 26 | 23 | 7 | 14 | 25 |
| Fork fixes undone | 21 | 3 | 21 | 3 | 21 |

Upstream's suite misses two classic bugs that the fork's suites catch:
- a conf change judged pending against the commit index rather than the applied index;
- a changed vote not marking the Ready MustSync.

It also misses 18 of the 21 undone fork fixes. Upstream's v3.6.0 suite also missed a ReadIndex read completing without a quorum; the regression test that came with upstream's read-index fix (taken by cherry-pick) now catches it.

Mutants 42 to 46 were added on 2026-10-14, with the cherry-picked read-index fix, the check the fork added to it, and two fixes the simulator found once it retried reads: the pre-vote crash and the candidate that never learned the higher term. The simulator's 150 clusters per mutant kill none of the five. The first three need hostile or retried input in a particular order, and the last two a crash or a configuration at one particular moment; the simulator found those two only after tens of thousands of clusters. Unit tests construct each directly.

One mutant survives every suite: restoring a snapshot whose index equals the commit index. That case is redundant rather than wrong. Such a snapshot carries nothing the log does not already hold, and raft's next check (whether the log already has the snapshot's last entry) turns the restore into a fast-forward of the commit index.

| Kind | Mutant | Upstream | Fork tests | Simulator |
|---|---|---|---|---|
| classic | isUpToDate accepts a log one entry short | killed | - | killed |
| classic | commit entries of earlier terms by count (Figure 8) | killed | - | - |
| classic | follower commits beyond the entries it got | killed | - | - |
| classic | vote again in a term when no leader is known | killed | - | killed |
| classic | do not record a granted vote | killed | - | killed |
| classic | candidate does not vote for itself | killed | - | killed |
| classic | quorum index off by one | killed | killed | killed |
| classic | vote quorum off by one | killed | killed | killed |
| classic | progress accepts stale match | killed | - | - |
| classic | append below commit index accepted | killed | - | - |
| classic | vote response sent before the vote is durable | killed | killed | killed |
| classic | append response sent before entries are durable | killed | killed | killed |
| classic | pending conf change judged against commit, not apply | - | - | killed |
| classic | new leader allows a conf change at once | killed | - | killed |
| classic | safe read completes without a quorum | killed | killed | killed |
| classic | reads before the leader commits in its term | killed | killed | killed |
| classic | check quorum never steps down | killed | - | - |
| classic | transfer cannot override a lease | killed | killed | killed |
| classic | campaign with unapplied conf changes | killed | - | - |
| classic | restore a snapshot at the commit index | - | - | - |
| classic | truncated unstable entries stay in progress | killed | - | - |
| classic | flow control never pauses | killed | - | - |
| classic | vote change not synced | - | - | killed |
| classic | progress update ignores probing | killed | - | - |
| classic | candidate ignores lost votes | killed | - | - |
| fork fix undone | no commit inference from later conf changes | - | killed | - |
| fork fix undone | no rescue campaigns | - | killed | - |
| fork fix undone | lease without heartbeat rounds | - | killed | - |
| fork fix undone | no restart guard | - | killed | - |
| fork fix undone | removed leader steps down at once | killed | killed | - |
| fork fix undone | snapshot restored while applying | - | killed | - |
| fork fix undone | single-voter read without checking the voter | - | killed | killed |
| fork fix undone | single-voter read before a commit in its term | - | killed | - |
| fork fix undone | lease kept after MsgTimeoutNow | - | killed | - |
| fork fix undone | stale leader not answered | killed | killed | - |
| fork fix undone | snapshot Ready not synced | killed | killed | killed |
| fork fix undone | peer messages not validated | - | killed | - |
| fork fix undone | zero apply limit pauses for good | - | killed | killed |
| fork fix undone | unbounded message nesting | - | killed | - |
| fork fix undone | transfer dropped while conf changes are pending | - | killed | - |
| fork fix undone | no validation of conf change enums | - | killed | - |
| fork fix undone | transfer forwarded with its term | - | killed | - |
| fork fix undone | read acknowledgements of any length decoded | - | killed | - |
| fork fix undone | read acknowledgement beyond the reads registered accepted | - | killed | - |
| classic | reads confirmed by the request's own context again | killed | killed | - |
| fork fix undone | node without a term answers a rejected pre-vote | - | killed | - |
| fork fix undone | lower-term vote requests ignored again | - | killed | - |
