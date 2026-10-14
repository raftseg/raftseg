# Mutants for the raft fork (paths relative to the raft repository's root). "upstream" mutants break raft logic present
# upstream; "fork" mutants undo one of the fork's fixes.
M = []
def m(kind, label, file, old, new):
    M.append(dict(kind=kind, label=label, file=file, old=old, new=new))

U, F = "upstream", "fork"
m(U, "isUpToDate accepts a log one entry short", "log.go",
  "their.term == our.term && their.index >= our.index", "their.term == our.term && their.index+1 >= our.index")
m(U, "commit entries of earlier terms by count (Figure 8)", "log.go",
  "if at.term != 0 && at.index > l.committed && l.matchTerm(at) {", "if at.term != 0 && at.index > l.committed {")
m(U, "follower commits beyond the entries it got", "log.go",
  "l.commitTo(min(committed, lastnewi))", "l.commitTo(min(committed, l.lastIndex()))")
m(U, "vote again in a term when no leader is known", "raft.go",
  "(r.Vote == None && r.lead == None) ||", "(r.lead == None) ||")
m(U, "do not record a granted vote", "raft.go",
  "				r.electionElapsed = 0\n				r.Vote = m.From", "				r.electionElapsed = 0")
m(U, "candidate does not vote for itself", "raft.go",
  "	r.Vote = r.id\n", "\n")
m(U, "quorum index off by one", "quorum/majority.go",
  "pos := n - (n/2 + 1)", "pos := n - n/2")
m(U, "vote quorum off by one", "quorum/majority.go",
  "q := len(c)/2 + 1", "q := len(c) / 2")
m(U, "progress accepts stale match", "tracker/progress.go",
  "	if n <= pr.Match {\n		return false\n	}\n	pr.Match = n", "	if n < pr.Match {\n		return false\n	}\n	pr.Match = n")
m(U, "append below commit index accepted", "raft.go",
  "if a.prev.index < r.raftLog.committed {", "if a.prev.index+1 < r.raftLog.committed {")
m(U, "vote response sent before the vote is durable", "raft.go",
  "if m.Type == pb.MsgAppResp || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVoteResp {", "if m.Type == pb.MsgAppResp || m.Type == pb.MsgPreVoteResp {")
m(U, "append response sent before entries are durable", "raft.go",
  "if m.Type == pb.MsgAppResp || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVoteResp {", "if m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVoteResp {")
m(U, "pending conf change judged against commit, not apply", "raft.go",
  "alreadyPending := r.pendingConfIndex > r.raftLog.applied", "alreadyPending := r.pendingConfIndex > r.raftLog.committed")
m(U, "new leader allows a conf change at once", "raft.go",
  "	r.pendingConfIndex = r.raftLog.lastIndex()\n\n	traceBecomeLeader(r)", "	r.pendingConfIndex = r.raftLog.committed\n\n	traceBecomeLeader(r)")
m(U, "safe read completes without a quorum", "read_only.go",
  "newConfirmedReads := uint64(c.CommittedIndex(ro))", "newConfirmedReads := ro.confirmedReads + uint64(len(ro.unconfirmedReads))")
m(U, "reads before the leader commits in its term", "raft.go",
  "	return r.raftLog.zeroTermOnOutOfBounds(r.raftLog.term(r.raftLog.committed)) == r.Term", "	return true")
m(U, "check quorum never steps down", "raft.go",
  "if !r.trk.QuorumActive() && r.isVoter() {", "if false {")
m(U, "transfer cannot override a lease", "raft.go",
  "			if !force && inLease {", "			if inLease {")
m(U, "campaign with unapplied conf changes", "raft.go",
  "	if r.hasUnappliedConfChanges() {\n		r.logger.Warningf(\"%x cannot campaign", "	if false {\n		r.logger.Warningf(\"%x cannot campaign")
m(U, "restore a snapshot at the commit index", "raft.go",
  "	if s.Metadata.Index <= r.raftLog.committed {\n		return false\n	}\n	if r.raftLog.applying", "	if s.Metadata.Index < r.raftLog.committed {\n		return false\n	}\n	if r.raftLog.applying")
m(U, "truncated unstable entries stay in progress", "log_unstable.go",
  "		u.offsetInProgress = min(u.offsetInProgress, fromIndex)\n", "\n")
m(U, "flow control never pauses", "tracker/progress.go",
  "	case StateProbe:\n		return pr.MsgAppFlowPaused", "	case StateProbe:\n		return false")
m(U, "vote change not synced", "rawnode.go",
  "return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term", "return entsnum != 0 || st.Term != prevst.Term")
m(U, "progress update ignores probing", "raft.go",
  "if pr.MaybeUpdate(m.Index) || (pr.Match == m.Index && pr.State == tracker.StateProbe) {", "if pr.MaybeUpdate(m.Index) {")
m(U, "candidate ignores lost votes", "raft.go",
  "		case quorum.VoteLost:\n			// pb.MsgPreVoteResp contains future term of pre-candidate\n			// m.Term > r.Term; reuse r.Term\n			r.becomeFollower(r.Term, None)", "		case quorum.VoteLost:")

m(F, "no commit inference from later conf changes", "raft.go",
  "func (r *raft) inferConfChangeCommit() {\n	if r.disableConfChangeValidation {", "func (r *raft) inferConfChangeCommit() {\n	if true {")
m(F, "no rescue campaigns", "raft.go",
  "} else if r.pastElectionTimeout() && r.mayRescue() {", "} else if false && r.mayRescue() {")
m(F, "lease without heartbeat rounds", "raft.go",
  "	q := r.trk.Voters.CommittedIndex(leaseIndexer{r})\n	if q == 0 {\n		return false\n	}", "	q := r.trk.Voters.CommittedIndex(leaseIndexer{r})\n	if true {\n		return true\n	}")
m(F, "no restart guard", "raft.go",
  "		r.startGuard = r.electionTimeout\n", "\n")
m(F, "removed leader steps down at once", "raft.go",
  "		if r.maybeCommit() {\n			r.bcastAppend()\n		}\n		r.handOff()\n		return cs", "		if r.stepDownOnRemoval {\n			r.becomeFollower(r.Term, None)\n		}\n		return cs")
m(F, "snapshot restored while applying", "raft.go",
  "	if r.raftLog.applying > r.raftLog.applied {\n		// Fork", "	if false {\n		// Fork")
m(F, "single-voter read without checking the voter", "raft.go",
  "	if r.trk.IsSingleton() && r.isVoter() {\n		if resp", "	if r.trk.IsSingleton() {\n		if resp")
m(F, "single-voter read before a commit in its term", "raft.go",
  "		if !r.committedEntryInCurrentTerm() {\n			r.pendingReadIndexMessages", "		if !r.trk.IsSingleton() && !r.committedEntryInCurrentTerm() {\n			r.pendingReadIndexMessages")
m(F, "lease kept after MsgTimeoutNow", "raft.go",
  "	r.leaseRevoked = true\n", "\n")
m(F, "stale leader not answered", "raft.go",
  "		if m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp {\n			// We have received", "		if (r.checkQuorum || r.preVote) && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {\n			// We have received")
m(F, "snapshot Ready not synced", "rawnode.go",
  " || !IsEmptySnap(rd.Snapshot)", "")
m(F, "peer messages not validated", "rawnode.go",
  "	if isPeerMessage(rn.raft.id, m) {", "	if false {")
m(F, "zero apply limit pauses for good", "log.go",
  "	return l.applyingEntsSize > 0 && l.applyingEntsSize >= l.maxApplyingEntsSize", "	return l.applyingEntsSize >= l.maxApplyingEntsSize")
m(F, "unbounded message nesting", "raftpb/raft.pb.go",
  "			if depth >= MaxMessageNesting {", "			if false {")
m(F, "transfer dropped while conf changes are pending", "raft.go",
  "	if r.pendingTimeoutNow && r.state == StateFollower && !r.hasUnappliedConfChanges() {", "	if false {")
m(F, "no validation of conf change enums", "validate.go",
  "	if cc.Transition < pb.ConfChangeTransitionAuto || cc.Transition > pb.ConfChangeTransitionJointExplicit {", "	if false {")
m(F, "transfer forwarded with its term", "raft.go",
  "		m.Term = 0\n		r.send(m)", "		r.send(m)")

# --- upstream's read-index fix (cherry-picked) and the check added to it
m(F, "read acknowledgements of any length decoded", "read_only.go",
  "\tif len(ctx) != 8 {\n\t\treturn\n\t}", "\tif len(ctx) == 0 {\n\t\treturn\n\t}")
m(F, "read acknowledgement beyond the reads registered accepted", "read_only.go",
  "\tif pos > ro.confirmedReads+uint64(len(ro.unconfirmedReads)) {\n\t\treturn\n\t}\n", "")
m(U, "reads confirmed by the request's own context again", "raft.go",
  "\t\tr.readOnly.recvAck(r.id, r.readOnly.heartbeatCtx())\n\t\tr.bcastHeartbeat()", "\t\tr.readOnly.recvAck(r.id, r.readOnly.heartbeatCtx())\n\t\tr.bcastHeartbeatWithCtx(m.Entries[0].Data)")
m(F, "node without a term answers a rejected pre-vote", "raft.go",
  "\t\t\tif r.Term != 0 {\n\t\t\t\tr.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})\n\t\t\t}",
  "\t\t\tr.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})")
m(F, "lower-term vote requests ignored again", "raft.go",
  "\t\t} else if m.Type == pb.MsgVote && r.lead == None {", "\t\t} else if false && m.Type == pb.MsgVote && r.lead == None {")
