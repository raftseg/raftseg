# Mutants for the raft fork (paths relative to the raft repository's root). "upstream" mutants break raft logic present
# upstream; "fork" mutants undo one of the fork's fixes.
#
# Retargeted to the fork on upstream main (google.golang.org/protobuf: pointer messages read through getters). One
# mutant of the fork on v3.6.0 is retired: "unbounded message nesting" undid the nesting limit the fork had added to
# gogo's decoder; protobuf's decoder enforces its own limit now, which raftpb's TestNestingLimit checks. Mutants 47
# onwards undo protections the fork added with the move to protobuf.
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
  "				r.electionElapsed = 0\n				r.Vote = m.GetFrom()", "				r.electionElapsed = 0")
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
  "if m.GetType() == pb.MsgAppResp || m.GetType() == pb.MsgVoteResp || m.GetType() == pb.MsgPreVoteResp {", "if m.GetType() == pb.MsgAppResp || m.GetType() == pb.MsgPreVoteResp {")
m(U, "append response sent before entries are durable", "raft.go",
  "if m.GetType() == pb.MsgAppResp || m.GetType() == pb.MsgVoteResp || m.GetType() == pb.MsgPreVoteResp {", "if m.GetType() == pb.MsgVoteResp || m.GetType() == pb.MsgPreVoteResp {")
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
  "			if !force && inLease {", "			if _ = force; inLease {")
m(U, "campaign with unapplied conf changes", "raft.go",
  "	if r.hasUnappliedConfChanges() {\n		r.logger.Warningf(\"%x cannot campaign", "	if false {\n		r.logger.Warningf(\"%x cannot campaign")
m(U, "restore a snapshot at the commit index", "raft.go",
  "	if s.GetMetadata().GetIndex() <= r.raftLog.committed {\n		return false\n	}\n	if r.raftLog.applying", "	if s.GetMetadata().GetIndex() < r.raftLog.committed {\n		return false\n	}\n	if r.raftLog.applying")
m(U, "truncated unstable entries stay in progress", "log_unstable.go",
  "		u.offsetInProgress = min(u.offsetInProgress, fromIndex)\n", "\n")
m(U, "flow control never pauses", "tracker/progress.go",
  "	case StateProbe:\n		return pr.MsgAppFlowPaused", "	case StateProbe:\n		return false")
m(U, "vote change not synced", "rawnode.go",
  "return entsnum != 0 || st.GetVote() != prevst.GetVote() || st.GetTerm() != prevst.GetTerm()", "return entsnum != 0 || st.GetTerm() != prevst.GetTerm()")
m(U, "progress update ignores probing", "raft.go",
  "if pr.MaybeUpdate(m.GetIndex()) || (pr.Match == m.GetIndex() && pr.State == tracker.StateProbe) {", "if pr.MaybeUpdate(m.GetIndex()) {")
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
  "		if m.GetType() == pb.MsgHeartbeat || m.GetType() == pb.MsgApp {\n			// We have received", "		if (r.checkQuorum || r.preVote) && (m.GetType() == pb.MsgHeartbeat || m.GetType() == pb.MsgApp) {\n			// We have received")
m(F, "snapshot Ready not synced", "rawnode.go",
  " || !IsEmptySnap(rd.Snapshot)", "")
m(F, "peer messages not validated", "rawnode.go",
  "	if isPeerMessage(rn.raft.id, m) {", "	if false {")
m(F, "zero apply limit pauses for good", "log.go",
  "	return l.applyingEntsSize > 0 && l.applyingEntsSize >= l.maxApplyingEntsSize", "	return l.applyingEntsSize >= l.maxApplyingEntsSize")
m(F, "transfer dropped while conf changes are pending", "raft.go",
  "	if r.pendingTimeoutNow && r.state == StateFollower && !r.hasUnappliedConfChanges() {", "	if false {")
m(F, "no validation of conf change enums", "validate.go",
  "	if t := cc.GetTransition(); t < pb.ConfChangeTransitionAuto || t > pb.ConfChangeTransitionJointExplicit {",
  "	if t := cc.GetTransition(); false && (t < pb.ConfChangeTransitionAuto || t > pb.ConfChangeTransitionJointExplicit) {")
m(F, "transfer forwarded with its term", "raft.go",
  "		fwd.Term = nil\n		r.send(fwd)", "		r.send(fwd)")

# --- upstream's read-index fix (cherry-picked) and the check added to it
m(F, "read acknowledgements of any length decoded", "read_only.go",
  "\tif len(ctx) != 8 {\n\t\treturn\n\t}", "\tif len(ctx) == 0 {\n\t\treturn\n\t}")
m(F, "read acknowledgement beyond the reads registered accepted", "read_only.go",
  "\tif pos > ro.confirmedReads+uint64(len(ro.unconfirmedReads)) {\n\t\treturn\n\t}\n", "")
m(U, "reads confirmed by the request's own context again", "raft.go",
  "\t\tr.readOnly.recvAck(r.id, r.readOnly.heartbeatCtx())\n\t\tr.bcastHeartbeat()", "\t\tr.readOnly.recvAck(r.id, r.readOnly.heartbeatCtx())\n\t\tr.bcastHeartbeatWithCtx(m.Entries[0].Data)")
m(F, "node without a term answers a rejected pre-vote", "raft.go",
  "\t\t\tif r.Term != 0 {\n\t\t\t\tr.send(&pb.Message{To: m.From, Term: new(r.Term), Type: voteRespMsgType(m.GetType()).Enum(), Reject: new(true)})\n\t\t\t}",
  "\t\t\tr.send(&pb.Message{To: m.From, Term: new(r.Term), Type: voteRespMsgType(m.GetType()).Enum(), Reject: new(true)})")
m(F, "lower-term vote requests ignored again", "raft.go",
  "\t\t} else if m.GetType() == pb.MsgVote && r.lead == None {", "\t\t} else if false && m.GetType() == pb.MsgVote && r.lead == None {")

# --- protections added with the move to protobuf (pointer messages, open enums, unknown fields kept)
m(F, "negative message types index the type tables", "util.go",
  "\treturn i >= 0 && i < len(arr) && arr[i]", "\treturn i < len(arr) && arr[i]")
m(F, "negative message types pass validation", "validate.go",
  "\tif t := m.GetType(); int(t) < 0 || int(t) >= len(peerMessage) || !peerMessage[t] {",
  "\tif t := m.GetType(); int(t) >= len(peerMessage) || !peerMessage[t] {")
m(F, "nil entries from a peer accepted", "validate.go",
  "\t\tif e == nil {\n\t\t\treturn invalid(m, \"entry %d is nil\", i)", "\t\tif false {\n\t\t\treturn invalid(m, \"entry %d is nil\", i)")
m(F, "entry types outside the enum accepted", "validate.go",
  "\t\tif e.GetType() < pb.EntryNormal || e.GetType() > pb.EntryConfChangeV2 {", "\t\tif false {")
m(F, "entries with unknown fields accepted", "validate.go",
  "\t\tif len(e.ProtoReflect().GetUnknown()) > 0 {", "\t\tif false {")
m(F, "forwarded requests change the caller's message", "raft.go",
  "\tfwd := shallowCopy(m)\n\tfwd.To = new(r.lead)", "\tfwd := m\n\tfwd.To = new(r.lead)")
m(F, "shallow copy drops a field", "raft.go",
  "\t\tReject:     m.Reject,\n", "")
