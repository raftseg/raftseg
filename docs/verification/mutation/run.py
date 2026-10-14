#!/usr/bin/env python3
# Runs a mutant catalogue against the test suites, one mutant at a time:
# apply it, run the tests, record whether they failed, restore the file.
#
#   python3 run.py seglog [i j ...]    # seglog_mutants.py against ./seglog and the seglog simulations
#   python3 run.py raft [i j ...]      # raft_mutants.py against upstream's tests, the fork's tests and the simulator
#
# raft lives in its own repository (github.com/raftseg/raft). Its checkout is
# found at $RAFT_DIR, or next to this repository, and must be in the go.work
# workspace that raftseg builds with, so that the simulator runs the mutant.
#
# Results are appended as JSON lines to results-<suite>.jsonl. Never edit or
# test the module while it runs: the mutated file is in place meanwhile.
import json, os, shutil, subprocess, sys, time

here = os.path.dirname(os.path.abspath(__file__))
root = os.path.abspath(os.path.join(here, '..', '..', '..'))
sys.path.insert(0, here)
raft_root = os.path.abspath(os.environ.get('RAFT_DIR', os.path.join(root, '..', 'raft')))
env = dict(os.environ, GOFLAGS='', GOPROXY='off', GOSUMDB='off', GOTOOLCHAIN='local')

# The fork's own tests of the raft package: everything not in upstream's suite.
FORK_RAFT_TESTS = '^(TestElectionRand|TestElectionRandDefault|TestApplyWithZeroSizeLimits|TestReadIndexAfterLeaderRemovedItself|TestMustSyncSnapshot|TestSnapshotWaitsForApplying|TestAnswersStaleLeader|TestDemotedLeaderHandsOff|TestSingleVoterReadAfterRestart|TestNoLeaseReadAfterTimeoutNow|TestLeaseNeedsRecentRound|TestLeaseStartGuard|TestStaleConfigurationCannotElect|TestRemovedLeaderRescues|TestVoteFromNodeThatLostItsTerm|TestForwardTransferWithTerm|TestValidateMessage|TestStepValidates|TestHeartbeatAckContextChecked|TestPreVoteRejectedWithoutTerm|TestCandidateLearnsHigherTerm|TestUndecodableConfChangeDoesNotPanicLeader|TestInvalidLocalConfChangeDropped|FuzzStepHostile|TestHostilePanicClasses|TestGoldenCompatibility|TestGoldenText|TestGoldenJSONRoundTrip|TestMalformedMatchesOriginal|TestNestingLimit|TestNestingCannotExhaustStack|TestHugeLengthsDoNotAllocate|TestDecodeAmplification|FuzzUnmarshal|FuzzUnmarshalInto|FuzzConfChangesFromString)$'


def go(args, extra=None, timeout=660, cwd=None):
    r = subprocess.run(['timeout', str(timeout), 'go'] + args, cwd=cwd or root, env=dict(env, **(extra or {})),
                       capture_output=True, text=True)
    out = r.stdout + r.stderr
    why = [l.strip() for l in out.splitlines()
           if l.startswith('--- FAIL') or 'panic:' in l or 'raftsim:' in l or 'build failed' in l][:3]
    return r.returncode != 0, ' / '.join(why)[:200]


def main():
    suite = sys.argv[1]
    if suite == 'seglog':
        from seglog_mutants import M
        base = os.path.join(root, 'seglog')
    else:
        from raft_mutants import M
        base = raft_root
    idx = [int(x) for x in sys.argv[2:]] or range(len(M))
    out = open(os.path.join(here, 'results-%s.jsonl' % suite), 'a')
    for i in idx:
        m = M[i]
        f = os.path.join(base, m['file'])
        orig = open(f).read()
        if orig.count(m['old']) != 1:
            print(i, 'NOMATCH', m['label'], flush=True)
            continue
        open(f, 'w').write(orig.replace(m['old'], m['new'], 1))
        t0 = time.time()
        try:
            if suite == 'seglog':
                killed, why = go(['test', '-short', '-count=1', '-failfast', '-timeout', '600s', './seglog'])
                if not killed:
                    killed, why = go(['test', '-short', '-count=1', '-failfast', '-timeout', '600s',
                                      '-run', 'Seglog|Mixed', './internal/raftsim'])
                rec = dict(i=i, label=m['label'], killed=killed, why=why)
            else:
                up, why_up = go(['test', './...', '-count=1', '-timeout', '120s', '-skip', FORK_RAFT_TESTS], cwd=raft_root)
                ft, why_ft = go(['test', './...', '-count=1', '-timeout', '120s', '-run', FORK_RAFT_TESTS], cwd=raft_root)
                sim, why_sim = go(['test', './internal/raftsim', '-count=1', '-timeout', '300s',
                                   '-run', 'TestSimulation$|TestDeterminism'], {'RAFTSIM_SEEDS': '150'})
                rec = dict(i=i, kind=m['kind'], label=m['label'], upstream=up, fork_tests=ft, sim=sim,
                           why=why_up or why_ft or why_sim)
        finally:
            open(f, 'w').write(orig)
        rec['secs'] = round(time.time() - t0)
        print(json.dumps(rec), flush=True)
        out.write(json.dumps(rec) + '\n')
        out.flush()


if __name__ == '__main__':
    main()
