package org.minicache.consensus.raft;

public interface RaftListener {
    void onBecomeLeader();
    void onBecomeFollower();
    void onBecomeCandidate();
    void onLogCommitted(String command);
}
