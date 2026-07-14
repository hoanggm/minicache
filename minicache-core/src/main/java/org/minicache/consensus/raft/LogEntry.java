package org.minicache.consensus.raft;

import java.io.Serializable;

public record LogEntry(int index, int term, String command) implements Serializable {

    @Override
    public String toString() {
        return index + "," + term + "," + command;
    }
}