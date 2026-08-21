package org.minicache.common;

public record TTLEntry(String key, long expireTime) implements Comparable<TTLEntry> {
    @Override
    public int compareTo(TTLEntry o) {
        return Long.compare(this.expireTime, o.expireTime);
    }
}
