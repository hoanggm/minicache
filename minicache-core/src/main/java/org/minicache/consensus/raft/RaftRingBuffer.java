package org.minicache.consensus.raft;

import java.util.ArrayList;
import java.util.List;

public class RaftRingBuffer {
    private LogEntry[] buffer;
    private int capacity;
    private int head;
    private int size;
    private int baseIndex;

    public RaftRingBuffer(int initialCapacity) {
        this.capacity = initialCapacity;
        this.buffer = new LogEntry[capacity];
        this.head = 0;
        this.size = 0;
        this.baseIndex = 0;
    }

    public int getBaseIndex() {
        return baseIndex;
    }

    public LogEntry get(int globalIndex) {
        if (globalIndex < baseIndex || globalIndex >= baseIndex + size) {
            throw new IndexOutOfBoundsException("Index: " + globalIndex + ", Size: " + size + ", Base: " + baseIndex);
        }
        int physicalIndex = (head + (globalIndex - baseIndex)) % capacity;
        return buffer[physicalIndex];
    }

    public void append(LogEntry entry) {
        ensureCapacity(size + 1);
        int physicalIndex = (head + size) % capacity;
        buffer[physicalIndex] = entry;
        size++;
    }

    public int size() {
        return size;
    }

    public int getLastIndex() {
        if (size == 0) return baseIndex - 1;
        return baseIndex + size - 1;
    }

    public void truncate(int fromGlobalIndex) {
        if (fromGlobalIndex < baseIndex) {
            clear();
            return;
        }
        int newSize = fromGlobalIndex - baseIndex;
        if (newSize < size) {
            for (int i = newSize; i < size; i++) {
                int physicalIndex = (head + i) % capacity;
                buffer[physicalIndex] = null;
            }
            size = newSize;
        }
    }

    public List<LogEntry> subList(int fromGlobalIndex) {
        List<LogEntry> result = new ArrayList<>();
        if (fromGlobalIndex >= baseIndex + size) {
            return result;
        }
        int start = Math.max(fromGlobalIndex, baseIndex);
        for (int i = start; i < baseIndex + size; i++) {
            result.add(get(i));
        }
        return result;
    }

    public List<LogEntry> subList(int fromGlobalIndex, int toGlobalIndex) {
        List<LogEntry> result = new ArrayList<>();
        int end = Math.min(toGlobalIndex, baseIndex + size);
        for (int i = Math.max(fromGlobalIndex, baseIndex); i < end; i++) {
            result.add(get(i));
        }
        return result;
    }

    public void clear() {
        for (int i = 0; i < capacity; i++) {
            buffer[i] = null;
        }
        size = 0;
        head = 0;
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity > capacity) {
            int newCapacity = capacity * 2;
            if (newCapacity < minCapacity) {
                newCapacity = minCapacity;
            }
            LogEntry[] newBuffer = new LogEntry[newCapacity];
            for (int i = 0; i < size; i++) {
                newBuffer[i] = buffer[(head + i) % capacity];
            }
            buffer = newBuffer;
            capacity = newCapacity;
            head = 0;
        }
    }

    public void compact(int upToGlobalIndex) {
        if (upToGlobalIndex <= baseIndex) return;
        int shift = upToGlobalIndex - baseIndex;
        if (shift >= size) {
            clear();
            baseIndex = upToGlobalIndex;
            return;
        }
        for (int i = 0; i < shift; i++) {
            int physicalIndex = (head + i) % capacity;
            buffer[physicalIndex] = null;
        }
        head = (head + shift) % capacity;
        size -= shift;
        baseIndex = upToGlobalIndex;
    }
}
