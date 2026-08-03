package org.minicache.struct.geohash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

public class GeoSkipList {
    private static final int MAX_LEVEL = 16;
    private static final double PROBABILITY = 0.5;

    public record GeoPoint(String member, double lat, double lon, long geohash) {
        public double distanceToInMeters(double targetLat, double targetLon) {
            double earthRadius = 6371000.0;
            double dLat = Math.toRadians(targetLat - this.lat);
            double dLon = Math.toRadians(targetLon - this.lon);
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                    Math.cos(Math.toRadians(this.lat)) * Math.cos(Math.toRadians(targetLat)) *
                            Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return earthRadius * c;
        }
    }

    public record GeoResult(String member, double distanceMeters, double lat, double lon) {}

    public static class Node {
        final long key;
        final Set<GeoPoint> points;
        final Node[] forward;

        public Node(long key, int level) {
            this.key = key;
            this.points = ConcurrentHashMap.newKeySet();
            this.forward = new Node[level];
        }
    }

    private final Node head;
    private int level;
    private final ReentrantLock writeLock = new ReentrantLock();

    public GeoSkipList() {
        this.head = new Node(Long.MIN_VALUE, MAX_LEVEL);
        this.level = 1;
    }

    private int randomLevel() {
        int lvl = 1;
        while (ThreadLocalRandom.current().nextDouble() < PROBABILITY && lvl < MAX_LEVEL) {
            lvl++;
        }
        return lvl;
    }

    public void put(long geohash, GeoPoint point) {
        Node[] update = new Node[MAX_LEVEL];
        writeLock.lock();
        try {
            Node current = head;
            for (int i = level - 1; i >= 0; i--) {
                while (current.forward[i] != null && current.forward[i].key < geohash) {
                    current = current.forward[i];
                }
                update[i] = current;
            }
            current = current.forward[0];

            // Nếu Key (Geohash) đã tồn tại -> Chỉ cần nhét GeoPoint vào Set của Node đó
            if (current != null && current.key == geohash) {
                current.points.removeIf(p -> p.member().equals(point.member()));
                current.points.add(point);
                return;
            }

            // Tạo Node mới trong SkipList
            int newLevel = randomLevel();
            if (newLevel > level) {
                for (int i = level; i < newLevel; i++) {
                    update[i] = head;
                }
                level = newLevel;
            }

            Node newNode = new Node(geohash, newLevel);
            newNode.points.add(point);

            for (int i = 0; i < newLevel; i++) {
                newNode.forward[i] = update[i].forward[i];
                update[i].forward[i] = newNode;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void removeMember(long geohash, String member) {
        writeLock.lock();
        try {
            Node current = head;
            for (int i = level - 1; i >= 0; i--) {
                while (current.forward[i] != null && current.forward[i].key < geohash) {
                    current = current.forward[i];
                }
            }
            current = current.forward[0];

            if (current != null && current.key == geohash) {
                current.points.removeIf(p -> p.member().equals(member));
            }
        } finally {
            writeLock.unlock();
        }
    }

    public List<GeoPoint> rangeScan(long minKey, long maxKey) {
        List<GeoPoint> result = new ArrayList<>();
        Node current = head;

        for (int i = level - 1; i >= 0; i--) {
            while (current.forward[i] != null && current.forward[i].key < minKey) {
                current = current.forward[i];
            }
        }
        current = current.forward[0];

        while (current != null && current.key <= maxKey) {
            result.addAll(current.points);
            current = current.forward[0];
        }

        return result;
    }

    public void clear() {
        writeLock.lock();
        try {
            for (int i = 0; i < MAX_LEVEL; i++) {
                this.head.forward[i] = null;
            }
            this.head.points.clear();
            this.level = 1;
        } finally {
            writeLock.unlock();
        }
    }

    public long clearAndCountFreedBytes() {
        writeLock.lock();
        try {
            long totalBytes = 0;
            Node current = head.forward[0];

            while (current != null) {
                for (GeoPoint p : current.points) {
                    long memberBytes = p.member().getBytes(StandardCharsets.UTF_8).length;
                    totalBytes += (48 + memberBytes + 16 + (16 * 8));
                }
                current = current.forward[0];
            }

            for (int i = 0; i < MAX_LEVEL; i++) {
                this.head.forward[i] = null;
            }
            this.head.points.clear();
            this.level = 1;

            return totalBytes;
        } finally {
            writeLock.unlock();
        }
    }
}
