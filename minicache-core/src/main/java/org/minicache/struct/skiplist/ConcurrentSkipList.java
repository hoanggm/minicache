package org.minicache.struct.skiplist;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentSkipList<K extends Comparable<K>, V> {

    // =========================================================================
    // ZKey (Khóa phức hợp dùng để so sánh Score -> Key)
    // =========================================================================
    private static final class ZKey<K extends Comparable<K>> implements Comparable<ZKey<K>> {
        private final double score;
        private final K key;

        ZKey(double score, K key) {
            if (key == null) {
                throw new NullPointerException("Key cannot be null");
            }
            this.score = score;
            this.key = key;
        }

        public double getScore() {
            return score;
        }

        public K getKey() {
            return key;
        }

        @Override
        public int compareTo(ZKey<K> o) {
            int scoreCompare = Double.compare(this.score, o.score);
            if (scoreCompare != 0) return scoreCompare;
            return this.key.compareTo(o.key);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ZKey<?> zKey = (ZKey<?>) o;
            return Double.compare(zKey.score, score) == 0 && key.equals(zKey.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(score, key);
        }
    }

    // =========================================================================
    // ForwardLink (Mối liên kết chứa con trỏ tiến và khoảng cách span)
    // =========================================================================
    private static final class ForwardLink<K extends Comparable<K>, V> {
        volatile Node<K, V> next;
        final AtomicInteger span;

        ForwardLink(Node<K, V> next, int span) {
            this.next = next;
            this.span = new AtomicInteger(span);
        }
    }

    // =========================================================================
    // Node (Mỗi mắt xích trên SkipList)
    // =========================================================================
    static final class Node<K extends Comparable<K>, V> {
        final ZKey<K> zKey;
        final V value;
        final int level;
        final ForwardLink<K, V>[] forward;

        @SuppressWarnings("unchecked")
        Node(ZKey<K> zKey, V value, int level) {
            this.zKey = zKey;
            this.value = value;
            this.level = level;
            this.forward = new ForwardLink[level + 1];
            for (int i = 0; i <= level; i++) {
                this.forward[i] = new ForwardLink<>(null, 0);
            }
        }
    }

    private static final int MAX_LEVEL = 16;
    private final Node<K, V> head;
    private final Node<K, V> tail;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private int length = 0;

    public ConcurrentSkipList() {
        head = new Node<>(null, null, MAX_LEVEL);
        tail = new Node<>(null, null, MAX_LEVEL);
        for (int i = 0; i <= MAX_LEVEL; i++) {
            head.forward[i].next = tail;
            head.forward[i].span.set(1);
        }
    }

    private int randomLevel() {
        int lvl = 0;
        while (lvl < MAX_LEVEL && ThreadLocalRandom.current().nextBoolean()) {
            lvl++;
        }
        return lvl;
    }

    @SuppressWarnings("unchecked")
    public boolean put(double score, K key, V value) {
        lock.writeLock().lock();
        try {
            ZKey<K> zKey = new ZKey<>(score, key);
            Node<K, V>[] update = new Node[MAX_LEVEL + 1];
            int[] rank = new int[MAX_LEVEL + 1];

            Node<K, V> curr = head;
            for (int i = MAX_LEVEL; i >= 0; i--) {
                rank[i] = (i == MAX_LEVEL) ? 0 : rank[i + 1];
                while (curr.forward[i].next != tail && curr.forward[i].next.zKey.compareTo(zKey) < 0) {
                    rank[i] += curr.forward[i].span.get();
                    curr = curr.forward[i].next;
                }
                update[i] = curr;
            }

            curr = curr.forward[0].next;
            if (curr != tail && curr.zKey.compareTo(zKey) == 0) {
                return false;
            }

            int topLevel = randomLevel();
            Node<K, V> newNode = new Node<>(zKey, value, topLevel);

            for (int i = 0; i <= MAX_LEVEL; i++) {
                if (i <= topLevel) {
                    newNode.forward[i].next = update[i].forward[i].next;
                    update[i].forward[i].next = newNode;

                    newNode.forward[i].span.set(update[i].forward[i].span.get() - (rank[0] - rank[i]));
                    update[i].forward[i].span.set((rank[0] - rank[i]) + 1);
                } else {
                    update[i].forward[i].span.incrementAndGet();
                }
            }

            length++;
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public V getByPosition(int index) {
        if (index < 0 || index >= size()) return null;

        lock.readLock().lock();
        try {
            Node<K, V> curr = head;
            int traversed = 0;

            for (int i = MAX_LEVEL; i >= 0; i--) {
                while (curr.forward[i].next != tail && (traversed + curr.forward[i].span.get()) <= (index + 1)) {
                    traversed += curr.forward[i].span.get();
                    curr = curr.forward[i].next;
                }
                if (traversed == (index + 1)) {
                    return curr.value;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return length;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Lấy ra danh sách các Value có Score nằm trong khoảng [minScore, maxScore]
     * Tốc độ xử lý: O(log N + M) - Hoàn toàn không bị chặn bởi luồng khác
     */
    public List<V> getRangeByScore(double minScore, double maxScore) {
        List<V> result = new ArrayList<>();

        lock.readLock().lock();
        try {
            Node<K, V> curr = head;

            // 1. Đi từ tầng cao nhất xuống để định vị nút tiệm cận sát lề trái của khoảng minScore
            for (int i = MAX_LEVEL; i >= 0; i--) {
                while (curr.forward[i].next != tail && curr.forward[i].next.zKey.getScore() < minScore) {
                    curr = curr.forward[i].next;
                }
            }

            // 2. Đã xuống tầng đáy (Level 0), dịch sang nút đầu tiên bên phải để bắt đầu quét tuyến tính
            curr = curr.forward[0].next;

            // 3. Quét dọc sang phải cho đến khi vượt quá maxScore hoặc chạm điểm cuối danh sách (tail)
            while (curr != tail) {
                // Nếu score của nút hiện tại vượt quá maxScore, dừng quét ngay lập tức (Tối ưu điểm dừng)
                if (curr.zKey.getScore() > maxScore) {
                    break;
                }

                // Nếu nằm trọn trong khoảng [minScore, maxScore], thu thập Value
                if (curr.zKey.getScore() >= minScore) {
                    result.add(curr.value);
                }

                // Dịch chuyển sang mắt xích tiếp theo ở tầng đáy
                curr = curr.forward[0].next;
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Lấy danh sách phần tử trong khoảng vị trí [start, stop] (0-based Index)
     * Tốc độ xử lý: O(log N + M) dựa trên việc cộng dồn bước sải (span)
     */
    public List<V> getRangeByPositions(int start, int stop) {
        List<V> result = new ArrayList<>();

        lock.readLock().lock();
        try {
            // Kiểm tra biên an toàn
            if (start < 0 || start > stop || start >= length) {
                return result;
            }
            if (stop >= length) {
                stop = length - 1;
            }

            Node<K, V> curr = head;
            int traversed = 0; // Biến tích lũy số bước nhảy qua dưới tầng đáy

            // 1. Định vị nhanh đến nút đứng ngay TRƯỚC vị trí 'start' bằng Span hành trình
            for (int i = MAX_LEVEL; i >= 0; i--) {
                while (curr.forward[i].next != tail && (traversed + curr.forward[i].span.get()) <= start) {
                    traversed += curr.forward[i].span.get();
                    curr = curr.forward[i].next;
                }
            }

            // 2. Dịch sang nút đầu tiên thuộc khoảng cần lấy (Vị trí chính xác là 'start')
            curr = curr.forward[0].next;

            // 3. Đi bộ dưới tầng đáy (Level 0) để thu thập đủ từ start -> stop
            int countToFetch = (stop - start) + 1;
            while (curr != tail && countToFetch > 0) {
                result.add(curr.value);
                curr = curr.forward[0].next; // Dịch sang phải tuần tự
                countToFetch--;
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Xóa một phần tử ra khỏi SkipList dựa trên cặp (Score, Key) cụ thể.
     * Tốc độ xử lý: O(log N) dựa trên việc cập nhật lại hệ số Span của các nút xung quanh.
     */
    @SuppressWarnings("unchecked")
    public V remove(double score, K key) {
        lock.writeLock().lock();
        try {
            ZKey<K> zKey = new ZKey<>(score, key);
            @SuppressWarnings("unchecked")
            Node<K, V>[] update = new Node[MAX_LEVEL + 1];

            // 1. Tìm kiếm và thu thập các nút cha đứng trước nút cần xóa ở tất cả các tầng
            Node<K, V> curr = head;
            for (int i = MAX_LEVEL; i >= 0; i--) {
                while (curr.forward[i].next != tail && curr.forward[i].next.zKey.compareTo(zKey) < 0) {
                    curr = curr.forward[i].next;
                }
                update[i] = curr;
            }

            // Dịch sang phải 1 bước ở tầng đáy để kiểm tra xem có đúng nút cần xóa không
            curr = curr.forward[0].next;
            if (curr == tail || curr.zKey.compareTo(zKey) != 0) {
                // Không tìm thấy phần tử để xóa
                return null;
            }

            // 2. Tiến hành ngắt liên kết và tái cấu trúc lại hệ số SPAN cho các tầng chỉ mục
            for (int i = 0; i <= MAX_LEVEL; i++) {
                if (update[i].forward[i].next == curr) {
                    int prevSpan = update[i].forward[i].span.get();
                    int currSpan = curr.forward[i].span.get();
                    update[i].forward[i].span.set(prevSpan + currSpan - 1);

                    // Bắc cầu liên kết qua nút bị xóa
                    update[i].forward[i].next = curr.forward[i].next;
                } else {
                    // Nút cần xóa không nằm trên tầng này -> Chỉ cần giảm bước sải đi 1
                    update[i].forward[i].span.decrementAndGet();
                }
            }

            length--;
            return curr.value;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Tính toán vị trí xếp hạng (Rank/Position) của một phần tử dựa trên cặp (Score, Key).
     * Tốc độ xử lý: O(log N) - Trả về vị trí 0-based Index (0 nếu đứng đầu, 1 đứng nhì...)
     */
    public int getRank(double score, K key) {
        lock.readLock().lock();
        try {
            ZKey<K> zKey = new ZKey<>(score, key);
            Node<K, V> curr = head;
            int rank = 0;

            // Đi từ tầng chỉ mục cao xuống thấp, liên tục cộng dồn các bước sải span vượt qua
            for (int i = MAX_LEVEL; i >= 0; i--) {
                while (curr.forward[i].next != tail && curr.forward[i].next.zKey.compareTo(zKey) <= 0) {
                    rank += curr.forward[i].span.get(); // Cộng dồn khoảng cách tích lũy
                    curr = curr.forward[i].next;
                }

                // Nếu đích đến khớp hoàn toàn với phần tử mục tiêu
                if (curr != head && curr.zKey.compareTo(zKey) == 0) {
                    return rank - 1; // Trả về 0-based index (-1 vì bỏ qua nút head ban đầu)
                }
            }
            return -1; // Không tìm thấy
        } finally {
            lock.readLock().unlock();
        }
    }
}