package org.minicache.struct.skiplist;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class VanillaSkipList<K extends Comparable<K>, V> {

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
    // ForwardLink (Mối liên kết chứa con trỏ tiến và khoảng cách span - LOCK-FREE THÔ)
    // =========================================================================
    private static final class ForwardLink<K extends Comparable<K>, V> {
        Node<K, V> next;
        int span;

        ForwardLink(Node<K, V> next, int span) {
            this.next = next;
            this.span = span;
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
    private int length = 0;

    public VanillaSkipList() {
        head = new Node<>(null, null, MAX_LEVEL);
        tail = new Node<>(null, null, MAX_LEVEL);
        for (int i = 0; i <= MAX_LEVEL; i++) {
            head.forward[i].next = tail;
            head.forward[i].span = 1;
        }
    }

    private int randomLevel() {
        int lvl = 0;
        while (lvl < MAX_LEVEL && ThreadLocalRandom.current().nextBoolean()) {
            lvl++;
        }
        return lvl;
    }

    /**
     * Thêm phần tử mới hoặc cập nhật giá trị (LOCK-FREE, TUẦN TỰ)
     */
    @SuppressWarnings("unchecked")
    public boolean put(double score, K key, V value) {
        ZKey<K> zKey = new ZKey<>(score, key);
        Node<K, V>[] update = new Node[MAX_LEVEL + 1];
        int[] rank = new int[MAX_LEVEL + 1];

        Node<K, V> curr = head;
        for (int i = MAX_LEVEL; i >= 0; i--) {
            rank[i] = (i == MAX_LEVEL) ? 0 : rank[i + 1];
            while (curr.forward[i].next != tail && curr.forward[i].next.zKey.compareTo(zKey) < 0) {
                rank[i] += curr.forward[i].span;
                curr = curr.forward[i].next;
            }
            update[i] = curr;
        }

        curr = curr.forward[0].next;
        if (curr != tail && curr.zKey.compareTo(zKey) == 0) {
            return false; // Đã tồn tại, bỏ qua (hoặc bạn có thể thêm logic cập nhật value tại đây)
        }

        int topLevel = randomLevel();
        Node<K, V> newNode = new Node<>(zKey, value, topLevel);

        // Chèn vào các tầng chỉ mục và tính toán lại SPAN bằng các toán tử gán thô nguyên bản
        for (int i = 0; i <= MAX_LEVEL; i++) {
            if (i <= topLevel) {
                newNode.forward[i].next = update[i].forward[i].next;
                update[i].forward[i].next = newNode;

                newNode.forward[i].span = update[i].forward[i].span - (rank[0] - rank[i]);
                update[i].forward[i].span = (rank[0] - rank[i]) + 1;
            } else {
                update[i].forward[i].span++;
            }
        }

        length++;
        return true;
    }

    /**
     * Lấy giá trị tại index logic (0-based) dựa trên các bước sải span
     */
    public V getByPosition(int index) {
        if (index < 0 || index >= size()) return null;

        Node<K, V> curr = head;
        int traversed = 0;

        for (int i = MAX_LEVEL; i >= 0; i--) {
            while (curr.forward[i].next != tail && (traversed + curr.forward[i].span) <= (index + 1)) {
                traversed += curr.forward[i].span;
                curr = curr.forward[i].next;
            }
            if (traversed == (index + 1)) {
                return curr.value;
            }
        }
        return null;
    }

    public int size() {
        return length;
    }

    /**
     * Quét lấy các Value nằm trong dải điểm [minScore, maxScore] tuần tự
     */
    public List<V> getRangeByScore(double minScore, double maxScore) {
        List<V> result = new ArrayList<>();
        Node<K, V> curr = head;

        // Định vị nhanh điểm xuất phát sát minScore
        for (int i = MAX_LEVEL; i >= 0; i--) {
            while (curr.forward[i].next != tail && curr.forward[i].next.zKey.getScore() < minScore) {
                curr = curr.forward[i].next;
            }
        }

        // Quét tuyến tính dưới tầng đáy (tầng 0)
        curr = curr.forward[0].next;
        while (curr != tail) {
            if (curr.zKey.getScore() > maxScore) {
                break;
            }
            if (curr.zKey.getScore() >= minScore) {
                result.add(curr.value);
            }
            curr = curr.forward[0].next;
        }

        return result;
    }

    /**
     * Lấy danh sách phần tử trong khoảng index [start, stop]
     */
    public List<V> getRangeByPositions(int start, int stop) {
        List<V> result = new ArrayList<>();

        if (start < 0 || start > stop || start >= length) {
            return result;
        }
        if (stop >= length) {
            stop = length - 1;
        }

        Node<K, V> curr = head;
        int traversed = 0;

        // Nhảy xa dựa trên span để định vị phần tử sát start
        for (int i = MAX_LEVEL; i >= 0; i--) {
            while (curr.forward[i].next != tail && (traversed + curr.forward[i].span) <= start) {
                traversed += curr.forward[i].span;
                curr = curr.forward[i].next;
            }
        }

        curr = curr.forward[0].next;
        int countToFetch = (stop - start) + 1;
        while (curr != tail && countToFetch > 0) {
            result.add(curr.value);
            curr = curr.forward[0].next;
            countToFetch--;
        }

        return result;
    }

    /**
     * Xóa phần tử bằng cách bắc cầu con trỏ và cập nhật lại span thô tuần tự
     */
    @SuppressWarnings("unchecked")
    public V remove(double score, K key) {
        ZKey<K> zKey = new ZKey<>(score, key);
        Node<K, V>[] update = new Node[MAX_LEVEL + 1];

        Node<K, V> curr = head;
        for (int i = MAX_LEVEL; i >= 0; i--) {
            while (curr.forward[i].next != tail && curr.forward[i].next.zKey.compareTo(zKey) < 0) {
                curr = curr.forward[i].next;
            }
            update[i] = curr;
        }

        curr = curr.forward[0].next;
        if (curr == tail || curr.zKey.compareTo(zKey) != 0) {
            return null;
        }

        // Tái cấu trúc liên kết và span trực tiếp không dùng CAS
        for (int i = 0; i <= MAX_LEVEL; i++) {
            if (update[i].forward[i].next == curr) {
                int prevSpan = update[i].forward[i].span;
                int currSpan = curr.forward[i].span;
                update[i].forward[i].span = prevSpan + currSpan - 1;

                update[i].forward[i].next = curr.forward[i].next;
            } else {
                update[i].forward[i].span--;
            }
        }

        length--;
        return curr.value;
    }

    /**
     * Lấy vị trí xếp hạng 0-based Index dựa trên hệ cộng dồn span
     */
    public int getRank(double score, K key) {
        ZKey<K> zKey = new ZKey<>(score, key);
        Node<K, V> curr = head;
        int rank = 0;

        for (int i = MAX_LEVEL; i >= 0; i--) {
            while (curr.forward[i].next != tail && curr.forward[i].next.zKey.compareTo(zKey) <= 0) {
                rank += curr.forward[i].span;
                curr = curr.forward[i].next;
            }

            if (curr != head && curr.zKey.compareTo(zKey) == 0) {
                return rank - 1;
            }
        }
        return -1;
    }
}