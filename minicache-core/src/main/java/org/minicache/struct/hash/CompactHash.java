package org.minicache.struct.hash;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public final class CompactHash {
    public static final int PROMOTION_THRESHOLD = 128;
    private static final int INITIAL_CAPACITY = 8;

    private Object[] array;
    private Map<String, String> map;
    private int size;
    private long estimatedBytes;

    public CompactHash() {
        this.array = new Object[INITIAL_CAPACITY];
        this.map = null;
        this.size = 0;
        // Chi phí RAM khởi tạo ban đầu: CompactHash Object Header (16B) + Object[] reference array (24B + 8*4B = 56B) = ~72 bytes
        this.estimatedBytes = 72L;
    }

    public record RemoveResult(String oldValue, long freedBytes) {}
    public record PutResult(String oldValue, long memoryDelta) {}

    public long getEstimatedBytes() {
        return estimatedBytes;
    }

    /**
     * Thêm hoặc cập nhật một field-value pair, trả về PutResult chứa giá trị cũ và memory delta
     */
    public PutResult put(String field, String value) {
        Objects.requireNonNull(field, "Field cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        // Mode 1: Đã nâng cấp lên HashMap
        if (map != null) {
            String oldVal = map.put(field, value);
            if (oldVal == null) {
                size++;
                long addedBytes = MemoryEstimator.estimateHashMapNode(field, value);
                this.estimatedBytes += addedBytes;
                return new PutResult(null, addedBytes);
            } else {
                long delta = MemoryEstimator.estimateString(value) - MemoryEstimator.estimateString(oldVal);
                this.estimatedBytes += delta;
                return new PutResult(oldVal, delta);
            }
        }

        // Mode 2: Đang ở dạng Flat Array - Tìm field đã tồn tại
        for (int i = 0; i < size * 2; i += 2) {
            if (field.equals(array[i])) {
                String oldVal = (String) array[i + 1];
                array[i + 1] = value; // Cập nhật value mới
                long delta = MemoryEstimator.estimateString(value) - MemoryEstimator.estimateString(oldVal);
                this.estimatedBytes += delta;
                return new PutResult(oldVal, delta);
            }
        }

        // Nếu field chưa tồn tại -> Kiểm tra ngưỡng để nâng cấp
        if (size >= PROMOTION_THRESHOLD) {
            long bytesBeforePromote = this.estimatedBytes;
            promoteToHashMap();
            map.put(field, value);
            size++;

            // Tính toán dung lượng thực tế sau khi chuyển đổi sang HashMap
            recalculateEstimatedBytes();
            long delta = this.estimatedBytes - bytesBeforePromote;
            return new PutResult(null, delta);
        }

        // Đảm bảo dung lượng mảng đủ chứa thêm phần tử mới
        ensureArrayCapacity(size + 1);

        // Ghi phần tử mới vào cuối mảng
        int idx = size * 2;
        array[idx] = field;
        array[idx + 1] = value;
        size++;

        // Kích thước của (field + value) + 8 bytes reference trong Object[]
        long addedBytes = MemoryEstimator.estimateString(field)
                + MemoryEstimator.estimateString(value)
                + 8L;
        this.estimatedBytes += addedBytes;

        return new PutResult(null, addedBytes);
    }

    /**
     * Lấy giá trị theo field
     */
    public String get(String field) {
        if (field == null) return null;

        if (map != null) {
            return map.get(field);
        }

        // Quét tuyến tính trên mảng phẳng (Rất nhanh do CPU Cache locality)
        for (int i = 0; i < size * 2; i += 2) {
            if (field.equals(array[i])) {
                return (String) array[i + 1];
            }
        }

        return null;
    }

    /**
     * Xóa field khỏi Hash
     */
    public RemoveResult remove(String field) {
        if (field == null) return new RemoveResult(null, 0L);

        // --- MODE 1: HASHMAP MODE ---
        if (map != null) {
            String oldVal = map.remove(field);
            if (oldVal != null) {
                size--;
                // Tính dung lượng RAM giải phóng: Node Overhead + Size(field + value) + slot reference
                long freed = MemoryEstimator.estimateHashMapNode(field, oldVal);
                this.estimatedBytes -= freed;
                return new RemoveResult(oldVal, freed);
            }
            return new RemoveResult(null, 0L);
        }

        // --- MODE 2: ARRAY MODE ---
        for (int i = 0; i < size * 2; i += 2) {
            if (field.equals(array[i])) {
                String oldVal = (String) array[i + 1];

                // Dồn mảng bằng System.arraycopy
                int moveCount = (size * 2) - (i + 2);
                if (moveCount > 0) {
                    System.arraycopy(array, i + 2, array, i, moveCount);
                }
                size--;

                // Xóa tham chiếu ở vị trí cuối để tránh Memory Leak / GC Hold
                array[size * 2] = null;
                array[size * 2 + 1] = null;

                // Dung lượng giải phóng: Kích thước của (field + value) + 8 bytes reference trong Object[]
                long freed = MemoryEstimator.estimateString(field)
                        + MemoryEstimator.estimateString(oldVal)
                        + 8L;

                this.estimatedBytes -= freed;
                return new RemoveResult(oldVal, freed);
            }
        }

        return new RemoveResult(null, 0L);
    }

    public boolean containsKey(String field) {
        return get(field) != null;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean isPromoted() {
        return map != null;
    }

    /**
     * Duyệt qua toàn bộ phần tử mà không tạo các Object trung gian (Zero-allocation iteration)
     */
    public void forEach(BiConsumer<String, String> action) {
        Objects.requireNonNull(action);
        if (map != null) {
            map.forEach(action);
        } else {
            for (int i = 0; i < size * 2; i += 2) {
                action.accept((String) array[i], (String) array[i + 1]);
            }
        }
    }

    /**
     * Export dữ liệu ra java.util.Map
     */
    public Map<String, String> toMap() {
        if (map != null) {
            return Collections.unmodifiableMap(map);
        }
        Map<String, String> resultMap = HashMap.newHashMap(size);
        for (int i = 0; i < size * 2; i += 2) {
            resultMap.put((String) array[i], (String) array[i + 1]);
        }
        return resultMap;
    }

    /**
     * Chuyển đổi mã hóa từ Array -> HashMap
     */
    private void promoteToHashMap() {
        // Tận dụng tính năng Java HashMap.newHashMap chỉ định chính xác số lượng mapping
        this.map = HashMap.newHashMap(size + 1);
        for (int i = 0; i < size * 2; i += 2) {
            map.put((String) array[i], (String) array[i + 1]);
        }
        this.array = null; // Hủy mảng cũ để GC thu hồi bộ nhớ
    }

    /**
     * Mở rộng mảng phẳng theo cấp số nhân (* 2)
     */
    private void ensureArrayCapacity(int targetEntries) {
        int targetCapacity = targetEntries * 2;
        if (targetCapacity > array.length) {
            int oldCapacityBytes = array.length * 4; // 4 bytes/ref trong Compressed OOPs
            int newCapacity = Math.min(array.length * 2, PROMOTION_THRESHOLD * 2);
            Object[] newArray = new Object[newCapacity];
            System.arraycopy(array, 0, newArray, 0, size * 2);
            this.array = newArray;
            int newCapacityBytes = newArray.length * 4;

            // Cập nhật delta dung lượng mảng mở rộng
            this.estimatedBytes += (newCapacityBytes - oldCapacityBytes);
        }
    }

    /**
     * Tính toán lại toàn bộ footprint RAM khi chuyển qua HashMap
     */
    private void recalculateEstimatedBytes() {
        if (map == null) return;
        long total = 40L; // CompactHash header & ref
        total += 48L; // HashMap object overhead & internal table
        for (Map.Entry<String, String> entry : map.entrySet()) {
            total += MemoryEstimator.estimateHashMapNode(entry.getKey(), entry.getValue());
        }
        this.estimatedBytes = total;
    }

    private static final class MemoryEstimator {
        private static final long STRING_OVERHEAD = 40L;
        private static final long HASHMAP_NODE_OVERHEAD = 32L;

        public static long estimateString(String s) {
            if (s == null) return 0L;
            return STRING_OVERHEAD + s.getBytes(StandardCharsets.UTF_8).length;
        }

        public static long estimateHashMapNode(String key, String value) {
            if (key == null && value == null) return 0L;
            long keySize = estimateString(key);
            long valueSize = estimateString(value);
            return HASHMAP_NODE_OVERHEAD + keySize + valueSize + 4L;
        }
    }
}