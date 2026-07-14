package org.minicache.struct.freqsketch;

import java.util.concurrent.atomic.AtomicLongArray;

public class FrequencySketch {
    private final AtomicLongArray table;
    private final int mask;

    public FrequencySketch(int expectedElements) {
        int tableSize = 1;
        while (tableSize < expectedElements) {
            tableSize <<= 1;
        }
        this.table = new AtomicLongArray(tableSize);
        this.mask = tableSize - 1;
    }

    /**
     * Tăng bộ đếm tần suất của một Key (Lock-free / Thread-safe)
     */
    public void increment(String key) {
        if (key == null) return;
        int hash = key.hashCode();

        // Tạo ra 4 hash khác nhau từ 1 hash gốc (Double Hashing / Spread)
        int h1 = hash & mask;
        int h2 = (hash >>> 16) & mask;

        // Tăng giá trị nguyên tử tại các ô nhớ (Sử dụng CAS nhẹ)
        table.incrementAndGet(h1);
        table.incrementAndGet(h2);
    }

    /**
     * Ước lượng tần suất xuất hiện của một Key trong thời gian qua
     */
    public long frequency(String key) {
        if (key == null) return 0;
        int hash = key.hashCode();

        int h1 = hash & mask;
        int h2 = (hash >>> 16) & mask;

        // Tần suất thực tế là giá trị nhỏ nhất trong các ô băm (để tránh sai số do đụng độ băm)
        return Math.min(table.get(h1), table.get(h2));
    }

    /**
     * Cơ chế Reset định kỳ của TinyLFU (Halving)
     * Giảm một nửa tất cả các bộ đếm để Cache không bị "bảo thủ" với các dữ liệu hot trong quá khứ
     */
    public void age() {
        for (int i = 0; i < table.length(); i++) {
            long value = table.get(i);
            // Chia đôi bằng dịch bit
            table.set(i, value >>> 1);
        }
    }

    public void reset() {
        for (int i = 0; i < table.length(); i++) {
            table.set(i, 0L);
        }
    }
}
