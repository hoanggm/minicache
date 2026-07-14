package org.minicache.struct.bloomfilter;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongArray;

public class BloomFilter<T> {
    private final AtomicLongArray bits;
    private final long numBits;
    private final int numHashFunctions;
    private final long size;

    /**
     * Khởi tạo Bloom Filter an toàn đa luồng
     *
     * @param expectedElements  Số lượng phần tử dự kiến (n)
     * @param falsePositiveRate Tỷ lệ dương tính giả mong muốn (p) (Ví dụ: 0.01 cho 1%)
     */
    public BloomFilter(int expectedElements, double falsePositiveRate) {
        if (expectedElements <= 0 || falsePositiveRate <= 0 || falsePositiveRate >= 1) {
            throw new IllegalArgumentException("Illegal Arguments");
        }

        // 1. Tính số lượng bit tối ưu
        double optimalBit = (-expectedElements * Math.log(falsePositiveRate)) / Math.pow(Math.log(2), 2);
        this.numBits = (long) Math.ceil(optimalBit);

        // 2. Tính số lượng hàm băm tối ưu
        this.numHashFunctions = (int) Math.ceil((double) numBits / expectedElements * Math.log(2));

        // 3. Khởi tạo mảng AtomicLongArray (Mỗi phần tử kiểu long chứa 64 bit)
        int arrayLength = (int) Math.ceil((double) numBits / 64);
        this.bits = new AtomicLongArray(arrayLength);

        // 4. Tính toán kích thước
        this.size = calculateExpectedMemoryBytes(expectedElements, falsePositiveRate);
    }

    /**
     * Thêm một phần tử vào filter (An toàn đa luồng - Lock-free)
     */
    public void add(T element) {
        if (element == null) return;

        long hash1 = element.hashCode();
        long hash2 = Objects.hash(element, "salt_thread_safe");
        long combinedHash = hash1;

        for (int i = 0; i < numHashFunctions; i++) {
            // Đảm bảo chỉ số bit luôn dương và nằm trong phạm vi numBits
            long bitIndex = (combinedHash & Long.MAX_VALUE) % numBits;
            this.setBitAtomic(bitIndex);
            // Công thức Double Hashing liên tục
            combinedHash += hash2;
        }
    }


    /**
     * Kiểm tra phần tử có thể tồn tại hay không (Thao tác đọc an toàn không chặn luồng)
     */
    public boolean mightContain(T element) {
        if (element == null) return false;

        long hash1 = element.hashCode();
        long hash2 = Objects.hash(element, "salt_thread_safe");
        long combinedHash = hash1;

        for (int i = 0; i < numHashFunctions; i++) {
            long bitIndex = (combinedHash & Long.MAX_VALUE) % numBits;
            if (!getBit(bitIndex)) {
                // Chắc chắn không tồn tại
                return false;
            }
            combinedHash += hash2;
        }
        // Có thể tồn tại
        return true;
    }

    /**
     * Reset BloomFilter bắng cách chuyển tất cả các bit về 0
     */
    public synchronized void reset() {
        for (int i = 0; i < this.bits.length(); i++) {
            this.bits.set(i, 0L);
        }
    }

    /**
     * Bật bit an toàn bằng kỹ thuật CAS (Compare-And-Swap) cấp phần cứng
     */
    private void setBitAtomic(long bitIndex) {
        int longIndex = (int) (bitIndex / 64);
        long mask = 1L << (bitIndex % 64);

        long currentValue;
        long newValue;
        do {
            currentValue = bits.get(longIndex);
            if ((currentValue & mask) != 0) {
                // Bit đã được bật bởi luồng khác, bỏ qua nhanh
                return;
            }
            newValue = currentValue | mask;
        } while (!bits.compareAndSet(longIndex, currentValue, newValue));
    }

    /**
     * Đọc trạng thái bit (Luôn đọc được giá trị mới nhất do cơ chế volatile của AtomicLongArray)
     */
    private boolean getBit(long bitIndex) {
        int longIndex = (int) (bitIndex / 64);
        long mask = 1L << (bitIndex % 64);
        return (bits.get(longIndex) & mask) != 0;
    }

    /**
     * Đếm số bit và tỷ lệ lấp đầy
     */
    private double getBitDensity() {
        long bitCount = 0;
        int length = this.bits.length();

        // Quét qua mảng AtomicLongArray
        for (int i = 0; i < length; i++) {
            bitCount += Long.bitCount(this.bits.get(i));
        }

        // Trả về tỷ lệ phần trăm (từ 0.0 đến 1.0)
        return (double) bitCount / this.numBits;
    }

    /**
     * Tính toán kích thước khi khởi tạo
     */
    private long calculateExpectedMemoryBytes(int expectedElements, double falsePositiveRate) {
        if (expectedElements <= 0 || falsePositiveRate <= 0 || falsePositiveRate >= 1) {
            throw new IllegalArgumentException("Illegal Arguments");
        }

        // 1. Tính số lượng bit tối ưu (m) theo công thức toán học
        double optimalBit = (-expectedElements * Math.log(falsePositiveRate)) / Math.pow(Math.log(2), 2);
        long numBits = (long) Math.ceil(optimalBit);

        // 2. Tính số lượng phần tử kiểu long cần thiết trong mảng (Mỗi long = 64 bit)
        int arrayLength = (int) Math.ceil((double) numBits / 64);

        // 3. Mỗi phần tử kiểu long chiếm 8 bytes trên RAM vật lý
        return (long) arrayLength * 8;
    }

    /**
     * Lấy kích thước BloomFilter
     */
    public long getSize() {
       return this.size;
    }
}
