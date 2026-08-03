package org.minicache.struct.geohash;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class GeoHashHelper {
    public static final double MIN_LAT = -85.05112878;
    public static final double MAX_LAT = 85.05112878;
    public static final double MIN_LON = -180.0;
    public static final double MAX_LON = 180.0;
    // 26-bit lat, 26-bit lon -> Total 52 bits
    private static final int BITS_PER_DIMENSION = 26;
    private static final long MAX_INDEX = (1L << BITS_PER_DIMENSION) - 1;
    private static final double EARTH_RADIUS_METERS = 6371000.0;
    private static final char[] BASE32_CHARS = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };

    private GeoHashHelper() {
    }

    public static String toBase32(long geohash) {
        int numChars = 11;
        char[] buf = new char[numChars];

        for (int i = 0; i < numChars; i++) {
            int shift = (numChars - 1 - i) * 5;
            int charIndex = (int) ((geohash >> shift) & 0x1F);
            buf[i] = BASE32_CHARS[charIndex];
        }

        return new String(buf);
    }

    /**
     * Lấy 8 ô Geohash lân cận của 1 mã geohash 52-bit
     * @return Map chứa tên hướng và mã geohash 52-bit tương ứng
     */
    public static Map<String, Long> getNeighbors(long geohash) {
        long[] grid = deinterleave(geohash);
        long latIdx = grid[0];
        long lonIdx = grid[1];

        Map<String, Long> neighbors = new HashMap<>(8);

        // Các hướng dịch chuyển dLat, dLon
        int[][] directions = {
                {1, 0},   // NORTH
                {-1, 0},  // SOUTH
                {0, 1},   // EAST
                {0, -1},  // WEST
                {1, 1},   // NORTH_EAST
                {1, -1},  // NORTH_WEST
                {-1, 1},  // SOUTH_EAST
                {-1, -1}  // SOUTH_WEST
        };

        String[] dirNames = {
                "N", "S", "E", "W", "NE", "NW", "SE", "SW"
        };

        for (int i = 0; i < directions.length; i++) {
            int dLat = directions[i][0];
            int dLon = directions[i][1];

            // Dịch chuyển Latitude và Clamp ở 2 cực [0, MAX_INDEX]
            long nLat = Math.min(Math.max(latIdx + dLat, 0), MAX_INDEX);

            // Dịch chuyển Longitude và Wrap-around qua đường ngày quốc tế (180 deg)
            long nLon = (lonIdx + dLon + (MAX_INDEX + 1)) % (MAX_INDEX + 1);

            long neighborHash = interleave(nLat, nLon);
            neighbors.put(dirNames[i], neighborHash);
        }

        return neighbors;
    }

    public static long encode(double lat, double lon) {
        long latBits = scaleToBits(lat, MIN_LAT, MAX_LAT, 26);
        long lonBits = scaleToBits(lon, MIN_LON, MAX_LON, 26);
        return interleave(latBits, lonBits);
    }

    private static long scaleToBits(double val, double min, double max, int bits) {
        val = Math.max(min, Math.min(max, val));
        double norm = (val - min) / (max - min);
        return (long) (norm * ((1L << bits) - 1));
    }

    private static long interleave(long latBits, long lonBits) {
        long result = 0;
        for (int i = 0; i < 26; i++) {
            result |= ((lonBits >> i) & 1L) << (2 * i);
            result |= ((latBits >> i) & 1L) << (2 * i + 1);
        }
        return result;
    }

    public static List<long[]> calculateGeoHashRanges(double minLat, double maxLat, double minLon, double maxLon) {
        List<long[]> ranges = new ArrayList<>();
        double latStep = (maxLat - minLat) / 3.0;
        double lonStep = (maxLon - minLon) / 3.0;

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                double cMinLat = minLat + i * latStep;
                double cMaxLat = minLat + (i + 1) * latStep;
                double cMinLon = minLon + j * lonStep;
                double cMaxLon = minLon + (j + 1) * lonStep;

                long h1 = GeoHashHelper.encode(cMinLat, cMinLon);
                long h2 = GeoHashHelper.encode(cMaxLat, cMaxLon);

                ranges.add(new long[]{Math.min(h1, h2), Math.max(h1, h2)});
            }
        }
        return ranges;
    }

    public static double[] decode(long geohash) {
        long[] deinterleaved = deinterleave(geohash);
        double lat = bitsToScale(deinterleaved[0], MIN_LAT, MAX_LAT, 26);
        double lon = bitsToScale(deinterleaved[1], MIN_LON, MAX_LON, 26);
        return new double[]{lat, lon};
    }

    private static long[] deinterleave(long geohash) {
        long lonBits = 0, latBits = 0;
        for (int i = 0; i < 26; i++) {
            lonBits |= ((geohash >> (2 * i)) & 1L) << i;
            latBits |= ((geohash >> (2 * i + 1)) & 1L) << i;
        }
        return new long[]{latBits, lonBits};
    }

    private static double bitsToScale(long bits, double min, double max, int numBits) {
        double norm = (double) bits / ((1L << numBits) - 1);
        return min + norm * (max - min);
    }

    /**
     * Đại diện cho một dải Geohash [minHash, maxHash] phục vụ Range Query trên GeoSkipList
     */
        public record GeoHashRange(long minHash, long maxHash) {
    }

    /**
     * Tính toán tập hợp các dải GeoHash bao phủ bán kính radiusMeters xung quanh (centerLat, centerLon)
     */
    public static List<GeoHashRange> coverRadius(double centerLat, double centerLon, double radiusMeters) {
        List<GeoHashRange> ranges = new ArrayList<>();

        // 1. Tính Bounding Box theo độ (Latitude & Longitude Delta)
        double latDelta = Math.toDegrees(radiusMeters / EARTH_RADIUS_METERS);
        // Tránh chia cho 0 ở 2 cực
        double cosLat = Math.cos(Math.toRadians(centerLat));
        double lonDelta = (cosLat > 1e-6) ? Math.toDegrees(radiusMeters / (EARTH_RADIUS_METERS * cosLat)) : latDelta;

        double minLat = Math.max(centerLat - latDelta, -85.05112878);
        double maxLat = Math.min(centerLat + latDelta, 85.05112878);
        double minLon = Math.max(centerLon - lonDelta, -180.0);
        double maxLon = Math.min(centerLon + lonDelta, 180.0);

        // 2. Chuyển Bounding Box sang chỉ số lưới Grid Index (26-bit)
        long minLatIdx = scaleLatToIndex(minLat);
        long maxLatIdx = scaleLatToIndex(maxLat);
        long minLonIdx = scaleLonToIndex(minLon);
        long maxLonIdx = scaleLonToIndex(maxLon);

        // 3. Quyết định Step Size (Giảm số bit cần quét tùy thuộc vào bán kính)
        // Bán kính càng lớn thì stepShift càng cao (bỏ qua các bit thấp)
        int stepShift = estimateStepShift(radiusMeters);
        long step = 1L << stepShift; // Bước nhảy lưới = 2^stepShift

        // 4. Duyệt qua lưới ô phủ Bounding Box
        for (long latIdx = (minLatIdx >> stepShift) << stepShift; latIdx <= maxLatIdx; latIdx += step) {
            for (long lonIdx = (minLonIdx >> stepShift) << stepShift; lonIdx <= maxLonIdx; lonIdx += step) {

                // Mã Geohash đại diện cho góc nhỏ nhất của ô
                long minHash = interleave(latIdx, lonIdx);

                // Mã Geohash đại diện cho góc lớn nhất của ô (phủ toàn bộ ô có kích thước 2^stepShift)
                long maxHash = interleave(latIdx + step - 1, lonIdx + step - 1);

                ranges.add(new GeoHashRange(minHash, maxHash));
            }
        }

        return ranges;
    }

    /**
     * Ước lượng số bit shift dựa trên bán kính truy vấn để tránh bùng nổ số lượng ô (Grid Explosion)
     */
    private static int estimateStepShift(double radiusMeters) {
        if (radiusMeters <= 50) return 0;       // Chi tiết tối đa (bước nhảy 1 ô = 2^0)
        if (radiusMeters <= 500) return 3;      // Bước nhảy 8 ô
        if (radiusMeters <= 5000) return 6;     // Bước nhảy 64 ô
        if (radiusMeters <= 50000) return 10;   // Bước nhảy 1024 ô
        return 14;                              // Cho vùng phủ rất lớn (> 50km)
    }

    private static long scaleLatToIndex(double lat) {
        double normalized = (lat + 85.05112878) / (2 * 85.05112878);
        return (long) (normalized * ((1L << BITS_PER_DIMENSION) - 1));
    }

    private static long scaleLonToIndex(double lon) {
        double normalized = (lon + 180.0) / 360.0;
        return (long) (normalized * ((1L << BITS_PER_DIMENSION) - 1));
    }
}
