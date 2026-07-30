package org.minicache.util;

import java.util.ArrayList;
import java.util.List;

public class GeoHashUtil {
    private static final double MIN_LAT = -85.05112878;
    private static final double MAX_LAT = 85.05112878;
    private static final double MIN_LON = -180.0;
    private static final double MAX_LON = 180.0;

    private GeoHashUtil() {
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

                long h1 = GeoHashUtil.encode(cMinLat, cMinLon);
                long h2 = GeoHashUtil.encode(cMaxLat, cMaxLon);

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
}
