package org.minicache.engine.segment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.minicache.common.Command;
import org.minicache.common.Value;
import org.minicache.struct.bloomfilter.BloomFilter;
import org.minicache.struct.freqsketch.FrequencySketch;
import org.minicache.struct.hash.CompactHash;
import org.minicache.struct.skiplist.ConcurrentSkipList;
import org.minicache.struct.geohash.GeoSkipList;
import org.minicache.struct.geohash.GeoHashHelper;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageEngine extends org.minicache.engine.StorageEngine {
    private static final Logger log = LogManager.getLogger(StorageEngine.class);
    private final int segmentMask;
    private final CacheSegment[] segments;
    private final AtomicLong globalOperationCount = new AtomicLong(0);
    private static final int RESET_PERIOD = 100000;
    private final Map<String, String> initCfg;

    public StorageEngine(long maxSize) {
        var segmentCount = getOptimalSegmentCount();
        if ((segmentCount & (segmentCount - 1)) != 0) {
            throw new IllegalArgumentException();
        }
        this.segmentMask = segmentCount - 1;
        var imaxCacheSizeTotal = maxSize * 1024L * 1024L;

        // estimate size of one key-value pair
        final int estimatedEntrySizeAsBytes = 512;
        int totalExpectedKeys = (int) (imaxCacheSizeTotal / estimatedEntrySizeAsBytes);

        long maxSegmentSize = imaxCacheSizeTotal / segmentCount;
        this.segments = new CacheSegment[segmentCount];
        int segmentExpectedKeys = Math.max(10000, totalExpectedKeys / segmentCount);

        for (int i = 0; i < segmentCount; i++) {
            this.segments[i] = new CacheSegment(maxSegmentSize, segmentExpectedKeys);
        }

        this.initCfg = Map.of(
                "segmentCount", String.valueOf(segmentCount),
                "maxSizePerSegment", String.valueOf(maxSegmentSize),
                "segmentExpectedKeys", String.valueOf(segmentExpectedKeys)
        );
    }

    public Map<String, String> getInitCfg() {
        return this.initCfg;
    }

    private CacheSegment getSegment(String key) {
        if (key == null) {
            throw new IllegalArgumentException();
        }
        int hash = key.hashCode();
        hash = hash ^ (hash >>> 16);
        int index = hash & segmentMask;
        return segments[index];
    }

    private int getOptimalSegmentCount() {
        int cores = Runtime.getRuntime().availableProcessors();
        int target = cores * 4;
        int n = 1;
        while (n < target) {
            n <<= 1;
        }
        return Math.clamp(n, 16, 256);
    }

    public String put(String key, String value, Long ttl, Boolean notExists) {
        log.info("PUT ===> key: {}, value: {}, not_exists: {}, ttl: {}", key, value, notExists, ttl);
        if (key == null || value == null)
            return "FAIL";

        CacheSegment segment = getSegment(key);
        String result = segment.put(key, value, ttl, notExists);

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }
        return result;
    }

    public String get(String key) {
        log.info("GET ===> key: {}", key);
        if (key == null) return null;
        return getSegment(key).get(key);
    }

    public Integer delete(String key) {
        log.info("DEL ===> key: {}", key);
        if (key == null) return 0;
        return getSegment(key).delete(key);
    }

    public Integer exists(String key) {
        log.info("EXISTS ===> key: {}", key);
        if (key == null) return 0;
        return getSegment(key).exists(key);
    }

    public Integer clear(Command command) {
        log.info("CLEAR ===> Clear all keys and reset to default");
        if (Command.CLEAR.equals(command)) {
            for (CacheSegment seg : segments) {
                seg.clear();
            }
            globalOperationCount.set(0);
            return 1;
        }
        return 0;
    }

    public String getAllKeys(Command command) {
        log.info("KEYS ===> Fetch all keys");
        if (Command.LST_KEY.equals(command)) {
            StringBuilder keys = new StringBuilder();
            keys.append("[");
            boolean isFirst = true;

            for (CacheSegment seg : segments) {
                seg.rwLock.readLock().lock();
                try {
                    for (String key : seg.pairsStorage.keySet()) {
                        if (!isFirst) {
                            keys.append(",");
                        }
                        keys.append(key);
                        isFirst = false;
                    }

                    for (String key : seg.bloomFiltersStorage.keySet()) {
                        if (!isFirst) {
                            keys.append(",");
                        }
                        keys.append(key);
                        isFirst = false;
                    }

                    for (String key : seg.skipListsStorage.keySet()) {
                        if (!isFirst) {
                            keys.append(",");
                        }
                        keys.append(key);
                        isFirst = false;
                    }

                    for (String key : seg.geoHashStorage.keySet()) {
                        if (!isFirst) {
                            keys.append(",");
                        }
                        keys.append(key);
                        isFirst = false;
                    }

                    for (String key : seg.hashStorage.keySet()) {
                        if (!isFirst) {
                            keys.append(",");
                        }
                        keys.append(key);
                        isFirst = false;
                    }
                } finally {
                    seg.rwLock.readLock().unlock();
                }
            }
            keys.append("]");
            return keys.toString();
        }
        return "[]";
    }

    public String initBloomFilter(String key, Integer expectedElements, Double falsePositiveRate) {
        log.info("BF.INIT ===> key: {}, expectedElements: {}, falsePositiveRate: {}", key,
                expectedElements, falsePositiveRate);
        if (key == null || expectedElements == null || falsePositiveRate == null)
            return "FAIL";

        CacheSegment segment = getSegment(key);
        String result = segment.initBloomFilter(key, expectedElements, falsePositiveRate);

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }
        return result;
    }

    public Integer removeBloomFilter(String key) {
        log.info("BF.RM ===> key: {}", key);
        if (key == null) return 0;

        CacheSegment segment = getSegment(key);
        return segment.removeBloomFilter(key);
    }

    public String addBloomFilter(String key, String value) {
        log.info("BF.ADD ===> key: {}, value: {}", key, value);
        if (key == null || value == null) return "FAIL";

        CacheSegment segment = getSegment(key);
        var result = segment.addBloomFilter(key, value);

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }
        return result;
    }

    public Integer resetBloomFilter(String key) {
        log.info("BF.RS ===> key: {}", key);
        if (key == null) return 0;

        CacheSegment segment = getSegment(key);
        var result = segment.resetBloomFilter(key);

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }
        return result;
    }

    public Integer existsBloomFilter(String key, String value) {
        log.info("BF.EXISTS ===> key: {}, value: {}", key, value);
        if (key == null) return 0;

        CacheSegment segment = getSegment(key);
        return segment.existsBloomFilter(key, value);
    }

    public String zRangeByPositions(String key, Integer start, Integer stop) {
        log.info("Z.RANGE ===> key: {}, start: {}, stop: {}", key, start, stop);

        CacheSegment segment = getSegment(key);
        return segment.zRangeByPositions(key, start - 1, stop - 1);
    }

    public String zRangeByScore(String key, Double minScore, Double maxScore) {
        log.info("Z.RSCR ===> key: {}, minScore: {}, maxScore: {}", key, minScore, maxScore);

        CacheSegment segment = getSegment(key);
        return segment.zRangeByScore(key, minScore, maxScore);
    }

    public String zGetByPosition(String key, Integer index) {
        log.info("Z.POS ===> key: {}, position: {}", key, index);

        CacheSegment segment = getSegment(key);
        return segment.zGetByPosition(key, index - 1);
    }

    public String zTop(String key, Integer top) {
        log.info("Z.TOP ===> key: {}, top: {}", key, top);

        CacheSegment segment = getSegment(key);
        return segment.zTop(key, top);
    }

    public Integer zIncrBy(String key, Double increment, String member) {
        log.info("Z.INCR ===> key: {}, member: {}, increment: {}", key, member, increment);

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }

        CacheSegment segment = getSegment(key);
        return segment.zIncrBy(key, increment, member);
    }

    public Integer zRem(String key, String member) {
        log.info("Z.RM ===> key: {}, member: {}", key, member);

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }

        CacheSegment segment = getSegment(key);
        return segment.zRem(key, member);
    }

    public Integer zDel(String key) {
        log.info("Z.DEL ===> key: {}", key);

        CacheSegment segment = getSegment(key);
        return segment.zDel(key);
    }

    public Integer zRank(String key, String member) {
        log.info("Z.RANK ===> key: {}, member: {}", key, member);

        CacheSegment segment = getSegment(key);
        return segment.zRank(key, member);
    }

    public String zAdd(String key, Double score, String member, String value) {
        log.info("Z.ADD ===> key: {}, score: {}, member: {}, value: {}", key, score, member, value);

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }

        CacheSegment segment = getSegment(key);
        return segment.zAdd(key, score, member, value);
    }

    public String zScore(String key, String member) {
        log.info("Z.SCR ===> key: {}, member: {}", key, member);
        if (key == null) return "FAIL";

        CacheSegment segment = getSegment(key);
        return segment.zScore(key, member);
    }

    public String geoAdd(String key, String member, Double lat, Double lon) {
        log.info("GEO.ADD ===> key: {}, member: {}, lat: {}, lon: {}", key, member, lat, lon);
        if (key == null || key.isBlank() || member == null || member.isBlank() || lat == null || lon == null) {
            return "FAIL";
        }

        if (lat < GeoHashHelper.MIN_LAT || lat > GeoHashHelper.MAX_LAT
                || lon < GeoHashHelper.MIN_LON || lon > GeoHashHelper.MAX_LON) {
            return "FAIL";
        }

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }

        CacheSegment segment = getSegment(key);
        return segment.geoAdd(key, member, lat, lon);
    }

    public String geoSearch(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) {
        log.info("GEO.SEARCH ===> key: {}, lat: {}, lon: {}, radius: {}, limit: {}", key,
                centerLat, centerLon, radiusMeters, limit);
        if (key == null || radiusMeters == null || radiusMeters <= 0 || centerLat == null || centerLon == null) {
            return "[]";
        }

        CacheSegment segment = getSegment(key);
        return segment.geoSearch(key, centerLat, centerLon, radiusMeters, limit);
    }

    public String geoDist(String key, String member1, String member2) {
        log.info("GEO.DIST ===> key: {}, member1: {}, member2: {}", key, member1, member2);

        CacheSegment segment = getSegment(key);
        return segment.geoDist(key, member1, member2);
    }

    public Integer geoDel(String key) {
        log.info("GEO.DEL ===> key: {}", key);
        if (key == null) return 0;

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }

        CacheSegment segment = getSegment(key);
        return segment.geoDel(key);
    }

    public String geoEncode(String key, String member) {
        log.info("GEO.ENCODE ===> key: {}, member: {}", key, member);

        CacheSegment segment = getSegment(key);
        return segment.geoEncode(key, member);
    }

    public Integer geoRem(String key, String member) {
        log.info("GEO.RM ===> key: {}, member: {}", key, member);
        if (key == null || key.isBlank() || member == null || member.isBlank()) {
            return 0;
        }

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }

        CacheSegment segment = getSegment(key);
        return segment.geoRem(key, member);
    }

    public String geoGet(String key, String member) {
        log.info("GEO.GET ===> key: {}, member: {}", key, member);
        if (key == null || key.isBlank() || member == null || member.isBlank()) {
            return null;
        }

        CacheSegment segment = getSegment(key);
        return segment.geoGet(key, member);
    }

    public String geoNb(String key, String member) {
        log.info("GEO.NB ===> key: {}, member: {}", key, member);
        if (key == null || key.isBlank() || member == null || member.isBlank()) {
            return null;
        }

        CacheSegment segment = getSegment(key);
        return segment.geoNb(key, member);
    }

    public Integer geoExists(String key, String member) {
        log.info("GEO.EXISTS ===> key: {}, member: {}", key, member);

        CacheSegment segment = getSegment(key);
        return segment.geoExists(key, member);
    }

    public String hSet(String key, String field, String value) {
        log.info("H.SET ===> key: {}, field: {}, value: {}", key, field, value);
        if (key == null || field == null || value == null) return "FAIL";

        CacheSegment segment = getSegment(key);
        String result = segment.hSet(key, field, value);

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }
        return result;
    }

    public String hGet(String key, String field) {
        log.info("H.GET ===> key: {}, field: {}", key, field);
        if (key == null || field == null) return null;

        CacheSegment segment = getSegment(key);
        return segment.hGet(key, field);
    }

    public Integer hRm(String key, String field) {
        log.info("H.RM ===> key: {}, field: {}", key, field);
        if (key == null || field == null) return 0;

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }

        CacheSegment segment = getSegment(key);
        return segment.hRm(key, field);
    }

    public Integer hDel(String key) {
        log.info("H.DEL ===> key: {}", key);
        if (key == null) return 0;

        if (globalOperationCount.incrementAndGet() % RESET_PERIOD == 0) {
            for (CacheSegment seg : segments) {
                seg.ageSketch();
            }
        }

        CacheSegment segment = getSegment(key);
        return segment.hDel(key);
    }

    public String hGetAll(String key) {
        log.info("H.ALL ===> key: {}", key);
        if (key == null) {
            return "{}";
        }

        CacheSegment segment = getSegment(key);
        return segment.hGetAll(key);
    }

    private static class CacheSegment {
        private final long maxSegmentSize;
        private final AtomicLong currentSizeBytes = new AtomicLong(0);
        private final Map<String, Value> pairsStorage = new HashMap<>();
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        private final Map<String, BloomFilter<String>> bloomFiltersStorage = new HashMap<>();
        private final Map<String, ConcurrentSkipList<String, String>> skipListsStorage = new HashMap<>();
        private final Map<String, ConcurrentHashMap<String, Double>> memberScoresStorage = new HashMap<>();
        private final Map<String, GeoSkipList> geoHashStorage = new HashMap<>();
        private final Map<String, Long> memberToGeoHashStorage = new HashMap<>();
        private final Map<String, CompactHash> hashStorage = new HashMap<>();
        private final String BLOOM_FILTERS_KEY_PREFIX = "bf_";
        private final String SKIP_LISTS_KEY_PREFIX = "zs_";
        private final String GEO_KEY_PREFIX = "geo_";
        private final String HASH_KEY_PREFIX = "hs_";
        private final FrequencySketch sketch;

        public CacheSegment(long maxSegmentSize, int segmentExpectedKeys) {
            this.maxSegmentSize = maxSegmentSize;
            this.sketch = new FrequencySketch(segmentExpectedKeys);
        }

        public String put(String key, String value, Long ttl, Boolean notExists) {
            rwLock.writeLock().lock();
            try {
                if (notExists != null && notExists) {
                    Value exVal = pairsStorage.get(key);
                    if (exVal != null) {
                        if (exVal.getTtl() == null || System.currentTimeMillis() < exVal.getTtl()) {
                            return "FAIL";
                        }
                    }
                }

                Value newVal = new Value();
                newVal.setData(value);
                if (ttl != null && ttl > 0) {
                    newVal.setTtl(System.currentTimeMillis() + ttl);
                }

                long newEntrySize = calculateSize(key, newVal.getData(), newVal.getTtl());
                newVal.setSize(newEntrySize);

                Value oldVal = pairsStorage.put(key, newVal);
                if (oldVal != null) {
                    currentSizeBytes.addAndGet(-oldVal.getSize());
                }

                currentSizeBytes.addAndGet(newEntrySize);
                sketch.increment(key);

                if (currentSizeBytes.get() > maxSegmentSize) {
                    evictUsingTinyLFU(key);
                }

                return "OK";
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public String get(String key) {
            rwLock.readLock().lock();
            boolean holdReadLock = true;
            try {
                if (pairsStorage.isEmpty()) {
                    return null;
                }

                Value value = pairsStorage.get(key);
                if (value != null) {
                    if (value.getTtl() != null && System.currentTimeMillis() > value.getTtl()) {
                        rwLock.readLock().unlock();
                        holdReadLock = false;

                        rwLock.writeLock().lock();
                        try {
                            Value curVal = pairsStorage.get(key);
                            if (curVal != null && curVal.getTtl() != null && System.currentTimeMillis() > curVal.getTtl()) {
                                pairsStorage.remove(key);
                                currentSizeBytes.addAndGet(-curVal.getSize());
                            }
                        } finally {
                            rwLock.writeLock().unlock();
                        }
                        return null;
                    }

                    sketch.increment(key);
                    return value.getData();
                }
                return null;
            } finally {
                if (holdReadLock) {
                    rwLock.readLock().unlock();
                }
            }
        }

        public Integer delete(String key) {
            rwLock.writeLock().lock();
            try {
                if (pairsStorage.isEmpty()) {
                    return 0;
                }
                Value value = pairsStorage.remove(key);
                if (value != null) {
                    currentSizeBytes.addAndGet(-value.getSize());
                    return 1;
                }
                return 0;
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public Integer exists(String key) {
            rwLock.readLock().lock();
            try {
                if (pairsStorage.isEmpty()) {
                    return 0;
                }
                Value val = pairsStorage.get(key);
                if (val == null) return 0;

                if (val.getTtl() != null && System.currentTimeMillis() > val.getTtl()) {
                    rwLock.readLock().unlock();
                    rwLock.writeLock().lock();
                    try {
                        Value curVal = pairsStorage.get(key);
                        if (curVal != null && curVal.getTtl() != null && System.currentTimeMillis() > curVal.getTtl()) {
                            pairsStorage.remove(key);
                            currentSizeBytes.addAndGet(-curVal.getSize());
                        }
                    } finally {
                        rwLock.writeLock().unlock();
                        rwLock.readLock().lock();
                    }
                    return 0;
                }
                return 1;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        private void evictUsingTinyLFU(String candidateKey) {
            long candidateFreq = sketch.frequency(candidateKey);

            // Vòng lặp dọn dẹp cho đến khi đạt ngưỡng an toàn
            while (currentSizeBytes.get() > maxSegmentSize) {

                // 1. Thu thập nhanh 5 mẫu ngẫu nhiên mà không cần copy toàn bộ Key (Tránh O(N) allocation)
                List<String> sampleKeys = getSampleKeys();
                if (sampleKeys.isEmpty()) {
                    break; // Cache đã trống rỗng hoàn toàn, không còn gì để xóa
                }

                // 2. Tìm ra Victim có tần suất thấp nhất trong số các mẫu
                String victimKey = null;
                long minFreq = Long.MAX_VALUE;

                for (String k : sampleKeys) {
                    long freq = sketch.frequency(k);
                    if (freq < minFreq) {
                        minFreq = freq;
                        victimKey = k;
                    }
                }

                if (victimKey == null) break;

                // 3. Cơ chế lọc TinyLFU: So sánh Tần suất của Ứng viên (Candidate) và Nạn nhân (Victim)
                long victimFreq = sketch.frequency(victimKey);

                if (candidateFreq > victimFreq || victimKey.equals(candidateKey)) {
                    evictKeyFromStorage(victimKey);
                    log.info("EVICT ===> key: {}", victimKey);
                } else {
                    evictKeyFromStorage(candidateKey);
                    log.info("EVICT ===> key: {}", candidateKey);
                    break;
                }
            }
        }

        private List<String> getSampleKeys() {
            int sampleSize = 5;
            List<String> samples = new ArrayList<>(sampleSize);
            java.util.concurrent.ThreadLocalRandom random = java.util.concurrent.ThreadLocalRandom.current();

            List<Set<String>> stores = new ArrayList<>();
            if (!pairsStorage.isEmpty()) stores.add(pairsStorage.keySet());
            if (!bloomFiltersStorage.isEmpty()) stores.add(bloomFiltersStorage.keySet());
            if (!skipListsStorage.isEmpty()) stores.add(skipListsStorage.keySet());
            if (!geoHashStorage.isEmpty()) stores.add(geoHashStorage.keySet());
            if (!hashStorage.isEmpty()) stores.add(hashStorage.keySet());

            if (stores.isEmpty()) return samples;

            // Bốc mẫu cho đến khi đủ số lượng yêu cầu
            while (samples.size() < sampleSize) {
                // 1. Chọn ngẫu nhiên
                Set<String> targetSet = stores.get(random.nextInt(stores.size()));

                // 2. Lấy nhanh một phần tử bằng Iterator (Bản chất bucket của CHM đã mang tính ngẫu nhiên tự nhiên)
                Iterator<String> iterator = targetSet.iterator();
                if (iterator.hasNext()) {
                    // Nhảy ngẫu nhiên một vài bước trong iterator để tăng tính xáo trộn
                    int skipSteps = random.nextInt(Math.min(100, targetSet.size()));
                    String selectedKey = null;
                    for (int i = 0; i <= skipSteps && iterator.hasNext(); i++) {
                        selectedKey = iterator.next();
                    }
                    if (selectedKey != null && !samples.contains(selectedKey)) {
                        samples.add(selectedKey);
                    }
                }

                // Cơ chế thoát an toàn nếu tổng số Key thực tế nhỏ hơn sampleLimit
                long totalCurrentKeys = (long) pairsStorage.size() + bloomFiltersStorage.size()
                        + skipListsStorage.size() + geoHashStorage.size() + hashStorage.size();
                if (samples.size() >= totalCurrentKeys) {
                    break;
                }
            }

            return samples;
        }

        private void evictKeyFromStorage(String key) {
            // 1. Kiểm tra và xóa khỏi KV Store
            Value removedPairs = pairsStorage.remove(key);
            if (removedPairs != null) {
                currentSizeBytes.addAndGet(-removedPairs.getSize());
                return;
            }

            // 2. Kiểm tra và xóa khỏi Bloom Filter Store
            BloomFilter<String> removedBloom = bloomFiltersStorage.remove(key);
            if (removedBloom != null) {
                // Sử dụng hàm tính toán RAM vật lý thực tế chuẩn xác của mảng AtomicLongArray
                long bloomSize = removedBloom.getSize();
                long keySize = key.getBytes(StandardCharsets.UTF_8).length;
                currentSizeBytes.addAndGet(-(bloomSize + keySize));
                return;
            }

            // 3. Kiểm tra và xóa khỏi SkipList Store
            ConcurrentSkipList<String, String> removedSkipList = skipListsStorage.remove(key);
            memberScoresStorage.remove(key);
            if (removedSkipList != null) {
                long keySize = key.getBytes(StandardCharsets.UTF_8).length;
                long totalFreedBytes = keySize + (removedSkipList.size() * 180L);
                currentSizeBytes.addAndGet(-totalFreedBytes);
            }

            // 4. Kiểm tra và xóa khỏi Geo Store
            GeoSkipList removedGeo = geoHashStorage.remove(key);
            if (removedGeo != null) {
                memberToGeoHashStorage.keySet().removeIf(k -> k.startsWith(key + ":"));
                long keySize = key.getBytes(StandardCharsets.UTF_8).length;
                currentSizeBytes.addAndGet(-keySize);
            }

            // 5. Kiểm tra và xóa khỏi Hash Store
            CompactHash removedHash = hashStorage.remove(key);
            if (removedHash != null) {
                long keyMetadataBytes = key.getBytes(StandardCharsets.UTF_8).length + 32;
                long totalFreedBytes = keyMetadataBytes + removedHash.getEstimatedBytes();
                currentSizeBytes.addAndGet(-totalFreedBytes);
            }
        }

        public void ageSketch() {
            rwLock.writeLock().lock();
            try {
                sketch.age();
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public void clear() {
            rwLock.writeLock().lock();
            try {
                pairsStorage.clear();
                bloomFiltersStorage.clear();
                skipListsStorage.clear();
                memberScoresStorage.clear();
                sketch.reset();
                currentSizeBytes.set(0);
                memberToGeoHashStorage.clear();
                geoHashStorage.clear();
                hashStorage.clear();
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        private long calculateSize(String key, String value, Long ttl) {
            if (key == null || value == null) return 0;
            long baseSize = key.getBytes(StandardCharsets.UTF_8).length
                    + value.getBytes(StandardCharsets.UTF_8).length;
            return (ttl != null && ttl > 0) ? baseSize + 8 : baseSize;
        }

        public String initBloomFilter(String key, Integer expectedElements, Double falsePositiveRate) {
            rwLock.writeLock().lock();
            try {
                key = BLOOM_FILTERS_KEY_PREFIX + key;
                BloomFilter<String> newEntry = new BloomFilter<>(expectedElements, falsePositiveRate);
                var keySize = key.getBytes(StandardCharsets.UTF_8).length;

                var exist = bloomFiltersStorage.get(key);
                if (exist != null) {
                    currentSizeBytes.addAndGet(-(exist.getSize() + keySize));
                }

                var newVal = bloomFiltersStorage.put(key, newEntry);
                if (newVal != null) {
                    currentSizeBytes.addAndGet((newVal.getSize() + keySize));
                }
                sketch.increment(key);

                if (currentSizeBytes.get() > maxSegmentSize) {
                    evictUsingTinyLFU(key);
                }

                return "OK";
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public Integer removeBloomFilter(String key) {
            rwLock.writeLock().lock();
            try {
                key = BLOOM_FILTERS_KEY_PREFIX + key;
                if (bloomFiltersStorage.isEmpty()) return 0;
                if (bloomFiltersStorage.get(key) == null) return 0;

                var value = bloomFiltersStorage.remove(key);
                if (value != null) {
                    var removedSize = key.getBytes(StandardCharsets.UTF_8).length + value.getSize();
                    currentSizeBytes.addAndGet(-removedSize);
                }
                return 1;
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public String addBloomFilter(String key, String value) {
            rwLock.readLock().lock();
            try {
                key = BLOOM_FILTERS_KEY_PREFIX + key;
                var bloom = bloomFiltersStorage.get(key);
                if (bloom == null) {
                    return "FAIL";
                }
                bloom.add(value);
                sketch.increment(key);
                return "OK";
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public Integer existsBloomFilter(String key, String value) {
            rwLock.readLock().lock();
            try {
                key = BLOOM_FILTERS_KEY_PREFIX + key;
                var bloom = bloomFiltersStorage.get(key);
                if (bloom == null) {
                    return 0;
                }
                if (value == null || value.isBlank()) {
                    return 1;
                }
                sketch.increment(key);
                return bloom.mightContain(value) ? 1 : 0;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public Integer resetBloomFilter(String key) {
            rwLock.readLock().lock();
            try {
                key = BLOOM_FILTERS_KEY_PREFIX + key;
                var bloom = bloomFiltersStorage.get(key);
                if (bloom == null) {
                    return 0;
                }
                bloom.reset();
                return 1;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String zScore(String key, String member) {
            rwLock.readLock().lock();
            try {
                key = SKIP_LISTS_KEY_PREFIX + key;
                var memScore = memberScoresStorage.get(key);
                if (memScore == null) {
                    return null;
                }
                Double score = memScore.get(member);
                return score != null ? String.valueOf(score) : null;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String zRangeByPositions(String key, int start, int stop) {
            rwLock.readLock().lock();
            try {
                String internalKey = SKIP_LISTS_KEY_PREFIX + key;
                ConcurrentSkipList<String, String> skipList = skipListsStorage.get(internalKey);

                if (skipList == null) {
                    return null;
                }

                sketch.increment(internalKey);
                var res = skipList.getRangeByPositions(start, stop);
                boolean isFirst = true;
                StringBuilder results = new StringBuilder("[");
                for (String val : res) {
                    if (!isFirst) {
                        results.append(",");
                    }
                    results.append(val);
                    isFirst = false;
                }
                results.append("]");
                return results.toString();
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String zTop(String key, int top) {
            rwLock.readLock().lock();
            try {
                String internalKey = SKIP_LISTS_KEY_PREFIX + key;
                ConcurrentSkipList<String, String> skipList = skipListsStorage.get(internalKey);

                if (skipList == null) {
                    return null;
                }

                sketch.increment(internalKey);
                var res = skipList.getRangeByPositions(0, top - 1);
                boolean isFirst = true;
                StringBuilder results = new StringBuilder("[");
                for (String val : res) {
                    if (!isFirst) {
                        results.append(",");
                    }
                    results.append(val);
                    isFirst = false;
                }
                results.append("]");
                return results.toString();
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String zRangeByScore(String key, double minScore, double maxScore) {
            rwLock.readLock().lock();
            try {
                String internalKey = SKIP_LISTS_KEY_PREFIX + key;
                ConcurrentSkipList<String, String> skipList = skipListsStorage.get(internalKey);

                if (skipList == null) {
                    return null;
                }

                sketch.increment(internalKey);
                var res = skipList.getRangeByScore(minScore, maxScore);
                boolean isFirst = true;
                StringBuilder results = new StringBuilder("[");
                for (String val : res) {
                    if (!isFirst) {
                        results.append(",");
                    }
                    results.append(val);
                    isFirst = false;
                }
                results.append("]");
                return results.toString();
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String zGetByPosition(String key, int index) {
            rwLock.readLock().lock();
            try {
                String internalKey = SKIP_LISTS_KEY_PREFIX + key;
                ConcurrentSkipList<String, String> skipList = skipListsStorage.get(internalKey);

                if (skipList == null) {
                    return null;
                }

                sketch.increment(internalKey);
                var res = skipList.getByPosition(index);
                if (res == null || res.isBlank()) {
                    return null;
                }
                return res;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public Integer zIncrBy(String key, double increment, String member) {
            rwLock.readLock().lock();
            try {
                String internalKey = SKIP_LISTS_KEY_PREFIX + key;

                ConcurrentSkipList<String, String> skipList = skipListsStorage.computeIfAbsent(
                        internalKey, k -> new ConcurrentSkipList<>()
                );
                ConcurrentHashMap<String, Double> memberScores = memberScoresStorage.computeIfAbsent(
                        internalKey, k -> new ConcurrentHashMap<>()
                );

                Double oldScore = memberScores.get(member);
                double newScore = (oldScore == null) ? increment : oldScore + increment;

                boolean check = false;
                String oldVal = "";
                if (oldScore != null) {
                    oldVal = skipList.remove(oldScore, member);
                    if (oldVal != null) {
                        check = true;
                    }
                }

                memberScores.put(member, newScore);
                skipList.put(newScore, member, oldVal);

                sketch.increment(internalKey);
                return check ? 1 : 0;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public Integer zRank(String key, String member) {
            rwLock.readLock().lock();
            try {
                String internalKey = SKIP_LISTS_KEY_PREFIX + key;
                ConcurrentSkipList<String, String> skipList = skipListsStorage.get(internalKey);
                ConcurrentHashMap<String, Double> memberScores = memberScoresStorage.get(internalKey);

                if (skipList == null || memberScores == null) return -1;

                Double score = memberScores.get(member);
                if (score == null) return -1;

                sketch.increment(internalKey);
                return skipList.getRank(score, member) + 1;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String zAdd(String key, double score, String member, String value) {
            rwLock.readLock().lock();
            try {
                String internalKey = SKIP_LISTS_KEY_PREFIX + key;

                ConcurrentSkipList<String, String> skipList = skipListsStorage.computeIfAbsent(
                        internalKey, k -> new ConcurrentSkipList<>()
                );
                ConcurrentHashMap<String, Double> memberScores = memberScoresStorage.computeIfAbsent(
                        internalKey, k -> new ConcurrentHashMap<>()
                );

                Double oldScore = memberScores.get(member);
                if (oldScore != null) {
                    if (oldScore == score) {
                        return "FAIL";
                    }
                    skipList.remove(oldScore, member);
                }

                memberScores.put(member, score);
                boolean isNew = skipList.put(score, member, value);

                if (isNew && oldScore == null) {
                    long memberBytes = member.getBytes(StandardCharsets.UTF_8).length;
                    long valueBytes = value.getBytes(StandardCharsets.UTF_8).length;
                    long estimatedNodeSizeBytes = 48 + memberBytes + valueBytes + (16 * 8);

                    currentSizeBytes.addAndGet(estimatedNodeSizeBytes);

                    if (currentSizeBytes.get() > maxSegmentSize) {
                        evictUsingTinyLFU(key);
                    }

                    sketch.increment(internalKey);
                    return "OK";
                }

                return "FAIL";
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public Integer zRem(String key, String member) {
            rwLock.readLock().lock();
            try {
                String internalKey = SKIP_LISTS_KEY_PREFIX + key;
                ConcurrentSkipList<String, String> skipList = skipListsStorage.get(internalKey);
                ConcurrentHashMap<String, Double> memberScores = memberScoresStorage.get(internalKey);

                if (skipList == null || memberScores == null) return 0;

                Double currentScore = memberScores.remove(member);
                if (currentScore == null) return 0;

                var removed = skipList.remove(currentScore, member);
                if (removed != null) {
                    long memberBytes = member.getBytes(StandardCharsets.UTF_8).length;
                    long estimatedNodeSizeBytes = 48 + memberBytes + (16 * 8);
                    currentSizeBytes.addAndGet(-estimatedNodeSizeBytes);

                    sketch.increment(internalKey);
                    return 1;
                }
                return 0;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public Integer zDel(String key) {
            rwLock.readLock().lock();
            try {
                String internalKey = SKIP_LISTS_KEY_PREFIX + key;
                ConcurrentSkipList<String, String> skipList = skipListsStorage.remove(internalKey);
                ConcurrentHashMap<String, Double> memberScores = memberScoresStorage.remove(internalKey);

                if (skipList != null || memberScores != null) {
                    long freedBytes = 0;
                    long keyBytes = internalKey.getBytes(StandardCharsets.UTF_8).length;
                    freedBytes += keyBytes;
                    if (skipList != null) {
                        int elementCount = skipList.size();
                        freedBytes += (elementCount * 180L);
                    }
                    currentSizeBytes.addAndGet(-freedBytes);
                    sketch.increment(internalKey);
                    return 1;
                }

                return 0;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String geoAdd(String key, String member, Double lat, Double lon) {
            rwLock.readLock().lock();
            try {
                String internalKey = GEO_KEY_PREFIX + key;
                GeoSkipList geoSkipList = geoHashStorage.computeIfAbsent(internalKey, k -> new GeoSkipList());

                long newGeoHash = GeoHashHelper.encode(lat, lon);
                GeoSkipList.GeoPoint newPoint = new GeoSkipList.GeoPoint(member, lat, lon, newGeoHash);
                String globalMemberKey = internalKey + ":" + member;
                Long oldGeoHash = memberToGeoHashStorage.put(globalMemberKey, newGeoHash);
                if (oldGeoHash != null && oldGeoHash != newGeoHash) {
                    geoSkipList.removeMember(oldGeoHash, member);
                }

                geoSkipList.put(newGeoHash, newPoint);
                if (oldGeoHash == null) {
                    long memberBytes = member.getBytes(StandardCharsets.UTF_8).length;
                    long estimatedNodeSizeBytes = 48 + memberBytes + 16 + (16 * 8);

                    currentSizeBytes.addAndGet(estimatedNodeSizeBytes);
                    if (currentSizeBytes.get() > maxSegmentSize) {
                        evictUsingTinyLFU(internalKey);
                    }
                }
                sketch.increment(internalKey);

                return "OK";
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String geoSearch(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) {
            rwLock.readLock().lock();
            try {
                String internalKey = GEO_KEY_PREFIX + key;
                GeoSkipList geoSkipList = geoHashStorage.get(internalKey);

                if (geoSkipList == null) {
                    return "[]";
                }

                // 1. Tính toán tập hợp các dải GeoHash bao phủ bán kính xung quanh điểm center
                List<GeoHashHelper.GeoHashRange> ranges = GeoHashHelper.coverRadius(centerLat, centerLon, radiusMeters);
                List<GeoSkipList.GeoResult> matchedResults = new ArrayList<>();

                // 2. Quét các range chính xác trong GeoSkipList
                for (GeoHashHelper.GeoHashRange range : ranges) {
                    List<GeoSkipList.GeoPoint> candidatePoints = geoSkipList.rangeScan(range.minHash(), range.maxHash());

                    for (GeoSkipList.GeoPoint point : candidatePoints) {
                        double distance = point.distanceToInMeters(centerLat, centerLon);
                        if (distance <= radiusMeters) {
                            matchedResults.add(new GeoSkipList.GeoResult(
                                    point.member(),
                                    distance,
                                    point.lat(),
                                    point.lon()
                            ));
                        }
                    }
                }

                if (matchedResults.isEmpty()) {
                    return "[]";
                }

                // 3. Lọc trùng member (giữ lại điểm gần nhất) & Sắp xếp theo khoảng cách tăng dần
                Map<String, GeoSkipList.GeoResult> uniqueMembersMap = new HashMap<>();
                for (GeoSkipList.GeoResult res : matchedResults) {
                    uniqueMembersMap.merge(res.member(), res, (oldVal, newVal) ->
                            newVal.distanceMeters() < oldVal.distanceMeters() ? newVal : oldVal
                    );
                }

                List<GeoSkipList.GeoResult> sortedResults = new ArrayList<>(uniqueMembersMap.values());
                sortedResults.sort(Comparator.comparingDouble(GeoSkipList.GeoResult::distanceMeters));
                if (limit != null && limit > 0 && limit < sortedResults.size()) {
                    sortedResults = sortedResults.subList(0, limit);
                }
                sketch.increment(internalKey);

                boolean isFirst = true;
                StringBuilder results = new StringBuilder("[");
                for (var val : sortedResults) {
                    if (!isFirst) {
                        results.append(",");
                    }
                    results.append(val.member());
                    isFirst = false;
                }
                results.append("]");
                return results.toString();
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String geoDist(String key, String member1, String member2) {
            rwLock.readLock().lock();
            try {
                if (key == null || member1 == null || member2 == null) {
                    return "-1.0";
                }

                String internalKey = GEO_KEY_PREFIX + key;
                if (!geoHashStorage.containsKey(internalKey)) {
                    return "-1.0";
                }

                // Tra cứu 52-bit Geohash theo key không gian riêng biệt
                Long hash1 = memberToGeoHashStorage.get(internalKey + ":" + member1);
                Long hash2 = memberToGeoHashStorage.get(internalKey + ":" + member2);

                if (hash1 == null || hash2 == null) {
                    return "-1.0";
                }

                if (hash1.equals(hash2) && member1.equals(member2)) {
                    return "0.0";
                }

                // Giải mã tọa độ và tính khoảng cách
                double[] coord1 = GeoHashHelper.decode(hash1);
                double[] coord2 = GeoHashHelper.decode(hash2);

                GeoSkipList.GeoPoint p1 = new GeoSkipList.GeoPoint(member1, coord1[0], coord1[1], hash1);
                var res = p1.distanceToInMeters(coord2[0], coord2[1]);
                return String.valueOf(res);
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public Integer geoDel(String key) {
            rwLock.writeLock().lock();
            try {
                String internalKey = GEO_KEY_PREFIX + key;
                GeoSkipList removedSkipList = geoHashStorage.remove(internalKey);

                if (removedSkipList != null) {
                    long freedNodeBytes = removedSkipList.clearAndCountFreedBytes();

                    memberToGeoHashStorage.keySet().removeIf(k -> k.startsWith(internalKey + ":"));

                    long keyBytes = internalKey.getBytes(StandardCharsets.UTF_8).length;
                    long totalFreedBytes = keyBytes + freedNodeBytes;

                    currentSizeBytes.addAndGet(-totalFreedBytes);
                    sketch.increment(internalKey);
                    return 1;
                }
                return 0;
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public Integer geoRem(String key, String member) {
            rwLock.writeLock().lock();
            try {
                String internalKey = GEO_KEY_PREFIX + key;
                GeoSkipList geoSkipList = geoHashStorage.get(internalKey);

                if (geoSkipList == null) {
                    return 0;
                }

                // 1. Tra cứu Geohash của member trong scope của key này
                String globalMemberKey = internalKey + ":" + member;
                Long geoHash = memberToGeoHashStorage.remove(globalMemberKey);

                if (geoHash == null) {
                    return 0;
                }

                // 2. Xóa member khỏi GeoSkipList
                geoSkipList.removeMember(geoHash, member);

                // 3. Hoàn lại dung lượng RAM ước tính đã giải phóng
                long memberBytes = member.getBytes(StandardCharsets.UTF_8).length;
                long estimatedNodeSizeBytes = 48 + memberBytes + 16 + (16 * 8);
                currentSizeBytes.addAndGet(-estimatedNodeSizeBytes);

                // 4. Cập nhật Frequency Sketch cho TinyLFU
                sketch.increment(internalKey);

                return 1;
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public String geoGet(String key, String member) {
            rwLock.readLock().lock();
            try {
                String internalKey = GEO_KEY_PREFIX + key;
                GeoSkipList geoSkipList = geoHashStorage.get(internalKey);

                if (geoSkipList == null) {
                    return null;
                }

                String globalMemberKey = internalKey + ":" + member;
                Long geoHash = memberToGeoHashStorage.get(globalMemberKey);

                if (geoHash == null) {
                    return null;
                }

                double[] coordinates = GeoHashHelper.decode(geoHash);
                double lat = coordinates[0];
                double lon = coordinates[1];

                sketch.increment(internalKey);
                return String.format(java.util.Locale.US, "[%.6f,%.6f]", lat, lon);
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String geoNb(String key, String member) {
            rwLock.readLock().lock();
            try {
                String internalKey = GEO_KEY_PREFIX + key;
                GeoSkipList geoSkipList = geoHashStorage.get(internalKey);
                if (geoSkipList == null) {
                    return null;
                }

                String globalMemberKey = internalKey + ":" + member;
                Long centerGeoHash = memberToGeoHashStorage.get(globalMemberKey);
                if (centerGeoHash == null) {
                    return null;
                }

                // Lấy 8 ô lân cận
                Map<String, Long> result = GeoHashHelper.getNeighbors(centerGeoHash);

                // build response
                StringBuilder res = new StringBuilder("{");
                for (Map.Entry<String, Long> entry : result.entrySet()) {
                    double[] coords = GeoHashHelper.decode(entry.getValue());
                    double lat = coords[0];
                    double lon = coords[1];
                    String val = String.format(Locale.US, "[%.6f,%.6f]", lat, lon);
                    res.append("\"").append(entry.getKey()).append("\"").append(":").append(val).append(",");
                }
                // push center
                res.append("\"CT\":");
                double[] coords = GeoHashHelper.decode(centerGeoHash);
                double lat = coords[0];
                double lon = coords[1];
                String valCt = String.format(Locale.US, "[%.6f,%.6f]", lat, lon);
                res.append(valCt);
                res.append("}");
                return res.toString();
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public Integer geoExists(String key, String member) {
            rwLock.readLock().lock();
            try {
                String internalKey = GEO_KEY_PREFIX + key;
                GeoSkipList geoSkipList = geoHashStorage.get(internalKey);
                if (geoSkipList == null) {
                    return 0;
                }

                String globalMemberKey = internalKey + ":" + member;
                Long centerGeoHash = memberToGeoHashStorage.get(globalMemberKey);
                if (centerGeoHash == null) {
                    return 0;
                }

                return 1;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String geoEncode(String key, String member) {
            rwLock.readLock().lock();
            try {
                String internalKey = GEO_KEY_PREFIX + key;
                GeoSkipList geoSkipList = geoHashStorage.get(internalKey);
                if (geoSkipList == null) {
                    return null;
                }

                String globalMemberKey = internalKey + ":" + member;
                Long centerGeoHash = memberToGeoHashStorage.get(globalMemberKey);
                if (centerGeoHash == null) {
                    return null;
                }

                return GeoHashHelper.toBase32(centerGeoHash);
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String hGet(String key, String field) {
            rwLock.readLock().lock();
            try {
                String internalKey = HASH_KEY_PREFIX + key;
                CompactHash hash = hashStorage.get(internalKey);
                if (hash == null) {
                    return null;
                }

                sketch.increment(internalKey);
                return hash.get(field);
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public String hSet(String key, String field, String value) {
            rwLock.writeLock().lock();
            try {
                String internalKey = HASH_KEY_PREFIX + key;
                boolean isNewHashKey = !hashStorage.containsKey(internalKey);

                CompactHash hash = hashStorage.computeIfAbsent(internalKey, k -> new CompactHash());
                CompactHash.PutResult putResult = hash.put(field, value);
                long keyMetadataBytes = isNewHashKey
                        ? internalKey.getBytes(StandardCharsets.UTF_8).length + 32
                        : 0;

                long totalDeltaBytes = keyMetadataBytes + putResult.memoryDelta();

                currentSizeBytes.addAndGet(totalDeltaBytes);
                sketch.increment(internalKey);

                if (currentSizeBytes.get() > maxSegmentSize) {
                    evictUsingTinyLFU(internalKey);
                }

                return "OK";
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public Integer hRm(String key, String field) {
            rwLock.writeLock().lock();
            try {
                String internalKey = HASH_KEY_PREFIX + key;
                CompactHash hash = hashStorage.get(internalKey);
                if (hash == null) return 0;

                CompactHash.RemoveResult removeResult = hash.remove(field);

                if (removeResult.oldValue() != null) {
                    currentSizeBytes.addAndGet(-removeResult.freedBytes());

                    if (hash.isEmpty()) {
                        hashStorage.remove(internalKey);
                        long freedKeyMetadata = internalKey.getBytes(StandardCharsets.UTF_8).length + 32 + hash.getEstimatedBytes();
                        currentSizeBytes.addAndGet(-freedKeyMetadata);
                    }

                    sketch.increment(internalKey);
                    return 1;
                }
                return 0;
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public String hGetAll(String key) {
            rwLock.readLock().lock();
            try {
                String internalKey = HASH_KEY_PREFIX + key;
                CompactHash hash = hashStorage.get(internalKey);
                if (hash == null || hash.isEmpty()) return "{}";

                sketch.increment(internalKey);

                StringBuilder json = new StringBuilder("{");
                boolean[] isFirst = {true};

                hash.forEach((k, v) -> {
                    if (!isFirst[0]) {
                        json.append(",");
                    }
                    json.append("\"").append(k).append("\":\"").append(v).append("\"");
                    isFirst[0] = false;
                });

                json.append("}");
                return json.toString();
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public Integer hDel(String key) {
            rwLock.writeLock().lock();
            try {
                String internalKey = HASH_KEY_PREFIX + key;
                CompactHash removedHash = hashStorage.remove(internalKey);

                if (removedHash != null) {
                    long keyMetadataBytes = internalKey.getBytes(StandardCharsets.UTF_8).length + 32;
                    long totalFreedBytes = keyMetadataBytes + removedHash.getEstimatedBytes();

                    currentSizeBytes.addAndGet(-totalFreedBytes);
                    sketch.increment(internalKey);
                    return 1;
                }

                return 0;
            } finally {
                rwLock.writeLock().unlock();
            }
        }
    }
}