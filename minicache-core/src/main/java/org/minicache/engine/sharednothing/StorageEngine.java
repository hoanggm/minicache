package org.minicache.engine.sharednothing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.minicache.common.AsyncMDC;
import org.minicache.common.Command;
import org.minicache.common.TTLEntry;
import org.minicache.common.Value;
import org.minicache.struct.bloomfilter.BloomFilter;
import org.minicache.struct.freqsketch.FrequencySketch;
import org.minicache.struct.geohash.GeoHashHelper;
import org.minicache.struct.geohash.GeoSkipList;
import org.minicache.struct.hash.CompactHash;
import org.minicache.struct.skiplist.VanillaSkipList;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class StorageEngine extends org.minicache.engine.StorageEngine {
    private static final Logger log = LogManager.getLogger(StorageEngine.class);
    private final int segmentMask;
    private final CacheSegment[] segments;
    private final Map<String, String> initCfg;
    private final ScheduledExecutorService ttlCleanerScheduler;
    private static final long CLEANUP_INTERVAL_MS = 1000 * 60 * 60;

    public StorageEngine(long maxSize) {
        var segmentCount = getOptimalSegmentCount();
        if ((segmentCount & (segmentCount - 1)) != 0) {
            throw new IllegalArgumentException();
        }
        this.segmentMask = segmentCount - 1;
        var imaxCacheSizeTotal = maxSize * 1024L * 1024L;

        final int estimatedEntrySizeAsBytes = 512;
        int totalExpectedKeys = (int) (imaxCacheSizeTotal / estimatedEntrySizeAsBytes);

        long maxSegmentSize = imaxCacheSizeTotal / segmentCount;
        this.segments = new CacheSegment[segmentCount];
        int segmentExpectedKeys = Math.max(10000, totalExpectedKeys / segmentCount);

        // Khởi tạo các Shard/Segment độc lập cùng Worker Thread chuyên trách
        for (int i = 0; i < segmentCount; i++) {
            this.segments[i] = new CacheSegment(i, maxSegmentSize, segmentExpectedKeys);
        }

        this.initCfg = Map.of(
                "segmentCount", String.valueOf(segmentCount),
                "maxSizePerSegment", String.valueOf(maxSegmentSize),
                "segmentExpectedKeys", String.valueOf(segmentExpectedKeys)
        );

        // Auto clean up expired key-value entry
        this.ttlCleanerScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "minicache-sharednothing-ttl-cleaner");
            t.setDaemon(true);
            return t;
        });

        this.ttlCleanerScheduler.scheduleAtFixedRate(
                this::triggerTtlCleanupAcrossShards,
                CLEANUP_INTERVAL_MS,
                CLEANUP_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    private void triggerTtlCleanupAcrossShards() {
        log.info("Expired key(s) will be cleaned up");
        try {
            for (CacheSegment segment : segments) {
                segment.submitTask(segment::internalCleanExpiredKeys);
            }
        } catch (Throwable t) {
            log.error("Error occurred during TTL cleanup execution", t);
        }
    }

    public void shutdown() {
        log.info("CacheEngine is shutting down...");
        if (ttlCleanerScheduler != null && !ttlCleanerScheduler.isShutdown()) {
            ttlCleanerScheduler.shutdown();
        }
        log.info("CacheEngine shutdown successfully");
    }

    public Map<String, String> getInitCfg() {
        return this.initCfg;
    }

    private CacheSegment getSegment(String key) {
        if (key == null) throw new IllegalArgumentException();
        int hash = key.hashCode();
        hash = hash ^ (hash >>> 16);
        int index = hash & segmentMask;
        return segments[index];
    }

    private int getOptimalSegmentCount() {
        return Integer.highestOneBit(Runtime.getRuntime().availableProcessors());
    }

    private <T> CompletableFuture<T> submitToShard(String key, Supplier<T> task) {
        if (key == null) {
            return CompletableFuture.completedFuture(null);
        }
        CacheSegment segment = getSegment(key);
        Supplier<T> mdcTask = AsyncMDC.wrap(task);
        CompletableFuture<T> future = new CompletableFuture<>();
        segment.submitTask(() -> {
            try {
                future.complete(mdcTask.get());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public CompletableFuture<String> putAsync(String key, String value, Long ttl, Boolean notExists) {
        log.info("PUT ===> key: {}, value: {}, not_exists: {}, ttl: {}", key, value, notExists, ttl);
        if (key == null || value == null) {
            return CompletableFuture.completedFuture("FAIL");
        }
        return submitToShard(key, () -> getSegment(key).internalPut(key, value, ttl, notExists));
    }

    public CompletableFuture<String> getAsync(String key) {
        log.info("GET ===> key: {}", key);
        return submitToShard(key, () -> getSegment(key).internalGet(key));
    }

    public CompletableFuture<Integer> deleteAsync(String key) {
        log.info("DEL ===> key: {}", key);
        return submitToShard(key, () -> getSegment(key).internalDelete(key));
    }

    public CompletableFuture<Integer> existsAsync(String key) {
        log.info("EXISTS ===> key: {}", key);
        return submitToShard(key, () -> getSegment(key).internalExists(key));
    }

    public CompletableFuture<Integer> clearAsync(Command command) {
        log.info("CLEAR ===> Clear all keys and reset to default");
        if (!Command.CLEAR.equals(command)) {
            return CompletableFuture.completedFuture(0);
        }

        CompletableFuture<?>[] futures = new CompletableFuture[segments.length];
        for (int i = 0; i < segments.length; i++) {
            CompletableFuture<Void> f = new CompletableFuture<>();
            futures[i] = f;
            int finalIdx = i;
            segments[i].submitTask(() -> {
                segments[finalIdx].internalClear();
                f.complete(null);
            });
        }
        return CompletableFuture.allOf(futures).thenApply(v -> 1);
    }

    public CompletableFuture<String> getAllKeysAsync(Command command) {
        log.info("KEYS ===> Fetch all keys");
        if (!Command.LST_KEY.equals(command)) {
            return CompletableFuture.completedFuture("[]");
        }

        @SuppressWarnings("unchecked")
        CompletableFuture<List<String>>[] futures = new CompletableFuture[segments.length];
        for (int i = 0; i < segments.length; i++) {
            CompletableFuture<List<String>> future = new CompletableFuture<>();
            futures[i] = future;
            CacheSegment seg = segments[i];
            seg.submitTask(() -> {
                List<String> shardKeys = new ArrayList<>();
                shardKeys.addAll(seg.pairsStorage.keySet());
                shardKeys.addAll(seg.bloomFiltersStorage.keySet());
                shardKeys.addAll(seg.skipListsStorage.keySet());
                shardKeys.addAll(seg.geoHashStorage.keySet());
                shardKeys.addAll(seg.hashStorage.keySet());
                future.complete(shardKeys);
            });
        }

        return CompletableFuture.allOf(futures).thenApply(v -> {
            StringBuilder keys = new StringBuilder("[");
            boolean isFirst = true;
            for (var f : futures) {
                for (String key : f.join()) {
                    if (!isFirst) keys.append(",");
                    keys.append(key);
                    isFirst = false;
                }
            }
            keys.append("]");
            return keys.toString();
        });
    }

    public CompletableFuture<String> initBloomFilterAsync(String key, Integer expectedElements, Double falsePositiveRate) {
        log.info("BF.INIT ===> key: {}, expectedElements: {}, falsePositiveRate: {}", key,
                expectedElements, falsePositiveRate);
        if (key == null || expectedElements == null || falsePositiveRate == null) {
            return CompletableFuture.completedFuture("FAIL");
        }
        return submitToShard(key, () -> getSegment(key).internalInitBloomFilter(key, expectedElements, falsePositiveRate));
    }

    public CompletableFuture<Integer> removeBloomFilterAsync(String key) {
        log.info("BF.RM ===> key: {}", key);
        return submitToShard(key, () -> getSegment(key).internalRemoveBloomFilter(key));
    }

    public CompletableFuture<String> addBloomFilterAsync(String key, String value) {
        log.info("BF.ADD ===> key: {}, value: {}", key, value);
        if (key == null || value == null) {
            return CompletableFuture.completedFuture("FAIL");
        }
        return submitToShard(key, () -> getSegment(key).internalAddBloomFilter(key, value));
    }

    public CompletableFuture<Integer> existsBloomFilterAsync(String key, String value) {
        log.info("BF.EXISTS ===> key: {}, value: {}", key, value);
        return submitToShard(key, () -> getSegment(key).internalExistsBloomFilter(key, value));
    }

    public CompletableFuture<Integer> resetBloomFilterAsync(String key) {
        log.info("BF.RS ===> key: {}", key);
        return submitToShard(key, () -> getSegment(key).internalResetBloomFilter(key));
    }

    public CompletableFuture<String> zScoreAsync(String key, String member) {
        log.info("Z.SCR ===> key: {}, member: {}", key, member);
        return submitToShard(key, () -> getSegment(key).internalZScore(key, member));
    }

    public CompletableFuture<String> zGetByPositionAsync(String key, Integer index) {
        log.info("Z.POS ===> key: {}, position: {}", key, index);
        return submitToShard(key, () -> getSegment(key).internalZGetByPosition(key, index - 1));
    }

    public CompletableFuture<Integer> zIncrByAsync(String key, Double increment, String member) {
        log.info("Z.INCR ===> key: {}, member: {}, increment: {}", key, member, increment);
        return submitToShard(key, () -> getSegment(key).internalZIncrBy(key, increment, member));
    }

    public CompletableFuture<Integer> zRankAsync(String key, String member) {
        log.info("Z.RANK ===> key: {}, member: {}", key, member);
        return submitToShard(key, () -> getSegment(key).internalZRank(key, member));
    }

    public CompletableFuture<String> zAddAsync(String key, Double score, String member, String value) {
        log.info("Z.ADD ===> key: {}, score: {}, member: {}, value: {}", key, score, member, value);
        return submitToShard(key, () -> getSegment(key).internalZAdd(key, score, member, value));
    }

    public CompletableFuture<String> zTopAsync(String key, Integer top) {
        log.info("Z.TOP ===> key: {}, top: {}", key, top);
        return submitToShard(key, () -> getSegment(key).internalZTop(key, top));
    }

    public CompletableFuture<Integer> zRemAsync(String key, String member) {
        log.info("Z.RM ===> key: {}, member: {}", key, member);
        return submitToShard(key, () -> getSegment(key).internalZRem(key, member));
    }

    public CompletableFuture<Integer> zDelAsync(String key) {
        log.info("Z.DEL ===> key: {}", key);
        return submitToShard(key, () -> getSegment(key).internalZDel(key));
    }

    public CompletableFuture<String> zRangeByPositionsAsync(String key, Integer start, Integer stop) {
        log.info("Z.RANGE ===> key: {}, start: {}, stop: {}", key, start, stop);
        return submitToShard(key, () -> getSegment(key).internalZRangeByPositions(key, start - 1, stop - 1));
    }

    public CompletableFuture<String> zRangeByScoreAsync(String key, Double minScore, Double maxScore) {
        log.info("Z.RSCR ===> key: {}, minScore: {}, maxScore: {}", key, minScore, maxScore);
        return submitToShard(key, () -> getSegment(key).internalZRangeByScore(key, minScore, maxScore));
    }

    public CompletableFuture<String> geoAddAsync(String key, String member, Double lat, Double lon) {
        log.info("GEO.ADD ===> key: {}, member: {}, lat: {}, lon: {}", key, member, lat, lon);
        return submitToShard(key, () -> getSegment(key).internalGeoAdd(key, member, lat, lon));
    }

    public CompletableFuture<Integer> geoDelAsync(String key) {
        log.info("GEO.DEL ===> key: {}", key);
        return submitToShard(key, () -> getSegment(key).internalGeoDel(key));
    }

    public CompletableFuture<Integer> geoRemAsync(String key, String member) {
        log.info("GEO.RM ===> key: {}, member: {}", key, member);
        return submitToShard(key, () -> getSegment(key).internalGeoRm(key, member));
    }

    public CompletableFuture<String> geoGetAsync(String key, String member) {
        log.info("GEO.GET ===> key: {}, member: {}", key, member);
        return submitToShard(key, () -> getSegment(key).internalGeoGet(key, member));
    }

    public CompletableFuture<String> geoNbAsync(String key, String member) {
        log.info("GEO.NB ===> key: {}, member: {}", key, member);
        return submitToShard(key, () -> getSegment(key).internalGeoNb(key, member));
    }

    public CompletableFuture<String> geoSearchAsync(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) {
        log.info("GEO.SEARCH ===> key: {}, lat: {}, lon: {}, radius: {}, limit: {}", key,
                centerLat, centerLon, radiusMeters, limit);
        return submitToShard(key, () -> getSegment(key).internalGeoSearch(key, centerLat, centerLon, radiusMeters, limit));
    }

    public CompletableFuture<String> geoDistAsync(String key, String member1, String member2) {
        log.info("GEO.DIST ===> key: {}, member1: {}, member2: {}", key, member1, member2);
        return submitToShard(key, () -> getSegment(key).internalGeoDist(key, member1, member2));
    }

    public CompletableFuture<Integer> geoExistsAsync(String key, String member) {
        log.info("GEO.EXIST ===> key: {}, member: {}", key, member);
        return submitToShard(key, () -> getSegment(key).internalGeoExists(key, member));
    }

    public CompletableFuture<String> geoEncodeAsync(String key, String member) {
        log.info("GEO.ENCODE ===> key: {}, member: {}", key, member);
        return submitToShard(key, () -> getSegment(key).internalGeoEncode(key, member));
    }

    public CompletableFuture<String> hSetAsync(String key, String field, String value) {
        log.info("H.SET ===> key: {}, field: {}, value: {}", key, field, value);
        return submitToShard(key, () -> getSegment(key).internalHSet(key, field, value));
    }

    public CompletableFuture<String> hGetAsync(String key, String field) {
        log.info("H.GET ===> key: {}, field: {}", key, field);
        return submitToShard(key, () -> getSegment(key).internalHGet(key, field));
    }

    public CompletableFuture<Integer> hRmAsync(String key, String field) {
        log.info("H.RM ===> key: {}, field: {}", key, field);
        return submitToShard(key, () -> getSegment(key).internalHRm(key, field));
    }

    public CompletableFuture<Integer> hDelAsync(String key) {
        log.info("H.DEL ===> key: {}", key);
        return submitToShard(key, () -> getSegment(key).internalHDel(key));
    }

    public CompletableFuture<String> hGetAllAsync(String key) {
        log.info("H.ALL ===> key: {}", key);
        return submitToShard(key, () -> getSegment(key).internalHGetAll(key));
    }

    public String put(String key, String value, Long ttl, Boolean notExists) {
        return null;
    }

    public String get(String key) {
        return null;
    }

    public Integer delete(String key) {
        return null;
    }

    public Integer exists(String key) {
        return null;
    }

    public Integer clear(Command command) {
        return null;
    }

    public String getAllKeys(Command command) {
        return null;
    }

    public String initBloomFilter(String key, Integer expectedElements, Double falsePositiveRate) {
        return null;
    }

    public Integer removeBloomFilter(String key) {
        return null;
    }

    public String addBloomFilter(String key, String value) {
        return null;
    }

    public Integer resetBloomFilter(String key) {
        return null;
    }

    public Integer existsBloomFilter(String key, String value) {
        return null;
    }

    public String zRangeByPositions(String key, Integer start, Integer stop) {
        return null;
    }

    public String zRangeByScore(String key, Double minScore, Double maxScore) {
        return null;
    }

    public String zGetByPosition(String key, Integer index) {
        return null;
    }

    public Integer zIncrBy(String key, Double increment, String member) {
        return null;
    }

    public Integer zRem(String key, String member) {
        return null;
    }

    public Integer zDel(String key) {
        return null;
    }

    public Integer zRank(String key, String member) {
        return null;
    }

    public String zTop(String key, Integer top) {
        return null;
    }

    public String zAdd(String key, Double score, String member, String value) {
        return null;
    }

    public String zScore(String key, String member) {
        return null;
    }

    public String geoAdd(String key, String member, Double lat, Double lon) {
        return null;
    }

    public String geoSearch(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) {
        return null;
    }

    public String geoDist(String key, String member1, String member2) {
        return null;
    }

    public Integer geoDel(String key) {
        return null;
    }

    public Integer geoRem(String key, String member) {
        return null;
    }

    public String geoGet(String key, String member) {
        return null;
    }

    public String geoNb(String key, String member) {
        return null;
    }

    public Integer geoExists(String key, String member) {
        return null;
    }

    public String geoEncode(String key, String member) {
        return null;
    }

    public String hSet(String key, String field, String value) {
        return null;
    }

    public String hGet(String key, String field) {
        return null;
    }

    public Integer hRm(String key, String field) {
        return null;
    }

    public Integer hDel(String key) {
        return null;
    }

    public String hGetAll(String key) {
        return null;
    }

    private static class CacheSegment {
        private final long maxSegmentSize;
        private long currentSizeBytes = 0;
        private final Map<String, Value> pairsStorage = new HashMap<>();
        private final Map<String, BloomFilter<String>> bloomFiltersStorage = new HashMap<>();
        private final Map<String, VanillaSkipList<String, String>> skipListsStorage = new HashMap<>();
        private final Map<String, Map<String, Double>> memberScoresStorage = new HashMap<>();
        private final Map<String, GeoSkipList> geoHashStorage = new HashMap<>();
        private final Map<String, Long> memberToGeoHashStorage = new HashMap<>();
        private final Map<String, CompactHash> hashStorage = new HashMap<>();
        private final String BLOOM_FILTERS_KEY_PREFIX = "bf_";
        private final String SKIP_LISTS_KEY_PREFIX = "zs_";
        private final String GEO_KEY_PREFIX = "geo_";
        private final String HASH_KEY_PREFIX = "hs_";
        private final FrequencySketch sketch;
        private final BlockingQueue<Runnable> taskQueue = new LinkedTransferQueue<>();
        private long localOperationCount = 0;
        private static final int RESET_PERIOD = 100000;
        private final PriorityQueue<TTLEntry> ttlHeap = new PriorityQueue<>();
        private static final int CLEANUP_BATCH_LIMIT = 100;

        public CacheSegment(int segmentId, long maxSegmentSize, int segmentExpectedKeys) {
            this.maxSegmentSize = maxSegmentSize;
            this.sketch = new FrequencySketch(segmentExpectedKeys);

            // Khởi chạy vòng lặp Event Loop vô hạn dành riêng cho Shard
            // Block thread khi queue trống (0% CPU idle)
            // Luồng vật lý cố định chịu trách nhiệm cho Shard này
            Thread.ofPlatform()
                    .name("minicache-shard-" + segmentId)
                    .start(() -> {
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                Runnable task = taskQueue.take();
                                task.run();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            } catch (Exception e) {
                                log.error("Error in Shard Event Loop", e);
                            }
                        }
                    });
        }

        public void submitTask(Runnable task) {
            taskQueue.add(task);
        }

        private void checkAndAgeLocal() {
            if (++localOperationCount % RESET_PERIOD == 0) {
                sketch.age();
            }
        }

        public void internalCleanExpiredKeys() {
            if (ttlHeap.isEmpty()) {
                return;
            }

            long now = System.currentTimeMillis();
            int cleanedCount = 0;

            while (!ttlHeap.isEmpty() && cleanedCount < CLEANUP_BATCH_LIMIT) {
                TTLEntry top = ttlHeap.peek();

                if (top.expireTime() > now) {
                    break;
                }

                ttlHeap.poll();

                Value value = pairsStorage.get(top.key());
                if (value != null && value.getTtl() != null && value.getTtl() <= now) {
                    pairsStorage.remove(top.key());
                    currentSizeBytes -= value.getSize();
                    cleanedCount++;
                }
            }
        }

        public String internalPut(String key, String value, Long ttl, Boolean notExists) {
            checkAndAgeLocal();
            if (notExists != null && notExists) {
                Value exVal = pairsStorage.get(key);
                if (exVal != null && (exVal.getTtl() == null || System.currentTimeMillis() < exVal.getTtl())) {
                    return "FAIL";
                }
            }

            Value newVal = new Value();
            newVal.setData(value);

            long expireTime = -1;
            if (ttl != null && ttl > 0) {
                expireTime = System.currentTimeMillis() + ttl;
                newVal.setTtl(expireTime);
            }

            long newEntrySize = calculateSize(key, newVal.getData(), newVal.getTtl());
            newVal.setSize(newEntrySize);

            Value oldVal = pairsStorage.put(key, newVal);
            if (oldVal != null) {
                currentSizeBytes -= oldVal.getSize();
            }

            if (expireTime > 0) {
                ttlHeap.offer(new TTLEntry(key, expireTime));
            }

            currentSizeBytes += newEntrySize;
            sketch.increment(key);

            if (currentSizeBytes > maxSegmentSize) {
                evictUsingTinyLFU(key);
            }
            return "OK";
        }

        public String internalGet(String key) {
            if (pairsStorage.isEmpty()) return null;

            Value value = pairsStorage.get(key);
            if (value != null) {
                if (value.getTtl() != null && System.currentTimeMillis() > value.getTtl()) {
                    pairsStorage.remove(key);
                    currentSizeBytes -= value.getSize();
                    return null;
                }
                sketch.increment(key);
                return value.getData();
            }
            return null;
        }

        public Integer internalDelete(String key) {
            if (pairsStorage.isEmpty()) return 0;
            Value value = pairsStorage.remove(key);
            if (value != null) {
                currentSizeBytes -= value.getSize();
                return 1;
            }
            return 0;
        }

        public Integer internalExists(String key) {
            if (pairsStorage.isEmpty()) return 0;
            Value val = pairsStorage.get(key);
            if (val == null) return 0;

            if (val.getTtl() != null && System.currentTimeMillis() > val.getTtl()) {
                pairsStorage.remove(key);
                currentSizeBytes -= val.getSize();
                return 0;
            }
            return 1;
        }

        public void internalClear() {
            pairsStorage.clear();
            bloomFiltersStorage.clear();
            skipListsStorage.clear();
            memberScoresStorage.clear();
            sketch.reset();
            currentSizeBytes = 0;
            localOperationCount = 0;
            geoHashStorage.clear();
            memberToGeoHashStorage.clear();
            hashStorage.clear();
            ttlHeap.clear();
        }

        private void evictUsingTinyLFU(String candidateKey) {
            long candidateFreq = sketch.frequency(candidateKey);

            while (currentSizeBytes > maxSegmentSize) {
                List<String> sampleKeys = getSampleKeys();
                if (sampleKeys.isEmpty()) break;

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

            while (samples.size() < sampleSize) {
                Set<String> targetSet = stores.get(random.nextInt(stores.size()));
                Iterator<String> iterator = targetSet.iterator();
                if (iterator.hasNext()) {
                    int skipSteps = random.nextInt(Math.min(100, targetSet.size()));
                    String selectedKey = null;
                    for (int i = 0; i <= skipSteps && iterator.hasNext(); i++) {
                        selectedKey = iterator.next();
                    }
                    if (selectedKey != null && !samples.contains(selectedKey)) {
                        samples.add(selectedKey);
                    }
                }
                long totalCurrentKeys = (long) pairsStorage.size() + bloomFiltersStorage.size()
                        + skipListsStorage.size() + geoHashStorage.size() + hashStorage.size();
                if (samples.size() >= totalCurrentKeys) break;
            }
            return samples;
        }

        private void evictKeyFromStorage(String key) {
            Value removedPairs = pairsStorage.remove(key);
            if (removedPairs != null) {
                currentSizeBytes -= removedPairs.getSize();
                return;
            }

            BloomFilter<String> removedBloom = bloomFiltersStorage.remove(key);
            if (removedBloom != null) {
                currentSizeBytes -= (removedBloom.getSize() + key.getBytes(StandardCharsets.UTF_8).length);
                return;
            }

            VanillaSkipList<String, String> removedSkipList = skipListsStorage.remove(key);
            memberScoresStorage.remove(key);
            if (removedSkipList != null) {
                long keySize = key.getBytes(StandardCharsets.UTF_8).length;
                currentSizeBytes -= (keySize + (removedSkipList.size() * 180L));
            }

            GeoSkipList removedGeo = geoHashStorage.remove(key);
            if (removedGeo != null) {
                memberToGeoHashStorage.keySet().removeIf(k -> k.startsWith(key + ":"));
                long keySize = key.getBytes(StandardCharsets.UTF_8).length;
                currentSizeBytes -= keySize;
            }

            CompactHash removedHash = hashStorage.remove(key);
            if (removedHash != null) {
                long keyMetadataBytes = key.getBytes(StandardCharsets.UTF_8).length + 32;
                long totalFreedBytes = keyMetadataBytes + removedHash.getEstimatedBytes();
                currentSizeBytes -= totalFreedBytes;
            }
        }

        private long calculateSize(String key, String value, Long ttl) {
            if (key == null || value == null) return 0;
            long baseSize = key.getBytes(StandardCharsets.UTF_8).length + value.getBytes(StandardCharsets.UTF_8).length;
            return (ttl != null && ttl > 0) ? baseSize + 8 : baseSize;
        }

        public String internalInitBloomFilter(String key, Integer expectedElements, Double falsePositiveRate) {
            checkAndAgeLocal();
            key = BLOOM_FILTERS_KEY_PREFIX + key;
            BloomFilter<String> newEntry = new BloomFilter<>(expectedElements, falsePositiveRate);
            var keySize = key.getBytes(StandardCharsets.UTF_8).length;

            var exist = bloomFiltersStorage.get(key);
            if (exist != null) currentSizeBytes -= (exist.getSize() + keySize);

            bloomFiltersStorage.put(key, newEntry);
            currentSizeBytes += (newEntry.getSize() + keySize);
            sketch.increment(key);

            if (currentSizeBytes > maxSegmentSize) evictUsingTinyLFU(key);
            return "OK";
        }

        public Integer internalRemoveBloomFilter(String key) {
            key = BLOOM_FILTERS_KEY_PREFIX + key;
            if (bloomFiltersStorage.isEmpty() || bloomFiltersStorage.get(key) == null) return 0;

            var value = bloomFiltersStorage.remove(key);
            if (value != null) {
                currentSizeBytes -= (key.getBytes(StandardCharsets.UTF_8).length + value.getSize());
            }
            return 1;
        }

        public String internalAddBloomFilter(String key, String value) {
            key = BLOOM_FILTERS_KEY_PREFIX + key;
            var bloom = bloomFiltersStorage.get(key);
            if (bloom == null) return "FAIL";
            bloom.add(value);
            sketch.increment(key);
            return "OK";
        }

        public Integer internalExistsBloomFilter(String key, String value) {
            key = BLOOM_FILTERS_KEY_PREFIX + key;
            var bloom = bloomFiltersStorage.get(key);
            if (bloom == null) return 0;
            if (value == null || value.isBlank()) return 1;
            sketch.increment(key);
            return bloom.mightContain(value) ? 1 : 0;
        }

        public Integer internalResetBloomFilter(String key) {
            key = BLOOM_FILTERS_KEY_PREFIX + key;
            var bloom = bloomFiltersStorage.get(key);
            if (bloom == null) return 0;
            bloom.reset();
            return 1;
        }

        public String internalZScore(String key, String member) {
            key = SKIP_LISTS_KEY_PREFIX + key;
            var memScore = memberScoresStorage.get(key);
            if (memScore == null) return null;
            Double score = memScore.get(member);
            return score != null ? String.valueOf(score) : null;
        }

        public String internalZRangeByPositions(String key, int start, int stop) {
            String internalKey = SKIP_LISTS_KEY_PREFIX + key;
            VanillaSkipList<String, String> skipList = skipListsStorage.get(internalKey);
            if (skipList == null) return null;

            sketch.increment(internalKey);
            var res = skipList.getRangeByPositions(start, stop);
            StringBuilder results = new StringBuilder("[");
            boolean isFirst = true;
            for (String val : res) {
                if (!isFirst) results.append(",");
                results.append(val);
                isFirst = false;
            }
            results.append("]");
            return results.toString();
        }

        public String internalZRangeByScore(String key, double minScore, double maxScore) {
            String internalKey = SKIP_LISTS_KEY_PREFIX + key;
            VanillaSkipList<String, String> skipList = skipListsStorage.get(internalKey);
            if (skipList == null) return null;

            sketch.increment(internalKey);
            var res = skipList.getRangeByScore(minScore, maxScore);
            StringBuilder results = new StringBuilder("[");
            boolean isFirst = true;
            for (String val : res) {
                if (!isFirst) results.append(",");
                results.append(val);
                isFirst = false;
            }
            results.append("]");
            return results.toString();
        }

        public String internalZGetByPosition(String key, int index) {
            String internalKey = SKIP_LISTS_KEY_PREFIX + key;
            VanillaSkipList<String, String> skipList = skipListsStorage.get(internalKey);
            if (skipList == null) return null;

            sketch.increment(internalKey);
            var res = skipList.getByPosition(index);
            return (res == null || res.isBlank()) ? null : res;
        }

        public Integer internalZIncrBy(String key, double increment, String member) {
            checkAndAgeLocal();
            String internalKey = SKIP_LISTS_KEY_PREFIX + key;

            VanillaSkipList<String, String> skipList = skipListsStorage.computeIfAbsent(internalKey, k -> new VanillaSkipList<>());
            Map<String, Double> memberScores = memberScoresStorage.computeIfAbsent(internalKey, k -> new HashMap<>());

            Double oldScore = memberScores.get(member);
            double newScore = (oldScore == null) ? increment : oldScore + increment;

            boolean check = false;
            String targetVal = member;
            if (oldScore != null) {
                String oldVal = skipList.remove(oldScore, member);
                if (oldVal != null) {
                    check = true;
                    targetVal = oldVal;
                }
            }

            memberScores.put(member, newScore);
            skipList.put(newScore, member, targetVal);
            sketch.increment(internalKey);
            return check ? 1 : 0;
        }

        public Integer internalZRank(String key, String member) {
            String internalKey = SKIP_LISTS_KEY_PREFIX + key;
            VanillaSkipList<String, String> skipList = skipListsStorage.get(internalKey);
            Map<String, Double> memberScores = memberScoresStorage.get(internalKey);

            if (skipList == null || memberScores == null) return -1;
            Double score = memberScores.get(member);
            if (score == null) return -1;

            sketch.increment(internalKey);
            return skipList.getRank(score, member) + 1;
        }

        public String internalZAdd(String key, double score, String member, String value) {
            checkAndAgeLocal();
            String internalKey = SKIP_LISTS_KEY_PREFIX + key;

            VanillaSkipList<String, String> skipList = skipListsStorage.computeIfAbsent(internalKey, k -> new VanillaSkipList<>());
            Map<String, Double> memberScores = memberScoresStorage.computeIfAbsent(internalKey, k -> new HashMap<>());

            Double oldScore = memberScores.get(member);
            if (oldScore != null) {
                if (oldScore == score) return "FAIL";
                skipList.remove(oldScore, member);
            }

            memberScores.put(member, score);
            boolean isNew = skipList.put(score, member, value);

            if (isNew && oldScore == null) {
                long memberBytes = member.getBytes(StandardCharsets.UTF_8).length;
                long valueBytes = value.getBytes(StandardCharsets.UTF_8).length;
                currentSizeBytes += (memberBytes + valueBytes + 32);
                if (currentSizeBytes > maxSegmentSize) evictUsingTinyLFU(internalKey);
            }
            return "OK";
        }

        public String internalZTop(String key, Integer top) {
            checkAndAgeLocal();
            String internalKey = SKIP_LISTS_KEY_PREFIX + key;
            var skipList = skipListsStorage.get(internalKey);

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
        }

        public Integer internalZRem(String key, String member) {
            checkAndAgeLocal();
            String internalKey = SKIP_LISTS_KEY_PREFIX + key;
            VanillaSkipList<String, String> skipList = skipListsStorage.get(internalKey);
            Map<String, Double> memberScores = memberScoresStorage.get(internalKey);

            if (skipList == null || memberScores == null) return 0;
            Double score = memberScores.remove(member);
            if (score != null) {
                String removed = skipList.remove(score, member);
                if (removed != null) {
                    long freedBytes = member.getBytes(StandardCharsets.UTF_8).length + removed.getBytes(StandardCharsets.UTF_8).length + 32;
                    currentSizeBytes = Math.max(0, currentSizeBytes - freedBytes);
                    return 1;
                }
            }
            return 0;
        }

        public Integer internalZDel(String key) {
            String internalKey = SKIP_LISTS_KEY_PREFIX + key;
            VanillaSkipList<String, String> skipList = skipListsStorage.remove(internalKey);
            memberScoresStorage.remove(internalKey);

            if (skipList != null) {
                long keySize = key.getBytes(StandardCharsets.UTF_8).length;
                long totalFreedBytes = keySize + (skipList.size() * 180L);
                currentSizeBytes = Math.max(0, currentSizeBytes - totalFreedBytes);
                return 1;
            }
            return 0;
        }

        public String internalGeoAdd(String key, String member, Double lat, Double lon) {
            if (lat < GeoHashHelper.MIN_LAT || lat > GeoHashHelper.MAX_LAT
                    || lon < GeoHashHelper.MIN_LON || lon > GeoHashHelper.MAX_LON) {
                return "FAIL";
            }
            checkAndAgeLocal();
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
                currentSizeBytes += (memberBytes + estimatedNodeSizeBytes + 32);
                if (currentSizeBytes > maxSegmentSize) evictUsingTinyLFU(internalKey);
            }
            sketch.increment(internalKey);

            return "OK";
        }

        public Integer internalGeoDel(String key) {
            checkAndAgeLocal();
            String internalKey = GEO_KEY_PREFIX + key;
            GeoSkipList removedSkipList = geoHashStorage.remove(internalKey);

            if (removedSkipList != null) {
                memberToGeoHashStorage.keySet().removeIf(k -> k.startsWith(internalKey + ":"));

                long keyBytes = internalKey.getBytes(StandardCharsets.UTF_8).length;
                currentSizeBytes -= keyBytes;
                return 1;
            }
            sketch.increment(internalKey);
            return 0;
        }

        public Integer internalGeoRm(String key, String member) {
            checkAndAgeLocal();
            String internalKey = GEO_KEY_PREFIX + key;
            GeoSkipList geoSkipList = geoHashStorage.get(internalKey);

            if (geoSkipList == null) {
                return 0;
            }

            String globalMemberKey = internalKey + ":" + member;
            Long geoHash = memberToGeoHashStorage.remove(globalMemberKey);

            if (geoHash == null) {
                return 0;
            }

            geoSkipList.removeMember(geoHash, member);

            long memberBytes = member.getBytes(StandardCharsets.UTF_8).length;
            long estimatedNodeSizeBytes = 48 + memberBytes + 16 + (16 * 8);
            currentSizeBytes -= estimatedNodeSizeBytes;

            sketch.increment(internalKey);
            return 1;
        }

        public String internalGeoGet(String key, String member) {
            checkAndAgeLocal();
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
        }

        public String internalGeoSearch(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) {
            checkAndAgeLocal();
            if (key == null || radiusMeters == null || radiusMeters <= 0 || centerLat == null || centerLon == null) {
                return "[]";
            }

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
        }

        public String internalGeoDist(String key, String member1, String member2) {
            checkAndAgeLocal();
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
        }

        public String internalGeoNb(String key, String member) {
            checkAndAgeLocal();
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
            res.append("\"CT\":");
            double[] coords = GeoHashHelper.decode(centerGeoHash);
            double lat = coords[0];
            double lon = coords[1];
            String valCt = String.format(Locale.US, "[%.6f,%.6f]", lat, lon);
            res.append(valCt);
            res.append("}");
            return res.toString();
        }

        public Integer internalGeoExists(String key, String member) {
            checkAndAgeLocal();
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
        }

        public String internalGeoEncode(String key, String member) {
            checkAndAgeLocal();
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
        }

        public String internalHGet(String key, String field) {
            checkAndAgeLocal();
            String internalKey = HASH_KEY_PREFIX + key;
            CompactHash hash = hashStorage.get(internalKey);
            if (hash == null) {
                return null;
            }

            sketch.increment(internalKey);
            return hash.get(field);
        }

        public String internalHSet(String key, String field, String value) {
            checkAndAgeLocal();
            String internalKey = HASH_KEY_PREFIX + key;
            boolean isNewHashKey = !hashStorage.containsKey(internalKey);

            CompactHash hash = hashStorage.computeIfAbsent(internalKey, k -> new CompactHash());
            CompactHash.PutResult putResult = hash.put(field, value);
            long keyMetadataBytes = isNewHashKey
                    ? internalKey.getBytes(StandardCharsets.UTF_8).length + 32
                    : 0;

            long totalDeltaBytes = keyMetadataBytes + putResult.memoryDelta();

            currentSizeBytes += totalDeltaBytes;
            sketch.increment(internalKey);

            if (currentSizeBytes > maxSegmentSize) {
                evictUsingTinyLFU(internalKey);
            }

            return "OK";
        }

        public Integer internalHRm(String key, String field) {
            checkAndAgeLocal();
            String internalKey = HASH_KEY_PREFIX + key;
            CompactHash hash = hashStorage.get(internalKey);
            if (hash == null) return 0;

            CompactHash.RemoveResult removeResult = hash.remove(field);

            if (removeResult.oldValue() != null) {
                currentSizeBytes -= removeResult.freedBytes();

                if (hash.isEmpty()) {
                    hashStorage.remove(internalKey);
                    long freedKeyMetadata = internalKey.getBytes(StandardCharsets.UTF_8).length + 32 + hash.getEstimatedBytes();
                    currentSizeBytes -= freedKeyMetadata;
                }

                sketch.increment(internalKey);
                return 1;
            }
            return 0;
        }

        public String internalHGetAll(String key) {
            checkAndAgeLocal();
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
        }

        public Integer internalHDel(String key) {
            checkAndAgeLocal();
            String internalKey = HASH_KEY_PREFIX + key;
            CompactHash removedHash = hashStorage.remove(internalKey);

            if (removedHash != null) {
                long keyMetadataBytes = internalKey.getBytes(StandardCharsets.UTF_8).length + 32;
                long totalFreedBytes = keyMetadataBytes + removedHash.getEstimatedBytes();

                currentSizeBytes -= totalFreedBytes;
                sketch.increment(internalKey);
                return 1;
            }

            return 0;
        }
    }
}