package org.minicache.engine.sharednothing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.minicache.common.Command;
import org.minicache.common.Value;
import org.minicache.struct.bloomfilter.BloomFilter;
import org.minicache.struct.skiplist.VanillaSkipList;
import org.minicache.struct.freqsketch.FrequencySketch;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class StorageEngine extends org.minicache.engine.StorageEngine {
    private static final Logger log = LogManager.getLogger(StorageEngine.class);
    private final int segmentMask;
    private final CacheSegment[] segments;
    private final Map<String, String> initCfg;

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
        // Số lượng Segment bằng đúng số Cores vật lý của CPU
        return Integer.highestOneBit(Runtime.getRuntime().availableProcessors());
    }

    public String put(String key, String value, Long ttl, Boolean notExists) {
        log.info("PUT ===> key: {}, value: {}, not_exists: {}, ttl: {}", key, value, notExists, ttl);
        if (key == null || value == null) return "FAIL";

        CacheSegment segment = getSegment(key);
        CompletableFuture<String> future = new CompletableFuture<>();

        segment.submitTask(() -> {
            String res = segment.internalPut(key, value, ttl, notExists);
            future.complete(res);
        });

        return future.join();
    }

    public String get(String key) {
        log.info("GET ===> key: {}", key);
        if (key == null) return null;

        CacheSegment segment = getSegment(key);
        CompletableFuture<String> future = new CompletableFuture<>();

        segment.submitTask(() -> {
            String res = segment.internalGet(key);
            future.complete(res);
        });

        return future.join();
    }

    public Integer delete(String key) {
        log.info("DEL ===> key: {}", key);
        if (key == null) return 0;

        CacheSegment segment = getSegment(key);
        CompletableFuture<Integer> future = new CompletableFuture<>();

        segment.submitTask(() -> {
            Integer res = segment.internalDelete(key);
            future.complete(res);
        });

        return future.join();
    }

    public Integer exists(String key) {
        log.info("EXISTS ===> key: {}", key);
        if (key == null) return 0;

        CacheSegment segment = getSegment(key);
        CompletableFuture<Integer> future = new CompletableFuture<>();

        segment.submitTask(() -> {
            Integer res = segment.internalExists(key);
            future.complete(res);
        });

        return future.join();
    }

    public Integer clear(Command command) {
        log.info("CLEAR ===> Clear all keys and reset to default");
        if (!Command.CLEAR.equals(command)) return 0;

        // Broadcast lệnh clear đến tất cả các Shards một cách bất đồng bộ
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
        CompletableFuture.allOf(futures).join();
        return 1;
    }

    @SuppressWarnings("unchecked")
    public String getAllKeys(Command command) {
        log.info("KEYS ===> Fetch all keys");
        if (!Command.LST_KEY.equals(command)) return "[]";

        // Gom Key từ tất cả các Shards một cách an toàn thông qua Thread tương ứng
        CompletableFuture<List<String>>[] futures = new CompletableFuture[segments.length];
        for (int i = 0; i < segments.length; i++) {
            CompletableFuture<List<String>> f = new CompletableFuture<>();
            futures[i] = f;
            CacheSegment seg = segments[i];
            seg.submitTask(() -> {
                List<String> shardKeys = new ArrayList<>();
                shardKeys.addAll(seg.pairsStorage.keySet());
                shardKeys.addAll(seg.bloomFiltersStorage.keySet());
                shardKeys.addAll(seg.skipListsStorage.keySet());
                f.complete(shardKeys);
            });
        }

        CompletableFuture.allOf(futures).join();

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
    }

    public String initBloomFilter(String key, Integer expectedElements, Double falsePositiveRate) {
        log.info("BF.INIT ===> key: {}, expectedElements: {}, falsePositiveRate: {}", key, expectedElements, falsePositiveRate);
        if (key == null || expectedElements == null || falsePositiveRate == null) return "FAIL";

        CacheSegment segment = getSegment(key);
        CompletableFuture<String> future = new CompletableFuture<>();

        segment.submitTask(() -> {
            String res = segment.internalInitBloomFilter(key, expectedElements, falsePositiveRate);
            future.complete(res);
        });

        return future.join();
    }

    public Integer removeBloomFilter(String key) {
        log.info("BF.RM ===> key: {}", key);
        if (key == null) return 0;

        CacheSegment segment = getSegment(key);
        CompletableFuture<Integer> future = new CompletableFuture<>();

        segment.submitTask(() -> {
            Integer res = segment.internalRemoveBloomFilter(key);
            future.complete(res);
        });

        return future.join();
    }

    public String addBloomFilter(String key, String value) {
        log.info("BF.ADD ===> key: {}, value: {}", key, value);
        if (key == null || value == null) return "FAIL";

        CacheSegment segment = getSegment(key);
        CompletableFuture<String> future = new CompletableFuture<>();

        segment.submitTask(() -> {
            String res = segment.internalAddBloomFilter(key, value);
            future.complete(res);
        });

        return future.join();
    }

    public Integer resetBloomFilter(String key) {
        log.info("BF.RS ===> key: {}", key);
        if (key == null) return 0;

        CacheSegment segment = getSegment(key);
        CompletableFuture<Integer> future = new CompletableFuture<>();

        segment.submitTask(() -> {
            Integer res = segment.internalResetBloomFilter(key);
            future.complete(res);
        });

        return future.join();
    }

    public Integer existsBloomFilter(String key, String value) {
        log.info("BF.EXISTS ===> key: {}, value: {}", key, value);
        if (key == null) return 0;

        CacheSegment segment = getSegment(key);
        CompletableFuture<Integer> future = new CompletableFuture<>();

        segment.submitTask(() -> {
            Integer res = segment.internalExistsBloomFilter(key, value);
            future.complete(res);
        });

        return future.join();
    }

    public String zRangeByPositions(String key, Integer start, Integer stop) {
        log.info("Z.RANGE ===> key: {}, start: {}, stop: {}", key, start, stop);
        CacheSegment segment = getSegment(key);
        CompletableFuture<String> future = new CompletableFuture<>();
        segment.submitTask(() -> future.complete(segment.internalZRangeByPositions(key, start - 1, stop - 1)));
        return future.join();
    }

    public String zRangeByScore(String key, Double minScore, Double maxScore) {
        log.info("Z.RSCR ===> key: {}, minScore: {}, maxScore: {}", key, minScore, maxScore);
        CacheSegment segment = getSegment(key);
        CompletableFuture<String> future = new CompletableFuture<>();
        segment.submitTask(() -> future.complete(segment.internalZRangeByScore(key, minScore, maxScore)));
        return future.join();
    }

    public String zGetByPosition(String key, Integer index) {
        log.info("Z.POS ===> key: {}, position: {}", key, index);
        CacheSegment segment = getSegment(key);
        CompletableFuture<String> future = new CompletableFuture<>();
        segment.submitTask(() -> future.complete(segment.internalZGetByPosition(key, index - 1)));
        return future.join();
    }

    public Integer zIncrBy(String key, Double increment, String member) {
        log.info("Z.INCR ===> key: {}, member: {}, increment: {}", key, member, increment);
        CacheSegment segment = getSegment(key);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        segment.submitTask(() -> future.complete(segment.internalZIncrBy(key, increment, member)));
        return future.join();
    }

    public Integer zRem(String key, String member) {
        log.info("Z.RM ===> key: {}", key);
        CacheSegment segment = getSegment(key);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        segment.submitTask(() -> future.complete(segment.internalZRem(key, member)));
        return future.join();
    }

    public Integer zDel(String key) {
        log.info("Z.DEL ===> key: {}", key);
        CacheSegment segment = getSegment(key);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        segment.submitTask(() -> future.complete(segment.internalZDel(key)));
        return future.join();
    }

    public Integer zRank(String key, String member) {
        log.info("Z.RANK ===> key: {}, member: {}", key, member);
        CacheSegment segment = getSegment(key);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        segment.submitTask(() -> future.complete(segment.internalZRank(key, member)));
        return future.join();
    }

    public String zAdd(String key, Double score, String member, String value) {
        log.info("Z.ADD ===> key: {}, score: {}, member: {}, value: {}", key, score, member, value);
        CacheSegment segment = getSegment(key);
        CompletableFuture<String> future = new CompletableFuture<>();
        segment.submitTask(() -> future.complete(segment.internalZAdd(key, score, member, value)));
        return future.join();
    }

    public String zScore(String key, String member) {
        log.info("Z.SCR ===> key: {}, member: {}", key, member);
        CacheSegment segment = getSegment(key);
        CompletableFuture<String> future = new CompletableFuture<>();
        segment.submitTask(() -> future.complete(segment.internalZScore(key, member)));
        return future.join();
    }

    private static class CacheSegment {
        private final long maxSegmentSize;
        private long currentSizeBytes = 0;
        private final Map<String, Value> pairsStorage = new HashMap<>();
        private final Map<String, BloomFilter<String>> bloomFiltersStorage = new HashMap<>();
        private final Map<String, VanillaSkipList<String, String>> skipListsStorage = new HashMap<>();
        private final Map<String, Map<String, Double>> memberScoresStorage = new HashMap<>();
        private final String BLOOM_FILTERS_KEY_PREFIX = "bf_";
        private final String SKIP_LISTS_KEY_PREFIX = "zs_";
        private final FrequencySketch sketch;
        private final BlockingQueue<Runnable> taskQueue = new LinkedTransferQueue<>();
        private long localOperationCount = 0;
        private static final int RESET_PERIOD = 100000;

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
            if (ttl != null && ttl > 0) {
                newVal.setTtl(System.currentTimeMillis() + ttl);
            }

            long newEntrySize = calculateSize(key, newVal.getData(), newVal.getTtl());
            newVal.setSize(newEntrySize);

            Value oldVal = pairsStorage.put(key, newVal);
            if (oldVal != null) {
                currentSizeBytes -= oldVal.getSize();
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
                long totalCurrentKeys = (long) pairsStorage.size() + bloomFiltersStorage.size() + skipListsStorage.size();
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
    }
}