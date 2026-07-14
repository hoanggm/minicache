package org.minicache.engine.single;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.minicache.common.Command;
import org.minicache.common.Value;
import org.minicache.struct.bloomfilter.BloomFilter;
import org.minicache.struct.skiplist.ConcurrentSkipList;
import org.minicache.struct.freqsketch.FrequencySketch;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CacheEngine extends org.minicache.engine.CacheEngine {
    private static final Logger log = LogManager.getLogger(CacheEngine.class);
    private final long maxCacheSize;
    private final AtomicLong currentSizeBytes;
    private final ConcurrentHashMap<String, Value> pairsStorage;
    private final ConcurrentHashMap<String, BloomFilter<String>> bloomFiltersStorage;
    private final ConcurrentHashMap<String, ConcurrentSkipList<String, String>> skipListsStorage;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> memberScoresStorage;
    private final FrequencySketch sketch;
    private final AtomicLong operationCount;
    private static final int RESET_PERIOD = 100000;
    private final String BLOOM_FILTERS_KEY_PREFIX = "bf_";
    private final String SKIP_LISTS_KEY_PREFIX = "zs_";

    public CacheEngine(long maxSize) {
        this.maxCacheSize = maxSize * 1024 * 1024;
        this.currentSizeBytes = new AtomicLong(0);
        this.pairsStorage = new ConcurrentHashMap<>();
        this.bloomFiltersStorage = new ConcurrentHashMap<>();
        this.skipListsStorage = new ConcurrentHashMap<>();
        memberScoresStorage = new ConcurrentHashMap<>();
        this.sketch = new FrequencySketch(10000000);
        this.operationCount = new AtomicLong(0);
    }

    private void evictUsingTinyLFU(String candidateKey) {
        long candidateFreq = sketch.frequency(candidateKey);

        // Vòng lặp dọn dẹp cho đến khi đạt ngưỡng an toàn
        while (currentSizeBytes.get() > maxCacheSize) {

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

        // Gom cả 3 keySet vào một danh sách các Set để tiện chọn lựa
        List<Set<String>> stores = new ArrayList<>();
        if (!pairsStorage.isEmpty()) stores.add(pairsStorage.keySet());
        if (!bloomFiltersStorage.isEmpty()) stores.add(bloomFiltersStorage.keySet());
        if (!skipListsStorage.isEmpty()) stores.add(skipListsStorage.keySet());

        if (stores.isEmpty()) return samples;

        // Bốc mẫu cho đến khi đủ số lượng yêu cầu
        while (samples.size() < sampleSize) {
            // 1. Chọn ngẫu nhiên 1 trong 3 Store (KV, Bloom, hoặc SkipList)
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
            long totalCurrentKeys = (long) pairsStorage.size() + bloomFiltersStorage.size() + skipListsStorage.size();
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
    }

    private long calculateSize(String key, String value, Long ttl) {
        if (key == null || value == null) return 0;

        long baseSize = key.getBytes(StandardCharsets.UTF_8).length
                + value.getBytes(StandardCharsets.UTF_8).length;

        return (ttl != null && ttl > 0) ? baseSize + 8 : baseSize;
    }

    public Map<String, String> getInitCfg() {
        return null;
    }

    public String put(String key, String value, Long ttl, Boolean notExists) {
        log.info("PUT ===> key: {}, value: {}, not_exists: {}, ttl: {}", key, value, notExists, ttl);
        if (key == null || value == null)
            return "FAIL";

        if (notExists != null && notExists) {
            var exVal = pairsStorage.get(key);
            if (exVal != null) {
                if (exVal.getTtl() == null || System.currentTimeMillis() < exVal.getTtl()) {
                    return "FAIL";
                }
            }
        }

        final Value newVal = new Value();
        newVal.setData(value);
        if (ttl != null && ttl > 0) {
            newVal.setTtl(System.currentTimeMillis() + ttl);
        }
        final long newEntrySize = calculateSize(key, newVal.getData(), newVal.getTtl());
        newVal.setSize(newEntrySize);

        pairsStorage.compute(key, (oldKey, oldVal) -> {
            if (oldVal != null) {
                currentSizeBytes.addAndGet(-oldVal.getSize());
            }
            currentSizeBytes.addAndGet(newEntrySize);

            return newVal;
        });
        sketch.increment(key);

        if (currentSizeBytes.get() > maxCacheSize) {
            evictUsingTinyLFU(key);
        }

        if (operationCount.incrementAndGet() % RESET_PERIOD == 0) {
            sketch.age();
        }

        return "OK";
    }

    public String get(String key) {
        log.info("GET ===> key: {}", key);
        if (key == null) return null;

        if (pairsStorage.isEmpty()) return null;

        var value = pairsStorage.get(key);
        if (value != null) {
            if (value.getTtl() != null && System.currentTimeMillis() > value.getTtl()) {
                if (pairsStorage.remove(key, value)) {
                    currentSizeBytes.addAndGet(-value.getSize());
                }
                return null;
            }
            sketch.increment(key);
        }
        return value != null ? value.getData() : null;
    }

    public Integer delete(String key) {
        log.info("DEL ===> key: {}", key);
        if (key == null) return 0;

        if (pairsStorage.isEmpty()) return 0;

        var value = pairsStorage.remove(key);
        if (value != null) {
            currentSizeBytes.addAndGet(-value.getSize());
        }
        return 1;
    }

    public Integer exists(String key) {
        log.info("EXISTS ===> key: {}", key);
        if (key == null) return 0;

        if (pairsStorage.isEmpty()) return 0;

        var val = pairsStorage.get(key);
        if (val == null) {
            return 0;
        }
        if (val.getTtl() != null && System.currentTimeMillis() > val.getTtl()) {
            if (pairsStorage.remove(key, val)) {
                currentSizeBytes.addAndGet(-val.getSize());
            }
            return 0;
        }

        return 1;
    }

    public Integer clear(Command command) {
        log.info("CLEAR ===> Clear all keys and reset to default");
        if (Command.CLEAR.equals(command)) {
            pairsStorage.clear();
            bloomFiltersStorage.clear();
            sketch.reset();
            memberScoresStorage.clear();
            skipListsStorage.clear();
            currentSizeBytes.set(0);
            operationCount.set(0);
            currentSizeBytes.set(0);
            return 1;
        }
        return 0;
    }

    public String getAllKeys(Command command) {
        log.info("KEYS ===> Fetch all keys");
        if (Command.LST_KEY.equals(command)) {
            // for key-value storage
            int totalKeys = pairsStorage.size();
            int estimatedCapacityPairs = totalKeys * 15 + 2;
            // for bloom-filter storage
            int totalBFKeys = bloomFiltersStorage.size();
            int estimatedCapacityBF = totalBFKeys * 15 + 2;
            // total
            int estimatedCapacity = estimatedCapacityPairs + estimatedCapacityBF;

            StringBuilder keys = new StringBuilder(estimatedCapacity);
            keys.append("[");

            var allKeys = pairsStorage.keys();
            boolean isFirst = true;
            while (allKeys.hasMoreElements()) {
                if (!isFirst) {
                    keys.append(",");
                }
                keys.append(allKeys.nextElement());
                isFirst = false;
            }

            var allBfKeys = bloomFiltersStorage.keys();
            while (allBfKeys.hasMoreElements()) {
                if (!isFirst) {
                    keys.append(",");
                }
                keys.append(allKeys.nextElement());
                isFirst = false;
            }

            var allSkipListKeys = skipListsStorage.keys();
            while (allSkipListKeys.hasMoreElements()) {
                if (!isFirst) {
                    keys.append(",");
                }
                keys.append(allKeys.nextElement());
                isFirst = false;
            }

            keys.append("]");
            return keys.toString();
        }
        return "[]";
    }

    public String initBloomFilter(String key, Integer expectedElements, Double falsePositiveRate) {
        log.info("BF.INIT ===> key: {}, expectedElements: {}, falsePositiveRate: {}", key,
                expectedElements, falsePositiveRate);
        key = BLOOM_FILTERS_KEY_PREFIX + key;

        var keySize = key.getBytes(StandardCharsets.UTF_8).length;
        bloomFiltersStorage.compute(key, (oldKey, oldVal) -> {
            BloomFilter<String> newEntry = new BloomFilter<>(expectedElements, falsePositiveRate);
            if (oldVal != null) {
                currentSizeBytes.addAndGet(-(oldVal.getSize() + keySize));
            }
            currentSizeBytes.addAndGet((newEntry.getSize() + keySize));

            return newEntry;
        });
        sketch.increment(key);

        if (currentSizeBytes.get() > maxCacheSize) {
            evictUsingTinyLFU(key);
        }

        if (operationCount.incrementAndGet() % RESET_PERIOD == 0) {
            sketch.age();
        }

        return "OK";
    }

    public Integer removeBloomFilter(String key) {
        log.info("BF.RM ===> key: {}", key);
        if (key == null) return 0;

        key = BLOOM_FILTERS_KEY_PREFIX + key;
        if (bloomFiltersStorage.isEmpty()) return 0;
        if (bloomFiltersStorage.get(key) == null) return 0;

        var value = bloomFiltersStorage.remove(key);
        if (value != null) {
            var removedSize = key.getBytes(StandardCharsets.UTF_8).length + value.getSize();
            currentSizeBytes.addAndGet(-removedSize);
        }
        return 1;
    }

    public String addBloomFilter(String key, String value) {
        log.info("BF.ADD ===> key: {}, value: {}", key, value);
        if (key == null || value == null) return "FAIL";

        key = BLOOM_FILTERS_KEY_PREFIX + key;
        var bloom = bloomFiltersStorage.get(key);
        if (bloom == null) {
            return "FAIL";
        }
        bloom.add(value);
        sketch.increment(key);

        if (operationCount.incrementAndGet() % RESET_PERIOD == 0) {
            sketch.age();
        }

        return "OK";
    }

    public Integer existsBloomFilter(String key, String value) {
        log.info("BF.EXISTS ===> key: {}, value: {}", key, value);
        if (key == null) return 0;

        key = BLOOM_FILTERS_KEY_PREFIX + key;
        var bloom = bloomFiltersStorage.get(key);
        if (bloom == null) {
            return 0;
        }
        if (value.isBlank()) {
            return 1;
        }
        sketch.increment(key);

        return bloom.mightContain(value) ? 1 : 0;
    }

    public Integer resetBloomFilter(String key) {
        log.info("BF.RS ===> key: {}", key);
        if (key == null) return 0;

        key = BLOOM_FILTERS_KEY_PREFIX + key;
        var bloom = bloomFiltersStorage.get(key);
        if (bloom == null) {
            return 0;
        }
        bloom.reset();
        return 1;
    }

    public String zRangeByPositions(String key, Integer start, Integer stop) {
        log.info("Z.RANGE ===> key: {}, start: {}, stop: {}", key, start, stop);
        String internalKey = SKIP_LISTS_KEY_PREFIX + key;
        ConcurrentSkipList<String, String> skipList = skipListsStorage.get(internalKey);

        if (skipList == null) {
            return "[]";
        }

        sketch.increment(internalKey);
        var res = skipList.getRangeByPositions(start - 1, stop - 1);
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

    public String zRangeByScore(String key, Double minScore, Double maxScore) {
        log.info("Z.RSCR ===> key: {}, minScore: {}, maxScore: {}", key, minScore, maxScore);
        String internalKey = SKIP_LISTS_KEY_PREFIX + key;
        ConcurrentSkipList<String, String> skipList = skipListsStorage.get(internalKey);

        if (skipList == null) {
            return "[]";
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
    }

    public String zGetByPosition(String key, Integer index) {
        log.info("Z.POS ===> key: {}, position: {}", key, index);
        String internalKey = SKIP_LISTS_KEY_PREFIX + key;
        ConcurrentSkipList<String, String> skipList = skipListsStorage.get(internalKey);

        if (skipList == null) {
            return null;
        }

        sketch.increment(internalKey);
        return skipList.getByPosition(index - 1);
    }

    public Integer zIncrBy(String key, Double increment, String member) {
        log.info("Z.INCR ===> key: {}, member: {}, increment: {}", key, member, increment);
        String internalKey = SKIP_LISTS_KEY_PREFIX + key;

        var oldVal = skipListsStorage.get(internalKey);
        if (oldVal == null) {
            return 0;
        }

        ConcurrentSkipList<String, String> skipList = skipListsStorage.computeIfAbsent(
                internalKey, k -> new ConcurrentSkipList<>()
        );
        ConcurrentHashMap<String, Double> memberScores = memberScoresStorage.computeIfAbsent(
                internalKey, k -> new ConcurrentHashMap<>()
        );

        Double oldScore = memberScores.get(member);
        double newScore = (oldScore == null) ? increment : oldScore + increment;

        boolean check = false;
        String removedVal = "";
        if (oldScore != null) {
            removedVal = skipList.remove(oldScore, member);
            if (removedVal != null) {
                check = true;
            }
        }

        memberScores.put(member, newScore);
        skipList.put(newScore, member, removedVal);

        sketch.increment(internalKey);

        if (operationCount.incrementAndGet() % RESET_PERIOD == 0) {
            sketch.age();
        }

        return check ? 1 : 0;
    }

    public Integer zRank(String key, String member) {
        log.info("Z.RANK ===> key: {}, member: {}", key, member);
        String internalKey = SKIP_LISTS_KEY_PREFIX + key;
        ConcurrentSkipList<String, String> skipList = skipListsStorage.get(internalKey);
        ConcurrentHashMap<String, Double> memberScores = memberScoresStorage.get(internalKey);

        if (skipList == null || memberScores == null) return -1;

        Double score = memberScores.get(member);
        if (score == null) return -1;

        sketch.increment(internalKey);
        return skipList.getRank(score, member) + 1;
    }

    public String zAdd(String key, Double score, String member, String value) {
        log.info("Z.ADD ===> key: {}, score: {}, member: {}, value: {}", key, score, member, value);
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

            if (currentSizeBytes.get() > maxCacheSize) {
                evictUsingTinyLFU(key);
            }

            sketch.increment(internalKey);

            if (operationCount.incrementAndGet() % RESET_PERIOD == 0) {
                sketch.age();
            }

            return "OK";
        }

        return "FAIL";
    }

    public Integer zRem(String key, String member) {
        log.info("Z.RM ===> key: {}, member: {}", key, member);
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

            if (operationCount.incrementAndGet() % RESET_PERIOD == 0) {
                sketch.age();
            }

            return 1;
        }
        return 0;
    }

    public String zScore(String key, String member) {
        log.info("Z.SCR ===> key: {}, member: {}", key, member);
        String internalKey = SKIP_LISTS_KEY_PREFIX + key;
        var memScore = memberScoresStorage.get(internalKey);
        if (memScore == null) {
            return "FAIL";
        }
        Double score = memScore.get(member);
        return String.valueOf(score);
    }

    public Integer zDel(String key) {
        log.info("Z.DEL ===> key: {}", key);
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
    }
}
