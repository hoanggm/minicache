package org.minicache.engine;

import org.minicache.common.Command;

import java.util.Map;

public abstract class CacheEngine {
    public abstract Map<String, String> getInitCfg();

    public abstract String put(String key, String value, Long ttl, Boolean notExists);

    public abstract String get(String key);

    public abstract Integer delete(String key);

    public abstract Integer exists(String key);

    public abstract Integer clear(Command command);

    public abstract String getAllKeys(Command command);

    public abstract String initBloomFilter(String key, Integer expectedElements, Double falsePositiveRate);

    public abstract Integer removeBloomFilter(String key);

    public abstract String addBloomFilter(String key, String value);

    public abstract Integer existsBloomFilter(String key, String value);

    public abstract Integer resetBloomFilter(String key);

    public abstract String zScore(String key, String member);

    public abstract String zGetByPosition(String key, Integer index);

    public abstract Integer zIncrBy(String key, Double increment, String member);

    public abstract Integer zRank(String key, String member);

    public abstract String zAdd(String key, Double score, String member, String value);

    public abstract Integer zRem(String key, String member);

    public abstract Integer zDel(String key);

    public abstract String zRangeByPositions(String key, Integer start, Integer stop);

    public abstract String zRangeByScore(String key, Double minScore, Double maxScore);
}
