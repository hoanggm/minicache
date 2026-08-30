package org.minicache.engine;

import org.minicache.common.Command;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class StorageEngine {
    public abstract Map<String, String> getInitCfg();

    public abstract void shutdown();

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

    public abstract String zTop(String key, Integer top);

    public abstract String geoAdd(String key, String member, Double lat, Double lon);

    public abstract String geoSearch(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit);

    public abstract String geoDist(String key, String member1, String member2);

    public abstract Integer geoDel(String key);

    public abstract Integer geoRem(String key, String member);

    public abstract String geoGet(String key, String member);

    public abstract String geoNb(String key, String member);

    public abstract Integer geoExists(String key, String member);

    public abstract String geoEncode(String key, String member);

    public abstract String hSet(String key, String field, String value);

    public abstract String hGet(String key, String field);

    public abstract Integer hRm(String key, String field);

    public abstract Integer hDel(String key);

    public abstract String hGetAll(String key);

    public abstract String fzAdd(String key, String word, Long frequency);

    public abstract String fzSearch(String key, String query, Integer topN);

    public abstract String fzSuggest(String key, String query, Integer topN, Integer maxEditDist);

    public abstract String fzGetExact(String key, String word);

    public abstract Integer fzIncrBy(String key, String word, Long increment);

    public abstract Integer fzRm(String key, String word);

    public abstract Integer fzDel(String key);

    public abstract Integer fzExists(String key, String word);

    public abstract String fzPhonetic(String key, String input, Integer limit);

    public abstract String fzRan(String key, Integer count);

    public CompletableFuture<String> putAsync(String key, String value, Long ttl, Boolean notExists) {
        return CompletableFuture.completedFuture(put(key, value, ttl, notExists));
    }

    public CompletableFuture<String> getAsync(String key) {
        return CompletableFuture.completedFuture(get(key));
    }

    public CompletableFuture<Integer> deleteAsync(String key) {
        return CompletableFuture.completedFuture(delete(key));
    }

    public CompletableFuture<Integer> existsAsync(String key) {
        return CompletableFuture.completedFuture(exists(key));
    }

    public CompletableFuture<Integer> clearAsync(Command command) {
        return CompletableFuture.completedFuture(clear(command));
    }

    public CompletableFuture<String> getAllKeysAsync(Command command) {
        return CompletableFuture.completedFuture(getAllKeys(command));
    }

    public CompletableFuture<String> initBloomFilterAsync(String key, Integer expectedElements, Double falsePositiveRate) {
        return CompletableFuture.completedFuture(initBloomFilter(key, expectedElements, falsePositiveRate));
    }

    public CompletableFuture<Integer> removeBloomFilterAsync(String key) {
        return CompletableFuture.completedFuture(removeBloomFilter(key));
    }

    public CompletableFuture<String> addBloomFilterAsync(String key, String value) {
        return CompletableFuture.completedFuture(addBloomFilter(key, value));
    }

    public CompletableFuture<Integer> existsBloomFilterAsync(String key, String value) {
        return CompletableFuture.completedFuture(existsBloomFilter(key, value));
    }

    public CompletableFuture<Integer> resetBloomFilterAsync(String key) {
        return CompletableFuture.completedFuture(resetBloomFilter(key));
    }

    public CompletableFuture<String> zScoreAsync(String key, String member) {
        return CompletableFuture.completedFuture(zScore(key, member));
    }

    public CompletableFuture<String> zGetByPositionAsync(String key, Integer index) {
        return CompletableFuture.completedFuture(zGetByPosition(key, index));
    }

    public CompletableFuture<Integer> zIncrByAsync(String key, Double increment, String member) {
        return CompletableFuture.completedFuture(zIncrBy(key, increment, member));
    }

    public CompletableFuture<Integer> zRankAsync(String key, String member) {
        return CompletableFuture.completedFuture(zRank(key, member));
    }

    public CompletableFuture<String> zAddAsync(String key, Double score, String member, String value) {
        return CompletableFuture.completedFuture(zAdd(key, score, member, value));
    }

    public CompletableFuture<String> zTopAsync(String key, Integer top) {
        return CompletableFuture.completedFuture(zTop(key, top));
    }

    public CompletableFuture<Integer> zRemAsync(String key, String member) {
        return CompletableFuture.completedFuture(zRem(key, member));
    }

    public CompletableFuture<Integer> zDelAsync(String key) {
        return CompletableFuture.completedFuture(zDel(key));
    }

    public CompletableFuture<String> zRangeByPositionsAsync(String key, Integer start, Integer stop) {
        return CompletableFuture.completedFuture(zRangeByPositions(key, start, stop));
    }

    public CompletableFuture<String> zRangeByScoreAsync(String key, Double minScore, Double maxScore) {
        return CompletableFuture.completedFuture(zRangeByScore(key, minScore, maxScore));
    }

    public CompletableFuture<String> geoAddAsync(String key, String member, Double lat, Double lon) {
        return CompletableFuture.completedFuture(geoAdd(key, member, lat, lon));
    }

    public CompletableFuture<Integer> geoDelAsync(String key) {
        return CompletableFuture.completedFuture(geoDel(key));
    }

    public CompletableFuture<Integer> geoRemAsync(String key, String member) {
        return CompletableFuture.completedFuture(geoRem(key, member));
    }

    public CompletableFuture<String> geoGetAsync(String key, String member) {
        return CompletableFuture.completedFuture(geoGet(key, member));
    }

    public CompletableFuture<String> geoSearchAsync(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) {
        return CompletableFuture.completedFuture(geoSearch(key, centerLat, centerLon, radiusMeters, limit));
    }

    public CompletableFuture<String> geoDistAsync(String key, String member1, String member2) {
        return CompletableFuture.completedFuture(geoDist(key, member1, member2));
    }

    public CompletableFuture<String> geoNbAsync(String key, String member) {
        return CompletableFuture.completedFuture(geoNb(key, member));
    }

    public CompletableFuture<Integer> geoExistsAsync(String key, String member) {
        return CompletableFuture.completedFuture(geoExists(key, member));
    }

    public CompletableFuture<String> geoEncodeAsync(String key, String member) {
        return CompletableFuture.completedFuture(geoEncode(key, member));
    }

    public CompletableFuture<String> hSetAsync(String key, String field, String value) {
        return CompletableFuture.completedFuture(hSet(key, field, value));
    }

    public CompletableFuture<String> hGetAsync(String key, String field) {
        return CompletableFuture.completedFuture(hGet(key, field));
    }

    public CompletableFuture<Integer> hRmAsync(String key, String field) {
        return CompletableFuture.completedFuture(hRm(key, field));
    }

    public CompletableFuture<Integer> hDelAsync(String key) {
        return CompletableFuture.completedFuture(hDel(key));
    }

    public CompletableFuture<String> hGetAllAsync(String key) {
        return CompletableFuture.completedFuture(hGetAll(key));
    }

    public CompletableFuture<String> fzAddAsync(String key, String word, Long frequency) {
        return CompletableFuture.completedFuture(fzAdd(key, word, frequency));
    }

    public CompletableFuture<String> fzSearchAsync(String key, String query, Integer topN) {
        return CompletableFuture.completedFuture(fzSearch(key, query, topN));
    }

    public CompletableFuture<String> fzSuggestAsync(String key, String query, Integer topN, Integer maxEditDist) {
        return CompletableFuture.completedFuture(fzSuggest(key, query, topN, maxEditDist));
    }

    public CompletableFuture<String> fzGetExactAsync(String key, String word) {
        return CompletableFuture.completedFuture(fzGetExact(key, word));
    }

    public CompletableFuture<Integer> fzIncrByAsync(String key, String word, Long increment) {
        return CompletableFuture.completedFuture(fzIncrBy(key, word, increment));
    }

    public CompletableFuture<Integer> fzRmAsync(String key, String word) {
        return CompletableFuture.completedFuture(fzRm(key, word));
    }

    public CompletableFuture<Integer> fzDelAsync(String key) {
        return CompletableFuture.completedFuture(fzDel(key));
    }

    public CompletableFuture<Integer> fzExistsAsync(String key, String word) {
        return CompletableFuture.completedFuture(fzExists(key, word));
    }

    public CompletableFuture<String> fzPhoneticAsync(String key, String input, Integer limit) {
        return CompletableFuture.completedFuture(fzPhonetic(key, input, limit));
    }

    public CompletableFuture<String> fzRanAsync(String key, Integer count) {
        return CompletableFuture.completedFuture(fzRan(key, count));
    }
}
