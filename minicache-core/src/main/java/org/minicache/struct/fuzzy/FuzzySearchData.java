package org.minicache.struct.fuzzy;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class FuzzySearchData {
    public record WordInfo(String word, long frequency, String soundexCode) {
    }

    public record SearchResult(String word, int editDistance, long frequency, double score) {
        @Override
        public String toString() {
            return String.format("{\"word\":\"%s\",\"frequency\":%d}", word, frequency);
        }
    }

    // 1. Dictionary lưu metadata của từ gốc
    private final Map<String, WordInfo> dictionary = new ConcurrentHashMap<>();

    // 2. Soundex Inverted Index: SoundexCode -> List<RawWord>
    private final Map<String, Set<String>> soundexIndex = new ConcurrentHashMap<>();

    // 3. SymSpell Delete Index (k=1): DeleteVariant -> List<RawWord>
    private final Map<String, Set<String>> symSpellK1Index = new ConcurrentHashMap<>();

    /**
     * Lấy thông tin Dictionary
     */
    public Map<String, WordInfo> getDictionary() {
        return dictionary;
    }

    /**
     * Nạp từ vào Cache Index
     */
    public void indexWord(String word, long frequency) {
        String cleanWord = word.toLowerCase().trim();
        String soundexCode = Soundex.encode(cleanWord);
        WordInfo wordInfo = new WordInfo(cleanWord, frequency, soundexCode);

        dictionary.put(cleanWord, wordInfo);

        Set<String> deletes = generateDeletesK1(cleanWord);
        deletes.add(cleanWord);
        for (String del : deletes) {
            symSpellK1Index.computeIfAbsent(del, k -> ConcurrentHashMap.newKeySet()).add(cleanWord);
        }

        soundexIndex.computeIfAbsent(soundexCode, k -> ConcurrentHashMap.newKeySet()).add(cleanWord);
    }

    /**
     * Hàm Search Hybrid chính kết hợp 2 giai đoạn
     */
    public List<SearchResult> search(String query, int topN, int maxEditDist) {
        if (query == null || query.isBlank()) return Collections.emptyList();

        String cleanQuery = query.toLowerCase().trim();

        // Fast Path: Nếu từ nhập đúng hoàn toàn và phổ biến
        if (dictionary.containsKey(cleanQuery)) {
            WordInfo info = dictionary.get(cleanQuery);
            return List.of(new SearchResult(info.word(), 0, info.frequency(), 1000.0));
        }

        // GIAI ĐOẠN 1: Collect Candidates
        Set<String> candidateWords = new HashSet<>();

        // 1.1 Candidate từ SymSpell (Xóa 1 ký tự từ query)
        Set<String> queryDeletes = generateDeletesK1(cleanQuery);
        queryDeletes.add(cleanQuery);
        for (String del : queryDeletes) {
            var matches = symSpellK1Index.get(del);
            if (matches != null) candidateWords.addAll(matches);
        }

        // 1.2 Candidate từ Soundex
        String querySoundex = Soundex.encode(cleanQuery);
        var soundexMatches = soundexIndex.get(querySoundex);
        if (soundexMatches != null) {
            candidateWords.addAll(soundexMatches);
        }

        if (candidateWords.isEmpty() || maxEditDist >= 1) {
            int queryLen = cleanQuery.length();
            for (String dictWord : dictionary.keySet()) {
                if (Math.abs(dictWord.length() - queryLen) <= maxEditDist) {
                    candidateWords.add(dictWord);
                }
            }
        }

        // GIAI ĐOẠN 2: Rescoring & Reranking trên danh sách Candidate
        return candidateWords.stream()
                .map(candidate -> {
                    WordInfo info = dictionary.get(candidate);
                    if (info == null) return null;

                    int distance = Levenshtein.calculate(cleanQuery, candidate);
                    boolean isSoundexMatch = info.soundexCode() != null && info.soundexCode().equals(querySoundex);

                    double score = calculateScore(distance, info.frequency(), isSoundexMatch);
                    return new SearchResult(candidate, distance, info.frequency(), score);
                })
                .filter(Objects::nonNull)
                // Lọc các kết quả có edit distance vượt quá maxEditDist
                .filter(res -> res.editDistance() <= maxEditDist)
                // Ưu tiên editDistance nhỏ hơn trước -> Sau đó mới so sánh Score giảm dần
                .sorted((a, b) -> {
                    if (a.editDistance() != b.editDistance()) {
                        return Integer.compare(a.editDistance(), b.editDistance());
                    }
                    return Double.compare(b.score(), a.score());
                })
                .limit(topN)
                .collect(Collectors.toList());
    }

    /**
     * Công thức tính điểm Scoring Hybrid
     */
    private double calculateScore(int distance, long frequency, boolean isSoundexMatch) {
        // Distance càng nhỏ điểm càng cao
        double distanceScore = (4.0 - distance) * 100;
        // Frequency lấy log để tránh áp đảo distance
        double freqScore = Math.log10(frequency + 1) * 10;
        // Cộng thưởng nếu trùng mã âm đọc Soundex
        double soundexBonus = isSoundexMatch ? 25.0 : 0.0;

        return distanceScore + freqScore + soundexBonus;
    }

    /**
     * Hàm helper sinh biến thể xóa 1 ký tự (k=1)
     */
    private Set<String> generateDeletesK1(String word) {
        Set<String> deletes = new HashSet<>();
        for (int i = 0; i < word.length(); i++) {
            deletes.add(word.substring(0, i) + word.substring(i + 1));
        }
        return deletes;
    }

    /**
     * Tăng tần suất xuất hiện của một từ (Dùng khi người dùng chọn từ đó hoặc crawl thêm dữ liệu)
     */
    public boolean incrementFrequency(String word, long frequency) {
        if (word == null || word.isBlank()) return false;
        String cleanWord = word.toLowerCase().trim();

        WordInfo existingInfo = dictionary.get(cleanWord);
        if (existingInfo != null) {
            WordInfo updatedInfo = new WordInfo(
                    existingInfo.word(),
                    existingInfo.frequency() + frequency,
                    existingInfo.soundexCode()
            );
            dictionary.put(cleanWord, updatedInfo);
        } else {
            addWord(cleanWord, frequency);
        }

        return true;
    }

    /**
     * 2. Lấy chính xác thông tin từ (Fast-path / Exact lookup O(1))
     */
    public WordInfo getExact(String word) {
        if (word == null) return null;
        return dictionary.get(word.toLowerCase().trim());
    }

    /**
     * Xóa một từ khỏi Dictionary và toàn bộ các bộ Index liên quan
     */
    public boolean removeWord(String word) {
        if (word == null || word.isBlank()) return false;
        String cleanWord = word.toLowerCase().trim();

        WordInfo removedInfo = dictionary.remove(cleanWord);
        if (removedInfo == null) return false;

        // 3.1 Xóa khỏi SymSpell K=1 Index
        Set<String> deletes = generateDeletesK1(cleanWord);
        deletes.add(cleanWord);
        for (String del : deletes) {
            var list = symSpellK1Index.get(del);
            if (list != null) {
                list.remove(cleanWord);
                if (list.isEmpty()) {
                    symSpellK1Index.remove(del);
                }
            }
        }

        // 3.2 Xóa khỏi Soundex Index
        String soundexCode = removedInfo.soundexCode();
        var soundexList = soundexIndex.get(soundexCode);
        if (soundexList != null) {
            soundexList.remove(cleanWord);
            if (soundexList.isEmpty()) {
                soundexIndex.remove(soundexCode);
            }
        }

        return true;
    }

    /**
     * Ước tính dung lượng bộ nhớ RAM mà Engine đang sử dụng (Bytes)
     */
    public long getEstimatedBytes() {
        long bytes = 0;

        // 4.1 Bộ nhớ của Dictionary (Map<String, WordInfo>)
        for (Map.Entry<String, WordInfo> entry : dictionary.entrySet()) {
            bytes += estimateStringBytes(entry.getKey());
            WordInfo info = entry.getValue();
            bytes += estimateStringBytes(info.word());
            bytes += estimateStringBytes(info.soundexCode());
            bytes += 8; // long/int frequency + references
        }

        // 4.2 Bộ nhớ của SymSpell K=1 Index
        for (Map.Entry<String, Set<String>> entry : symSpellK1Index.entrySet()) {
            bytes += estimateStringBytes(entry.getKey());
            for (String val : entry.getValue()) {
                bytes += estimateStringBytes(val);
            }
        }

        // 4.3 Bộ nhớ của Soundex Index
        for (Map.Entry<String, Set<String>> entry : soundexIndex.entrySet()) {
            bytes += estimateStringBytes(entry.getKey());
            for (String val : entry.getValue()) {
                bytes += estimateStringBytes(val);
            }
        }

        return bytes;
    }

    /**
     * Hàm phụ trợ tính dung lượng Chuỗi trong Java Overhead (String Object Header + byte[])
     */
    private long estimateStringBytes(String str) {
        if (str == null) return 0;
        return 24 + str.getBytes(StandardCharsets.UTF_8).length;
    }

    /**
     * Thêm một từ mới vào Dictionary và tiến hành build Index (SymSpell & Soundex)
     */
    public void addWord(String word, long frequency) {
        if (word == null || word.isBlank()) return;

        String cleanWord = word.toLowerCase().trim();

        // 1. Tính mã Soundex cho từ
        String soundexCode = Soundex.encode(cleanWord);

        // 2. Tạo đối tượng WordInfo và lưu vào Dictionary chính
        WordInfo info = new WordInfo(cleanWord, frequency, soundexCode);
        dictionary.put(cleanWord, info);

        // 3. Build SymSpell K=1 Index (Tạo các biến thể xóa 1 ký tự)
        Set<String> deletes = generateDeletesK1(cleanWord);
        deletes.add(cleanWord); // Thêm chính nó vào tập delete key
        for (String del : deletes) {
            symSpellK1Index.computeIfAbsent(del, k -> ConcurrentHashMap.newKeySet()).add(cleanWord);
        }

        // 4. Build Soundex Index (Gom nhóm các từ có cùng mã phát âm)
        soundexIndex.computeIfAbsent(soundexCode, k -> ConcurrentHashMap.newKeySet()).add(cleanWord);
    }

    /**
     * Lấy danh sách các từ có cùng mã Soundex O(1).
     */
    public List<String> getWordsBySoundex(String soundexCode) {
        if (soundexCode == null) return List.of();
        var results = soundexIndex.get(soundexCode);
        if (results == null) {
            return new ArrayList<>();
        }

        return results.stream().toList();
    }
}