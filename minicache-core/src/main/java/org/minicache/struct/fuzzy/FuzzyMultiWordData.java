package org.minicache.struct.fuzzy;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class FuzzyMultiWordData {
    private final FuzzySearchData fuzzySearchData;
    private final Map<String, Long> bigramIndex = new ConcurrentHashMap<>();
    private final Set<String> stopWords = Set.of(
            "a", "an", "the", "this", "that", "these", "those",
            "i", "me", "my", "we", "our", "you", "your", "he", "him", "she", "her", "it", "they", "them",
            "in", "on", "at", "by", "for", "with", "about", "against", "between", "into", "through",
            "to", "from", "up", "down", "out", "off", "over", "under",
            "and", "but", "if", "or", "because", "as", "until", "while", "so", "than", "too", "very",
            "is", "am", "are", "was", "were", "be", "been", "being", "have", "has", "had", "do", "does", "did"
    );

    public FuzzyMultiWordData(FuzzySearchData singleWordCache) {
        this.fuzzySearchData = singleWordCache;
    }

    public void indexPhrase(String phrase, long frequency) {
        if (phrase == null || phrase.isBlank()) return;
        String[] tokens = phrase.toLowerCase().trim().split("\\s+");

        for (int i = 0; i < tokens.length - 1; i++) {
            indexBigram(tokens[i], tokens[i + 1], frequency);
        }
    }

    public void indexBigram(String word1, String word2, long frequency) {
        String key = word1.toLowerCase() + "_" + word2.toLowerCase();
        bigramIndex.merge(key, frequency, Long::sum);
    }

    public record CorrectedPhrase(String phrase, double totalScore) {
        public String getPhrase() {
            return phrase;
        }

        @Override
        public String toString() {
            return "{\"phrase\":\"" + phrase + "\",\"score\":\"" + totalScore + "\"}";
        }
    }

    public List<CorrectedPhrase> processQuery(String rawQuery, int topN) {
        String[] tokens = rawQuery.toLowerCase().trim().split("\\s+");
        if (tokens.length == 0) return Collections.emptyList();

        List<List<FuzzySearchData.SearchResult>> tokenCandidatesMatrix = new ArrayList<>();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<List<FuzzySearchData.SearchResult>>> futures = new ArrayList<>();

            for (int i = 0; i < tokens.length; i++) {
                final String token = tokens[i];
                final boolean isLastToken = (i == tokens.length - 1);
                futures.add(executor.submit(() -> processSingleToken(token, isLastToken)));
            }

            for (var future : futures) {
                tokenCandidatesMatrix.add(future.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            return Collections.emptyList();
        }

        List<List<FuzzySearchData.SearchResult>> combinations = cartesianProduct(tokenCandidatesMatrix);

        return combinations.stream()
                .map(this::scorePhraseCombination)
                .sorted(Comparator.comparingDouble(CorrectedPhrase::totalScore).reversed())
                .limit(topN)
                .collect(Collectors.toList());
    }

    private List<FuzzySearchData.SearchResult> processSingleToken(String token, boolean isLastToken) {
        if (token.matches("\\d+") || stopWords.contains(token)) {
            return List.of(new FuzzySearchData.SearchResult(token, 0, 1000000, 500.0));
        }

        Map<String, FuzzySearchData.SearchResult> candidatesMap = new HashMap<>();
        Map<String, FuzzySearchData.WordInfo> dict = fuzzySearchData.getDictionary();

        Set<String> subWords = new HashSet<>();
        for (String phrase : dict.keySet()) {
            String[] parts = phrase.toLowerCase().split("\\s+");
            Collections.addAll(subWords, parts);
        }

        // 1. Exact Match
        if (subWords.contains(token)) {
            candidatesMap.put(token, new FuzzySearchData.SearchResult(token, 0, 100000L, 1000.0));
        }

        // 2. Prefix Match cho Token cuối
        if (isLastToken) {
            for (String subWord : subWords) {
                if (!subWord.equalsIgnoreCase(token) && subWord.startsWith(token)) {
                    double score = 800.0 + (token.length() * 10.0);
                    candidatesMap.put(subWord, new FuzzySearchData.SearchResult(subWord, 0, 50000L, score));
                }
            }
        }

        // 3. Fuzzy Match (Levenshtein Distance)
        for (String subWord : subWords) {
            int dist = computeLevenshteinDistance(token, subWord);
            if (dist > 0 && dist <= 2) {
                double score = 500.0 - (dist * 100.0);
                // Chỉ nhận kết quả Fuzzy nếu chưa tồn tại hoặc score mới tốt hơn
                candidatesMap.merge(subWord,
                        new FuzzySearchData.SearchResult(subWord, dist, 10000L, score),
                        (oldVal, newVal) -> newVal.score() > oldVal.score() ? newVal : oldVal);
            }
        }

        if (candidatesMap.isEmpty()) {
            return List.of(new FuzzySearchData.SearchResult(token, 0, 0, 10.0));
        }

        return candidatesMap.values().stream()
                .sorted(Comparator.comparingDouble(FuzzySearchData.SearchResult::score).reversed())
                .limit(5)
                .collect(Collectors.toList());
    }

    private CorrectedPhrase scorePhraseCombination(List<FuzzySearchData.SearchResult> combination) {
        StringBuilder phraseBuilder = new StringBuilder();
        double tokenScoreSum = 0;
        double bigramScoreSum = 0;
        double fullPhraseBonus = 0;

        for (int i = 0; i < combination.size(); i++) {
            var currentToken = combination.get(i);
            phraseBuilder.append(currentToken.word()).append(i == combination.size() - 1 ? "" : " ");
            tokenScoreSum += currentToken.score();

            if (i < combination.size() - 1) {
                String nextToken = combination.get(i + 1).word();
                String bigramKey = currentToken.word() + "_" + nextToken;
                Long bigramFreq = bigramIndex.getOrDefault(bigramKey, 0L);
                if (bigramFreq > 0) {
                    bigramScoreSum += Math.log10(bigramFreq + 1) * 200.0;
                }
            }
        }

        String constructedPhrase = phraseBuilder.toString();

        // Thưởng thêm điểm cực lớn nếu câu tạo thành nằm trọn vẹn hoặc là Prefix của câu mẫu trong Dictionary
        Map<String, FuzzySearchData.WordInfo> dict = fuzzySearchData.getDictionary();
        for (var entry : dict.entrySet()) {
            String targetPhrase = entry.getKey().toLowerCase();
            if (targetPhrase.startsWith(constructedPhrase) || constructedPhrase.startsWith(targetPhrase)) {
                fullPhraseBonus += 2000.0 + Math.log10(entry.getValue().frequency() + 1) * 50.0;
            }
        }

        double finalScore = tokenScoreSum + bigramScoreSum + fullPhraseBonus;
        return new CorrectedPhrase(constructedPhrase, finalScore);
    }

    private int computeLevenshteinDistance(String s1, String s2) {
        int[] costs = new int[s2.length() + 1];
        for (int i = 0; i <= s1.length(); i++) {
            int lastValue = i;
            for (int j = 0; j <= s2.length(); j++) {
                if (i == 0) {
                    costs[j] = j;
                } else {
                    if (j > 0) {
                        int newValue = costs[j - 1];
                        if (s1.charAt(i - 1) != s2.charAt(j - 1)) {
                            newValue = Math.min(Math.min(newValue, lastValue), costs[j]) + 1;
                        }
                        costs[j - 1] = lastValue;
                        lastValue = newValue;
                    }
                }
            }
            if (i > 0) costs[s2.length()] = lastValue;
        }
        return costs[s2.length()];
    }

    private <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
        List<List<T>> result = new ArrayList<>();
        cartesianHelper(lists, 0, new ArrayList<>(), result);
        return result;
    }

    private <T> void cartesianHelper(List<List<T>> lists, int depth, List<T> current, List<List<T>> result) {
        if (depth == lists.size()) {
            result.add(new ArrayList<>(current));
            return;
        }
        for (T item : lists.get(depth)) {
            current.add(item);
            cartesianHelper(lists, depth + 1, current, result);
            current.remove(current.size() - 1);
        }
    }
}