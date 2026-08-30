package org.minicache.struct.fuzzy;

public class Soundex {
    public static String encode(String word) {
        if (word == null || word.isEmpty()) return "0000";

        String cleanWord = word.toUpperCase().replaceAll("[^A-Z]", "");
        if (cleanWord.isEmpty()) return "0000";

        char firstChar = cleanWord.charAt(0);
        StringBuilder code = new StringBuilder();
        code.append(firstChar);

        char lastDigit = getCode(firstChar);

        for (int i = 1; i < cleanWord.length(); i++) {
            char currentDigit = getCode(cleanWord.charAt(i));
            if (currentDigit != '0' && currentDigit != lastDigit) {
                code.append(currentDigit);
                lastDigit = currentDigit;
            } else if (currentDigit == '0') {
                lastDigit = '0';
            }
            if (code.length() == 4) break;
        }

        while (code.length() < 4) {
            code.append('0');
        }

        return code.toString();
    }

    private static char getCode(char c) {
        return switch (c) {
            case 'B', 'F', 'P', 'V' -> '1';
            case 'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z' -> '2';
            case 'D', 'T' -> '3';
            case 'L' -> '4';
            case 'M', 'N' -> '5';
            case 'R' -> '6';
            default -> '0'; // A, E, I, O, U, H, W, Y
        };
    }
}
