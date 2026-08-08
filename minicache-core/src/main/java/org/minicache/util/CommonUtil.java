package org.minicache.util;

import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;

public class CommonUtil {
    private CommonUtil() {
    }

    public static String formatDate(Temporal date, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return formatter.format(date);
    }

    public static boolean isInteger(String str) {
        if (str == null) {
            return false;
        }
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
