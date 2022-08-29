package org.apache.flink.connector.redis.table.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * @author weilai
 */
public class DefaultUtils {

    public static <T extends CharSequence> T ifBlack(T str, T defaultStr) {
        return StringUtils.defaultIfBlank(str, defaultStr);
    }

    public static int ifBlack(String str, int defaultValue) {
        return StringUtils.isBlank(str) ? defaultValue : Integer.parseInt(str);
    }
}
