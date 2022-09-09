package org.apache.flink.connector.redis.table.utils;

import java.util.function.Supplier;

/**
 * @author weilai
 */
public class DefaultUtils {

    public static <T> T get(Supplier<T> supplier, Supplier<T> defaultValue) {
        T t = supplier.get();
        if (t != null) {
            return t;
        }
        return defaultValue.get();
    }
}
