package org.apache.flink.connector.redis.table.internal.extension;

/**
 * Helper Class for hold a value.
 * @author weilai
 */
public class Holder<T> {

    private volatile T value;

    public T get() {
        return value;
    }

    public void set(T value) {
        this.value = value;
    }
}
