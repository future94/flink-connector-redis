package org.apache.flink.connector.redis.table.internal.function;

/**
 * @author weilai
 */
@FunctionalInterface
public interface DataFunction<R, O, K, Res> {

    Res apply(R r, O o, K k);
}