package org.apache.flink.connector.redis.table.internal.enums;

/**
 * 未命中缓存模式
 * @author weilai
 */
public enum CacheMissModel {

    /**
     * 如果缓存中没有，直接忽略返回null
     */
    IGNORE,

    /**
     * 如果缓存中没有，刷新缓存后重拾
     */
    REFRESH,
}
