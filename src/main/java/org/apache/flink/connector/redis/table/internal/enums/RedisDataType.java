package org.apache.flink.connector.redis.table.internal.enums;

/**
 * <p>Redis数据集合
 * @author weilai
 */
public enum RedisDataType {

    STRING,

    HASH,

    LIST,

    SET,

    SORTED_SET,

    PUB_SUB,

    HYPER_LOG_LOG
}
