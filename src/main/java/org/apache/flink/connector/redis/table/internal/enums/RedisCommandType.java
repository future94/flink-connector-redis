package org.apache.flink.connector.redis.table.internal.enums;

/**
 * <p>支持的Redis命令
 * @author weilai
 */
public enum RedisCommandType {

    GET(RedisDataType.STRING, RedisOperationType.READ),

    SET(RedisDataType.STRING, RedisOperationType.CREATE),

    HGET(RedisDataType.HASH, RedisOperationType.READ),

    HSET(RedisDataType.HASH, RedisOperationType.CREATE),

    LRANGE(RedisDataType.LIST, RedisOperationType.READ),

    LPUSH(RedisDataType.LIST, RedisOperationType.CREATE),

    RPUSH(RedisDataType.LIST, RedisOperationType.CREATE),
    ;

    private final RedisDataType dataType;

    private final RedisOperationType operationType;

    RedisCommandType(RedisDataType dataType, RedisOperationType operationType) {
        this.dataType = dataType;
        this.operationType = operationType;
    }
}
