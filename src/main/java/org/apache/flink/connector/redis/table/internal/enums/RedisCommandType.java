package org.apache.flink.connector.redis.table.internal.enums;

/**
 * <p>支持的Redis命令
 * @author weilai
 */
public enum RedisCommandType {

    GET(RedisDataType.STRING, RedisOperationType.READ),

    HGET(RedisDataType.HASH, RedisOperationType.READ),

    LRANGE(RedisDataType.LIST, RedisOperationType.READ),
    ;

    private final RedisDataType dataType;

    private final RedisOperationType operationType;

    RedisCommandType(RedisDataType dataType, RedisOperationType operationType) {
        this.dataType = dataType;
        this.operationType = operationType;
    }
}
