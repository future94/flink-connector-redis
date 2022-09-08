package org.apache.flink.connector.redis.table.internal.converter;

import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

/**
 * @author weilai
 */
public interface RedisConverter {

    /**
     * 支持的命令类型
     */
    RedisCommandType support();
}
