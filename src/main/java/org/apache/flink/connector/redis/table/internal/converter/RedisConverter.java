package org.apache.flink.connector.redis.table.internal.converter;

import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

import java.io.Serializable;

/**
 * @author weilai
 */
public interface RedisConverter extends Serializable {

    /**
     * 支持的命令类型
     */
    RedisCommandType support();
}
