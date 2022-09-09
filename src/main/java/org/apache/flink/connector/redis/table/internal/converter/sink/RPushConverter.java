package org.apache.flink.connector.redis.table.internal.converter.sink;

import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

/**
 * @author weilai
 */
public class RPushConverter extends BaseRedisSinkConverter{

    @Override
    public RedisCommandType support() {
        return RedisCommandType.RPUSH;
    }

    @Override
    protected void doSend(RedisCommand redisCommand, DataParser dataParser) {
        redisCommand.rpush(dataParser.getKey(), dataParser.getValue());
    }
}
