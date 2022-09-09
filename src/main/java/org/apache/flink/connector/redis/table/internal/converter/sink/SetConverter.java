package org.apache.flink.connector.redis.table.internal.converter.sink;

import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

/**
 * @author weilai
 */
public class SetConverter extends BaseRedisSinkConverter {

    @Override
    public RedisCommandType support() {
        return RedisCommandType.SET;
    }

    @Override
    protected void doSend(RedisCommand redisCommand, DataParser dataParser) {
        redisCommand.set(dataParser.getKey(), dataParser.getValue());
    }
}
