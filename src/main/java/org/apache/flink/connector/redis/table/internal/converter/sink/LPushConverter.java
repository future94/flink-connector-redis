package org.apache.flink.connector.redis.table.internal.converter.sink;

import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.converter.DataParser;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

/**
 * @author weilai
 */
public class LPushConverter extends BaseRedisSinkConverter{

    @Override
    public RedisCommandType support() {
        return RedisCommandType.LPUSH;
    }

    @Override
    protected void doSend(RedisCommand redisCommand, DataParser dataParser) {
        redisCommand.lpush(dataParser.getKey(), dataParser.getValue());
    }
}
