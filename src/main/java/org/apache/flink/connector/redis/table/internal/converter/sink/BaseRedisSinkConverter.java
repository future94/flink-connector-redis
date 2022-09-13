package org.apache.flink.connector.redis.table.internal.converter.sink;

import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.converter.DataParser;

/**
 * @author weilai
 */
public abstract class BaseRedisSinkConverter implements RedisSinkConverter {

    @Override
    public void convert(final RedisCommand redisCommand, final DataParser dataParser) {
        doSend(redisCommand, dataParser);
    }

    abstract protected void doSend(RedisCommand redisCommand, DataParser dataParser);
}
