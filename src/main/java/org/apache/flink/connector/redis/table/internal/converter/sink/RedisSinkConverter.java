package org.apache.flink.connector.redis.table.internal.converter.sink;

import org.apache.flink.connector.redis.table.internal.annotation.SPI;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.converter.DataParser;
import org.apache.flink.connector.redis.table.internal.converter.RedisConverter;

/**
 * <p>将Table数据转换为Redis要写入的数据
 *
 * @author weilai
 */
@SPI
public interface RedisSinkConverter extends RedisConverter {

    void convert(final RedisCommand redisCommand, final DataParser dataParser);
}
