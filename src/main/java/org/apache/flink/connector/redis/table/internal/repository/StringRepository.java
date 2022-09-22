package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.connector.redis.table.internal.serializer.StringRedisSerializer;

/**
 * @author weilai
 */
public abstract class StringRepository<T> extends EntityRepository<T> {

    @Override
    protected RedisSerializer<String> getValueSerializer() {
        return new StringRedisSerializer();
    }
}
