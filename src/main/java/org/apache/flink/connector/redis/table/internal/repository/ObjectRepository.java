package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.serializer.ObjectRedisSerializer;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;

/**
 * @author weilai
 */
public abstract class ObjectRepository<T> extends BaseRepository<T> {

    @Override
    protected RedisSerializer<T> getValueSerializer() {
        return new ObjectRedisSerializer<>(entityClass);
    }
}
