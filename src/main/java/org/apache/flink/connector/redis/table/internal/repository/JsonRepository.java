package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.serializer.JsonRedisSerializer;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;

/**
 * @author weilai
 */
public abstract class JsonRepository<T> extends BaseRepository<T> {

    @Override
    protected RedisSerializer<T> getValueSerializer() {
        return new JsonRedisSerializer<>(entityClass);
    }
}
