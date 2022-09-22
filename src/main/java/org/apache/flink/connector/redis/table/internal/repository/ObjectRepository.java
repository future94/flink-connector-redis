package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.serializer.ObjectRedisSerializer;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;

/**
 * @author weilai
 */
public abstract class ObjectRepository<T> extends EntityRepository<T> {

    @Override
    protected RedisSerializer<Object> getValueSerializer() {
        return new ObjectRedisSerializer();
    }
}
