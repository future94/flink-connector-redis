package org.apache.flink.connector.redis.table.internal.serializer;

import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

/**
 * @author weilai
 */
public abstract class BaseRedisSerializer<V> implements RedisSerializer<V> {

    protected Class<V> valueClass;

    @SuppressWarnings("unchecked")
    public BaseRedisSerializer() {
        valueClass = (Class<V>) ((ParameterizedTypeImpl) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    @Override
    public Class<V> getValueClass() {
        return valueClass;
    }
}
