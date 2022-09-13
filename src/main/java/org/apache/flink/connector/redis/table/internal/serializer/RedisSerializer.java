package org.apache.flink.connector.redis.table.internal.serializer;

import org.apache.flink.connector.redis.table.internal.annotation.SPI;
import org.apache.flink.connector.redis.table.internal.exception.SerializationException;
import org.apache.flink.table.data.binary.BinaryStringData;

import java.io.Serializable;

/**
 * <p>redis编/解码器
 * @author weilai
 */
@SPI
public interface RedisSerializer<V> extends Serializable {

    /**
     * 标识符
     */
    String identifier();

    /**
     * 序列化
     * 当作用与key的时候，TableAPI的key是{@link BinaryStringData}类型
     */
    byte[] serialize(Object t) throws SerializationException;

    /**
     * 反序列化
     */
    V deserialize(byte[] bytes) throws SerializationException;

    Class<V> getValueClass();
}
