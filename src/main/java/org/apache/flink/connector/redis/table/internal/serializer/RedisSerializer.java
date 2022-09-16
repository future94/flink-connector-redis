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
     * 序列化
     */
    byte[] serialize(V t) throws SerializationException;

    /**
     * 反序列化
     */
    V deserialize(byte[] bytes) throws SerializationException;

    Class<V> getValueClass();
}
