package org.apache.flink.connector.redis.table.internal.serializer;

import org.apache.flink.connector.redis.table.internal.exception.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author weilai
 */
public class ObjectRedisSerializer<V> extends BaseRedisSerializer<V> {

    public static final String IDENTIFIER = "object";

    public ObjectRedisSerializer() {
        super();
    }

    @SuppressWarnings("unchecked")
    public ObjectRedisSerializer(Class<?> clazz) {
        super((Class<V>) clazz);
    }

    @Override
    public byte[] serialize(V object) throws SerializationException {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(object);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("序列化失败", e);
        }
    }

    @Override
    public V deserialize(byte[] bytes) throws SerializationException {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            return (V) objectInputStream.readObject();
        } catch (Exception e) {
            throw new SerializationException("反序列化失败", e);
        }
    }
}
