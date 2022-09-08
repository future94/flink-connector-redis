package org.apache.flink.connector.redis.table.internal.serializer;

import org.apache.flink.connector.redis.table.internal.exception.SerializationException;

import java.nio.charset.StandardCharsets;

/**
 * <p>默认的String方式
 * @author weilai
 */
public class StringRedisSerializer extends BaseRedisSerializer<String>{

    public static final String IDENTIFIER = "string";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public byte[] serialize(Object data) throws SerializationException {
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String deserialize(byte[] bytes) throws SerializationException {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
