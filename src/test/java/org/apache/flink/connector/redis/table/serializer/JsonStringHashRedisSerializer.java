package org.apache.flink.connector.redis.table.serializer;

import com.google.gson.Gson;
import org.apache.flink.connector.redis.table.internal.exception.SerializationException;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.table.data.binary.BinaryStringData;

import java.nio.charset.StandardCharsets;

/**
 * @author weilai
 */
public class JsonStringHashRedisSerializer implements RedisSerializer<JsonStringHashTestDTO> {

    private static final String IDENTIFIER = "jsonStringHash";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public byte[] serialize(BinaryStringData t) throws SerializationException {
        return new Gson().toJson(t.toString()).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public JsonStringHashTestDTO deserialize(byte[] bytes) throws SerializationException {
        return new Gson().fromJson(new String(bytes, StandardCharsets.UTF_8), JsonStringHashTestDTO.class);
    }
}
