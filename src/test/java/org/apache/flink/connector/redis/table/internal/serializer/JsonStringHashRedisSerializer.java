package org.apache.flink.connector.redis.table.internal.serializer;

import com.google.gson.Gson;
import org.apache.flink.connector.redis.table.internal.exception.SerializationException;

import java.nio.charset.StandardCharsets;

/**
 * @author weilai
 */
public class JsonStringHashRedisSerializer extends BaseRedisSerializer<JsonStringHashTestDTO> {

    @Override
    public byte[] serialize(JsonStringHashTestDTO t) throws SerializationException {
        return new Gson().toJson(t).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public JsonStringHashTestDTO deserialize(byte[] bytes) throws SerializationException {
        return new Gson().fromJson(new String(bytes, StandardCharsets.UTF_8), JsonStringHashTestDTO.class);
    }
}
