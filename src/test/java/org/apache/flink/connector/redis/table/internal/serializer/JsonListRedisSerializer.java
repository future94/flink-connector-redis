package org.apache.flink.connector.redis.table.internal.serializer;

import com.google.gson.Gson;
import org.apache.flink.connector.redis.table.internal.exception.SerializationException;

import java.nio.charset.StandardCharsets;

/**
 * @author weilai
 */
public class JsonListRedisSerializer extends BaseRedisSerializer<JsonListTestDTO> {

    @Override
    public byte[] serialize(JsonListTestDTO t) throws SerializationException {
        return new Gson().toJson(t).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public JsonListTestDTO deserialize(byte[] bytes) throws SerializationException {
        return new Gson().fromJson(new String(bytes, StandardCharsets.UTF_8), JsonListTestDTO.class);
    }
}
