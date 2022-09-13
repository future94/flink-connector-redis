package org.apache.flink.connector.redis.table.internal.serializer;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.connector.redis.table.internal.annotation.RedisField;
import org.apache.flink.connector.redis.table.internal.annotation.RedisKey;
import org.apache.flink.connector.redis.table.internal.exception.SerializationException;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * @author weilai
 */
public class JsonRedisSerializer<V> extends BaseRedisSerializer<V> {

    private static final String IDENTIFIER = "json";

    private static final Gson gson;

    static {
        gson = new GsonBuilder().addSerializationExclusionStrategy(new ExclusionStrategy() {
            @Override
            public boolean shouldSkipField(FieldAttributes fieldAttributes) {
                return Objects.nonNull(fieldAttributes.getAnnotation(RedisKey.class))
                        || Objects.nonNull(fieldAttributes.getAnnotation(RedisField.class));
            }

            @Override
            public boolean shouldSkipClass(Class<?> aClass) {
                return false;
            }
        }).create();
    }

    public JsonRedisSerializer() {
        super();
    }

    public JsonRedisSerializer(Class<?> clazz) {
        super((Class<V>) clazz);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public byte[] serialize(Object t) throws SerializationException {
        return gson.toJson(t).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public V deserialize(byte[] bytes) throws SerializationException {
        return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), getValueClass());
    }
}
