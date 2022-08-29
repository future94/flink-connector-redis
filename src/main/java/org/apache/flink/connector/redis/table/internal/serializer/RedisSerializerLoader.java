package org.apache.flink.connector.redis.table.internal.serializer;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * <p>SPI方式加载编/解码器
 *
 * <p>加载META-INF/services/org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer中的类
 * @author weilai
 */
@SuppressWarnings("rawtypes")
public class RedisSerializerLoader {

    private static final List<RedisSerializer> CONVERT_LIST = new LinkedList<>();

    static {
        ServiceLoader.load(RedisSerializer.class).iterator().forEachRemaining(CONVERT_LIST::add);
    }

    public static RedisSerializer get(String identifier) {
        for (RedisSerializer redisSerializer : CONVERT_LIST) {
            if (redisSerializer.identifier().equals(identifier)) {
                return redisSerializer;
            }
        }
        throw new UnsupportedOperationException("没有找到对应的redis serializer: " + identifier);
    }
}
