package org.apache.flink.connector.redis.table.internal.serializer;

import org.apache.flink.connector.redis.table.serializer.JsonListRedisSerializer;
import org.apache.flink.connector.redis.table.serializer.JsonListTestDTO;
import org.junit.Test;

/**
 * @author weilai
 */
public class RedisSerializerTest {

    @Test
    public void getValueClass() {
        RedisSerializer<String> serializer1 = new StringRedisSerializer();
        System.out.println(serializer1.getValueClass());
        RedisSerializer<JsonListTestDTO> serializer2 = new JsonListRedisSerializer();
        System.out.println(serializer2.getValueClass());
    }
}