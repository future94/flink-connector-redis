package org.apache.flink.connector.redis.table.internal.annotation;

import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.serializer.JsonRedisSerializer;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.connector.redis.table.internal.serializer.StringRedisSerializer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author weilai
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RedisRepository {

    /**
     * <p>唯一标识
     *
     * <li>如果通过SPI的方式加载，这里可以不设置，
     * <li>如果通过SCAN的方式加载，这里可以必须设置，否则不会被载入
     */
    String value() default "";

    Class<? extends RedisSerializer> keySerializer() default StringRedisSerializer.class;
    
    Class<? extends RedisSerializer> valueSerializer() default JsonRedisSerializer.class;

    RedisCommandType selectCommand() default RedisCommandType.NONE;

    RedisCommandType insertCommand() default RedisCommandType.NONE;

    RedisCommandType updateCommand() default RedisCommandType.NONE;

    RedisCommandType deleteCommand() default RedisCommandType.NONE;
}
