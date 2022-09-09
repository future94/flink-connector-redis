package org.apache.flink.connector.redis.table.utils;

import org.apache.flink.connector.redis.table.internal.annotation.RedisEntity;
import org.junit.Test;

/**
 * @author weilai
 */
public class ClassUtilsTest {

    @Test
    public void defaultClass() {
        System.out.println(ClassUtils.scanClasses("org.apache.flink.connector.redis.table.internal.entity"));
    }

    @Test
    public void scanClasses() throws ClassNotFoundException {
        for (String scanClass : ClassUtils.scanClasses(getClass().getClassLoader(), "org.apache.flink.connector.redis.table.internal.entity")) {
            RedisEntity entity = Class.forName(scanClass).getAnnotation(RedisEntity.class);
            System.out.println(entity);
        }
    }
}