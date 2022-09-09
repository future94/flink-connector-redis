package org.apache.flink.connector.redis.table.internal.entity;

import lombok.Data;
import org.apache.flink.connector.redis.table.internal.annotation.RedisEntity;
import org.apache.flink.connector.redis.table.internal.annotation.RedisField;
import org.apache.flink.connector.redis.table.internal.annotation.RedisKey;
import org.apache.flink.connector.redis.table.internal.annotation.RedisValue;

/**
 * @author weilai
 */
@Data
@RedisEntity("test")
public class TestEntity {

    @RedisKey
    private String username;

    @RedisField
    private String level;

    @RedisValue
    private String age;
}
