package org.apache.flink.connector.redis.table.internal.entity;

import lombok.Data;
import org.apache.flink.connector.redis.table.internal.annotation.RedisEntity;
import org.apache.flink.connector.redis.table.internal.annotation.RedisKey;

/**
 * @author weilai
 */
@Data
@RedisEntity("testJson")
public class TestJsonEntity {

    @RedisKey
    private String username;

    private String desc;

    private String title;

    private Integer login_time;
}
