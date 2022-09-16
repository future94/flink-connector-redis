package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.entity.TestJsonEntity;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

/**
 * @author weilai
 */
public class RPushTestRepository extends JsonRepository<TestJsonEntity>{

    @Override
    protected RedisCommandType insertCommand() {
        return RedisCommandType.RPUSH;
    }
}
