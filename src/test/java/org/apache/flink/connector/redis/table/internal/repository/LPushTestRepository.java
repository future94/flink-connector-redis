package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;
import org.apache.flink.connector.redis.table.internal.entity.TestJsonEntity;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

/**
 * @author weilai
 */
@RedisRepository(value = "lpush", insertCommand = RedisCommandType.LPUSH)
public class LPushTestRepository extends BaseRepository<TestJsonEntity>{

}
