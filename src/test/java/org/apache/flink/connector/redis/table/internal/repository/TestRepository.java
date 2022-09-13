package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;
import org.apache.flink.connector.redis.table.internal.entity.TestEntity;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

/**
 * @author weilai
 */
@RedisRepository(insertCommand = RedisCommandType.SET)
public class TestRepository extends BaseRepository<TestEntity>{

}
