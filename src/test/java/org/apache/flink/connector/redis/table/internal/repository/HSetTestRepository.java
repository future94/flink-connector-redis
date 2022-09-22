package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;
import org.apache.flink.connector.redis.table.internal.entity.HSetTestJsonEntity;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

/**
 * @author weilai
 */
@RedisRepository(value = "hset", insertCommand = RedisCommandType.HSET)
public class HSetTestRepository extends EntityRepository<HSetTestJsonEntity> {

}
