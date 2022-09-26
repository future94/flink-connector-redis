package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author weilai
 */
public class EntityRepositoryTest {

    @Test
    public void insertCommand() {
        TestRepository repository = new TestRepository();
        Assert.assertEquals(repository.insertCommand(), RedisCommandType.HSET);
    }

    @RedisRepository(value = "insertCommand", insertCommand = RedisCommandType.HSET)
    static class TestRepository extends EntityRepository<String> {

    }
}