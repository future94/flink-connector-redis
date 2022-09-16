package org.apache.flink.connector.redis.table.internal.repository;

import lombok.Data;
import org.apache.flink.connector.redis.table.internal.annotation.RedisKey;
import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.io.Serializable;

/**
 * @author weilai
 */
public class ObjectRepositoryTest {

    @Test
    public void setObjectRepository() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("" +
                "create table sink_table (" +
                "   username varchar, " +
                "   desc varchar," +
                "   title varchar," +
                "   login_time int" +
                ") with (" +
                "   'connector'='redis', " +
                "   'model'='single', " +
                "   'single.node'='192.168.10.14:6379', " +
                "   'password'='password', " +
                "   'database'='5', " +
                "   'scan'='org.apache.flink.connector.redis.table.internal.repository'," +
                "   'repository'='testObjectRepository'" +
                ")");
        String sql = " insert into sink_table values ('setObjectRepository', '描述', '标题', 1234567)";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.print();
    }

    @Data
    public static class ObjectEntityTest implements Serializable {

        private static final long serialVersionUID = 1L;

        @RedisKey
        private String username;

        private String desc;

        private String title;

        private Integer login_time;
    }

    @RedisRepository(value = "testObjectRepository", insertCommand = RedisCommandType.SET)
    public static class TestObjectRepository extends ObjectRepository<ObjectEntityTest> {

    }
}