package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * @author weilai
 */
public class IndexRepositoryTest {

    @Test
    public void insertSetIndexSimple() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("" +
                "create table sink_table (" +
                "   username varchar, " +
                "   age varchar" +
                ") with (" +
                "   'connector'='redis', " +
                "   'model'='single', " +
                "   'single.node'='192.168.10.14:6379', " +
                "   'password'='password', " +
                "   'value.serializer'='string', " +
                "   'database'='5', " +
                "   'command'='set')");
        String sql = " insert into sink_table (username, age) values ('insertSetIndexSimple', 'setValue1')";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.print();
    }

    @Test
    public void insertSetIndexJson() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("" +
                "create table sink_table (" +
                "   username varchar, " +
                "   age varchar," +
                "   level varchar," +
                "   login_time varchar," +
                "   mobile varchar" +
                ") with (" +
                "   'connector'='redis', " +
                "   'model'='single', " +
                "   'single.node'='192.168.10.14:6379', " +
                "   'password'='password', " +
                "   'value.serializer'='json', " +
                "   'database'='5', " +
                "   'command'='set')");
        String sql = " insert into sink_table (username, age, level, login_time, mobile) values ('insertSetIndexJson', 'setValue1', 'level1', '123', '186')";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.print();
    }

    @Test
    public void insertSetIndexObject() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("" +
                "create table sink_table (" +
                "   username varchar, " +
                "   age varchar," +
                "   level varchar," +
                "   login_time varchar," +
                "   mobile varchar" +
                ") with (" +
                "   'connector'='redis', " +
                "   'model'='single', " +
                "   'single.node'='192.168.10.14:6379', " +
                "   'password'='password', " +
                "   'value.serializer'='object', " +
                "   'database'='5', " +
                "   'command'='set')");
        String sql = " insert into sink_table (username, age, level, login_time, mobile) values ('insertSetIndexObject', 'setValue1', 'level1', '123', '186')";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.print();
    }
}