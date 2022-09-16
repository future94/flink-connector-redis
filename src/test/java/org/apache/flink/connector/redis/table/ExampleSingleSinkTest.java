package org.apache.flink.connector.redis.table;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * @author weilai
 */
@Slf4j
public class ExampleSingleSinkTest {

    @Test
    public void setString() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("" +
                "create table sink_table (" +
                "   username varchar, " +
                "   age varchar," +
                "   login_time int" +
                ") with (" +
                "   'connector'='redis', " +
                "   'model'='single', " +
                "   'single.node'='192.168.10.14:6379', " +
                "   'password'='password', " +
                "   'database'='5', " +
                "   'repository'='test', " +
                "   'command'='set')");
        String sql = " insert into sink_table (username, age) values ('setString1', 'setValue1')";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.print();
    }

    @Test
    public void setJson() throws Exception {
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
                "   'repository'='testJson', " +
                "   'value.serializer'='jsonStringHash'" +
                ")");
        String sql = " insert into sink_table values ('setJson', '描述', '标题', 1234567)";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.print();
    }

    @Test
    public void hSetJson() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("" +
                "create table sink_table (" +
                "   username varchar, " +
                "   level varchar, " +
                "   desc varchar," +
                "   title varchar," +
                "   login_time int" +
                ") with (" +
                "   'connector'='redis', " +
                "   'model'='single', " +
                "   'single.node'='192.168.10.14:6379', " +
                "   'password'='password', " +
                "   'database'='5', " +
                "   'repository'='hset', " +
                "   'scan'='org.apache.flink.connector.redis.table.internal.repository', " +
                "   'value.serializer'='jsonStringHash'" +
                ")");
        String sql = " insert into sink_table values ('hSetJson', 'hSetField', '描述', '标题', 1234567)";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.print();
    }

    @Test
    public void lPushJson() throws Exception {
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
                "   'repository'='lpush', " +
                "   'scan'='org.apache.flink.connector.redis.table.internal.repository'" +
                ")");
        String sql = " insert into sink_table values ('lPushJson', '描述', '标题', 1234567)";
        tEnv.executeSql(sql).print();
    }

    @Test
    public void rPushJson() throws Exception {
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
                "   'repository'='rpush'" +
                ")");
        String sql = " insert into sink_table values ('rPushJson', '描述', '标题', 1234567)";
        tEnv.executeSql(sql).print();
    }
}
