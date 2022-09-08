package org.apache.flink.connector.redis.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * @author weilai
 */
public class ExampleSingleSinkTest {

    @Test
    public void setString() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("" +
                "create table sink_table (" +
                "   username varchar, " +
                "   level varchar" +
                ") with (" +
                "   'connector'='redis', " +
                "   'model'='single', " +
                "   'single.node'='192.168.10.14:6379', " +
                "   'password'='password', " +
                "   'database'='5', " +
                "   'command'='set')");
        String sql = " insert into sink_table values ('setString', 'test11')";
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
                "   'value.serializer'='jsonStringHash', " +
                "   'command'='set')");
        String sql = " insert into sink_table values ('setJson', '描述', '标题', 123456)";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.print();
    }
}