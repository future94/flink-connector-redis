package org.apache.flink.connector.redis.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * @author weilai
 */
public class ExampleSingleTest {

    /**
     * <p>Redis使用GET命令匹配，存储的值为String类型
     *
     * <pre>redis数据的格式:
     *          key =>  value
     *          5   =>  123456
     *          6   =>  1234567
     */
    @Test
    public void getString() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String dim = "create table dim_table(name varchar, login_time time(3) ) with ( " +
                "'connector'='redis', " +
                "'model'='single', " +
                "'single.node'='192.168.10.14:6379', " +
                "'password'='password', " +
                "'database'='5', " +
                "'command'='get')";
        String source = "create table source_table(username varchar, level varchar, proctime as procTime()) "
                + "with ('connector'='datagen',  'rows-per-second'='1', "
                + "'fields.username.kind'='sequence',  'fields.username.start'='5',  'fields.username.end'='6',"
                + "'fields.level.kind'='sequence',  'fields.level.start'='15',  'fields.level.end'='16'"
                + ")";
        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        String sql = " select s.username, s.level,  d.login_time from source_table s"
                + "  left join dim_table for system_time as of s.proctime as d "
                + " on d.name = s.username";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table).print();
        env.execute("getString");
    }

    /**
     * <p>Redis使用GET命令匹配，存储的值为Json类型
     *
     * <pre>redis数据的格式:
     *             key  =>  value
     *             25   =>  {"login_time":123456,"title":"wei","desc":"lai"}
     */
    @Test
    public void getJson() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String dim = "create table dim_table(" +
                "name varchar, title varchar, login_time time(3), desc varchar ) with ( " +
                "'connector'='redis', " +
                "'model'='single', " +
                "'single.node'='192.168.10.14:6379', " +
                "'password'='password', " +
                "'database'='5', " +
                "'command'='get', " +
                "'value.serializer'='json')";
        String source = "create table source_table(username varchar, level varchar, proctime as procTime()) "
                + "with ('connector'='datagen',  'rows-per-second'='1', "
                + "'fields.username.kind'='sequence',  'fields.username.start'='25',  'fields.username.end'='26',"
                + "'fields.level.kind'='sequence',  'fields.level.start'='35',  'fields.level.end'='36')";
        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        String sql = " select s.username, d.desc, s.level, d.login_time, d.title from source_table s"
                + "  left join dim_table for system_time as of s.proctime as d "
                + " on d.name = s.username";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table).print();
        env.execute("getJson");
    }

    /**
     * <p>Redis使用HGET命令匹配，存储的值为String类型，ON的条件有两个，并需要用HASH的KEY进行匹配
     *
     * <pre>redis数据的格式:
     *          key => field =>  value
     *          45  => 55    =>  wei
     */
    @Test
    public void hGetStringHasHashKey() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String dim = "create table dim_table(name varchar, level varchar, desc varchar) with ( " +
                "'connector'='redis', " +
                "'model'='single', " +
                "'single.node'='192.168.10.14:6379', " +
                "'password'='password', " +
                "'database'='5', " +
                "'command'='hget')";
        String source = "create table source_table(username varchar, level varchar, proctime as procTime()) "
                + "with ('connector'='datagen',  'rows-per-second'='1', "
                + "'fields.username.kind'='sequence',  'fields.username.start'='45',  'fields.username.end'='46',"
                + "'fields.level.kind'='sequence',  'fields.level.start'='55',  'fields.level.end'='56'"
                + ")";
        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        String sql = " select s.username, s.level, d.name, d.desc from source_table s"
                + "  left join dim_table for system_time as of s.proctime as d "
                + " on d.name = s.username and d.level = s.level";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table).print();
        env.execute("hGetStringHasHashKey");
    }

    /**
     * <p>Redis使用HGET命令匹配，存储的值为String类型，ON的条件有一个，即不需要用HASH的KEY进行匹配，只通过HASH的FIELD进行匹配，而且在维度表要展示HASH的KEY
     *
     * <pre>redis数据的格式:
     *          key => field =>  value
     *          45  => 55    =>  wei
     */
    @Test
    public void hGetStringNoHashKeyShow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String dim = "create table dim_table(name varchar, level varchar, desc varchar) with ( " +
                "'connector'='redis', " +
                "'model'='single', " +
                "'single.node'='192.168.10.14:6379', " +
                "'password'='password', " +
                "'database'='5', " +
                "'command'='hget', " +
                "'hash.key' = '45')";
        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='45',  'fields.username.end'='46',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='55',  'fields.level.end'='56'"
                        + ")";
        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        String sql = " select s.username, s.level, d.name, d.level, d.desc from source_table s"
                + "  left join dim_table for system_time as of s.proctime as d "
                + " on d.level = s.level";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table).print();
        env.execute("hGetStringNoHasHashKey");
    }

    /**
     * <p>Redis使用HGET命令匹配，存储的值为String类型，ON的条件有一个，即不需要用HASH的KEY进行匹配，只通过HASH的FIELD进行匹配，而且在维度表不需要要展示HASH的KEY
     *
     * <pre>redis数据的格式:
     *          key => field =>  value
     *          45  => 55    =>  wei
     */
    @Test
    public void hGetStringNoHashKeyNotShow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String dim = "create table dim_table(level varchar, desc varchar) with ( " +
                "'connector'='redis', " +
                "'model'='single', " +
                "'single.node'='192.168.10.14:6379', " +
                "'password'='password', " +
                "'database'='5', " +
                "'command'='hget', " +
                "'hash.key' = '45')";
        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='45',  'fields.username.end'='46',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='55',  'fields.level.end'='56'"
                        + ")";
        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        String sql = " select s.username, s.level, d.level, d.desc from source_table s"
                + "  left join dim_table for system_time as of s.proctime as d "
                + " on d.level = s.level";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table).print();
        env.execute("hGetStringNoHasHashKey");
    }

    /**
     * <p>Redis使用HGET命令匹配，存储的值为Json类型，ON的条件有两个，并需要用HASH的KEY进行匹配
     *
     * <pre>redis数据的格式:
     *             key => field =>  value
     *             65  => 75    =>  {"login_time":123456,"title":"wei","desc":"lai"}
     */
    @Test
    public void hGetJsonHasHashKey() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String dim = "create table dim_table(name varchar, level varchar, title varchar, login_time time(3), desc varchar) with ( " +
                "'connector'='redis', " +
                "'model'='single', " +
                "'single.node'='192.168.10.14:6379', " +
                "'password'='password', " +
                "'database'='5', " +
                "'value.serializer'='json'," +
                "'command'='hget')";
        String source = "create table source_table(username varchar, level varchar, proctime as procTime()) "
                + "with ('connector'='datagen',  'rows-per-second'='1', "
                + "'fields.username.kind'='sequence',  'fields.username.start'='65',  'fields.username.end'='66',"
                + "'fields.level.kind'='sequence',  'fields.level.start'='75',  'fields.level.end'='76'"
                + ")";
        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        String sql = " select s.username, s.level, d.name, d.login_time, d.level, d.desc, d.title from source_table s"
                + "  left join dim_table for system_time as of s.proctime as d "
                + " on d.name = s.username and d.level = s.level";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table).print();
        env.execute("hGetStringHasHashKey");
    }

    /**
     * <p>Redis使用HGET命令匹配，存储的值为Json类型，ON的条件有一个，即不需要用HASH的KEY进行匹配，只通过HASH的FIELD进行匹配，而且在维度表要展示HASH的KEY
     *
     * <pre>redis数据的格式:
     *             key => field =>  value
     *             65  => 75    =>  {"login_time":123456,"title":"wei","desc":"lai"}
     */
    @Test
    public void hGetJsonNoHashKey() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String dim = "create table dim_table(level varchar, title varchar, login_time time(3), desc varchar) with ( " +
                "'connector'='redis', " +
                "'model'='single', " +
                "'single.node'='192.168.10.14:6379', " +
                "'password'='password', " +
                "'database'='5', " +
                "'hash.key'='65'," +
                "'value.serializer'='json'," +
                "'command'='hget')";
        String source = "create table source_table(username varchar, level varchar, proctime as procTime()) "
                + "with ('connector'='datagen',  'rows-per-second'='1', "
                + "'fields.username.kind'='sequence',  'fields.username.start'='65',  'fields.username.end'='66',"
                + "'fields.level.kind'='sequence',  'fields.level.start'='75',  'fields.level.end'='76'"
                + ")";
        tEnv.executeSql(source);
        tEnv.executeSql(dim);
        String sql = " select s.username, s.level, d.login_time, d.level, d.desc, d.title from source_table s"
                + "  left join dim_table for system_time as of s.proctime as d "
                + " on d.level = s.level";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table).print();
        env.execute("hGetStringHasHashKey");
    }
}
