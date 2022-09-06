- github: https://github.com/future94/flink-connector-redis
- gitee : https://gitee.com/future94/flink-connector-redis

---

<p align="center">
    <a target="_blank" href="https://github.com/future94/flink-connector-redis/blob/master/LICENSE">
        <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg?label=license" />
    </a>
    <a target="_blank" href="https://www.oracle.com/technetwork/java/javase/downloads/index.html">
        <img src="https://img.shields.io/badge/JDK-8+-green.svg" />
    </a>
   <a target="_blank" href='https://github.com/future94/flink-connector-redis'>
        <img src="https://img.shields.io/github/forks/future94/flink-connector-redis.svg" alt="github forks"/>
   </a>
   <a target="_blank" href='https://github.com/future94/flink-connector-redis'>
        <img src="https://img.shields.io/github/stars/future94/flink-connector-redis.svg" alt="github stars"/>
   </a>
   <a target="_blank" href='https://github.com/future94/flink-connector-redis'>
        <img src="https://img.shields.io/github/contributors/future94/flink-connector-redis.svg" alt="github contributors"/>
   </a>
</p>

# Stargazers over time

[![Stargazers over time](https://starchart.cc/future94/flink-connector-redis.svg)](https://starchart.cc/future94/flink-connector-redis.svg)

## 为什么写这个项目

对比其他的`flink-connector-redis`
，基本上已经停止维护或者使用不是很方便，因公司也要使用，所以写这个项目。目前还在快速开发中，并且会持续更新中。如果您有什么需要暂时还不支持，可以提交[issues](https://github.com/future94/flink-connector-redis/issues)
.

---

# Features

* Table API Redis Sink
* Table API Redis Source(非维度表直接查询)
* Streaming Redis Sink
* Streaming Redis Source
* More Redis Command

---

# Development environment

* JDK 1.8
* Flink 1.14.5
* Jedis 4.2.3

---

# Quick Start

## 使用方式

```xml

<dependency>
    <groupId>io.github.future94</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <version>1.0.SNAPSHOT</version>
</dependency>
```

## 支持功能

* Redis单机模式、主从模式(读写分离)、集群模式
* Redis Source DIM层维度表查询
* 自定义Redis编解码器(复杂的value也可以关联到维度表中)
* 自定义Redis命令数据转换

## 支持的Redis命令

目前只开发了维表查询，其他命令后续支持。

| Sink | Source |
|------|--------|
|      | get    |
|      | hget   |

## With配置可选项

| 字段               | 默认值    | 类型      | 说明                                                                                                                                           |
|------------------|--------|---------|----------------------------------------------------------------------------------------------------------------------------------------------|
| connector        | (无)    | String  | 如果使用，固定填写`redis`                                                                                                                             |
| model            | single | String  | Redis的模式：single(单机)、master_slave(主从/读写分离)、cluster(集群)                                                                                        |
| password         | (无)    | String  | Redis的密码                                                                                                                                     |
| single.node      | (无)    | String  | Redis单机模式的地址，格式为`ip[:port]`，如果端口不写默认为`6379`                                                                                                  |
| master.node      | (无)    | String  | Redis主从模式Master的地址，格式为`ip[:port]`，如果端口不写默认为`6379`                                                                                            |
| slave.nodes      | (无)    | String  | Redis主从模式Slave的地址，格式为`ip[:port][:weight][,ip:port:weight]`，支持多个Slave节点（用英文逗号,分隔），如果端口不写默认为`6379` ，权重不写默认为1(多节点都为1或相等就是轮询)，如果指定权重，按固定格式也要指定端口 |
| cluster.nodes    | (无)    | String  | Redis主从模式Slave的地址，格式为`ip[:port][,ip:port]` ，如果端口不写默认为`6379`                                                                                  |
| command          | (无)    | String  | 运行的Redis命令，使用上面支持的命令                                                                                                                         |
| timeout          | 1000ms | Integer | 链接超时时间                                                                                                                                       |
| database         | 0      | Integer | 单机模式链接的数据库                                                                                                                                   |
| max.total        | 8      | Integer | 最大链接数                                                                                                                                        |
| max.idle         | 8      | Integer | 最大保持连接数                                                                                                                                      |
| min.idle         | 0      | Integer | 最小保持连接数                                                                                                                                      |
| key.serializer   | string | String  | Redis的KEY编解码器，默认使用`string`方式，通过`SPI`方式可以更改，下面会说                                                                                              |
| value.serializer | string | String  | Redis的VALUE编解码器，默认使用`string`方式，通过`SPI`方式可以更改，下面会说                                                                                            |
| hash.key         | (无)    | String  | Redis如果使用Hash数据结构，如果要关联的表中没有key的信息，可以在这里指定，这样就可以只通过Field进行关联，类似String结构的key => value                                                         |

## 自定义编解码器

通过`SPI`方式可以对redis的解码方式进行自定义，这**非常重要**，因为很多情况下维度信息不仅仅是单纯的string，我们通常会存储一些Pojo类，如`json`、`byte[]`
等格式写入，通过自定义编辑码器我们会更方便的关联到创建的维度表中(不需要严格顺序匹配，后面说)。

#### 自定义方式

在`resources/META-INF/services/org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer`
文件中指定自定义类，该类要实现`org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer`接口。

```java
public interface RedisSerializer<V> extends Serializable {

    /**
     * 标识符，这个就是您在key.serializer和value.serializer中指定的值
     */
    String identifier();

    /**
     * 序列化
     */
    byte[] serialize(BinaryStringData t) throws SerializationException;

    /**
     * 反序列化
     */
    V deserialize(byte[] bytes) throws SerializationException;

}
```

## 自定义命令数据转换

通过`SPI`方式可以对redis的运行返回的数据转为FlinkTableAPI数据进行自定义，这作用是如果你有自己定制的转换方式或者觉得该框架提供的实现不好或者不方便时可以自己实现(还是欢迎pr)。

#### 自定义方式

在`resources/META-INF/services/org.apache.flink.connector.redis.table.internal.converter.RedisCommandToRowConverter`
文件中指定自定义类，
该类要实现`org.apache.flink.connector.redis.table.internal.converter.RedisCommandToRowConverter`接口。
也可以继承`org.apache.flink.connector.redis.table.internal.converter.BaseRedisCommandToRowConverter`抽象类。

```java
public interface RedisCommandToRowConverter {

    /**
     * 支持的命令类型
     */
    RedisCommandType support();

    /**
     * 转换数据
     * @param redisCommand          运行环境
     * @param columnNameList        字段名集合
     * @param columnDataTypeList    字段类型集合
     * @param readOptions           读取参数配置
     * @param keys                  联表Key[]
     * @return 转换的数据
     * @throws Exception            转换失败
     */
    Optional<GenericRowData> convert(final RedisCommand redisCommand, final List<String> columnNameList, final List<DataType> columnDataTypeList, final RedisReadOptions readOptions, final Object[] keys) throws Exception;
}
```

# Quick Start

## 维度关联

### GET

#### 编解码器为`string`。

**这个格式是严格按照顺序进行匹配的(因为只有key value两个值)，维度表第一个值为key，维度表第二个值为value，如果大于两个字典，其他值也全部为null**。

```java
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
        // name为get的key，login_time为get拿到的value
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
        // 通过redis的key进行关联(d.name)
        String sql = " select s.username, s.level,  d.login_time from source_table s"
                + "  left join dim_table for system_time as of s.proctime as d "
                + " on d.name = s.username";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table).print();
        env.execute("getString");
    }
}
```

#### 自定义编解码器，用`json`举例。

**这个格式是不按照顺序进行匹配的**

* 如果反序列化的POJO与维度表字段相同，则会按POJO字段名字与Table字段名对应解析，**这时候会没有key的信息**。
* 如果反序列化的POJO与维度表字段不相同，则Table表第一个字段值为key，其他会按POJO字段名字与Table字段名对应解析，如果有其他值也全部为null。

在`resources/META-INF/services/org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer`文件中添加下面。

```text
org.apache.flink.connector.redis.table.serializer.JsonListRedisSerializer
```

```java
public class JsonRedisSerializer implements RedisSerializer<JsonTestDTO> {

    private static final String IDENTIFIER = "json";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public byte[] serialize(BinaryStringData t) throws SerializationException {
        return new Gson().toJson(t.toString()).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public JsonTestDTO deserialize(byte[] bytes) throws SerializationException {
        return new Gson().fromJson(new String(bytes, StandardCharsets.UTF_8), JsonTestDTO.class);
    }
}

public class JsonTestDTO {

    private String desc;

    private Integer login_time;

    private String title;
}
```

具体使用

```java
public class ExampleSingleTest {
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
        // name为key值，title、login_time、desc为反序列化解析出的值
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
        // 通过redis的key进行关联(d.name)
        String sql = " select s.username, d.desc, s.level, d.login_time, d.title from source_table s"
                + "  left join dim_table for system_time as of s.proctime as d "
                + " on d.name = s.username";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table).print();
        env.execute("getJson");
    }
}
```

### HGET

#### 编解码器为`string`。

**这个格式是严格按照顺序进行匹配的**，关联可以通过ON指定两个条件，也可以通过配置指定`hash.key`只通过一个ON条件关联field。
如果维度信息在不同的hashKey中，那么您必须指定两个ON条件才能分别找到，如果都在一个hash中，则可以省略。

* 如果维度表有两个字段，那么`第一个值为field`，维度表第二个值为value。
* 如果维度表有三个字段，那么`第一个值为field`，维度表第二个值为value。
* 多余三个字段，多余的值全部为null。

```java
public class ExampleSingleTest {
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
        // 这里指定了3个字段，所以key为name，field为level，value为desc
        // 也可以指定2个字段（level varchar, desc varchar），则field为level，value为desc
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
        // 这里指定了两个条件(d.name = s.username and d.level = s.level)
        // 也可以指定一个条件(d.level = s.level)，然后通过with参数指定hash的key
        String sql = " select s.username, s.level, d.name, d.desc from source_table s"
                + "  left join dim_table for system_time as of s.proctime as d "
                + " on d.name = s.username and d.level = s.level";
        Table table = tEnv.sqlQuery(sql);
        tEnv.toDataStream(table).print();
        env.execute("hGetStringHasHashKey");
    }
}
```

#### 自定义编解码器，用`json`举例。

**这个格式是不按照顺序进行匹配的**，key和信息可以通过with的`hash.key`指定，也可以通过两个ON条件。

* 如果反序列化的POJO与维度表字段相同，则会按POJO字段名字与Table字段名对应解析，**这时候会没有key和field的信息**。
* 如果反序列化的POJO与维度表字段不相同
    * 维度表字段大于POJO一个，则`Table表第一个字段值为field`，其他会按POJO字段名字与Table字段名对应解析，**这时候会没有key的信息**。如果有其他值也全部为null。
    * 维度表字段大于POJO两个，则`Table表第一个字段值为key`，`第二个字段值为field`，其他会按POJO字段名字与Table字段名对应解析，如果有其他值也全部为null。
    * 其他情况`RuntimeException`。

```java
public class ExampleSingleTest {

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
}
```