package org.apache.flink.connector.redis.table.internal.options;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.redis.table.internal.enums.CacheLoadModel;
import org.apache.flink.connector.redis.table.internal.enums.CacheMissModel;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.enums.RedisModel;
import org.apache.flink.connector.redis.table.internal.repository.IndexRepository;
import org.apache.flink.connector.redis.table.internal.serializer.StringRedisSerializer;

import java.util.Map;

/**
 * <p>TableAPI的Redis配置
 *
 * @author weilai
 */
@PublicEvolving
public class RedisConnectorOptions {

    public static final ConfigOption<RedisModel> MODEL =
            ConfigOptions.key("model")
                    .enumType(RedisModel.class)
                    .defaultValue(RedisModel.SINGLE)
                    .withDescription("redis运行模式.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .defaultValue("")
                    .withDescription("redis密码.");

    public static final ConfigOption<String> SINGLE_CONFIG =
            ConfigOptions.key("single.node")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis单机节点地址.");

    public static final ConfigOption<String> MASTER_CONFIG =
            ConfigOptions.key("master.node")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis主节点地址.");

    public static final ConfigOption<String> SLAVE_CONFIG =
            ConfigOptions.key("slave.nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis从节点地址.");

    public static final ConfigOption<String> CLUSTER_CONFIG =
            ConfigOptions.key("cluster.nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis集群节点地址.");

    public static final ConfigOption<RedisCommandType> COMMAND =
            ConfigOptions.key("command")
                    .enumType(RedisCommandType.class)
                    .noDefaultValue()
                    .withDescription("redis命令.");

    public static final ConfigOption<Integer> TIMEOUT =
            ConfigOptions.key("timeout")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("redis链接超时时间.");

    public static final ConfigOption<Integer> DATABASE =
            ConfigOptions.key("database")
                    .intType()
                    .defaultValue(0)
                    .withDescription("redis数据库.");

    public static final ConfigOption<Integer> MAX_IDLE =
            ConfigOptions.key("max.idle")
                    .intType()
                    .defaultValue(8)
                    .withDescription("redis最大保持连接数.");

    public static final ConfigOption<Integer> MIN_IDLE =
            ConfigOptions.key("min.idle")
                    .intType()
                    .defaultValue(0)
                    .withDescription("redis最小保持连接数.");

    public static final ConfigOption<Integer> MAX_TOTAL =
            ConfigOptions.key("max.total")
                    .intType()
                    .defaultValue(8)
                    .withDescription("redis最大链接数.");

    public static final ConfigOption<String> KEY_SERIALIZER =
            ConfigOptions.key("key.serializer")
                    .stringType()
                    .defaultValue(StringRedisSerializer.IDENTIFIER)
                    .withDescription("redis的key编解码器.");

    public static final ConfigOption<String> VALUE_SERIALIZER =
            ConfigOptions.key("value.serializer")
                    .stringType()
                    .defaultValue(StringRedisSerializer.IDENTIFIER)
                    .withDescription("redis的value编解码器.");

    public static final ConfigOption<String> HASH_KEY =
            ConfigOptions.key("hash.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis的hash数据类型的key.");

    public static final ConfigOption<String> LIST_KEY =
            ConfigOptions.key("list.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis的list数据类型的key.");

    public static final ConfigOption<CacheLoadModel> CACHE_LOAD_MODEL =
            ConfigOptions.key("cache.load")
                    .enumType(CacheLoadModel.class)
                    .defaultValue(CacheLoadModel.LAZY)
                    .withDescription("缓存加载策略.");

    public static final ConfigOption<Map<String, String>> CACHE_FIELD_NAMES =
            ConfigOptions.key("cache.field-names")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("缓存中参数联表的字段.");

    public static final ConfigOption<CacheMissModel> CACHE_MISS_MODEL =
            ConfigOptions.key("cache.miss")
                    .enumType(CacheMissModel.class)
                    .defaultValue(CacheMissModel.REFRESH)
                    .withDescription("缓存未命中策略.");

    public static final ConfigOption<String> SCAN =
            ConfigOptions.key("scan")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("扫描的包路径.");

    public static final ConfigOption<String> SCAN_REPOSITORY =
            ConfigOptions.key("scan.repository")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis数据访问层类扫描包路径.");

    public static final ConfigOption<String> REPOSITORY =
            ConfigOptions.key("repository")
                    .stringType()
                    .defaultValue(IndexRepository.IDENTIFIER)
                    .withDescription("redis数据访问层类.");
}
