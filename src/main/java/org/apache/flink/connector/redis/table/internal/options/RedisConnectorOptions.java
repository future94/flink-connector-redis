package org.apache.flink.connector.redis.table.internal.options;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.enums.RedisModel;
import org.apache.flink.connector.redis.table.internal.serializer.StringRedisSerializer;

/**
 * <p>TableAPI的Redis配置
 * @author weilai
 */
@PublicEvolving
public class RedisConnectorOptions {

    public static final ConfigOption<RedisModel> MODEL =
            ConfigOptions.key("model")
                    .enumType(RedisModel.class)
                    .defaultValue(RedisModel.SINGLE)
                    .withDescription("The redis command.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The redis command.");

    public static final ConfigOption<String> SINGLE_CONFIG =
            ConfigOptions.key("single.node")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> MASTER_CONFIG =
            ConfigOptions.key("master.node")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> SLAVE_CONFIG =
            ConfigOptions.key("slave.nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> CLUSTER_CONFIG =
            ConfigOptions.key("cluster.nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<RedisCommandType> COMMAND =
            ConfigOptions.key("command")
                    .enumType(RedisCommandType.class)
                    .noDefaultValue()
                    .withDescription("The redis command.");

    public static final ConfigOption<Integer> TIMEOUT =
            ConfigOptions.key("timeout")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("The redis command.");

    public static final ConfigOption<Integer> DATABASE =
            ConfigOptions.key("database")
                    .intType()
                    .defaultValue(0)
                    .withDescription("The redis command.");

    public static final ConfigOption<Integer> MAX_IDLE =
            ConfigOptions.key("max.idle")
                    .intType()
                    .defaultValue(8)
                    .withDescription("The redis command.");

    public static final ConfigOption<Integer> MIN_IDLE =
            ConfigOptions.key("min.idle")
                    .intType()
                    .defaultValue(0)
                    .withDescription("The redis command.");

    public static final ConfigOption<Integer> MAX_TOTAL =
            ConfigOptions.key("max.total")
                    .intType()
                    .defaultValue(8)
                    .withDescription("The redis command.");

    public static final ConfigOption<String> KEY_SERIALIZER =
            ConfigOptions.key("key.serializer")
                    .stringType()
                    .defaultValue(StringRedisSerializer.IDENTIFIER)
                    .withDescription("The redis command.");

    public static final ConfigOption<String> VALUE_SERIALIZER =
            ConfigOptions.key("value.serializer")
                    .stringType()
                    .defaultValue(StringRedisSerializer.IDENTIFIER)
                    .withDescription("The redis command.");

    public static final ConfigOption<String> HASH_KEY =
            ConfigOptions.key("hash.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The redis command.");
}
