package org.apache.flink.connector.redis.table;

import lombok.SneakyThrows;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.table.internal.enums.RedisModel;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisLookupOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializerLoader;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.CLUSTER_CONFIG;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.COMMAND;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.DATABASE;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.HASH_KEY;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.KEY_SERIALIZER;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.MASTER_CONFIG;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.MAX_IDLE;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.MAX_TOTAL;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.MIN_IDLE;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.MODEL;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.PASSWORD;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.SINGLE_CONFIG;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.SLAVE_CONFIG;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.TIMEOUT;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.VALUE_SERIALIZER;

/**
 * <p>Redis动态表工厂
 * @author weilai
 */
public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "redis";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return null;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        String command = RedisConnectorOptions.COMMAND.key();
        if (context.getCatalogTable().getOptions().containsKey(command)) {
            context.getCatalogTable()
                    .getOptions()
                    .put(
                            command,
                            context.getCatalogTable()
                                    .getOptions()
                                    .get(command)
                                    .toUpperCase());
        }
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        return new RedisDynamicTableSource(
                getRedisConnectionOptions(helper.getOptions()),
                getRedisReadOptions(helper.getOptions()),
                getRedisLookupOptions(helper.getOptions()),
                context.getCatalogTable().getResolvedSchema());
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(MODEL);
        requiredOptions.add(COMMAND);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(SINGLE_CONFIG);
        optionalOptions.add(MASTER_CONFIG);
        optionalOptions.add(SLAVE_CONFIG);
        optionalOptions.add(CLUSTER_CONFIG);
        optionalOptions.add(TIMEOUT);
        optionalOptions.add(DATABASE);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(KEY_SERIALIZER);
        optionalOptions.add(VALUE_SERIALIZER);
        optionalOptions.add(HASH_KEY);
        return optionalOptions;
    }

    @SneakyThrows
    private RedisReadOptions getRedisReadOptions(ReadableConfig readableConfig) {
        String keySerializer = readableConfig.get(KEY_SERIALIZER);
        String valueSerializer = readableConfig.get(VALUE_SERIALIZER);
        String hashKey = readableConfig.get(HASH_KEY);
        return RedisReadOptions.builder()
                .keySerializer(RedisSerializerLoader.get(keySerializer))
                .valueSerializer(RedisSerializerLoader.get(valueSerializer))
                .hashKey(hashKey)
                .build();
    }

    private RedisLookupOptions getRedisLookupOptions(ReadableConfig readableConfig) {
        return RedisLookupOptions.builder().build();
    }

    private RedisConnectionOptions getRedisConnectionOptions(ReadableConfig readableConfig) {
        RedisModel redisModel = readableConfig.get(MODEL);
        RedisConnectionOptions builder = RedisConnectionOptions.builder()
                .redisModel(redisModel)
                .command(readableConfig.get(COMMAND))
                .timeout(readableConfig.get(TIMEOUT))
                .maxTotal(readableConfig.get(MAX_TOTAL))
                .maxIdle(readableConfig.get(MAX_IDLE))
                .minIdle(readableConfig.get(MIN_IDLE))
                .build();
        switch (redisModel) {
            case SINGLE:
                readableConfig.getOptional(SINGLE_CONFIG).ifPresent(singleConfig -> builder.setSingleConfig(parse(singleConfig)));
                break;
            case MASTER_SLAVE:
                builder.setMasterConfig(parse(readableConfig.get(MASTER_CONFIG)));
                String slaveConfig = readableConfig.get(SLAVE_CONFIG);
                String[] slaveNodes = slaveConfig.split(",");
                if (slaveNodes.length < 1) {
                    throw new IllegalArgumentException("slave.nodes配置格式不正确");
                }
                List<RedisConnectionOptions.Config> slaveConfigList = new ArrayList<>(slaveNodes.length);
                for (String node : slaveNodes) {
                    slaveConfigList.add(parse(node));
                }
                builder.setSlaveConfigs(slaveConfigList);
                break;
            case CLUSTER:
                readableConfig.getOptional(CLUSTER_CONFIG).ifPresent(clusterConfig -> {
                    String[] clusterNodes = clusterConfig.split(",");
                    if (clusterNodes.length < 1) {
                        throw new IllegalArgumentException("cluster.nodes配置格式不正确");
                    }
                    List<RedisConnectionOptions.Config> clusterConfigList = new ArrayList<>(clusterNodes.length);
                    for (String node : clusterNodes) {
                        clusterConfigList.add(parse(node));
                    }
                    builder.setClusterConfigs(clusterConfigList);
                });
                break;
        }
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        readableConfig.getOptional(DATABASE).ifPresent(builder::setDatabase);
        return builder;
    }

    private RedisConnectionOptions.Config parse(String node) {
        String[] split = node.split(":");
        if (split.length < 1) {
            throw new IllegalArgumentException("node配置格式不正确");
        }
        RedisConnectionOptions.Config build = RedisConnectionOptions.Config.builder()
                .host(split[0])
                .build();
        if (split.length > 1) {
            build.setPort(Integer.parseInt(split[1]));
        }
        if (split.length > 2) {
            build.setWeight(Integer.parseInt(split[2]));
        }
        return build;
    }

    private void validateConfigOptions(ReadableConfig config) {

        checkAllOrNone(config, new ConfigOption[]{MASTER_CONFIG, SLAVE_CONFIG});

        if (config.get(MAX_TOTAL) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            MAX_TOTAL.key(), config.get(MAX_TOTAL)));
        }

        if (config.get(MAX_IDLE) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            MAX_IDLE.key(), config.get(MAX_IDLE)));
        }

        if (config.get(MIN_IDLE) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option must be in second granularity and shouldn't be smaller than 1 second, but is %s.",
                            MIN_IDLE.key(),
                            config.get(MIN_IDLE)));
        }
    }

    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption<?> configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames =
                Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(
                configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n"
                        + String.join("\n", propertyNames));
    }

}
