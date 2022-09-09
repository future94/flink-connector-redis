package org.apache.flink.connector.redis.table;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.table.internal.annotation.RedisEntity;
import org.apache.flink.connector.redis.table.internal.annotation.redisRepository;
import org.apache.flink.connector.redis.table.internal.enums.CacheLoadModel;
import org.apache.flink.connector.redis.table.internal.enums.RedisModel;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisLookupOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializerLoader;
import org.apache.flink.connector.redis.table.utils.ClassUtils;
import org.apache.flink.connector.redis.table.utils.DefaultUtils;
import org.apache.flink.table.catalog.ResolvedSchema;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.CACHE_FIELD_NAMES;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.CACHE_LOAD_MODEL;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.CACHE_MISS_MODEL;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.CLUSTER_CONFIG;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.COMMAND;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.DATABASE;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.ENTITY;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.HASH_KEY;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.KEY_SERIALIZER;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.LIST_KEY;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.MASTER_CONFIG;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.MAX_IDLE;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.MAX_TOTAL;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.MIN_IDLE;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.MODEL;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.PASSWORD;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.REPOSITORY;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.SCAN;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.SCAN_ENTITY;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.SCAN_REPOSITORY;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.SINGLE_CONFIG;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.SLAVE_CONFIG;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.TIMEOUT;
import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.VALUE_SERIALIZER;

/**
 * <p>Redis动态表工厂
 *
 * @author weilai
 */
public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "redis";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = getTableFactoryHelper(context);
        return new RedisDynamicTableSink(
                getRedisConnectionOptions(helper.getOptions()),
                getRedisReadOptions(helper.getOptions()),
                getRedisLookupOptions(helper.getOptions()),
                context.getCatalogTable().getResolvedSchema());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = getTableFactoryHelper(context);
        return new RedisDynamicTableSource(
                getRedisConnectionOptions(helper.getOptions()),
                getRedisReadOptions(helper.getOptions()),
                getRedisLookupOptions(helper.getOptions()),
                context.getCatalogTable().getResolvedSchema());
    }

    private FactoryUtil.TableFactoryHelper getTableFactoryHelper(Context context) {
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
        validateConfigOptions(config, context.getCatalogTable().getResolvedSchema());
        return helper;
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
        optionalOptions.add(LIST_KEY);
        optionalOptions.add(CACHE_LOAD_MODEL);
        optionalOptions.add(CACHE_FIELD_NAMES);
        optionalOptions.add(CACHE_MISS_MODEL);
        optionalOptions.add(SCAN);
        optionalOptions.add(SCAN_ENTITY);
        optionalOptions.add(SCAN_REPOSITORY);
        optionalOptions.add(ENTITY);
        optionalOptions.add(REPOSITORY);
        return optionalOptions;
    }

    @SneakyThrows
    private RedisReadOptions getRedisReadOptions(ReadableConfig readableConfig) {
        String keySerializer = readableConfig.get(KEY_SERIALIZER);
        String valueSerializer = readableConfig.get(VALUE_SERIALIZER);
        String entityName = readableConfig.get(ENTITY);
        String repositoryName = readableConfig.get(REPOSITORY);
        Class<?> entityClass = null;
        Class<?> repositoryClass = null;
        if (StringUtils.isNotBlank(entityName) && StringUtils.isNotBlank(repositoryName)) {
            readableConfig.get(SCAN);
            entityClass = ClassUtils.scanClassOne(DefaultUtils.get(() -> readableConfig.get(SCAN_ENTITY), () -> readableConfig.get(SCAN)), RedisEntity.class, (anno -> ((RedisEntity) anno).value().equals(entityName)), () -> entityName + "获取失败");
//            repositoryClass = ClassUtils.scanClassOne(DefaultUtils.get(() -> readableConfig.get(SCAN_REPOSITORY), () -> readableConfig.get(SCAN)), redisRepository.class, (anno -> ((redisRepository) anno).value().equals(repositoryName)), () -> repositoryName + "获取失败");
        }
        return RedisReadOptions.builder()
                .keySerializer(RedisSerializerLoader.get(keySerializer))
                .valueSerializer(RedisSerializerLoader.get(valueSerializer))
                .command(readableConfig.get(COMMAND))
                .entity(entityClass)
                .repository(repositoryClass)
                .hashKey(readableConfig.get(HASH_KEY))
                .listKey(readableConfig.get(LIST_KEY))
                .cacheLoadModel(readableConfig.get(CACHE_LOAD_MODEL))
                .cacheFieldNames(readableConfig.get(CACHE_FIELD_NAMES))
                .cacheMissModel(readableConfig.get(CACHE_MISS_MODEL))
                .build();
    }

    private RedisLookupOptions getRedisLookupOptions(ReadableConfig readableConfig) {
        return RedisLookupOptions.builder().build();
    }

    private RedisConnectionOptions getRedisConnectionOptions(ReadableConfig readableConfig) {
        RedisModel redisModel = readableConfig.get(MODEL);
        RedisConnectionOptions builder = RedisConnectionOptions.builder()
                .redisModel(redisModel)
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

    private void validateConfigOptions(ReadableConfig config, ResolvedSchema resolvedSchema) {

        checkAllOrNone(config, new ConfigOption[]{MASTER_CONFIG, SLAVE_CONFIG});

        checkAllOrNone(config, new ConfigOption[]{ENTITY, REPOSITORY});

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

        CacheLoadModel cacheLoadModel = config.get(CACHE_LOAD_MODEL);
        Optional<Map<String, String>> cacheFieldNamesOptional = config.getOptional(CACHE_FIELD_NAMES);
        if (CacheLoadModel.INITIAL.equals(cacheLoadModel)) {
            if (!cacheFieldNamesOptional.isPresent()) {
                throw new IllegalArgumentException(CacheLoadModel.INITIAL.name() + "模式下必须配置" + CACHE_FIELD_NAMES.key());
            }
            cacheFieldNamesOptional.ifPresent(cacheFieldNames -> {
                List<String> columnNames = resolvedSchema.getColumnNames();
                for (String value : cacheFieldNames.values()) {
                    String[] split = value.split(",");
                    for (String columnName : split) {
                        if (!columnNames.contains(columnName)) {
                            throw new IllegalArgumentException(value + "字段不存在");
                        }
                    }
                }
            });
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
