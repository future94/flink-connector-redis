package org.apache.flink.connector.redis.table.internal.converter.source;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.converter.RedisDataConverter;
import org.apache.flink.connector.redis.table.internal.enums.CacheMissModel;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.extension.ExtensionLoader;
import org.apache.flink.connector.redis.table.internal.function.DataFunction;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.connector.redis.table.utils.ReflectUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>将Redis命令返回数据转换为Table数据基础类
 *
 * @author weilai
 */
@Slf4j
public abstract class BaseRedisSourceConverter implements RedisSourceConverter {

    protected static final String DELIMITER = "~";

    private final AtomicBoolean loadingCache = new AtomicBoolean(false);

    protected final Cache<String, Object> cache = CacheBuilder.newBuilder().build();

    @Override
    public Optional<GenericRowData> convert(final RedisCommand redisCommand, final List<String> columnNameList, final List<DataType> columnDataTypeList, final RedisReadOptions readOptions, final Object[] keys) throws Exception {
        String cacheKey = genCacheKey(readOptions, keys);
        if (StringUtils.isNotBlank(cacheKey)) {
            GenericRowData cacheResult = (GenericRowData) cache.getIfPresent(cacheKey);
            if (cacheResult != null) {
                return Optional.of(cacheResult);
            }
            if (CacheMissModel.IGNORE.equals(readOptions.getCacheMissModel())) {
                return Optional.empty();
            }
        }
        if (CacheMissModel.REFRESH.equals(readOptions.getCacheMissModel())) {
            RedisCommandType support = this.support();
            if (RedisCommandType.GET.equals(support) || RedisCommandType.HGET.equals(support)) {
                return processAccurateMatch(redisCommand, columnNameList, columnDataTypeList, readOptions, keys);
            } else if (RedisCommandType.LRANGE.equals(support)) {
                return processFullMatch(redisCommand, columnNameList, columnDataTypeList, readOptions, keys, cacheKey);
            }
        }
        return Optional.empty();
    }

    @Override
    public void clearCache() {
        cache.cleanUp();
        loadingCache.compareAndSet(true, false);
    }

    @Override
    public void loadCache(RedisCommand redisCommand, RedisReadOptions readOptions, List<String> columnNameList, List<DataType> columnDataTypeList) throws Exception {
        if (!loadingCache.compareAndSet(false, true)) {
            return;
        }
        for (Map.Entry<String, String> entry : readOptions.getCacheFieldNames().entrySet()) {
            String redisKey = entry.getKey();
            log.debug("load redis cache, key : [{}]", redisKey);
            List<byte[]> dataList = redisCommand.lrange(redisKey.getBytes(StandardCharsets.UTF_8));
            String field = entry.getValue();
            String[] fieldNames = field.split(",");
            final RedisSerializer<?> valueSerializer = getValueSerializer(readOptions);
            for (byte[] bytes : dataList) {
                valueSerializer.deserialize(bytes);
                Object deserialize = valueSerializer.deserialize(bytes);
                final GenericRowData rowData = new GenericRowData(columnNameList.size());
                dataPojo(rowData, columnNameList, columnDataTypeList, DataResult.builder().key(redisKey).build(), deserialize);
                StringBuilder cacheKeyBuilder = new StringBuilder();
                cacheKeyBuilder.append(redisKey).append(DELIMITER);
                for (String fieldName : fieldNames) {
                    cacheKeyBuilder.append(ReflectUtils.getFieldValue(deserialize, fieldName)).append(DELIMITER);
                }
                cache.put(StringUtils.removeEnd(cacheKeyBuilder.toString(), DELIMITER), rowData);
            }
        }
    }

    /**
     * 全量筛选
     */
    private Optional<GenericRowData> processFullMatch(RedisCommand redisCommand, List<String> columnNameList, List<DataType> columnDataTypeList, RedisReadOptions readOptions, Object[] keys, String cacheKey) throws Exception {
        final RedisSerializer<?> valueSerializer = getValueSerializer(readOptions);
        synchronized (cache) {
            GenericRowData genericRowData = (GenericRowData) cache.getIfPresent(cacheKey);
            if (genericRowData != null) {
                return Optional.of(genericRowData);
            }
            DataResult dataResult = getDataFunction().apply(redisCommand, readOptions, keys);
            List<byte[]> valueByte = dataResult.getPayload();
            for (byte[] bytes : valueByte) {
                Object deserialize = valueSerializer.deserialize(bytes);
                final GenericRowData rowData = new GenericRowData(columnNameList.size());
                dataPojo(rowData, columnNameList, columnDataTypeList, dataResult, deserialize);
                cache.put(genAllCacheKey(readOptions, columnNameList, keys, deserialize), rowData);
            }
        }
        GenericRowData genericRowData = (GenericRowData) cache.getIfPresent(cacheKey);
        if (genericRowData != null) {
            return Optional.of(genericRowData);
        }
        return Optional.empty();
    }

    /**
     * 精准匹配
     */
    private Optional<GenericRowData> processAccurateMatch(RedisCommand redisCommand, List<String> columnNameList, List<DataType> columnDataTypeList, RedisReadOptions readOptions, Object[] keys) throws Exception {
        DataResult dataResult = getDataFunction().apply(redisCommand, readOptions, keys);
        List<byte[]> valueByte = dataResult.getPayload();
        if (CollectionUtils.isEmpty(valueByte) || Objects.isNull(valueByte.get(0))) {
            // 根据SQL规范，联表查询不到的时候，不返回，即字段都是null
            return Optional.empty();
        }
        final RedisSerializer<?> valueSerializer = getValueSerializer(readOptions);
        Object deserialize;
        // 当前查询方式只有一个
        deserialize = valueSerializer.deserialize(valueByte.get(0));
        final GenericRowData rowData = new GenericRowData(columnNameList.size());
        if (deserialize instanceof String) {
            dataString(rowData, columnDataTypeList, dataResult, (String) deserialize);
        } else {
            dataPojo(rowData, columnNameList, columnDataTypeList, dataResult, deserialize);
        }
        return Optional.of(rowData);
    }

    protected RedisSerializer<String> getKeySerializer(RedisReadOptions readOptions) {
        return ExtensionLoader.getExtensionLoader(RedisSerializer.class).getExtension(readOptions.getKeySerializer());
    }

    protected RedisSerializer<?> getValueSerializer(RedisReadOptions readOptions) {
        return ExtensionLoader.getExtensionLoader(RedisSerializer.class).getExtension(readOptions.getValueSerializer());
    }

    /**
     * 为所有数据生成缓存key
     *
     * @param readOptions    读取配置
     * @param columnNameList 维度表字段名称集合
     * @param keys           ON的联表值
     * @param deserialize    redis中反序列化的值
     * @return 缓存key
     */
    protected String genAllCacheKey(RedisReadOptions readOptions, List<String> columnNameList, Object[] keys, Object deserialize) {
        throw new UnsupportedOperationException("不支持为所有缓存生成hashKey");
    }

    /**
     * 获取当前值的缓存key
     *
     * @param readOptions 读取配置
     * @param keys        ON的联表值
     * @return 缓存key，如果为null则表示不支持缓存
     */
    protected String genCacheKey(final RedisReadOptions readOptions, final Object[] keys) {
        return null;
    }

    /**
     * 运行redis后返回的结果
     */
    protected abstract DataFunction<RedisCommand, RedisReadOptions, Object[], DataResult> getDataFunction();

    /**
     * RedisString类型的转换方式
     *
     * @param rowData            要返回的数据
     * @param columnDataTypeList 字段类型集合
     * @param dataResult         Redis返回的运行结果
     * @param deserialize        Redis返回的运行对象
     */
    protected abstract void dataString(final GenericRowData rowData, final List<DataType> columnDataTypeList, final DataResult dataResult, String deserialize);

    /**
     * RedisString类型的转换方式
     *
     * @param rowData            要返回的数据
     * @param columnDataTypeList 字段类型集合
     * @param dataResult         Redis返回的运行结果
     * @param deserialize        Redis返回的运行对象
     * @throws Exception 转换异常
     */
    protected abstract void dataPojo(final GenericRowData rowData, final List<String> columnNameList, final List<DataType> columnDataTypeList, final DataResult dataResult, Object deserialize) throws Exception;

    protected void genRowData(GenericRowData rowData, List<String> columnNameList, List<DataType> columnDataTypeList, Object deserialize, Class<?> deserializeClass, int prePosition) throws NoSuchFieldException, IllegalAccessException {
        for (int i = prePosition; i < columnNameList.size(); i++) {
            String columnName = columnNameList.get(i);
            DataType columnDataType = columnDataTypeList.get(i);
            Field field = deserializeClass.getDeclaredField(columnName);
            field.setAccessible(true);
            rowData.setField(i, RedisDataConverter.from(columnDataType.getLogicalType(), field.get(deserialize)));
        }
    }

    /**
     * Redis运行结果
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class DataResult {

        /**
         * key
         */
        private String key;

        /**
         * hash field
         */
        private String field;

        /**
         * 查询结果
         */
        private List<byte[]> payload;
    }
}
