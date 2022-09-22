package org.apache.flink.connector.redis.table.internal.converter.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.function.DataFunction;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.connector.redis.table.utils.ReflectUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.LIST_KEY;

/**
 * <p>LRANGE命令转换方式
 * @author weilai
 */
public class LRangeConverter extends BaseRedisSourceConverter {

    @Override
    public RedisCommandType support() {
        return RedisCommandType.LRANGE;
    }

    @Override
    protected DataFunction<RedisCommand, RedisReadOptions, Object[], DataResult> getDataFunction() {
        return (redis, options, keys) -> {
            final String listKey = options.getListKey();
            final RedisSerializer<String> keySerializer = getKeySerializer(options);
            String key;
            if (StringUtils.isBlank(listKey)) {
                if (keys.length < 1) {
                    throw new RuntimeException("lrange联表查询ON条件数量错误，您可以指定" + LIST_KEY.key() + "配置或者传递正确的条件");
                }
                if (keys.length < 2) {
                    throw new RuntimeException("lrange联表查询除了key外，请ON再至少指定一个ON条件来进行维度查询");
                }
                key = keys[0].toString();
            } else {
                key = listKey;
                if (keys.length < 1) {
                    throw new IllegalArgumentException("lrange联表请至少指定一个ON条件来进行维度查询");
                }
            }
            return DataResult.builder().key(key).payload(redis.lrange(keySerializer.serialize(key))).build();
        };
    }

    /**
     * 如果with指定了list.key，那么连表中就不会认为还存在listKey信息
     */
    @Override
    protected String genAllCacheKey(RedisReadOptions readOptions, List<String> columnNameList, Object[] keys, Object deserialize) {
        StringBuilder cacheKeyBuffer;
        // table有key为1，没有为0
        int prePosition = columnNameList.size() - deserialize.getClass().getDeclaredFields().length;
        int start = 1;
        if (StringUtils.isBlank(readOptions.getListKey())) {
            cacheKeyBuffer = new StringBuilder();
            cacheKeyBuffer.append(keys[0]).append(DELIMITER);
            start++;
        } else {
            cacheKeyBuffer = new StringBuilder(readOptions.getListKey() + DELIMITER);
        }
        cacheKeyBuffer.append(ReflectUtils.getFieldValue(deserialize, columnNameList.get(prePosition))).append(DELIMITER);
        for (int i = start; i < keys.length; i++) {
            cacheKeyBuffer.append(ReflectUtils.getFieldValue(deserialize, columnNameList.get(i)));
        }
        return StringUtils.removeEnd(cacheKeyBuffer.toString(), DELIMITER);
    }

    /**
     * 如果with指定了list.key，那么连表中就不会认为还存在listKey信息
     */
    @Override
    protected String genCacheKey(RedisReadOptions readOptions, Object[] keys) {
        if (StringUtils.isBlank(readOptions.getListKey())) {
            return Arrays.stream(keys).map(String::valueOf).collect(Collectors.joining(DELIMITER));
        } else {
            return readOptions.getListKey() + DELIMITER + Arrays.stream(keys).map(String::valueOf).collect(Collectors.joining(DELIMITER));
        }
    }

    @Override
    protected void dataString(GenericRowData rowData, final List<DataType> columnDataTypeList, DataResult dataResult, String deserialize) {
        throw new UnsupportedOperationException("不支持String类型");
    }

    @Override
    protected void dataPojo(GenericRowData rowData, List<String> columnNameList, List<DataType> columnDataTypeList, DataResult dataResult, Object deserialize) throws Exception{
        Class<?> deserializeClass = deserialize.getClass();
        int prePosition = columnNameList.size() - deserializeClass.getDeclaredFields().length;
        if (prePosition == 0) {
            // 没有key和field信息
        } else if (prePosition == 1) {
            rowData.setField(0, dataResult.getKey());
        } else {
            throw new RuntimeException("不正确的字段个数");
        }
        genRowData(rowData, columnNameList, columnDataTypeList, deserialize, deserializeClass, prePosition);
    }
}
