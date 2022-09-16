package org.apache.flink.connector.redis.table.internal.converter.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.converter.RedisDataConverter;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.redis.table.internal.options.RedisConnectorOptions.HASH_KEY;

/**
 * <p>HGET命令转换方式
 * @author weilai
 */
public class HGetConverter extends BaseRedisSourceConverter {

    @Override
    public RedisCommandType support() {
        return RedisCommandType.HGET;
    }

    @Override
    protected DataSourceFunction<RedisCommand, RedisReadOptions, Object[], DataResult> getDataFunction() {
        return (redis, options, keys) -> {
            final String hashKey = options.getHashKey();
            final RedisSerializer<String> keySerializer = options.getKeySerializer();
            String key;
            String field;
            if (StringUtils.isBlank(hashKey)) {
                if (keys.length != 2) {
                    throw new RuntimeException("hget联表查询ON条件数量错误，您可以指定" + HASH_KEY.key() + "配置或者传递正确的条件");
                }
                key = keys[0].toString();
                field = keys[1].toString();
            } else {
                key = hashKey;
                field = keys[0].toString();
            }
            return DataResult.builder().key(key).field(field).payload(Collections.singletonList(redis.hget(keySerializer.serialize(key), keySerializer.serialize(field)))).build();
        };
    }

    @Override
    protected void dataString(GenericRowData rowData, final List<DataType> columnDataTypeList, DataResult dataResult, String deserialize) {
        if (columnDataTypeList.size() == 2) {
            rowData.setField(0, dataResult.getField());
            rowData.setField(1, RedisDataConverter.from(columnDataTypeList.get(1).getLogicalType(), deserialize));
        } else if (columnDataTypeList.size() == 3) {
            rowData.setField(0, dataResult.getKey());
            rowData.setField(1, dataResult.getField());
            rowData.setField(2, RedisDataConverter.from(columnDataTypeList.get(1).getLogicalType(), deserialize));
        } else {
            throw new RuntimeException("不正确的字段个数");
        }
    }

    @Override
    protected void dataPojo(GenericRowData rowData, List<String> columnNameList, List<DataType> columnDataTypeList, DataResult dataResult, Object deserialize) throws Exception{
        Class<?> deserializeClass = deserialize.getClass();
        int prePosition = columnNameList.size() - deserializeClass.getDeclaredFields().length;
        if (prePosition == 0) {
            // 没有key和field信息
        } else if (prePosition == 1) {
            rowData.setField(0, dataResult.getField());
        } else if (prePosition == 2) {
            rowData.setField(0, dataResult.getKey());
            rowData.setField(1, dataResult.getField());
        } else {
            throw new RuntimeException("不正确的字段个数");
        }
        genRowData(rowData, columnNameList, columnDataTypeList, deserialize, deserializeClass, prePosition);
    }
}
