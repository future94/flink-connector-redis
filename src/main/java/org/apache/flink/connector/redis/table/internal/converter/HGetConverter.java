package org.apache.flink.connector.redis.table.internal.converter;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;

import java.lang.reflect.Field;
import java.util.List;

/**
 * <p>HGET命令转换方式
 * @author weilai
 */
public class HGetConverter extends BaseRedisCommandToRowConverter{

    @Override
    public RedisCommandType support() {
        return RedisCommandType.HGET;
    }

    @Override
    protected DataFunction<RedisCommand, RedisReadOptions, Object[], DataResult> getDataFunction() {
        return (redis, options, keys) -> {
            final String hashKey = options.getHashKey();
            final RedisSerializer<?> keySerializer = options.getKeySerializer();
            BinaryStringData key;
            BinaryStringData field;
            if (StringUtils.isBlank(hashKey)) {
                if (keys.length != 2) {
                    throw new RuntimeException("hget连表查询ON条件数量错误，您可以指定hash.key配置或者传递正确的条件");
                }
                key = (BinaryStringData) keys[0];
                field = (BinaryStringData) keys[1];
            } else {
                key = BinaryStringData.fromString(hashKey);
                field = (BinaryStringData) keys[0];
            }
            return DataResult.builder().key(key).field(field).payload(redis.hget(keySerializer.serialize(key), keySerializer.serialize(field))).build();
        };
    }

    @Override
    protected void dataString(GenericRowData rowData, final List<DataType> columnDataTypeList, DataResult dataResult, String deserialize) {
        if (columnDataTypeList.size() == 2) {
            rowData.setField(0, dataResult.getField());
            rowData.setField(1, RedisDataToTableDataConverter.convert(columnDataTypeList.get(1).getLogicalType(), deserialize));
        } else if (columnDataTypeList.size() == 3) {
            rowData.setField(0, dataResult.getKey());
            rowData.setField(1, dataResult.getField());
            rowData.setField(2, RedisDataToTableDataConverter.convert(columnDataTypeList.get(1).getLogicalType(), deserialize));
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
        for (int i = prePosition; i < columnNameList.size(); i++) {
            String columnName = columnNameList.get(i);
            DataType columnDataType = columnDataTypeList.get(i);
            Field field = deserializeClass.getDeclaredField(columnName);
            field.setAccessible(true);
            rowData.setField(i, RedisDataToTableDataConverter.convert(columnDataType.getLogicalType(), field.get(deserialize)));
        }
    }
}
