package org.apache.flink.connector.redis.table.internal.converter;

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
 * <p>GET命令转换方式
 * @author weilai
 */
public class GetConverter extends BaseRedisCommandToRowConverter {

    @Override
    public RedisCommandType support() {
        return RedisCommandType.GET;
    }

    @Override
    protected DataFunction<RedisCommand, RedisReadOptions, Object[], DataResult> getDataFunction() {
        return (redis, options, keys) -> {
            BinaryStringData key = (BinaryStringData) keys[0];
            final RedisSerializer<?> keySerializer = options.getKeySerializer();
            return DataResult.builder().key(key).payload(redis.get(keySerializer.serialize(key))).build();
        };
    }

    @Override
    protected void dataString(GenericRowData rowData, final List<DataType> columnDataTypeList, DataResult dataResult, String deserialize) {
        rowData.setField(0, dataResult.getKey());
        rowData.setField(1, RedisDataToTableDataConverter.convert(columnDataTypeList.get(1).getLogicalType(), deserialize));
    }

    @Override
    protected void dataPojo(GenericRowData rowData, List<String> columnNameList, List<DataType> columnDataTypeList, DataResult dataResult, Object deserialize) throws Exception{
        rowData.setField(0, dataResult.getKey());
        for (int i = 1; i < columnNameList.size(); i++) {
            String columnName = columnNameList.get(i);
            DataType columnDataType = columnDataTypeList.get(i);
            Field field = deserialize.getClass().getDeclaredField(columnName);
            field.setAccessible(true);
            rowData.setField(i, RedisDataToTableDataConverter.convert(columnDataType.getLogicalType(), field.get(deserialize)));
        }
    }
}
