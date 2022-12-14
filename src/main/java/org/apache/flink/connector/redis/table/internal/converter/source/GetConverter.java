package org.apache.flink.connector.redis.table.internal.converter.source;

import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.converter.RedisDataConverter;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.function.DataFunction;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

/**
 * <p>GET命令转换方式
 * @author weilai
 */
public class GetConverter extends BaseRedisSourceConverter {

    @Override
    public RedisCommandType support() {
        return RedisCommandType.GET;
    }

    @Override
    protected DataFunction<RedisCommand, RedisReadOptions, Object[], DataResult> getDataFunction() {
        return (redis, options, keys) -> {
            BinaryStringData key = (BinaryStringData) keys[0];
            final RedisSerializer<String> keySerializer = getKeySerializer(options);
            return DataResult.builder().key(key.toString()).payload(Collections.singletonList(redis.get(keySerializer.serialize(key.toString())))).build();
        };
    }

    @Override
    protected void dataString(GenericRowData rowData, final List<DataType> columnDataTypeList, DataResult dataResult, String deserialize) {
        rowData.setField(0, RedisDataConverter.from(columnDataTypeList.get(0).getLogicalType(), dataResult.getKey()));
        rowData.setField(1, RedisDataConverter.from(columnDataTypeList.get(1).getLogicalType(), deserialize));
    }

    @Override
    protected void dataPojo(GenericRowData rowData, List<String> columnNameList, List<DataType> columnDataTypeList, DataResult dataResult, Object deserialize) throws Exception{
        rowData.setField(0, RedisDataConverter.from(columnDataTypeList.get(0).getLogicalType(), dataResult.getKey()));
        for (int i = 1; i < columnNameList.size(); i++) {
            String columnName = columnNameList.get(i);
            DataType columnDataType = columnDataTypeList.get(i);
            Field field = deserialize.getClass().getDeclaredField(columnName);
            field.setAccessible(true);
            rowData.setField(i, RedisDataConverter.from(columnDataType.getLogicalType(), field.get(deserialize)));
        }
    }
}
