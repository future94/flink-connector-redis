package org.apache.flink.connector.redis.table.internal.converter.sink;

import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.converter.RedisDataConverter;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.utils.ClassUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.lang.reflect.Field;
import java.util.List;

/**
 * @author weilai
 */
public class SetConverter extends BaseRedisSinkConverter {

    @Override
    public RedisCommandType support() {
        return RedisCommandType.SET;
    }

    @Override
    protected DataSinkFunction<RedisCommand, RedisReadOptions, List<String>, List<DataType>, RowData> getDataFunction() {
        return (redisCommand, readOptions, columnNameList, columnDataTypeList, rowData) -> {
            Class<?> valueClass = readOptions.getValueSerializer().getValueClass();
            Object key = RedisDataConverter.to(columnDataTypeList.get(0).getLogicalType(), rowData, 0);
            if (ClassUtils.isSimpleValueType(valueClass)) {
                Object value = RedisDataConverter.to(columnDataTypeList.get(1).getLogicalType(), rowData, 1);
                send(redisCommand, readOptions, key, value);
            } else {
                Object value = valueClass.newInstance();
                for (Field field : valueClass.getDeclaredFields()) {
                    field.setAccessible(true);
                    int pos = columnNameList.indexOf(field.getName());
                    Object fieldValue = RedisDataConverter.to(columnDataTypeList.get(pos).getLogicalType(), rowData, pos);
                    field.set(value, fieldValue);
                }
                send(redisCommand, readOptions, key, value);
            }
        };
    }

    private void send(RedisCommand redisCommand, RedisReadOptions readOptions, Object key, Object value) {
        redisCommand.set(readOptions.getKeySerializer().serialize(key), readOptions.getValueSerializer().serialize(value));
    }
}
