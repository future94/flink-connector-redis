package org.apache.flink.connector.redis.table.internal.converter.sink;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.connector.redis.table.internal.annotation.RedisField;
import org.apache.flink.connector.redis.table.internal.annotation.RedisKey;
import org.apache.flink.connector.redis.table.internal.annotation.RedisValue;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.converter.RedisDataConverter;
import org.apache.flink.connector.redis.table.internal.exception.NotFoundElementException;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.connector.redis.table.utils.ClassUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author weilai
 */
public abstract class BaseRedisSinkConverter implements RedisSinkConverter {

    @Override
    public void convert(final RedisCommand redisCommand, final RedisReadOptions readOptions, final List<String> columnNameList, final List<DataType> columnDataTypeList, final RowData rowData) {
        DataParser dataParser = parser(readOptions, columnNameList, columnDataTypeList, rowData);
        doSend(redisCommand, dataParser);
    }

    abstract protected void doSend(RedisCommand redisCommand, DataParser dataParser);

    @SneakyThrows
    protected DataParser parser(RedisReadOptions readOptions, List<String> columnNameList, List<DataType> columnDataTypeList, RowData rowData) {
        DataParser dataParser = new DataParser();
        Class<?> entity = readOptions.getEntity();
        for (Field field : entity.getDeclaredFields()) {
            field.setAccessible(true);
            extracted(RedisKey.class, columnNameList, columnDataTypeList, rowData, field, readOptions.getKeySerializer(), dataParser::setKey);
            extracted(RedisField.class, columnNameList, columnDataTypeList, rowData, field, readOptions.getKeySerializer(), dataParser::setField);
            extracted(RedisValue.class, columnNameList, columnDataTypeList, rowData, field, readOptions.getValueSerializer(), dataParser::setValue);
        }
        Class<?> valueClass = readOptions.getValueSerializer().getValueClass();
        if (!ClassUtils.isSimpleValueType(valueClass)) {
            Object value = valueClass.newInstance();
            for (Field field : valueClass.getDeclaredFields()) {
                field.setAccessible(true);
                int pos = columnNameList.indexOf(field.getName());
                Object fieldValue = RedisDataConverter.to(columnDataTypeList.get(pos).getLogicalType(), rowData, pos);
                field.set(value, fieldValue);
            }
            dataParser.setValue(readOptions.getValueSerializer().serialize(value));
        } else {
            if (dataParser.getValue() == null) {
                throw new NotFoundElementException("获取Value值为NULL,可以通过添加org.apache.flink.connector.redis.table.internal.annotation.RedisValue注解指定");
            }
        }
        return dataParser;
    }

    private <T extends Annotation> void extracted(Class<T> annotationClass, List<String> columnNameList, List<DataType> columnDataTypeList, RowData rowData, Field field, RedisSerializer<?> serializer, Consumer<byte[]> consumer) {
        if (field.getAnnotation(annotationClass) != null) {
            int pos = columnNameList.indexOf(field.getName());
            if (pos != -1) {
                consumer.accept(serializer.serialize(RedisDataConverter.to(columnDataTypeList.get(pos).getLogicalType(), rowData, pos)));
            }
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class DataParser {

        /**
         * key
         */
        private byte[] key;

        /**
         * field
         */
        private byte[] field;

        /**
         * value
         */
        private byte[] value;
    }
}
