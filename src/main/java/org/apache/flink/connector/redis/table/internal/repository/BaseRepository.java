package org.apache.flink.connector.redis.table.internal.repository;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.redis.table.internal.annotation.RedisField;
import org.apache.flink.connector.redis.table.internal.annotation.RedisKey;
import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;
import org.apache.flink.connector.redis.table.internal.annotation.RedisValue;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.command.RedisCommandBuilder;
import org.apache.flink.connector.redis.table.internal.converter.DataParser;
import org.apache.flink.connector.redis.table.internal.converter.RedisDataConverter;
import org.apache.flink.connector.redis.table.internal.converter.sink.RedisSinkConverter;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.extension.ExtensionLoader;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author weilai
 */
@Slf4j
public abstract class BaseRepository<T> implements Repository<T> {

    protected Class<T> entityClass;

    private RedisSerializer<?> keySerializer;

    private RedisSerializer<T> valueSerializer;

    private RedisCommand redisCommand;

    private RedisSinkConverter selectConverter;

    private RedisSinkConverter insertConverter;

    private RedisSinkConverter updateConverter;

    private RedisSinkConverter deleteConverter;

    private List<String> columnNameList;

    private List<DataType> columnDataTypeList;

    @SuppressWarnings("unchecked")
    public BaseRepository() {
        entityClass = (Class<T>) ((ParameterizedTypeImpl) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    @Override
    public void init(RedisConnectionOptions connectionOptions, RedisReadOptions readOptions, List<String> columnNameList, List<DataType> columnDataTypeList) {
        this.redisCommand = RedisCommandBuilder.build(connectionOptions);
        this.redisCommand.connect(connectionOptions);
        this.columnNameList = columnNameList;
        this.columnDataTypeList = columnDataTypeList;
        this.selectConverter = getConverter(selectCommand());
        this.insertConverter = getConverter(insertCommand());
        this.updateConverter = getConverter(updateCommand());
        this.deleteConverter = getConverter(deleteCommand());
        this.keySerializer = getKeySerializer(readOptions);
        this.valueSerializer = getValueSerializer(readOptions);
    }

    @SneakyThrows
    private RedisSerializer<T> getKeySerializer(RedisReadOptions readOptions) {
        RedisRepository redisRepository = getClass().getAnnotation(RedisRepository.class);
        RedisSerializer<?> serializer;
        if (redisRepository != null && redisRepository.keySerializer() != null) {
            serializer = redisRepository.keySerializer().newInstance();
        } else {
            serializer = readOptions.getKeySerializer();
        }
        if (serializer == null) {
            throw new IllegalArgumentException("Key Serializer不存在");
        }
        return (RedisSerializer<T>) serializer;
    }

    @SneakyThrows
    private RedisSerializer<T> getValueSerializer(RedisReadOptions readOptions) {
        RedisRepository redisRepository = getClass().getAnnotation(RedisRepository.class);
        RedisSerializer<?> serializer;
        if (redisRepository != null && redisRepository.valueSerializer() != null) {
            serializer = redisRepository.valueSerializer().getConstructor(Class.class).newInstance(entityClass);
        } else {
            serializer = readOptions.getValueSerializer();
        }
        if (serializer == null) {
            throw new IllegalArgumentException("Value Serializer不存在");
        }
        return (RedisSerializer<T>) serializer;
    }

    private RedisSinkConverter getConverter(RedisCommandType commandType) {
        if (commandType == null) {
            return null;
        }
        return ExtensionLoader.getExtensionLoader(RedisSinkConverter.class).getExtension(commandType.identify());
    }

    @Override
    public void close() {
        Optional.ofNullable(redisCommand).ifPresent(RedisCommand::close);
    }

    @Override
    public List<T> list() {
        return null;
    }

    @Override
    public void insert(RowData rowData) {
        try {
            insertConverter.convert(redisCommand, parser(rowData));
        } catch (Exception e) {
            log.error("插入失败", e);
        }
    }

    @Override
    public void update(RowData rowData) {
        try {
            updateConverter.convert(redisCommand, parser(rowData));
        } catch (Exception e) {
            log.error("插入失败", e);
        }
    }

    @Override
    public void delete(RowData rowData) {
        try {
            deleteConverter.convert(redisCommand, parser(rowData));
        } catch (Exception e) {
            log.error("插入失败", e);
        }
    }

    protected RedisCommandType selectCommand() {
        RedisRepository redisRepository = getClass().getAnnotation(RedisRepository.class);
        if (redisRepository != null && !redisRepository.selectCommand().equals(RedisCommandType.NONE)) {
            return redisRepository.selectCommand();
        }
        return null;
    }

    protected RedisCommandType insertCommand() {
        RedisRepository redisRepository = getClass().getAnnotation(RedisRepository.class);
        if (redisRepository != null && !redisRepository.insertCommand().equals(RedisCommandType.NONE)) {
            return redisRepository.insertCommand();
        }
        return null;
    }

    protected RedisCommandType updateCommand() {
        RedisRepository redisRepository = getClass().getAnnotation(RedisRepository.class);
        if (redisRepository != null && !redisRepository.updateCommand().equals(RedisCommandType.NONE)) {
            return redisRepository.updateCommand();
        }
        return null;
    }

    protected RedisCommandType deleteCommand() {
        RedisRepository redisRepository = getClass().getAnnotation(RedisRepository.class);
        if (redisRepository != null && !redisRepository.deleteCommand().equals(RedisCommandType.NONE)) {
            return redisRepository.deleteCommand();
        }
        return null;
    }

    @SneakyThrows
    protected DataParser parser(RowData rowData) {
        DataParser dataParser = new DataParser();
        for (Field field : entityClass.getDeclaredFields()) {
            field.setAccessible(true);
            extracted(RedisKey.class, columnNameList, columnDataTypeList, rowData, field, keySerializer, dataParser::setKey);
            extracted(RedisField.class, columnNameList, columnDataTypeList, rowData, field, keySerializer, dataParser::setField);
            extracted(RedisValue.class, columnNameList, columnDataTypeList, rowData, field, valueSerializer, dataParser::setValue);
        }
        Object value = entityClass.newInstance();
        for (Field field : entityClass.getDeclaredFields()) {
            field.setAccessible(true);
            int pos = columnNameList.indexOf(field.getName());
            Object fieldValue = RedisDataConverter.to(columnDataTypeList.get(pos).getLogicalType(), rowData, pos);
            field.set(value, fieldValue);
        }
        dataParser.setValue(valueSerializer.serialize(value));
        return dataParser;
    }

    private <A extends Annotation> void extracted(Class<A> annotationClass, List<String> columnNameList, List<DataType> columnDataTypeList, RowData rowData, Field field, RedisSerializer<?> serializer, Consumer<byte[]> consumer) {
        if (field.getAnnotation(annotationClass) != null) {
            int pos = columnNameList.indexOf(field.getName());
            if (pos != -1) {
                consumer.accept(serializer.serialize(RedisDataConverter.to(columnDataTypeList.get(pos).getLogicalType(), rowData, pos)));
            }
        }
    }
}
