package org.apache.flink.connector.redis.table.internal.repository;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.redis.table.internal.annotation.RedisField;
import org.apache.flink.connector.redis.table.internal.annotation.RedisKey;
import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;
import org.apache.flink.connector.redis.table.internal.annotation.RedisValue;
import org.apache.flink.connector.redis.table.internal.converter.DataParser;
import org.apache.flink.connector.redis.table.internal.converter.RedisDataConverter;
import org.apache.flink.connector.redis.table.internal.converter.sink.RedisSinkConverter;
import org.apache.flink.connector.redis.table.internal.converter.source.RedisSourceConverter;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

/**
 * @author weilai
 */
@Slf4j
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class EntityRepository<T> extends BaseRepository<T> {

    // TODO
    private RedisSourceConverter selectConverter;

    private RedisSinkConverter insertConverter;

    private RedisSinkConverter updateConverter;

    private RedisSinkConverter deleteConverter;

    public EntityRepository() {
        entityClass = (Class<T>) ((ParameterizedTypeImpl) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    @Override
    public void init(RedisConnectionOptions connectionOptions, RedisReadOptions readOptions, List<String> columnNameList, List<DataType> columnDataTypeList) {
        super.init(connectionOptions, readOptions, columnNameList, columnDataTypeList);
        // TODO
        this.selectConverter = getSourceConverter(selectCommand());
        this.insertConverter = getSinkConverter(insertCommand());
        this.updateConverter = getSinkConverter(updateCommand());
        this.deleteConverter = getSinkConverter(deleteCommand());
    }

    // TODO
    @Override
    public List<T> list() {
        return null;
    }

    @Override
    public Optional<GenericRowData> join(Object... keys) throws Exception {
        return selectConverter.convert(redisCommand, columnNameList, columnDataTypeList, readOptions, keys);
    }

    @Override
    public void loadCache() throws Exception {
        selectConverter.loadCache(redisCommand, readOptions, columnNameList, columnDataTypeList);
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
        T value = entityClass.newInstance();
        for (Field field : entityClass.getDeclaredFields()) {
            if ("serialVersionUID".equals(field.getName())) {
                continue;
            }
            field.setAccessible(true);
            if (dataParser.getKey() == null) {
                RedisKey redisKey = field.getAnnotation(RedisKey.class);
                if (redisKey != null) {
                    int pos = columnNameList.indexOf(field.getName());
                    if (pos != -1) {
                        dataParser.setKey(keySerializer.serialize(RedisDataConverter.to(columnDataTypeList.get(pos).getLogicalType(), rowData, pos).toString()));
                        continue;
                    }
                }
            }
            if (dataParser.getField() == null) {
                RedisField redisField = field.getAnnotation(RedisField.class);
                if (redisField != null) {
                    int pos = columnNameList.indexOf(field.getName());
                    if (pos != -1) {
                        dataParser.setField(keySerializer.serialize(RedisDataConverter.to(columnDataTypeList.get(pos).getLogicalType(), rowData, pos).toString()));
                        continue;
                    }
                }
            }
            if (dataParser.getValue() == null) {
                RedisValue redisValue = field.getAnnotation(RedisValue.class);
                if (redisValue != null) {
                    int pos = columnNameList.indexOf(field.getName());
                    if (pos != -1) {
                        dataParser.setValue(valueSerializer.serialize(RedisDataConverter.to(columnDataTypeList.get(pos).getLogicalType(), rowData, pos)));
                        continue;
                    }
                }
            }
            int pos = columnNameList.indexOf(field.getName());
            Object fieldValue = RedisDataConverter.to(columnDataTypeList.get(pos).getLogicalType(), rowData, pos);
            field.set(value, fieldValue);
        }
        if (dataParser.getValue() == null) {
            dataParser.setValue(valueSerializer.serialize(value));
        }
        return dataParser;
    }
}
