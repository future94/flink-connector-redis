package org.apache.flink.connector.redis.table.internal.repository;

import lombok.SneakyThrows;
import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.command.RedisCommandBuilder;
import org.apache.flink.connector.redis.table.internal.converter.sink.RedisSinkConverter;
import org.apache.flink.connector.redis.table.internal.converter.source.RedisSourceConverter;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.extension.ExtensionLoader;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * @author weilai
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class BaseRepository<T> implements Repository<T> {

    protected Class<T> entityClass;

    protected RedisSerializer<String> keySerializer;

    protected RedisSerializer<? super Object> valueSerializer;

    protected RedisCommand redisCommand;

    protected RedisReadOptions readOptions;

    protected List<String> columnNameList;

    protected List<DataType> columnDataTypeList;

    @Override
    public void init(RedisConnectionOptions connectionOptions, RedisReadOptions readOptions, List<String> columnNameList, List<DataType> columnDataTypeList) {
        this.redisCommand = RedisCommandBuilder.build(connectionOptions);
        this.redisCommand.connect(connectionOptions);
        this.readOptions = readOptions;
        this.columnNameList = columnNameList;
        this.columnDataTypeList = columnDataTypeList;
        this.keySerializer = getKeySerializer();
        this.valueSerializer = getValueSerializer();
    }

    @Override
    public void close() {
        Optional.ofNullable(redisCommand).ifPresent(RedisCommand::close);
    }

    protected RedisSerializer<String> getKeySerializer() {
        return getKeySerializer(readOptions);
    }

    protected RedisSerializer getValueSerializer() {
        return getValueSerializer(readOptions);
    }

    protected RedisSinkConverter getSinkConverter(RedisCommandType commandType) {
        if (commandType == null) {
            return null;
        }
        return ExtensionLoader.getExtensionLoader(RedisSinkConverter.class).getExtension(commandType.identify());
    }

    protected RedisSourceConverter getSourceConverter(RedisCommandType commandType) {
        if (commandType == null) {
            return null;
        }
        return ExtensionLoader.getExtensionLoader(RedisSourceConverter.class).getExtension(commandType.identify());
    }

    @SneakyThrows
    private RedisSerializer<String> getKeySerializer(RedisReadOptions readOptions) {
        RedisRepository redisRepository = getClass().getAnnotation(RedisRepository.class);
        RedisSerializer<String> serializer;
        if (redisRepository != null && redisRepository.keySerializer() != null) {
            serializer = redisRepository.keySerializer().newInstance();
        } else {
            serializer = ExtensionLoader.getExtensionLoader(RedisSerializer.class).getExtension(readOptions.getKeySerializer());
        }
        if (serializer == null) {
            throw new IllegalArgumentException("Key Serializer不存在");
        }
        return serializer;
    }

    @SneakyThrows
    private RedisSerializer getValueSerializer(RedisReadOptions readOptions) {
        RedisRepository redisRepository = getClass().getAnnotation(RedisRepository.class);
        RedisSerializer serializer;
        if (redisRepository != null && redisRepository.valueSerializer() != null) {
            serializer = redisRepository.valueSerializer().getConstructor(Class.class).newInstance(entityClass);
        } else {
            serializer = ExtensionLoader.getExtensionLoader(RedisSerializer.class).getExtension(readOptions.getValueSerializer());
        }
        if (serializer == null) {
            throw new IllegalArgumentException("Value Serializer不存在");
        }
        return serializer;
    }
}
