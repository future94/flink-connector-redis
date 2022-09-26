package org.apache.flink.connector.redis.table.internal.repository;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.redis.table.internal.converter.DataParser;
import org.apache.flink.connector.redis.table.internal.converter.RedisDataConverter;
import org.apache.flink.connector.redis.table.internal.converter.sink.RedisSinkConverter;
import org.apache.flink.connector.redis.table.internal.converter.source.RedisSourceConverter;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.enums.RedisOperationType;
import org.apache.flink.connector.redis.table.internal.extension.ExtensionLoader;
import org.apache.flink.connector.redis.table.internal.function.DataFunction;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.JsonRedisSerializer;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.connector.redis.table.utils.ClassUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author weilai
 */
@Slf4j
@SuppressWarnings({"unchecked", "rawtypes"})
public class IndexRepository extends BaseRepository<RowData> {

    public static final String IDENTIFIER = "index";

    private RedisSinkConverter sinkConverter;

    private RedisSourceConverter sourceConverter;

    @Override
    public void init(RedisConnectionOptions connectionOptions, RedisReadOptions readOptions, List<String> columnNameList, List<DataType> columnDataTypeList) {
        super.init(connectionOptions, readOptions, columnNameList, columnDataTypeList);
        RedisCommandType command = readOptions.getCommand();
        if (command.getOperationType().equals(RedisOperationType.READ)) {
            this.sourceConverter = getSourceConverter(command);
        } else {
            this.sinkConverter = getSinkConverter(command);
        }
    }

    @Override
    protected RedisSerializer getValueSerializer() {
        if (JsonRedisSerializer.IDENTIFIER.equals(readOptions.getValueSerializer())) {
            return new JsonRedisSerializer<>(ArrayList.class);
        }
        return ExtensionLoader.getExtensionLoader(RedisSerializer.class).getExtension(readOptions.getValueSerializer());
    }

    @Override
    public List<RowData> list() {
        return null;
    }


    @Override
    public Optional<GenericRowData> join(Object... keys) throws Exception {
        checkNotNull(sourceConverter, "No sourceConverter supplied.");
        return sourceConverter.convert(redisCommand, columnNameList, columnDataTypeList, readOptions, keys);
    }

    @Override
    public void loadCache() throws Exception {
        if (sourceConverter != null) {
            sourceConverter.loadCache(redisCommand, readOptions, columnNameList, columnDataTypeList);
        } else {
            log.warn("sourceConverter未初始化");
        }
    }

    @Override
    public void clearCache(){
        if (sourceConverter != null) {
            sourceConverter.clearCache();
        } else {
            log.warn("sourceConverter未初始化");
        }
    }

    @Override
    public void insert(RowData rowData) {
        checkNotNull(sinkConverter, "No sinkConverter supplied.");
        sinkConverter.convert(redisCommand, parser(rowData));
    }

    @Override
    public void update(RowData rowData) {
        insert(rowData);
    }

    @Override
    public void delete(RowData rowData) {
        throw new UnsupportedOperationException("暂不支持");
    }

    @SneakyThrows
    protected DataParser parser(RowData rowData) {
        DataParser dataParser = new DataParser();
        ArrayList<Object> valueList = null;
        for (int pos = 0; pos < rowData.getArity(); pos++) {
            if (pos == 0) {
                dataParser.setKey(dataFunction().apply(keySerializer, rowData, pos));
                continue;
            }
            if (pos == 1 && readOptions.getCommand().equals(RedisCommandType.HSET)) {
                dataParser.setField(dataFunction().apply(keySerializer, rowData, pos));
                continue;
            }
            if (ClassUtils.isSimpleValueType(getValueSerializer().getValueClass())) {
                dataParser.setValue(dataFunction().apply(valueSerializer, rowData, pos));
                break;
            }
            if (valueList == null) {
                valueList = new ArrayList<>();
            }
            valueList.add(convertData(rowData, pos));
        }
        if (valueList != null) {
            dataParser.setValue(valueSerializer.serialize(valueList));
        }
        return dataParser;
    }

    private DataFunction<RedisSerializer, RowData, Integer, byte[]> dataFunction() {
        return (redisSerializer, rowData, pos) -> redisSerializer.serialize(convertData(rowData, pos));
    }

    private String convertData(RowData rowData, Integer pos) {
        return RedisDataConverter.to(columnDataTypeList.get(pos).getLogicalType(), rowData, pos).toString();
    }

}
