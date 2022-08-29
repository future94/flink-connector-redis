package org.apache.flink.connector.redis.table.internal.converter;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * <p>将Redis命令返回数据转换为Table数据基础类
 * @author weilai
 */
public abstract class BaseRedisCommandToRowConverter implements RedisCommandToRowConverter {

    @Override
    public Optional<GenericRowData> convert(final RedisCommand redisCommand, final List<String> columnNameList, final List<DataType> columnDataTypeList, final RedisReadOptions readOptions, final Object[] keys) throws Exception {
        final GenericRowData rowData = new GenericRowData(columnNameList.size());
        final RedisSerializer<?> valueSerializer = readOptions.getValueSerializer();
        Object deserialize = null;
        DataResult dataResult = getDataFunction().apply(redisCommand, readOptions, keys);
        byte[] valueByte = dataResult.getPayload();
        if (valueByte != null) {
            deserialize = valueSerializer.deserialize(valueByte);
        }
        if (deserialize == null) {
            // 根据SQL规范，联表查询不到的时候，不返回，即字段都是null
            return Optional.empty();
        } else if (deserialize instanceof String) {
            dataString(rowData, columnDataTypeList, dataResult, (String) deserialize);
            return Optional.of(rowData);
        } else {
            dataPojo(rowData, columnNameList, columnDataTypeList, dataResult, deserialize);
            return Optional.of(rowData);
        }
    }

    /**
     * 运行redis后返回的结果
     */
    protected abstract DataFunction<RedisCommand, RedisReadOptions, Object[], DataResult> getDataFunction();

    /**
     * RedisString类型的转换方式
     * @param rowData               要返回的数据
     * @param columnDataTypeList    字段类型集合
     * @param dataResult            Redis返回的运行结果
     * @param deserialize           Redis返回的运行对象
     */
    protected abstract void dataString(final GenericRowData rowData, final List<DataType> columnDataTypeList, final DataResult dataResult, String deserialize);

    /**
     * RedisString类型的转换方式
     * @param rowData               要返回的数据
     * @param columnDataTypeList    字段类型集合
     * @param dataResult            Redis返回的运行结果
     * @param deserialize           Redis返回的运行对象
     * @throws Exception            转换一场
     */
    protected abstract void dataPojo(final GenericRowData rowData, final List<String> columnNameList, final List<DataType> columnDataTypeList, final DataResult dataResult, Object deserialize) throws Exception;

    /**
     * 运行Redis返回结果
     * @param <R>       Redis运行环境
     * @param <O>       读取参数配置
     * @param <K>       联表key[]
     * @param <Res>     结果
     */
    @FunctionalInterface
    public interface DataFunction<R, O, K, Res> {

        Res apply(R redis, O options, K keys);
    }

    /**
     * Redis运行结果
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class DataResult {

        /**
         * key
         */
        private BinaryStringData key;

        /**
         * hash field
         */
        private BinaryStringData field;

        /**
         * 查询结果
         */
        private byte[] payload;
    }
}
