package org.apache.flink.connector.redis.table.internal.converter.sink;

import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.List;

/**
 * @author weilai
 */
public abstract class BaseRedisSinkConverter implements RedisSinkConverter {

    @Override
    public void convert(final RedisCommand redisCommand, final RedisReadOptions readOptions, final List<String> columnNameList, final List<DataType> columnDataTypeList, final RowData rowData) throws Exception {
        getDataFunction().apply(redisCommand, readOptions, columnNameList, columnDataTypeList, rowData);
    }

    protected abstract DataSinkFunction<RedisCommand, RedisReadOptions, List<String>, List<DataType>, RowData> getDataFunction();

    /**
     * 运行Redis返回结果
     *
     * @param <R> Redis运行环境
     * @param <O> 读取参数配置
     * @param <D> 要写入的对象
     */
    @FunctionalInterface
    public interface DataSinkFunction<R, O, N, T, D> {

        void apply(R redis, O options, N name, T type, D rowData) throws Exception;
    }
}
