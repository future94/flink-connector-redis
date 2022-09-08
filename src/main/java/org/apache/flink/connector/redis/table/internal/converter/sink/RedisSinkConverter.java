package org.apache.flink.connector.redis.table.internal.converter.sink;

import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.converter.RedisConverter;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.List;

/**
 * <p>将Table数据转换为Redis要写入的数据
 *
 * @author weilai
 */
public interface RedisSinkConverter extends RedisConverter {

    void convert(final RedisCommand redisCommand, final RedisReadOptions readOptions, final List<String> columnNameList, final List<DataType> columnDataTypeList, final RowData rowData) throws Exception;
}
