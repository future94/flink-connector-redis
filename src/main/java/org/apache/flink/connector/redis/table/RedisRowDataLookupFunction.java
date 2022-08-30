package org.apache.flink.connector.redis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.command.RedisCommandBuilder;
import org.apache.flink.connector.redis.table.internal.converter.RedisCommandToRowConverterLoader;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisLookupOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * <p>Redis行数据查找功能
 * @author weilai
 */
@Internal
public class RedisRowDataLookupFunction extends TableFunction<RowData> {

    private final List<String> columnNameList;

    private final List<DataType> columnDataTypeList;

    private final RedisCommandType redisCommandType;

    private final RedisCommand redisCommand;

    private final RedisReadOptions readOptions;

    public RedisRowDataLookupFunction(RedisConnectionOptions connectionOptions, RedisReadOptions readOptions, RedisLookupOptions lookupOptions, ResolvedSchema physicalSchema) {
        checkNotNull(connectionOptions, "No RedisConnectionOptions supplied.");
        checkNotNull(readOptions, "No readOptions supplied.");
        checkNotNull(lookupOptions, "No lookupOptions supplied.");
        checkNotNull(physicalSchema, "No physicalSchema supplied.");
        this.readOptions = readOptions;
        this.columnNameList = physicalSchema.getColumnNames();
        this.columnDataTypeList = physicalSchema.getColumnDataTypes();
        if (columnNameList.size() != columnDataTypeList.size()) {
            throw new RuntimeException("字段信息获取失败");
        }
        this.redisCommandType = connectionOptions.getCommand();
        this.redisCommand = RedisCommandBuilder.build(connectionOptions);
    }

    /**
     * 连表的时候，on的条件有一个，这里的key[]就是几个
     */
    public void eval(Object... keys) throws Exception {
        RedisCommandToRowConverterLoader.get(redisCommandType).convert(redisCommand, columnNameList, columnDataTypeList, readOptions, keys).ifPresent(this::collect);
    }
}