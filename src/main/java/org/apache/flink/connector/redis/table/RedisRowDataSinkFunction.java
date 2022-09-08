package org.apache.flink.connector.redis.table;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.command.RedisCommandBuilder;
import org.apache.flink.connector.redis.table.internal.converter.sink.RedisSinkConverterLoader;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisLookupOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.Optional;

/**
 * @author weilai
 */
@Slf4j
public class RedisRowDataSinkFunction extends RichSinkFunction<RowData> {

    private final RedisConnectionOptions connectionOptions;
    private final RedisReadOptions readOptions;
    private final RedisLookupOptions lookupOptions;

    /**
     * 动态表字段名集合
     */
    private final List<String> columnNameList;

    /**
     * 动态表字段类型集合
     */
    private final List<DataType> columnDataTypeList;

    /**
     * Redis运行环境
     */
    private RedisCommand redisCommand;

    public RedisRowDataSinkFunction(RedisConnectionOptions connectionOptions, RedisReadOptions readOptions, RedisLookupOptions lookupOptions, ResolvedSchema physicalSchema) {
        this.connectionOptions = connectionOptions;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
        this.columnNameList = physicalSchema.getColumnNames();
        this.columnDataTypeList = physicalSchema.getColumnDataTypes();
        if (columnNameList.size() != columnDataTypeList.size()) {
            throw new RuntimeException("字段信息获取失败");
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.redisCommand = RedisCommandBuilder.build(connectionOptions);
        this.redisCommand.connect(connectionOptions);
    }

    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        RowKind kind = rowData.getRowKind();
        switch (kind) {
            case INSERT:
                RedisSinkConverterLoader.get(readOptions.getCommand()).convert(redisCommand, readOptions, columnNameList, columnDataTypeList, rowData);
                break;
            case UPDATE_AFTER:
                break;
            case DELETE:
                break;
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException("不支持的RowKind类型" + kind.name());
        }
    }

    @Override
    public void close() throws Exception {
        Optional.ofNullable(redisCommand).ifPresent(RedisCommand::close);
    }
}
