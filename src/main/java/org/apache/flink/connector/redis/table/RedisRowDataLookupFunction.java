package org.apache.flink.connector.redis.table;

import lombok.SneakyThrows;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.command.RedisCommandBuilder;
import org.apache.flink.connector.redis.table.internal.converter.source.RedisSourceConverterLoader;
import org.apache.flink.connector.redis.table.internal.enums.CacheLoadModel;
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
    private final RedisCommand redisCommand;

    /**
     * 读取参数配置
     */
    private final RedisReadOptions readOptions;

    @SneakyThrows
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
        this.redisCommand = RedisCommandBuilder.build(connectionOptions);
        if (CacheLoadModel.INITIAL.equals(readOptions.getCacheLoadModel())) {
            RedisSourceConverterLoader.get(readOptions.getCommand()).loadCache(redisCommand, readOptions, columnNameList, columnDataTypeList);
        }
    }

    /**
     * 联表的时候，on的条件有一个，这里的key[]就是几个
     */
    public void eval(Object... keys) throws Exception {
        RedisSourceConverterLoader.get(readOptions.getCommand()).convert(redisCommand, columnNameList, columnDataTypeList, readOptions, keys).ifPresent(this::collect);
    }
}
