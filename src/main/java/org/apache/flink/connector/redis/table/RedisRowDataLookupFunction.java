package org.apache.flink.connector.redis.table;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.redis.table.internal.enums.CacheLoadModel;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisLookupOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.connector.redis.table.internal.repository.Repository;
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
@Slf4j
@Internal
public class RedisRowDataLookupFunction extends TableFunction<RowData> {

    private final RedisConnectionOptions connectionOptions;

    private final RedisReadOptions readOptions;

    private final List<String> columnNameList;

    private final List<DataType> columnDataTypeList;

    private volatile Repository<?> repository;

    public RedisRowDataLookupFunction(RedisConnectionOptions connectionOptions, RedisReadOptions readOptions, RedisLookupOptions lookupOptions, ResolvedSchema physicalSchema) {
        checkNotNull(connectionOptions, "No RedisConnectionOptions supplied.");
        checkNotNull(readOptions, "No readOptions supplied.");
        checkNotNull(lookupOptions, "No lookupOptions supplied.");
        checkNotNull(physicalSchema, "No physicalSchema supplied.");
        if (physicalSchema.getColumnNames().size() != physicalSchema.getColumnDataTypes().size()) {
            throw new RuntimeException("字段信息获取失败");
        }
        this.connectionOptions = connectionOptions;
        this.readOptions = readOptions;
        this.columnNameList = physicalSchema.getColumnNames();
        this.columnDataTypeList = physicalSchema.getColumnDataTypes();
    }

    /**
     * 联表的时候，on的条件有一个，这里的key[]就是几个
     */
    public void eval(Object... keys) throws Exception {
        init();
        repository.join(keys).ifPresent(this::collect);
    }

    /**
     * 反序列化不会调用构造器，不能在构造器中初始化
     */
    private void init() {
        if (repository == null) {
            synchronized (this) {
                if (repository == null) {
                    Repository<?> repository = readOptions.getRepository();
                    repository.init(connectionOptions, readOptions, columnNameList, columnDataTypeList);
                    if (CacheLoadModel.INITIAL.equals(readOptions.getCacheLoadModel())) {
                        try {
                            repository.loadCache();
                        } catch (Exception e) {
                            log.error("初始化缓存失败", e);
                        }
                    }
                    this.repository = repository;
                }
            }
        }
    }
}
