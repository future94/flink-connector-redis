package org.apache.flink.connector.redis.table;

import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisLookupOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;

/**
 * <p>Redis动态表资源
 * @author weilai
 */
public class RedisDynamicTableSource implements LookupTableSource {

    private final RedisConnectionOptions connectionOptions;
    private final RedisReadOptions readOptions;
    private final RedisLookupOptions lookupOptions;

    private static final String SUMMARY_STRING = "Redis";

    /**
     * {@link TableSchema}过时了
     */
    private final ResolvedSchema physicalSchema;

    public RedisDynamicTableSource(RedisConnectionOptions options, RedisReadOptions readOptions, RedisLookupOptions lookupOptions, ResolvedSchema physicalSchema) {
        this.connectionOptions = options;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
        this.physicalSchema = physicalSchema;
    }


    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return TableFunctionProvider.of(
                new RedisRowDataLookupFunction(
                        connectionOptions, readOptions, lookupOptions, physicalSchema));
    }
//
//    @Override
//    public ChangelogMode getChangelogMode() {
//        return ChangelogMode.insertOnly();
//    }
//
//    @Override
//    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
//        return null;
//    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(connectionOptions, readOptions, lookupOptions, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return SUMMARY_STRING;
    }

//    @Override
//    public void applyLimit(long limit) {
//        this.limit = limit;
//    }
//
//    @Override
//    public boolean supportsNestedProjection() {
//        // Redis doesn't support nested projection
//        return false;
//    }
//
//    @Override
//    public void applyProjection(int[][] projectedFields) {
//        this.physicalSchema = new ResolvedSchema(physicalSchema.getColumns(), physicalSchema.getWatermarkSpecs(), physicalSchema.getPrimaryKey().orElse(null));
//    }
}
