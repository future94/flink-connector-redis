package org.apache.flink.connector.redis.table;

import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisLookupOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author weilai
 */
public class RedisDynamicTableSink implements DynamicTableSink {

    private final RedisConnectionOptions connectionOptions;
    private final RedisReadOptions readOptions;
    private final RedisLookupOptions lookupOptions;

    /**
     * {@link TableSchema}过时了
     */
    private final ResolvedSchema physicalSchema;

    private static final String SUMMARY_STRING = "redis";

    public RedisDynamicTableSink(RedisConnectionOptions connectionOptions, RedisReadOptions readOptions, RedisLookupOptions lookupOptions, ResolvedSchema physicalSchema) {
        checkNotNull(connectionOptions, "No RedisConnectionOptions supplied.");
        checkNotNull(readOptions, "No readOptions supplied.");
        checkNotNull(lookupOptions, "No lookupOptions supplied.");
        checkNotNull(physicalSchema, "No physicalSchema supplied.");
        this.connectionOptions = connectionOptions;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(
                new RedisRowDataSinkFunction(connectionOptions, readOptions, lookupOptions, physicalSchema));
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(connectionOptions, readOptions, lookupOptions, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return SUMMARY_STRING;
    }
}
