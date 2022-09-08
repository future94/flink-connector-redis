package org.apache.flink.connector.redis.table.internal.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * <p>将Redis的数据值与TableAPI中的值转化
 * @author weilai
 */
public class RedisDataConverter {

    public static Object from(LogicalType logicalType, Object value) {
        return getFromConvert(logicalType).apply(value.toString());
    }

    public static Object to(LogicalType logicalType, RowData rowData, int pos) {
        return getToConvert(logicalType).apply(rowData, pos);
    }

    /**
     * 通过{@link LogicalTypeUtils#toInternalConversionClass(LogicalType)}工具可以知道要返回的数据类型
     */
    private static Function<String, Object> getFromConvert(LogicalType logicalType) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        int precision;
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                return BinaryStringData::fromString;
            case BOOLEAN:
                return Boolean::valueOf;
            case BINARY:
            case VARBINARY:
                return value -> Base64.getDecoder().decode(value);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return value -> DecimalData.fromBigDecimal(new BigDecimal(value), decimalType.getPrecision(), decimalType.getScale());
            case TINYINT:
                return Byte::valueOf;
            case SMALLINT:
                return Short::valueOf;
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return Integer::valueOf;
            case TIME_WITHOUT_TIME_ZONE:
                // 支持毫秒，所以精度只能是0～3
                precision = LogicalTypeChecks.getPrecision(logicalType);
                if (precision < 0 || precision > 3) {
                    throw new UnsupportedOperationException("Time类型的精度只能是[0~3]");
                }
                return Integer::valueOf;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return Long::valueOf;
            case FLOAT:
                return Float::valueOf;
            case DOUBLE:
                return Double::valueOf;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // 支持毫秒，所以精度只能是0～3
                precision = LogicalTypeChecks.getPrecision(logicalType);
                if (precision < 0 || precision > 3) {
                    throw new UnsupportedOperationException("Time类型的精度只能是[0~3]");
                }
                return value -> TimestampData.fromEpochMillis(Long.parseLong(value));
            default:
                throw new UnsupportedOperationException("不支持的类型转换: " + logicalType.asSummaryString());
        }
    }

    private static BiFunction<RowData, Integer, Object> getToConvert(LogicalType logicalType) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        int precision;
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                return (rowData, pos) -> rowData.getString(pos).toString();
            case BOOLEAN:
                return RowData::getBoolean;
            case BINARY:
            case VARBINARY:
                return (rowData, pos) -> Base64.getEncoder().encodeToString(rowData.getBinary(pos));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return (rowData, pos) -> rowData.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
            case TINYINT:
                return RowData::getByte;
            case SMALLINT:
                return RowData::getShort;
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return RowData::getInt;
            case TIME_WITHOUT_TIME_ZONE:
                // 支持毫秒，所以精度只能是0～3
                precision = LogicalTypeChecks.getPrecision(logicalType);
                if (precision < 0 || precision > 3) {
                    throw new UnsupportedOperationException("Time类型的精度只能是[0~3]");
                }
                return RowData::getInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return RowData::getLong;
            case FLOAT:
                return RowData::getFloat;
            case DOUBLE:
                return RowData::getDouble;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // 支持毫秒，所以精度只能是0～3
                precision = LogicalTypeChecks.getPrecision(logicalType);
                if (precision < 0 || precision > 3) {
                    throw new UnsupportedOperationException("Time类型的精度只能是[0~3]");
                }
                return (rowData, pos) -> rowData.getTimestamp(pos, precision).getMillisecond();
            default:
                throw new UnsupportedOperationException("不支持的类型转换: " + logicalType.asSummaryString());
        }
    }
}
