package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.annotation.SPI;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.List;

/**
 * @author weilai
 */
@SPI
public interface Repository<T> extends Serializable {

    void init(RedisConnectionOptions connectionOptions, RedisReadOptions readOptions, List<String> columnNameList, List<DataType> columnDataTypeList);

    void close();

    List<T> list();

    void insert(RowData rowData);

    void update(RowData entity);

    void delete(RowData entity);
}
