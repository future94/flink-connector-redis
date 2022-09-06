package org.apache.flink.connector.redis.table.internal.converter;

import org.apache.flink.connector.redis.table.internal.command.RedisCommand;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.options.RedisReadOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * <p>将Redis命令返回数据转换为Table数据
 * @author weilai
 */
public interface RedisCommandToRowConverter {

    /**
     * 支持的命令类型
     */
    RedisCommandType support();

    /**
     * 转换数据
     * @param redisCommand          运行环境
     * @param columnNameList        字段名集合
     * @param columnDataTypeList    字段类型集合
     * @param readOptions           读取参数配置
     * @param keys                  联表Key[]
     * @return                      转换的数据
     * @throws Exception            转换失败
     */
    Optional<GenericRowData> convert(final RedisCommand redisCommand, final List<String> columnNameList, final List<DataType> columnDataTypeList, final RedisReadOptions readOptions, final Object[] keys) throws Exception;

    /**
     * 清除所有缓存
     */
    void clearCache();

    /**
     * 初始化缓存
     * @param redisCommand          运行环境
     * @param readOptions           读取参数配置
     * @param columnNameList        字段名集合
     * @param columnDataTypeList    读取参数配置
     * @throws Exception            初始化失败
     */
    void loadCache(RedisCommand redisCommand, RedisReadOptions readOptions, List<String> columnNameList, List<DataType> columnDataTypeList) throws Exception;
}
