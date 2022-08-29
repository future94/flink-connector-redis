package org.apache.flink.connector.redis.table.internal.command;

import org.apache.flink.connector.redis.table.internal.enums.RedisModel;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;

/**
 * <p>构建Redis命令运行
 * @author weilai
 */
public class RedisCommandBuilder {

    public static RedisCommand build(RedisConnectionOptions options) {
        RedisModel redisModel = options.getRedisModel();
        switch (redisModel) {
            case SINGLE:
                return new JedisPoolCommand(options);
            case MASTER_SLAVE:
                return new JedisMasterSlaveCommand(options);
            case CLUSTER:
                return new JedisClusterCommand(options);
            default:
                throw new UnsupportedOperationException("不支持的Redis模式:" + redisModel.name());
        }
    }
}
