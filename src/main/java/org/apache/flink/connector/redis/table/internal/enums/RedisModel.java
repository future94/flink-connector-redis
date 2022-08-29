package org.apache.flink.connector.redis.table.internal.enums;

/**
 * <p>Redis运行模式
 * @author weilai
 */
public enum RedisModel {

    /**
     * 单机
     */
    SINGLE,

    /**
     * 主从复制(读写分离)
     */
    MASTER_SLAVE,

    /**
     * 集群
     */
    CLUSTER,
    ;
}
