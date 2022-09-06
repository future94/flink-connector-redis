package org.apache.flink.connector.redis.table.internal.enums;

/**
 * 缓存初始化模式
 * @author weilai
 */
public enum CacheLoadModel {

    /**
     * 启动时加载好数据生成cache
     */
    INITIAL,

    /**
     * 使用的时候在去加载数据生成cache
     */
    LAZY,

}
