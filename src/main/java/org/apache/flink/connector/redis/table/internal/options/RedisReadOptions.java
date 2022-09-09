package org.apache.flink.connector.redis.table.internal.options;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.redis.table.internal.enums.CacheLoadModel;
import org.apache.flink.connector.redis.table.internal.enums.CacheMissModel;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;

import java.io.Serializable;
import java.util.Map;

/**
 * <p>Redis读取配置
 * @author weilai
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RedisReadOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private RedisSerializer<?> keySerializer;

    private RedisSerializer<?> valueSerializer;

    private RedisCommandType command;

    private String scan;

    private String entityScan;

    private String repositoryScan;

    private Class<?> entity;

    private Class<?> repository;

    private String hashKey;

    private String listKey;

    private Map<String, String> cacheFieldNames;

    private CacheLoadModel cacheLoadModel;

    private CacheMissModel cacheMissModel;

}
