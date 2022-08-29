package org.apache.flink.connector.redis.table.internal.options;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.redis.table.internal.serializer.RedisSerializer;

import java.io.Serializable;

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

    private String hashKey;
}
