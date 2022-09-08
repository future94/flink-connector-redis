package org.apache.flink.connector.redis.table.internal.converter.source;

import org.apache.flink.connector.redis.table.internal.converter.source.RedisSourceConverter;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

import java.util.LinkedList;
import java.util.ServiceLoader;

/**
 * <p>SPI方式加载转换器
 *
 * <p>加载META-INF/services/org.apache.flink.connector.redis.table.internal.converter.source.RedisSourceConverter中的类
 * @author weilai
 */
public class RedisSourceConverterLoader {

    private static final LinkedList<RedisSourceConverter> CONVERT_LIST = new LinkedList<>();

    static {
        ServiceLoader.load(RedisSourceConverter.class).iterator().forEachRemaining(CONVERT_LIST::add);
    }

    public static RedisSourceConverter get(RedisCommandType commandType) {
        for (RedisSourceConverter converter : CONVERT_LIST) {
            if (converter.support().equals(commandType)) {
                return converter;
            }
        }
        throw new UnsupportedOperationException("不支持的Redis命令: " + commandType.name());
    }
}