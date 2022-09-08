package org.apache.flink.connector.redis.table.internal.converter.sink;

import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

import java.util.LinkedList;
import java.util.ServiceLoader;

/**
 * <p>SPI方式加载转换器
 *
 * <p>加载META-INF/services/org.apache.flink.connector.redis.table.internal.converter.sink.RedisSinkConverter中的类
 * @author weilai
 */
public class RedisSinkConverterLoader {

    private static final LinkedList<RedisSinkConverter> CONVERT_LIST = new LinkedList<>();

    static {
        ServiceLoader.load(RedisSinkConverter.class).iterator().forEachRemaining(CONVERT_LIST::add);
    }

    public static RedisSinkConverter get(RedisCommandType commandType) {
        for (RedisSinkConverter converter : CONVERT_LIST) {
            if (converter.support().equals(commandType)) {
                return converter;
            }
        }
        throw new UnsupportedOperationException("不支持的Redis命令: " + commandType.name());
    }
}