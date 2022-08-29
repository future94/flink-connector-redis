package org.apache.flink.connector.redis.table.internal.converter;

import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;

import java.util.LinkedList;
import java.util.ServiceLoader;

/**
 * <p>SPI方式加载转换器
 *
 * <p>加载META-INF/services/org.apache.flink.connector.redis.table.internal.converter.RedisCommandToRowConverter中的类
 * @author weilai
 */
public class RedisCommandToRowConverterLoader {
    private static final LinkedList<RedisCommandToRowConverter> CONVERT_LIST = new LinkedList<>();

    static {
        ServiceLoader.load(RedisCommandToRowConverter.class).iterator().forEachRemaining(CONVERT_LIST::add);
    }

    public static RedisCommandToRowConverter get(RedisCommandType commandType) {
        for (RedisCommandToRowConverter converter : CONVERT_LIST) {
            if (converter.support().equals(commandType)) {
                return converter;
            }
        }
        throw new UnsupportedOperationException("不支持的Redis命令: " + commandType.name());
    }
}