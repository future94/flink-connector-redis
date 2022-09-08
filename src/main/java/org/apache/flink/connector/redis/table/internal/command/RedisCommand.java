package org.apache.flink.connector.redis.table.internal.command;

import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;

import java.io.Serializable;
import java.util.List;

/**
 * <p>运行redis命令
 * @author weilai
 */
public interface RedisCommand extends Serializable {

    /**
     * 建立链接
     */
    void connect(RedisConnectionOptions options);

    /**
     * 断开链接
     */
    void close();

    /**
     * 运行get命令
     * @param key   序列化后的键
     * @return      未序列化的值
     */
    byte[] get(byte[] key);

    /**
     * 运行set命令
     * @param key   序列化后的键
     * @param value 序列化后的值
     */
    void set(byte[] key, byte[] value);

    /**
     * 运行hget命令
     * @param key   序列化后的键
     * @param field 序列化后的键
     * @return      未序列化的值
     */
    byte[] hget(byte[] key, byte[] field);

    /**
     * 运行lrange命令
     * @param key   序列化后的键
     * @return      未序列化的值
     */
    List<byte[]> lrange(byte[] key);
}
