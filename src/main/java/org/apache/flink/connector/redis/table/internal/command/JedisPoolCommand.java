package org.apache.flink.connector.redis.table.internal.command;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

/**
 * <p>redis单机方式
 * @author weilai
 */
@Slf4j
public class JedisPoolCommand implements RedisCommand{

    private transient volatile JedisPool jedisPool;

    private final RedisConnectionOptions options;

    public JedisPoolCommand(RedisConnectionOptions options) {
        this.options = options;
    }

    @Override
    public void connect(RedisConnectionOptions options) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(options.getMaxIdle());
        poolConfig.setMinIdle(options.getMinIdle());
        poolConfig.setMaxTotal(options.getMaxTotal());
        RedisConnectionOptions.Config singleConfig = options.getSingleConfig();
        jedisPool = new JedisPool(poolConfig, singleConfig.getHost(), singleConfig.getPort(), options.getTimeout(), options.getPassword(), options.getDatabase());
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
            jedisPool = null;
        }
    }

    @Override
    public byte[] get(byte[] key) {
        return sendCommand(jedis -> jedis.get(key));
    }


    @Override
    public void set(byte[] key, byte[] value) {
        sendCommand(jedis -> jedis.set(key, value).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return sendCommand(jedis -> jedis.hget(key, field));
    }

    @Override
    public void hset(byte[] key, byte[] field, byte[] value) {
        sendCommand(jedis -> jedis.hset(key, field, value));
    }

    @Override
    public List<byte[]> lrange(byte[] key) {
        return sendCommand(jedis -> jedis.lrange(key, 0, jedis.llen(key)));
    }

    @Override
    public void lpush(byte[] key, byte[] value) {
        sendCommand(jedis -> jedis.lpush(key, value));
    }

    @Override
    public void rpush(byte[] key, byte[] value) {
        sendCommand(jedis -> jedis.rpush(key, value));
    }

    private <T> T sendCommand(Function<Jedis, T> function) {
        if (jedisPool == null) {
            synchronized (JedisPoolCommand.class) {
                if (jedisPool == null) {
                    connect(options);
                }
            }
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return function.apply(jedis);
        } catch (Exception e) {
            log.error("redis运行命令错误, errorMessage:[{}]", e.getMessage());
            throw new RuntimeException(e);
        }

    }

}
