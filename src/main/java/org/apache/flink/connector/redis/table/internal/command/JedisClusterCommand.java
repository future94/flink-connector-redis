package org.apache.flink.connector.redis.table.internal.command;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * <p>redis集群方式
 * @author weilai
 */
@Slf4j
public class JedisClusterCommand implements RedisCommand{

    private transient volatile JedisCluster jedisCluster;

    private final RedisConnectionOptions options;

    public JedisClusterCommand(RedisConnectionOptions options) {
        this.options = options;
    }

    @Override
    public void connect(RedisConnectionOptions options) {
        ConnectionPoolConfig poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxIdle(options.getMaxIdle());
        poolConfig.setMinIdle(options.getMinIdle());
        poolConfig.setMaxTotal(options.getMaxTotal());
        List<RedisConnectionOptions.Config> clusterConfigs = options.getClusterConfigs();
        Set<HostAndPort> nodes = new HashSet<>();
        for (RedisConnectionOptions.Config clusterConfig : clusterConfigs) {
            nodes.add(new HostAndPort(clusterConfig.getHost(), clusterConfig.getPort()));
        }
        this.jedisCluster = new JedisCluster(nodes, options.getTimeout(), options.getTimeout(), options.getTimeout(), options.getPassword(), poolConfig);
    }

    @Override
    public byte[] get(byte[] key) {
        return sendCommand(jedisCluster -> jedisCluster.get(key));
    }


    @Override
    public void set(byte[] key, byte[] value) {
        sendCommand(jedisCluster -> jedisCluster.set(key, value).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return sendCommand(jedisCluster -> jedisCluster.hget(key, field));
    }

    @Override
    public List<byte[]> lrange(byte[] key) {
        return sendCommand(jedisCluster -> jedisCluster.lrange(key, 0, jedisCluster.llen(key)));
    }

    private <T> T sendCommand(Function<JedisCluster, T> function) {
        if (jedisCluster == null) {
            synchronized (JedisClusterCommand.class) {
                if (jedisCluster == null) {
                    connect(options);
                }
            }
        }
        try {
            return function.apply(jedisCluster);
        } catch (Exception e) {
            log.error("redis运行命令错误, errorMessage:[{}]", e.getMessage());
            throw new RuntimeException(e);
        } finally {
            jedisCluster.close();
        }
    }
}
