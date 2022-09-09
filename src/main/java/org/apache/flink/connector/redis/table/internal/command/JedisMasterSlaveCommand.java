package org.apache.flink.connector.redis.table.internal.command;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.connector.redis.table.internal.options.RedisConnectionOptions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * <p>redis主从方式(读写分离)，读使用权重方式，如果权重一样就是轮询
 * @author weilai
 */
@Slf4j
public class JedisMasterSlaveCommand implements RedisCommand {

    private transient volatile JedisPool masterJedisPool;

    private transient final List<NodePool> slaveJedisPool = new ArrayList<>();

    private static Integer totalWeight = 0;

    private final RedisConnectionOptions options;

    public JedisMasterSlaveCommand(RedisConnectionOptions options) {
        this.options = options;
    }

    @Override
    public void connect(RedisConnectionOptions options) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(options.getMaxIdle());
        poolConfig.setMinIdle(options.getMinIdle());
        poolConfig.setMaxTotal(options.getMaxTotal());
        RedisConnectionOptions.Config masterConfig = options.getMasterConfig();
        List<RedisConnectionOptions.Config> slaveConfigs = options.getSlaveConfigs();
        this.masterJedisPool = new JedisPool(poolConfig, masterConfig.getHost(), masterConfig.getPort(), options.getTimeout(), options.getPassword(), options.getDatabase());
        for (RedisConnectionOptions.Config slaveConfig : slaveConfigs) {
            this.slaveJedisPool.add(new NodePool(poolConfig, options, slaveConfig));
        }
        slaveJedisPool.forEach(node -> totalWeight += node.getEffectiveWeight());
    }

    @Override
    public void close() {
        if (masterJedisPool != null) {
            masterJedisPool.close();
            masterJedisPool = null;
        }
        if (CollectionUtils.isNotEmpty(slaveJedisPool)) {
            slaveJedisPool.forEach(nodePool -> nodePool.getJedisPool().close());
            slaveJedisPool.clear();
        }
    }

    @Override
    public byte[] get(byte[] key) {
        return sendCommand(jedis -> jedis.get(key), false);
    }


    @Override
    public void set(byte[] key, byte[] value) {
        sendCommand(jedis -> jedis.set(key, value).getBytes(StandardCharsets.UTF_8), true);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return sendCommand(jedis -> jedis.hget(key, field), false);
    }

    @Override
    public void hset(byte[] key, byte[] field, byte[] value) {
        sendCommand(jedis -> jedis.hset(key, field, value), true);
    }

    @Override
    public List<byte[]> lrange(byte[] key) {
        return sendCommand(jedis -> jedis.lrange(key, 0, jedis.llen(key)), false);
    }

    @Override
    public void lpush(byte[] key, byte[] value) {
        sendCommand(jedis -> jedis.lpush(key, value), true);
    }

    @Override
    public void rpush(byte[] key, byte[] value) {
        sendCommand(jedis -> jedis.rpush(key, value), true);
    }

    private <T> T sendCommand(Function<Jedis, T> function, boolean write) {
        if (masterJedisPool == null || slaveJedisPool.isEmpty()) {
            synchronized (JedisMasterSlaveCommand.class) {
                if (masterJedisPool == null || slaveJedisPool.isEmpty()) {
                    connect(options);
                }
            }
        }
        if (write) {
            try (Jedis jedis = masterJedisPool.getResource()) {
                return function.apply(jedis);
            } catch (Exception e) {
                log.error("redis运行命令错误, errorMessage:[{}]", e.getMessage());
                throw new RuntimeException(e);
            }
        } else {
            try (JedisPool jedisPool = selectSlaveNodePool(); Jedis jedis = jedisPool.getResource()) {
                return function.apply(jedis);
            } catch (Exception e) {
                log.error("redis运行命令错误, errorMessage:[{}]", e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    private JedisPool selectSlaveNodePool() {
        // 保存轮询选中的节点信息
        NodePool nodeOfMaxWeight;
        synchronized (slaveJedisPool) {
            if (slaveJedisPool.size() == 1) {
                return slaveJedisPool.get(0).getJedisPool();
            }
            // 选出当前权重最大的节点
            NodePool tempNodeOfMaxWeight = null;
            for (NodePool node : slaveJedisPool) {
                if (tempNodeOfMaxWeight == null)
                    tempNodeOfMaxWeight = node;
                else
                    tempNodeOfMaxWeight = tempNodeOfMaxWeight.compareTo(node) > 0 ? tempNodeOfMaxWeight : node;
            }
            // new个新的节点实例来保存信息，否则引用指向同一个堆实例，后面的set操作将会修改节点信息
            nodeOfMaxWeight = new NodePool(Objects.requireNonNull(tempNodeOfMaxWeight).getJedisPool(), tempNodeOfMaxWeight.getWeight(), tempNodeOfMaxWeight.getEffectiveWeight(), tempNodeOfMaxWeight.getCurrentWeight());
            // 调整当前权重比：按权重（effectiveWeight）的比例进行调整，确保请求分发合理。
            tempNodeOfMaxWeight.setCurrentWeight(tempNodeOfMaxWeight.getCurrentWeight() - totalWeight);
            slaveJedisPool.forEach(node -> node.setCurrentWeight(node.getCurrentWeight() + node.getEffectiveWeight()));
        }
        return nodeOfMaxWeight.getJedisPool();
    }

    @Data
    @AllArgsConstructor
    private static class NodePool implements Comparable<NodePool> {

        private final JedisPool jedisPool;

        private final Integer weight;

        private Integer effectiveWeight;

        private Integer currentWeight;

        public NodePool(JedisPoolConfig poolConfig, RedisConnectionOptions options, RedisConnectionOptions.Config slaveConfig) {
            this.jedisPool = new JedisPool(poolConfig, slaveConfig.getHost(), slaveConfig.getPort(), options.getTimeout(), options.getPassword(), options.getDatabase());
            this.weight = slaveConfig.getWeight();
            this.effectiveWeight = this.weight;
            this.currentWeight = this.weight;
        }

        @Override
        public int compareTo(NodePool node) {
            return currentWeight > node.currentWeight ? 1 : (currentWeight.equals(node.currentWeight) ? 0 : -1);
        }
    }
}
