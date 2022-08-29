package org.apache.flink.connector.redis.table.internal.options;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.redis.table.internal.enums.RedisCommandType;
import org.apache.flink.connector.redis.table.internal.enums.RedisModel;

import java.io.Serializable;
import java.util.List;

/**
 * <p>Redis链接配置
 * @author weilai
 */
@Data
@PublicEvolving
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RedisConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_PORT = 6379;

    private RedisModel redisModel;

    private Config singleConfig;

    private Config masterConfig;

    private List<Config> slaveConfigs;

    private List<Config> clusterConfigs;

    private RedisCommandType command;

    private String password;

    private int database;

    private int timeout;

    private int maxTotal;

    private int maxIdle;

    private int minIdle;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Config implements Serializable {

        private static final long serialVersionUID = 1L;

        private String host;

        @Builder.Default
        private int port = DEFAULT_PORT;

        @Builder.Default
        private int weight = 1;
    }

}
