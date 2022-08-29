package org.apache.flink.connector.redis.table.internal.options;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author weilai
 */
public class RedisLookupOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    public RedisLookupOptions(long cacheMaxSize, long cacheExpireMs, int maxRetryTimes) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheExpireMs() {
        return cacheExpireMs;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RedisLookupOptions) {
            RedisLookupOptions options = (RedisLookupOptions) o;
            return Objects.equals(cacheMaxSize, options.cacheMaxSize)
                    && Objects.equals(cacheExpireMs, options.cacheExpireMs)
                    && Objects.equals(maxRetryTimes, options.maxRetryTimes);
        } else {
            return false;
        }
    }

    /** Builder of {@link RedisLookupOptions}. */
    public static class Builder {
        private long cacheMaxSize = -1L;
        private long cacheExpireMs = -1L;
        private int maxRetryTimes = 0;

        /** optional, lookup cache max size, over this value, the old data will be eliminated. */
        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        /** optional, lookup cache expire mills, over this time, the old data will expire. */
        public Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        /** optional, max retry times for jdbc connector. */
        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public RedisLookupOptions build() {
            return new RedisLookupOptions(cacheMaxSize, cacheExpireMs, maxRetryTimes);
        }
    }
}
