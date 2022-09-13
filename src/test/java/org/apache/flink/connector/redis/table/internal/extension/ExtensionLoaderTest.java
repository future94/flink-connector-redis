package org.apache.flink.connector.redis.table.internal.extension;

import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;
import org.apache.flink.connector.redis.table.internal.repository.Repository;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author weilai
 */
public class ExtensionLoaderTest {

    @Test
    public void getExtensionSPI() {
        Repository<?> repository = ExtensionLoader.getExtensionLoader(Repository.class).getExtension("string");
        Assert.assertNotNull(repository);
    }

    @Test
    public void getExtensionPackage() {
        Repository<?> repository = ExtensionLoader.getExtensionLoader(Repository.class).getExtension("scan", "org.apache.flink.connector.redis.table.internal.repository", RedisRepository.class);
        Assert.assertNotNull(repository);
    }
}