package org.apache.flink.connector.redis.table.internal.repository;

import org.apache.flink.connector.redis.table.internal.annotation.RedisRepository;

/**
 * @author weilai
 */
@RedisRepository("scan")
public class StringScanRepository extends BaseRepository<String>{
}
