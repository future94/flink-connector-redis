package org.apache.flink.connector.redis.table.internal.serializer;

import lombok.Data;

/**
 * @author weilai
 */
@Data
public class JsonListTestDTO {

    private String level;

    private String desc;

    private Integer login_time;

    private String title;
}
