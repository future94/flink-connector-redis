package org.apache.flink.connector.redis.table.internal.serializer;

import lombok.Data;

/**
 * @author weilai
 */
@Data
public class JsonStringHashTestDTO {

    private String desc;

    private Integer login_time;

    private String title;
}
