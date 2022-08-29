package org.apache.flink.connector.redis.table.serializer;

import lombok.Data;

/**
 * @author weilai
 */
@Data
public class JsonTestDTO {

    private String desc;

    private Integer login_time;

    private String title;
}
