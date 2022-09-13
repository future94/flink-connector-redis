package org.apache.flink.connector.redis.table.internal.converter;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author weilai
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DataParser {

    /**
     * key
     */
    private byte[] key;

    /**
     * field
     */
    private byte[] field;

    /**
     * value
     */
    private byte[] value;
}