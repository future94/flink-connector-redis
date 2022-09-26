package org.apache.flink.connector.redis.table.internal.exception;

/**
 * @author weilai
 */
public class InsertException extends RuntimeException{

    public InsertException(String message) {
        super(message);
    }
}
