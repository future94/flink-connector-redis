package org.apache.flink.connector.redis.table.internal.exception;

/**
 * @author weilai
 */
public class NotFoundElementException extends RuntimeException {

    public NotFoundElementException(String message) {
        super(message);
    }
}
