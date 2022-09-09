package org.apache.flink.connector.redis.table.internal.exception;

/**
 * @author weilai
 */
public class TooManyElementsException extends RuntimeException {

    public TooManyElementsException(String message) {
        super(message);
    }
}
