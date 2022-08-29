package org.apache.flink.connector.redis.table.internal.exception;

/**
 * <p>序列化异常
 * @author weilai
 */
public class SerializationException extends RuntimeException{

    /**
     * Constructs a new {@link SerializationException} instance.
     *
     * @param msg
     */
    public SerializationException(String msg) {
        super(msg);
    }

    /**
     * Constructs a new {@link SerializationException} instance.
     *
     * @param msg the detail message.
     * @param cause the nested exception.
     */
    public SerializationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
