package io.qkits.sparklivy.exception;

public class SparkJobSubmitException extends RuntimeException {
    public SparkJobSubmitException() {
        super();
    }

    public SparkJobSubmitException(String message) {
        super(message);
    }

    public SparkJobSubmitException(String message, Throwable cause) {
        super(message, cause);
    }

    public SparkJobSubmitException(Throwable cause) {
        super(cause);
    }

    protected SparkJobSubmitException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
